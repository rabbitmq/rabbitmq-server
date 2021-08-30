%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%


%% This module handles the node-wide memory statistics.
%% It receives statistics from all queues, counts the desired
%% queue length (in seconds), and sends this information back to
%% queues.

-module(rabbit_memory_monitor).

-behaviour(gen_server2).

-export([start_link/0, register/2, deregister/1,
         report_ram_duration/2, stop/0, conserve_resources/3, memory_use/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(process, {pid, reported, sent, callback, monitor}).

-record(state, {timer,                %% 'internal_update' timer
                queue_durations,      %% ets #process
                queue_duration_sum,   %% sum of all queue_durations
                queue_duration_count, %% number of elements in sum
                desired_duration,     %% the desired queue duration
                disk_alarm            %% disable paging, disk alarm has fired
               }).

-define(SERVER, ?MODULE).
-define(TABLE_NAME, ?MODULE).

%% If all queues are pushed to disk (duration 0), then the sum of
%% their reported lengths will be 0. If memory then becomes available,
%% unless we manually intervene, the sum will remain 0, and the queues
%% will never get a non-zero duration.  Thus when the mem use is <
%% SUM_INC_THRESHOLD, increase the sum artificially by SUM_INC_AMOUNT.
-define(SUM_INC_THRESHOLD, 0.95).
-define(SUM_INC_AMOUNT, 1.0).

-define(EPSILON, 0.000001). %% less than this and we clamp to 0

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

-spec start_link() -> rabbit_types:ok_pid_or_error().

start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec register(pid(), {atom(),atom(),[any()]}) -> 'ok'.

register(Pid, MFA = {_M, _F, _A}) ->
    gen_server2:call(?SERVER, {register, Pid, MFA}, infinity).

-spec deregister(pid()) -> 'ok'.

deregister(Pid) ->
    gen_server2:cast(?SERVER, {deregister, Pid}).

-spec report_ram_duration
        (pid(), float() | 'infinity') -> number() | 'infinity'.

report_ram_duration(Pid, QueueDuration) ->
    gen_server2:call(?SERVER,
                     {report_ram_duration, Pid, QueueDuration}, infinity).

-spec stop() -> 'ok'.

stop() ->
    gen_server2:cast(?SERVER, stop).

%% Paging should be enabled/disabled only in response to disk resource alarms
%% for the current node.
conserve_resources(Pid, disk, {_, Conserve, Node}) when node(Pid) =:= Node ->
    gen_server2:cast(Pid, {disk_alarm, Conserve});
conserve_resources(_Pid, _Source, _Conserve) ->
    ok.

memory_use(Type) ->
    vm_memory_monitor:get_memory_use(Type).

%%----------------------------------------------------------------------------
%% Gen_server callbacks
%%----------------------------------------------------------------------------

init([]) ->
    {ok, Interval} = application:get_env(rabbit, memory_monitor_interval),
    {ok, TRef} = timer:send_interval(Interval, update),

    Ets = ets:new(?TABLE_NAME, [set, private, {keypos, #process.pid}]),
    Alarms = rabbit_alarm:register(self(), {?MODULE, conserve_resources, []}),
    {ok, internal_update(
           #state { timer                = TRef,
                    queue_durations      = Ets,
                    queue_duration_sum   = 0.0,
                    queue_duration_count = 0,
                    desired_duration     = infinity,
                    disk_alarm           = lists:member(disk, Alarms)})}.

handle_call({report_ram_duration, Pid, QueueDuration}, From,
            State = #state { queue_duration_sum = Sum,
                             queue_duration_count = Count,
                             queue_durations = Durations,
                             desired_duration = SendDuration }) ->

    [Proc = #process { reported = PrevQueueDuration }] =
        ets:lookup(Durations, Pid),

    gen_server2:reply(From, SendDuration),

    {Sum1, Count1} =
        case {PrevQueueDuration, QueueDuration} of
            {infinity, infinity} -> {Sum, Count};
            {infinity, _}        -> {Sum + QueueDuration,    Count + 1};
            {_, infinity}        -> {Sum - PrevQueueDuration, Count - 1};
            {_, _}               -> {Sum - PrevQueueDuration + QueueDuration,
                                     Count}
        end,
    true = ets:insert(Durations, Proc #process { reported = QueueDuration,
                                                 sent = SendDuration }),
    {noreply, State #state { queue_duration_sum = zero_clamp(Sum1),
                             queue_duration_count = Count1 }};

handle_call({register, Pid, MFA}, _From,
            State = #state { queue_durations = Durations }) ->
    MRef = erlang:monitor(process, Pid),
    true = ets:insert(Durations, #process { pid = Pid, reported = infinity,
                                            sent = infinity, callback = MFA,
                                            monitor = MRef }),
    {reply, ok, State};

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast({disk_alarm, Alarm}, State = #state{disk_alarm = Alarm}) ->
    {noreply, State};

handle_cast({disk_alarm, Alarm}, State) ->
    {noreply, internal_update(State#state{disk_alarm = Alarm})};

handle_cast({deregister, Pid}, State) ->
    {noreply, internal_deregister(Pid, true, State)};

handle_cast(stop, State) ->
    {stop, normal, State};

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(update, State) ->
    {noreply, internal_update(State)};

handle_info({'DOWN', _MRef, process, Pid, _Reason}, State) ->
    {noreply, internal_deregister(Pid, false, State)};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state { timer = TRef }) ->
    timer:cancel(TRef),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%----------------------------------------------------------------------------
%% Internal functions
%%----------------------------------------------------------------------------

zero_clamp(Sum) when Sum < ?EPSILON -> 0.0;
zero_clamp(Sum)                     -> Sum.

internal_deregister(Pid, Demonitor,
                    State = #state { queue_duration_sum = Sum,
                                     queue_duration_count = Count,
                                     queue_durations = Durations }) ->
    case ets:lookup(Durations, Pid) of
        [] -> State;
        [#process { reported = PrevQueueDuration, monitor = MRef }] ->
            true = case Demonitor of
                       true  -> erlang:demonitor(MRef);
                       false -> true
                   end,
            {Sum1, Count1} =
                case PrevQueueDuration of
                    infinity -> {Sum, Count};
                    _        -> {zero_clamp(Sum - PrevQueueDuration),
                                 Count - 1}
                end,
            true = ets:delete(Durations, Pid),
            State #state { queue_duration_sum = Sum1,
                           queue_duration_count = Count1 }
    end.

internal_update(State = #state{queue_durations  = Durations,
                               desired_duration = DesiredDurationAvg,
                               disk_alarm       = DiskAlarm}) ->
    DesiredDurationAvg1 = desired_duration_average(State),
    ShouldInform = should_inform_predicate(DiskAlarm),
    case ShouldInform(DesiredDurationAvg, DesiredDurationAvg1) of
        true  -> inform_queues(ShouldInform, DesiredDurationAvg1, Durations);
        false -> ok
    end,
    State#state{desired_duration = DesiredDurationAvg1}.

desired_duration_average(#state{disk_alarm           = true}) ->
    infinity;
desired_duration_average(#state{disk_alarm           = false,
                                queue_duration_sum   = Sum,
                                queue_duration_count = Count}) ->
    {ok, LimitThreshold} =
        application:get_env(rabbit, vm_memory_high_watermark_paging_ratio),
    MemoryRatio = memory_use(ratio),
    if MemoryRatio =:= infinity ->
            0.0;
       MemoryRatio < LimitThreshold orelse Count == 0 ->
            infinity;
       MemoryRatio < ?SUM_INC_THRESHOLD ->
            ((Sum + ?SUM_INC_AMOUNT) / Count) / MemoryRatio;
       true ->
            (Sum / Count) / MemoryRatio
    end.

inform_queues(ShouldInform, DesiredDurationAvg, Durations) ->
    true =
        ets:foldl(
          fun (Proc = #process{reported = QueueDuration,
                               sent     = PrevSendDuration,
                               callback = {M, F, A}}, true) ->
                  case ShouldInform(PrevSendDuration, DesiredDurationAvg)
                      andalso ShouldInform(QueueDuration, DesiredDurationAvg) of
                      true  -> ok = erlang:apply(
                                      M, F, A ++ [DesiredDurationAvg]),
                               ets:insert(
                                 Durations,
                                 Proc#process{sent = DesiredDurationAvg});
                      false -> true
                  end
          end, true, Durations).

%% In normal use, we only inform queues immediately if the desired
%% duration has decreased, we want to ensure timely paging.
should_inform_predicate(false) -> fun greater_than/2;
%% When the disk alarm has gone off though, we want to inform queues
%% immediately if the desired duration has *increased* - we want to
%% ensure timely stopping paging.
should_inform_predicate(true) ->  fun (D1, D2) -> greater_than(D2, D1) end.

greater_than(infinity, infinity) -> false;
greater_than(infinity, _D2)      -> true;
greater_than(_D1,      infinity) -> false;
greater_than(D1,       D2)       -> D1 > D2.
