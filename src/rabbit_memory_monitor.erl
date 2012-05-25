%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%


%% This module handles the node-wide memory statistics.
%% It receives statistics from all queues, counts the desired
%% queue length (in seconds), and sends this information back to
%% queues.

-module(rabbit_memory_monitor).

-behaviour(gen_server2).

-export([start_link/0, register/2, deregister/1,
         report_ram_duration/2, stop/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(process, {pid, reported, sent, callback, monitor}).

-record(state, {timer,                %% 'internal_update' timer
                queue_durations,      %% ets #process
                queue_duration_sum,   %% sum of all queue_durations
                queue_duration_count, %% number of elements in sum
                desired_duration      %% the desired queue duration
               }).

-define(SERVER, ?MODULE).
-define(DEFAULT_UPDATE_INTERVAL, 2500).
-define(TABLE_NAME, ?MODULE).

%% Because we have a feedback loop here, we need to ensure that we
%% have some space for when the queues don't quite respond as fast as
%% we would like, or when there is buffering going on in other parts
%% of the system. In short, we aim to stay some distance away from
%% when the memory alarms will go off, which cause backpressure (of
%% some sort) on producers. Note that all other Thresholds are
%% relative to this scaling.
-define(MEMORY_LIMIT_SCALING, 0.4).

-define(LIMIT_THRESHOLD, 0.5). %% don't limit queues when mem use is < this

%% If all queues are pushed to disk (duration 0), then the sum of
%% their reported lengths will be 0. If memory then becomes available,
%% unless we manually intervene, the sum will remain 0, and the queues
%% will never get a non-zero duration.  Thus when the mem use is <
%% SUM_INC_THRESHOLD, increase the sum artificially by SUM_INC_AMOUNT.
-define(SUM_INC_THRESHOLD, 0.95).
-define(SUM_INC_AMOUNT, 1.0).

-define(EPSILON, 0.000001). %% less than this and we clamp to 0

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/0 :: () -> rabbit_types:ok_pid_or_error()).
-spec(register/2 :: (pid(), {atom(),atom(),[any()]}) -> 'ok').
-spec(deregister/1 :: (pid()) -> 'ok').
-spec(report_ram_duration/2 ::
        (pid(), float() | 'infinity') -> number() | 'infinity').
-spec(stop/0 :: () -> 'ok').

-endif.

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [], []).

register(Pid, MFA = {_M, _F, _A}) ->
    gen_server2:call(?SERVER, {register, Pid, MFA}, infinity).

deregister(Pid) ->
    gen_server2:cast(?SERVER, {deregister, Pid}).

report_ram_duration(Pid, QueueDuration) ->
    gen_server2:call(?SERVER,
                     {report_ram_duration, Pid, QueueDuration}, infinity).

stop() ->
    gen_server2:cast(?SERVER, stop).

%%----------------------------------------------------------------------------
%% Gen_server callbacks
%%----------------------------------------------------------------------------

init([]) ->
    {ok, TRef} = timer:send_interval(?DEFAULT_UPDATE_INTERVAL, update),

    Ets = ets:new(?TABLE_NAME, [set, private, {keypos, #process.pid}]),

    {ok, internal_update(
           #state { timer                = TRef,
                    queue_durations      = Ets,
                    queue_duration_sum   = 0.0,
                    queue_duration_count = 0,
                    desired_duration     = infinity })}.

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

internal_update(State = #state { queue_durations = Durations,
                                 desired_duration = DesiredDurationAvg,
                                 queue_duration_sum = Sum,
                                 queue_duration_count = Count }) ->
    MemoryLimit = ?MEMORY_LIMIT_SCALING * vm_memory_monitor:get_memory_limit(),
    MemoryRatio = case MemoryLimit > 0.0 of
                      true  -> erlang:memory(total) / MemoryLimit;
                      false -> infinity
                  end,
    DesiredDurationAvg1 =
        if MemoryRatio =:= infinity ->
                0.0;
           MemoryRatio < ?LIMIT_THRESHOLD orelse Count == 0 ->
                infinity;
           MemoryRatio < ?SUM_INC_THRESHOLD ->
                ((Sum + ?SUM_INC_AMOUNT) / Count) / MemoryRatio;
           true ->
                (Sum / Count) / MemoryRatio
        end,
    State1 = State #state { desired_duration = DesiredDurationAvg1 },

    %% only inform queues immediately if the desired duration has
    %% decreased
    case DesiredDurationAvg1 == infinity orelse
        (DesiredDurationAvg /= infinity andalso
         DesiredDurationAvg1 >= DesiredDurationAvg) of
        true ->
            ok;
        false ->
            true =
                ets:foldl(
                  fun (Proc = #process { reported = QueueDuration,
                                         sent = PrevSendDuration,
                                         callback = {M, F, A} }, true) ->
                          case should_send(QueueDuration, PrevSendDuration,
                                           DesiredDurationAvg1) of
                              true  -> ok = erlang:apply(
                                              M, F, A ++ [DesiredDurationAvg1]),
                                       ets:insert(
                                         Durations,
                                         Proc #process {
                                           sent = DesiredDurationAvg1});
                              false -> true
                          end
                  end, true, Durations)
    end,
    State1.

should_send(infinity, infinity,  _) -> true;
should_send(infinity,        D, DD) -> DD < D;
should_send(D,        infinity, DD) -> DD < D;
should_send(D1,             D2, DD) -> DD < lists:min([D1, D2]).
