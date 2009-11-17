%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%


%% This module handles the node-wide memory statistics.
%% It receives statistics from all queues, counts the desired
%% queue length (in seconds), and sends this information back to
%% queues.
%%
%% Normally, messages are exchanged like that:
%%
%%           (1)      (2)                     (3)
%% Timer      |        |
%%            v        v
%% Queue -----+--------+-----<***hibernated***>------------->
%%            | ^      | ^                     ^
%%            v |      v |                     |
%% Monitor X--*-+--X---*-+--X------X----X-----X+----------->
%%
%% Or to put it in words. Queue periodically sends (casts) 'push_queue_duration'
%% message to the Monitor (cases 1 and 2 on the asciiart above). Monitor
%% _always_ replies with a 'set_queue_duration' cast. This way,
%% we're pretty sure that the Queue is not hibernated.
%% Monitor periodically recounts numbers ('X' on asciiart). If, during this
%% update we notice that a queue was using too much memory, we send a message
%% back. This will happen even if the queue is hibernated, as we really do want
%% it to reduce its memory footprint.
%%
%%
%% The main job of this module, is to make sure that all the queues have
%% more or less the same number of seconds till become drained.
%% This average, seconds-till-queue-is-drained, is then multiplied by
%% the ratio of Total/Used memory. So, if we can 'afford' more memory to be
%% used, we'll report greater number back to the queues. In the out of
%% memory case, we are going to reduce the average drain-seconds.
%% To acheive all this we need to accumulate the information from every
%% queue, and count an average from that.
%%
%%  real_queue_duration_avg = avg([drain_from_queue_1, queue_2, queue_3, ...])
%%  memory_overcommit = allowed_memory / used_memory
%%  desired_queue_duration_avg = real_queue_duration_avg * memory_overcommit


-module(rabbit_memory_monitor).
-include("rabbit.hrl").

-behaviour(gen_server2).

-export([start_link/0, update/0, register/2, report_queue_duration/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {timer,                %% 'internal_update' timer
                queue_durations,      %% ets, (qpid, last_reported, last_sent)
                queue_duration_sum,   %% sum of all queue_durations
                queue_duration_count, %% number of elements in sum
                memory_limit,         %% how much memory we intend to use
                memory_ratio,         %% how much more memory we can use
                desired_duration,     %% the desired queue duration
                callbacks             %% a dict of qpid -> {M,F,A}s
               }).

-define(SERVER, ?MODULE).
-define(DEFAULT_UPDATE_INTERVAL, 2500).
-define(TABLE_NAME, ?MODULE).
-define(MAX_QUEUE_DURATION, 60*60*24). % 1 day

%% If user disabled vm_memory_monitor, let's assume 1GB of memory we can use.
-define(MEMORY_SIZE_FOR_DISABLED_VMM, 1073741824).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/0 :: () -> 'ignore' | {'error', _} | {'ok', pid()}).
-spec(update/0 :: () -> 'ok').
-spec(register/2 :: (pid(), {atom(),atom(),[any()]}) -> 'ok').
-spec(report_queue_duration/2 :: (pid(), float() | 'infinity') -> number()).

-endif.

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------

start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [], []).

update() ->
    gen_server2:cast(?SERVER, update).

register(Pid, MFA = {_M, _F, _A}) ->
    gen_server2:call(?SERVER, {register, Pid, MFA}, infinity).

report_queue_duration(Pid, QueueDuration) ->
    gen_server2:call(rabbit_memory_monitor,
                     {report_queue_duration, Pid, QueueDuration}, infinity).


%%----------------------------------------------------------------------------
%% Gen_server callbacks
%%----------------------------------------------------------------------------

init([]) ->
    %% We should never use more memory than user requested. As the memory
    %% manager doesn't really know how much memory queues are using, we shall
    %% try to remain safe distance from real throttle limit.
    MemoryLimit = trunc(get_memory_limit() * 0.6),

    {ok, TRef} = timer:apply_interval(?DEFAULT_UPDATE_INTERVAL,
                                      ?SERVER, update, []),
    {ok, internal_update(
           #state{timer                = TRef,
                  queue_durations      = ets:new(?TABLE_NAME, [set, private]),
                  queue_duration_sum   = 0.0,
                  queue_duration_count = 0,
                  memory_limit         = MemoryLimit,
                  memory_ratio         = 1.0,
                  desired_duration     = infinity,
                  callbacks            = dict:new()})}.

handle_call({report_queue_duration, Pid, QueueDuration}, From,
            State = #state{queue_duration_sum = Sum,
                           queue_duration_count = Count,
                           queue_durations = Durations,
                           desired_duration = SendDuration}) ->
    [{_Pid, PrevQueueDuration, PrevSendDuration}] = ets:lookup(Durations, Pid),
    SendDuration1 =
        case QueueDuration < 1 andalso PrevSendDuration == infinity of
            true -> infinity;
            false -> SendDuration
        end,
    gen_server2:reply(From, SendDuration1),

    QueueDuration1 = case QueueDuration > ?MAX_QUEUE_DURATION of
                         true  -> infinity;
                         false -> QueueDuration
                     end,

    {Sum1, Count1} =
            case {PrevQueueDuration, QueueDuration1} of
                {infinity, infinity} -> {Sum, Count};
                {infinity, _}        -> {Sum + QueueDuration1,    Count + 1};
                {_, infinity}        -> {Sum - PrevQueueDuration, Count - 1};
                {_, _} -> {Sum - PrevQueueDuration + QueueDuration1, Count}
            end,
    true = ets:insert(Durations, {Pid, QueueDuration1, SendDuration1}),
    {noreply, State#state{queue_duration_sum = Sum1,
                          queue_duration_count = Count1}};

handle_call({register, Pid, MFA}, _From, State =
            #state{queue_durations = Durations, callbacks = Callbacks}) ->
    _MRef = erlang:monitor(process, Pid),
    true = ets:insert(Durations, {Pid, infinity, infinity}),
    {reply, ok, State#state{callbacks = dict:store(Pid, MFA, Callbacks)}};

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(update, State) ->
    {noreply, internal_update(State)};

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({'DOWN', _MRef, process, Pid, _Reason},
            State = #state{queue_duration_sum = Sum,
                           queue_duration_count = Count,
                           queue_durations = Durations,
                           callbacks = Callbacks}) ->
    [{_Pid, PrevQueueDuration, _PrevSendDuration}] = ets:lookup(Durations, Pid),
    Sum1 = case PrevQueueDuration of
               infinity -> Sum;
               _        -> Sum - PrevQueueDuration
           end,
    true = ets:delete(State#state.queue_durations, Pid),
    {noreply, State#state{queue_duration_sum = Sum1,
                          queue_duration_count = Count-1,
                          callbacks = dict:erase(Pid, Callbacks)}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{timer = TRef}) ->
    timer:cancel(TRef),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%%----------------------------------------------------------------------------
%% Internal functions
%%----------------------------------------------------------------------------

internal_update(State = #state{memory_limit = Limit,
                               queue_durations = Durations,
                               desired_duration = DesiredDurationAvg,
                               queue_duration_sum = Sum,
                               queue_duration_count = Count,
                               callbacks = Callbacks}) ->
    %% available memory / used memory
    MemoryRatio = Limit / erlang:memory(total),
    AvgDuration = case Count of
                      0 -> infinity;
                      _ -> Sum / Count
                  end,
    DesiredDurationAvg1 =
        case AvgDuration of
            infinity -> infinity;
            AvgQueueDuration -> lists:max([0, AvgQueueDuration * MemoryRatio])
        end,
    State1 = State#state{memory_ratio = MemoryRatio,
                         desired_duration = DesiredDurationAvg1},

    %% only inform queues immediately if the desired duration has
    %% decreased
    case (DesiredDurationAvg == infinity andalso DesiredDurationAvg /= infinity)
        orelse (DesiredDurationAvg1 < DesiredDurationAvg) of
        true ->
            %% If we have pessimistic information, we need to inform
            %% queues to reduce it's memory usage when needed. This
            %% sometimes wakes up queues from hibernation.
            true = ets:foldl(
                     fun ({Pid, QueueDuration, PrevSendDuration}, true) ->
                             case DesiredDurationAvg1 <
                                 lists:min([PrevSendDuration, QueueDuration]) of
                                 true ->
                                     ok =
                                         set_queue_duration(
                                           Pid, DesiredDurationAvg1, Callbacks),
                                     ets:insert(Durations,
                                                {Pid, QueueDuration,
                                                 DesiredDurationAvg1});
                                 _ -> true
                             end
                     end, true, Durations);
        false -> ok
    end,
    State1.

get_memory_limit() ->
    case vm_memory_monitor:get_memory_limit() of
        undefined -> ?MEMORY_SIZE_FOR_DISABLED_VMM;
        A -> A
    end.

set_queue_duration(Pid, QueueDuration, Callbacks) ->
    {M,F,A} = dict:fetch(Pid, Callbacks),
    ok = erlang:apply(M, F, A++[QueueDuration]).
