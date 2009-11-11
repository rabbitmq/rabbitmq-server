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

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([update/0]).

-export([register/1, push_queue_duration/2]).

-record(state, {timer,               %% 'internal_update' timer
                queue_durations,     %% ets, (qpid, last_reported, last_sent)
                queue_duration_sum,  %% sum of all queue_durations
                queue_duration_count,%% number of elements in sum
                memory_limit,        %% how much memory we intend to use
                memory_ratio         %% how much more memory we can use
               }).

-define(SERVER, ?MODULE).
-define(DEFAULT_UPDATE_INTERVAL_MS, 2500).
-define(TABLE_NAME, ?MODULE).
-define(MAX_QUEUE_DURATION, 60*60*24). % 1 day

%% If user disabled vm_memory_monitor, let's assume 1GB of memory we can use.
-define(MEMORY_SIZE_FOR_DISABLED_VMM, 1073741824).

%%----------------------------------------------------------------------------
-ifdef(use_specs).
-type(state() :: #state{timer               :: timer:tref(),
                        queue_durations     :: tid(),
                        queue_duration_sum  :: float(),
                        queue_duration_count:: non_neg_integer(),
                        memory_limit        :: pos_integer(),
                        memory_ratio        :: float() }).

-spec(start_link/0 :: () -> 'ignore' | {'error', _} | {'ok', pid()}).
-spec(register/1 :: (pid()) -> 'ok').
-spec(push_queue_duration/2 :: (pid(), float() | 'infinity') -> 'ok').

-spec(init/1 :: ([]) -> {'ok', state()}).

-spec(internal_update/1 :: (state()) -> state()).
-endif.

%%----------------------------------------------------------------------------

start_link() ->
    gen_server2:start_link({local, ?SERVER}, ?MODULE, [], []).

update() ->
    gen_server2:cast(?SERVER, update).

%%----------------------------------------------------------------------------

register(Pid) ->
    gen_server2:cast(?SERVER, {register, Pid}).

push_queue_duration(Pid, QueueDuration) ->
    gen_server2:call(rabbit_memory_monitor,
                     {push_queue_duration, Pid, QueueDuration}).

%%----------------------------------------------------------------------------

get_memory_limit() ->
    case vm_memory_monitor:get_memory_limit() of
        undefined -> ?MEMORY_SIZE_FOR_DISABLED_VMM;
        A -> A
    end.

init([]) ->
    %% We should never use more memory than user requested. As the memory
    %% manager doesn't really know how much memory queues are using, we shall
    %% try to remain safe distance from real throttle limit.
    MemoryLimit = trunc(get_memory_limit() * 0.6),

    {ok, TRef} = timer:apply_interval(?DEFAULT_UPDATE_INTERVAL_MS,
                                      ?SERVER, update, []),
    {ok, #state{timer                = TRef,
                queue_durations      = ets:new(?TABLE_NAME, [set, private]),
                queue_duration_sum   = 0.0,
                queue_duration_count = 0,
                memory_limit         = MemoryLimit,
                memory_ratio         = 1.0}}.

get_avg_duration(#state{queue_duration_sum = Sum,
                        queue_duration_count = Count}) ->
    case Count of
        0 -> infinity;
        _ -> Sum / Count
    end.

get_desired_duration(State = #state{memory_ratio = Ratio}) ->
    case get_avg_duration(State) of
        infinity         -> infinity;
        AvgQueueDuration -> AvgQueueDuration * Ratio
    end.

handle_call({push_queue_duration, Pid, QueueDuration}, From,
            State = #state{queue_duration_sum = Sum,
                           queue_duration_count = Count,
                           queue_durations = Durations}) ->
    SendDuration = get_desired_duration(State),
    gen_server2:reply(From, SendDuration),

    QueueDuration1 = case QueueDuration > ?MAX_QUEUE_DURATION of
                         true  -> infinity;
                         false -> QueueDuration
                     end,

    [{_Pid, PrevQueueDuration, _PrevSendDuration}] = ets:lookup(Durations, Pid),
    {Sum1, Count1} =
            case {PrevQueueDuration, QueueDuration1} of
                {infinity, infinity} -> {Sum, Count};
                {infinity, _}        -> {Sum + QueueDuration1,    Count + 1};
                {_, infinity}        -> {Sum - PrevQueueDuration, Count - 1};
                {_, _} -> {Sum - PrevQueueDuration + QueueDuration1, Count}
            end,
    true = ets:insert(Durations, {Pid, QueueDuration1, SendDuration}),
    {noreply, State#state{queue_duration_sum = Sum1,
                          queue_duration_count = Count1}};

handle_call(_Request, _From, State) ->
    {noreply, State}.


handle_cast(update, State) ->
    {noreply, internal_update(State)};

handle_cast({register, Pid}, State) ->
    _MRef = erlang:monitor(process, Pid),
    true = ets:insert(State#state.queue_durations, {Pid, infinity, infinity}),
    {noreply, State};

handle_cast(_Request, State) ->
    {noreply, State}.


handle_info({'DOWN', _MRef, process, Pid, _Reason},
            State = #state{queue_duration_sum = Sum,
                           queue_duration_count = Count,
                           queue_durations = Durations}) ->
    [{_Pid, PrevQueueDuration, _PrevSendDuration}] = ets:lookup(Durations, Pid),
    Sum1 = case PrevQueueDuration of
               infinity -> Sum;
               _        -> Sum - PrevQueueDuration
           end,
    true = ets:delete(State#state.queue_durations, Pid),
    {noreply, State#state{queue_duration_sum = Sum1,
                          queue_duration_count = Count-1}};

handle_info(_Info, State) ->
    {noreply, State}.


terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

set_queue_duration(Pid, QueueDuration) ->
    gen_server2:pcast(Pid, 7, {set_queue_duration, QueueDuration}).

internal_update(State = #state{memory_limit = Limit,
                               queue_durations = Durations}) ->
    DesiredDurationAvg = get_desired_duration(State),
    %% available memory / used memory
    MemoryRatio = Limit / erlang:memory(total),
    State1 = State#state{memory_ratio = MemoryRatio},
    DesiredDurationAvg1 = get_desired_duration(State1),

    %% only inform queues immediately if the desired duration has
    %% decreased
    case DesiredDurationAvg1 < DesiredDurationAvg of
        true ->
            %% If we have pessimistic information, we need to inform
            %% queues to reduce it's memory usage when needed. This
            %% sometimes wakes up queues from hibernation.
            true = ets:foldl(
                     fun ({Pid, QueueDuration, PrevSendDuration}, true) ->
                             case DesiredDurationAvg1 <
                                 lists:min([PrevSendDuration, QueueDuration]) of
                                 true ->
                                     set_queue_duration(Pid,
                                                        DesiredDurationAvg1),
                                     ets:insert(Durations,
                                                {Pid, QueueDuration,
                                                 DesiredDurationAvg1});
                                 _ -> true
                             end
                     end, true, Durations);
        false -> ok
    end,
    State1.

