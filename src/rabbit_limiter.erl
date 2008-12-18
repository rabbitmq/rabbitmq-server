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

-module(rabbit_limiter).

-behaviour(gen_server).

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2]).
-export([start_link/1]).
-export([set_prefetch_count/2, can_send/2, decrement_capacity/2]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(set_prefetch_count/2 :: (pid(), non_neg_integer()) -> 'ok').
-spec(can_send/2 :: (pid(), pid()) -> bool()).
-spec(decrement_capacity/2 :: (pid(), non_neg_integer()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

-record(lim, {prefetch_count = 0,
              ch_pid,
              queues = dict:new(),
              in_use = 0}).

%---------------------------------------------------------------------------
% API
%---------------------------------------------------------------------------

% Kicks this pig
start_link(ChPid) ->
    {ok, Pid} = gen_server:start_link(?MODULE, [ChPid], []),
    Pid.

set_prefetch_count(LimiterPid, PrefetchCount) ->
    gen_server:cast(LimiterPid, {prefetch_count, PrefetchCount}).

% Queries the limiter to ask whether the queue can deliver a message
% without breaching a limit
can_send(LimiterPid, QPid) ->
    gen_server:call(LimiterPid, {can_send, QPid}).

% Lets the limiter know that a queue has received an ack from a consumer
% and hence can reduce the in-use-by-that queue capcity information
decrement_capacity(LimiterPid, Magnitude) ->
    gen_server:cast(LimiterPid, {decrement_capacity, Magnitude}).

%---------------------------------------------------------------------------
% gen_server callbacks
%---------------------------------------------------------------------------

init([ChPid]) ->
    {ok, #lim{ch_pid = ChPid} }.

% This queuries the limiter to ask if it is possible to send a message without
% breaching a limit for this queue process
handle_call({can_send, QPid}, _From, State = #lim{in_use = InUse}) ->
    NewState = monitor_queue(QPid, State),
    case limit_reached(NewState) of
        true  -> {reply, false, NewState};
        false ->
                {reply, true, NewState#lim{in_use = InUse + 1}}
    end.

% When the new limit is larger than the existing limit,
% notify all queues and forget about queues with an in-use
% capcity of zero
handle_cast({prefetch_count, PrefetchCount},
            State = #lim{prefetch_count = CurrentLimit})
            when PrefetchCount > CurrentLimit ->
    notify_queues(State),
    NewState = demonitor_all(State),
    {noreply, NewState#lim{prefetch_count = PrefetchCount,
                        in_use = 0}};

% Removes the queue process from the set of monitored queues
handle_cast({unregister_queue, QPid}, State = #lim{}) ->
    NewState = decrement_in_use(1, State),
    {noreply, demonitor_queue(QPid, NewState)};

% Default setter of the prefetch count
handle_cast({prefetch_count, PrefetchCount}, State) ->
    {noreply, State#lim{prefetch_count = PrefetchCount}};

% This is an asynchronous ack from a queue that it has received an ack from
% a queue. This allows the limiter to update the the in-use-by-that queue
% capacity infromation.
handle_cast({decrement_capacity, Magnitude}, State = #lim{in_use = InUse}) ->
    NewState = decrement_in_use(Magnitude, State),
    ShouldNotify = limit_reached(State) and not(limit_reached(NewState)),
    if
        ShouldNotify ->
            notify_queues(State),
            NextState = demonitor_all(State),
            {noreply, NextState#lim{in_use = InUse - Magnitude}};
        true ->
            {noreply, NewState}
    end.

handle_info(_, State) ->
    {noreply, State}.

terminate(_, _) ->
    ok.

code_change(_, State, _) ->
    State.

%---------------------------------------------------------------------------
% Internal plumbing
%---------------------------------------------------------------------------

% Starts to monitor a particular queue
monitor_queue(QPid, State = #lim{queues = Queues}) ->
    case dict:is_key(QPid, Queues) of
        false -> MonitorRef = erlang:monitor(process, QPid),
                 State#lim{queues = dict:store(QPid, MonitorRef, Queues)};
        true  -> State
    end.

% Stops monitoring a particular queue
demonitor_queue(QPid, State = #lim{queues = Queues}) ->
    MonitorRef = dict:fetch(QPid, Queues),
    true = erlang:demonitor(MonitorRef),
    State#lim{queues = dict:erase(QPid, Queues)}.

% Stops monitoring all queues
demonitor_all(State = #lim{queues = Queues}) ->
    dict:map(fun(_, Ref) -> true = erlang:demonitor(Ref) end, Queues),
    State#lim{queues = dict:new()}.

% Reduces the in-use-count of the queue by a specific magnitude
decrement_in_use(_, State = #lim{in_use = 0}) ->
    State#lim{in_use = 0};

decrement_in_use(Magnitude, State = #lim{in_use = InUse}) ->
    State#lim{in_use = InUse - Magnitude}.

% Unblocks every queue that this limiter knows about
notify_queues(#lim{ch_pid = ChPid, queues = Queues}) ->
    dict:map(fun(Q, _) -> rabbit_amqqueue:unblock(Q, ChPid) end, Queues).

% A prefetch limit of zero means unlimited
limit_reached(#lim{prefetch_count = 0}) ->
    false;

% Works out whether the limit is breached for the current limiter state
limit_reached(#lim{prefetch_count = Limit, in_use = InUse}) ->
    InUse == Limit.

