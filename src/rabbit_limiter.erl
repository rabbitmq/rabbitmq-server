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

%%----------------------------------------------------------------------------
%% API
%-%---------------------------------------------------------------------------

start_link(ChPid) ->
    {ok, Pid} = gen_server:start_link(?MODULE, [ChPid], []),
    Pid.

set_prefetch_count(LimiterPid, PrefetchCount) ->
    gen_server:cast(LimiterPid, {prefetch_count, PrefetchCount}).

%% Ask the limiter whether the queue can deliver a message without
%% breaching a limit
can_send(LimiterPid, QPid) ->
    gen_server:call(LimiterPid, {can_send, QPid}).

%% Let the limiter know that the channel has received some acks from a
%% consumer
decrement_capacity(LimiterPid, Magnitude) ->
    gen_server:cast(LimiterPid, {decrement_capacity, Magnitude}).

%%----------------------------------------------------------------------------
%% gen_server callbacks
%%----------------------------------------------------------------------------

init([ChPid]) ->
    {ok, #lim{ch_pid = ChPid} }.

handle_call({can_send, QPid}, _From, State = #lim{in_use = InUse}) ->
    case limit_reached(State) of
        true  -> {reply, false, remember_queue(QPid, State)};
        false -> {reply, true, State#lim{in_use = InUse + 1}}
    end.

handle_cast({prefetch_count, PrefetchCount},
            State = #lim{prefetch_count = CurrentLimit}) ->
    NewState = State#lim{prefetch_count = PrefetchCount},
    {noreply, if PrefetchCount > CurrentLimit -> forget_queues(NewState);
                 true                         -> NewState
              end};

handle_cast({decrement_capacity, Magnitude}, State) ->
    NewState = decrement_in_use(Magnitude, State),
    ShouldNotify = limit_reached(State) and not(limit_reached(NewState)),
    {noreply, if ShouldNotify -> forget_queues(NewState);
                 true         -> NewState
              end}.

handle_info({'DOWN', _MonitorRef, _Type, QPid, _Info},
            State = #lim{queues = Queues}) ->
    {noreply, State#lim{queues = dict:erase(QPid, Queues)}}.

terminate(_, _) ->
    ok.

code_change(_, State, _) ->
    State.

%%----------------------------------------------------------------------------
%% Internal plumbing
%%----------------------------------------------------------------------------

remember_queue(QPid, State = #lim{queues = Queues}) ->
    case dict:is_key(QPid, Queues) of
        false -> MonitorRef = erlang:monitor(process, QPid),
                 State#lim{queues = dict:store(QPid, MonitorRef, Queues)};
        true  -> State
    end.

forget_queues(State = #lim{ch_pid = ChPid, queues = Queues}) ->
    ok = dict:fold(fun(Q, Ref, ok) ->
                           true = erlang:demonitor(Ref),
                           rabbit_amqqueue:unblock(Q, ChPid)
                   end, ok, Queues),
    State#lim{queues = dict:new()}.

decrement_in_use(_, State = #lim{in_use = 0}) ->
    State#lim{in_use = 0};
decrement_in_use(Magnitude, State = #lim{in_use = InUse}) ->
    State#lim{in_use = InUse - Magnitude}.

limit_reached(#lim{prefetch_count = 0}) ->
    false;
limit_reached(#lim{prefetch_count = Limit, in_use = InUse}) ->
    InUse >= Limit.
