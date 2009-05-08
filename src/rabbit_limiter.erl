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
-export([start_link/1, shutdown/1]).
-export([limit/2, can_send/2, ack/2, register/2, unregister/2]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(maybe_pid() :: pid() | 'undefined').

-spec(start_link/1 :: (pid()) -> pid()).
-spec(shutdown/1 :: (maybe_pid()) -> 'ok').
-spec(limit/2 :: (maybe_pid(), non_neg_integer()) -> 'ok').
-spec(can_send/2 :: (maybe_pid(), pid()) -> bool()).
-spec(ack/2 :: (maybe_pid(), non_neg_integer()) -> 'ok').
-spec(register/2 :: (maybe_pid(), pid()) -> 'ok').
-spec(unregister/2 :: (maybe_pid(), pid()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

-record(lim, {prefetch_count = 0,
              ch_pid,
              queues = dict:new(), % QPid -> {MonitorRef, Notify}
              volume = 0}).
%% 'Notify' is a boolean that indicates whether a queue should be
%% notified of a change in the limit or volume that may allow it to
%% deliver more messages via the limiter's channel.

%%----------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------

start_link(ChPid) ->
    {ok, Pid} = gen_server:start_link(?MODULE, [ChPid], []),
    Pid.

shutdown(undefined) ->
    ok;
shutdown(LimiterPid) ->
    unlink(LimiterPid),
    gen_server2:cast(LimiterPid, shutdown).

limit(undefined, 0) ->
    ok;
limit(LimiterPid, PrefetchCount) ->
    gen_server2:cast(LimiterPid, {limit, PrefetchCount}).

%% Ask the limiter whether the queue can deliver a message without
%% breaching a limit
can_send(undefined, _QPid) ->
    true;
can_send(LimiterPid, QPid) ->
    rabbit_misc:with_exit_handler(
      fun () -> true end,
      fun () -> gen_server2:call(LimiterPid, {can_send, QPid}, infinity) end).

%% Let the limiter know that the channel has received some acks from a
%% consumer
ack(undefined, _Count) -> ok;
ack(LimiterPid, Count) -> gen_server2:cast(LimiterPid, {ack, Count}).

register(undefined, _QPid) -> ok;
register(LimiterPid, QPid) -> gen_server2:cast(LimiterPid, {register, QPid}).

unregister(undefined, _QPid) -> ok;
unregister(LimiterPid, QPid) -> gen_server2:cast(LimiterPid, {unregister, QPid}).

%%----------------------------------------------------------------------------
%% gen_server callbacks
%%----------------------------------------------------------------------------

init([ChPid]) ->
    {ok, #lim{ch_pid = ChPid} }.

handle_call({can_send, QPid}, _From, State = #lim{volume = Volume}) ->
    case limit_reached(State) of
        true  -> {reply, false, limit_queue(QPid, State)};
        false -> {reply, true, State#lim{volume = Volume + 1}}
    end.

handle_cast(shutdown, State) ->
    {stop, normal, State};

handle_cast({limit, PrefetchCount}, State) ->
    {noreply, maybe_notify(State, State#lim{prefetch_count = PrefetchCount})};

handle_cast({ack, Count}, State = #lim{volume = Volume}) ->
    NewVolume = if Volume == 0 -> 0;
                   true        -> Volume - Count
                end,
    {noreply, maybe_notify(State, State#lim{volume = NewVolume})};

handle_cast({register, QPid}, State) ->
    {noreply, remember_queue(QPid, State)};

handle_cast({unregister, QPid}, State) ->
    {noreply, forget_queue(QPid, State)}.

handle_info({'DOWN', _MonitorRef, _Type, QPid, _Info}, State) ->
    {noreply, forget_queue(QPid, State)}.

terminate(_, _) ->
    ok.

code_change(_, State, _) ->
    State.

%%----------------------------------------------------------------------------
%% Internal plumbing
%%----------------------------------------------------------------------------

maybe_notify(OldState, NewState) ->
    case limit_reached(OldState) andalso not(limit_reached(NewState)) of
        true  -> notify_queues(NewState);
        false -> NewState
    end.

limit_reached(#lim{prefetch_count = Limit, volume = Volume}) ->
    Limit =/= 0 andalso Volume >= Limit.

remember_queue(QPid, State = #lim{queues = Queues}) ->
    case dict:is_key(QPid, Queues) of
        false -> MRef = erlang:monitor(process, QPid),
                 State#lim{queues = dict:store(QPid, {MRef, false}, Queues)};
        true  -> State
    end.

forget_queue(QPid, State = #lim{ch_pid = ChPid, queues = Queues}) ->
    case dict:find(QPid, Queues) of
        {ok, {MRef, _}} ->
            true = erlang:demonitor(MRef),
            ok = rabbit_amqqueue:unblock(QPid, ChPid),
            State#lim{queues = dict:erase(QPid, Queues)};
        error -> State
    end.

limit_queue(QPid, State = #lim{queues = Queues}) ->
    UpdateFun = fun ({MRef, _}) -> {MRef, true} end,
    State#lim{queues = dict:update(QPid, UpdateFun, Queues)}.

notify_queues(State = #lim{ch_pid = ChPid, queues = Queues}) ->
    {QList, NewQueues} =
        dict:fold(fun (_QPid, {_, false}, Acc) -> Acc;
                      (QPid, {MRef, true}, {L, D}) ->
                          {[QPid | L], dict:store(QPid, {MRef, false}, D)}
                  end, {[], Queues}, Queues),
    case length(QList) of
        0 -> ok;
        L ->
            %% We randomly vary the position of queues in the list,
            %% thus ensuring that each queue has an equal chance of
            %% being notified first.
            {L1, L2} = lists:split(random:uniform(L), QList),
            [ok = rabbit_amqqueue:unblock(Q, ChPid) || Q <- L2 ++ L1],
            ok
    end,
    State#lim{queues = NewQueues}.
