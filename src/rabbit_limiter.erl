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
%% Copyright (c) 2007-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_limiter).

-behaviour(gen_server2).

-export([start_link/0]).
%% channel API
-export([new/1, limit/3, unlimit/1, block/1, unblock/1,
         is_limited/1, is_blocked/1, is_active/1, get_limit/1, ack/2, pid/1]).
%% queue API
-export([client/1, activate/1, can_send/2, resume/1, forget/1, is_suspended/1]).
%% callbacks
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2, prioritise_call/3]).

%%----------------------------------------------------------------------------

-record(lstate, {pid, limited, blocked}).
-record(qstate, {pid, state}).

-ifdef(use_specs).

-type(lstate() :: #lstate{pid     :: pid(),
                          limited :: boolean(),
                          blocked :: boolean()}).
-type(qstate() :: #qstate{pid :: pid(),
                          state :: 'dormant' | 'active' | 'suspended'}).

-spec(start_link/0 :: () -> rabbit_types:ok_pid_or_error()).
-spec(new/1 :: (pid()) -> lstate()).

-spec(limit/3      :: (lstate(), non_neg_integer(), non_neg_integer()) ->
                           lstate()).
-spec(unlimit/1    :: (lstate()) -> lstate()).
-spec(block/1      :: (lstate()) -> lstate()).
-spec(unblock/1    :: (lstate()) -> lstate()).
-spec(is_limited/1 :: (lstate()) -> boolean()).
-spec(is_blocked/1 :: (lstate()) -> boolean()).
-spec(is_active/1  :: (lstate()) -> boolean()).
-spec(get_limit/1  :: (lstate()) -> non_neg_integer()).
-spec(ack/2        :: (lstate(), non_neg_integer()) -> 'ok').
-spec(pid/1        :: (lstate()) -> pid()).

-spec(client/1       :: (pid()) -> qstate()).
-spec(activate/1     :: (qstate()) -> qstate()).
-spec(can_send/2     :: (qstate(), boolean()) ->
                             {'continue' | 'suspend', qstate()}).
-spec(resume/1       :: (qstate()) -> qstate()).
-spec(forget/1       :: (qstate()) -> undefined).
-spec(is_suspended/1 :: (qstate()) -> boolean()).

-endif.

%%----------------------------------------------------------------------------

-record(lim, {prefetch_count = 0,
              ch_pid,
              blocked = false,
              queues = orddict:new(), % QPid -> {MonitorRef, Notify}
              volume = 0}).
%% 'Notify' is a boolean that indicates whether a queue should be
%% notified of a change in the limit or volume that may allow it to
%% deliver more messages via the limiter's channel.

%%----------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------

start_link() -> gen_server2:start_link(?MODULE, [], []).

new(Pid) ->
    %% this a 'call' to ensure that it is invoked at most once.
    ok = gen_server:call(Pid, {new, self()}),
    #lstate{pid = Pid, limited = false, blocked = false}.

limit(L, PrefetchCount, UnackedCount) when PrefetchCount > 0 ->
    ok = gen_server:call(L#lstate.pid, {limit, PrefetchCount, UnackedCount}),
    L#lstate{limited = true}.

unlimit(L) ->
    ok = gen_server:call(L#lstate.pid, unlimit),
    L#lstate{limited = false}.

block(L) ->
    ok = gen_server:call(L#lstate.pid, block),
    L#lstate{blocked = true}.

unblock(L) ->
    ok = gen_server:call(L#lstate.pid, unblock),
    L#lstate{blocked = false}.

is_limited(#lstate{limited = Limited}) -> Limited.

is_blocked(#lstate{blocked = Blocked}) -> Blocked.

is_active(L) -> is_limited(L) orelse is_blocked(L).

get_limit(#lstate{limited = false}) -> 0;
get_limit(L) -> gen_server:call(L#lstate.pid, get_limit).

ack(#lstate{limited = false}, _AckCount) -> ok;
ack(L, AckCount) -> gen_server:cast(L#lstate.pid, {ack, AckCount}).

pid(#lstate{pid = Pid}) -> Pid.

client(Pid) -> #qstate{pid = Pid, state = dormant}.

activate(L = #qstate{state = dormant}) ->
    ok = gen_server:cast(L#qstate.pid, {register, self()}),
    L#qstate{state = active};
activate(L) -> L.

%% Ask the limiter whether the queue can deliver a message without
%% breaching a limit.
can_send(L = #qstate{state = active}, AckRequired) ->
    rabbit_misc:with_exit_handler(
      fun () -> {continue, L} end,
      fun () -> Msg = {can_send, self(), AckRequired},
                case gen_server2:call(L#qstate.pid, Msg, infinity) of
                    true  -> {continue, L};
                    false -> {suspend, L#qstate{state = suspended}}
                end
      end);
can_send(L, _AckRequired) -> {continue, L}.

resume(L) -> L#qstate{state = active}.

forget(#qstate{state = dormant}) -> undefined;
forget(L) ->
    ok = gen_server:cast(L#qstate.pid, {unregister, self()}),
    undefined.

is_suspended(#qstate{state = suspended}) -> true;
is_suspended(#qstate{})                  -> false.

%%----------------------------------------------------------------------------
%% gen_server callbacks
%%----------------------------------------------------------------------------

init([]) -> {ok, #lim{}}.

prioritise_call(get_limit, _From, _State) -> 9;
prioritise_call(_Msg,      _From, _State) -> 0.

handle_call({new, ChPid}, _From, State = #lim{ch_pid = undefined}) ->
    {reply, ok, State#lim{ch_pid = ChPid}};

handle_call({limit, PrefetchCount, UnackedCount}, _From, State) ->
    %% assertion
    true = State#lim.prefetch_count == 0 orelse
        State#lim.volume == UnackedCount,
    {reply, ok, maybe_notify(State, State#lim{prefetch_count = PrefetchCount,
                                              volume         = UnackedCount})};

handle_call(unlimit, _From, State) ->
    {reply, ok, maybe_notify(State, State#lim{prefetch_count = 0,
                                              volume         = 0})};

handle_call(block, _From, State) ->
    {reply, ok, State#lim{blocked = true}};

handle_call(unblock, _From, State) ->
    {reply, ok, maybe_notify(State, State#lim{blocked = false})};

handle_call(get_limit, _From, State = #lim{prefetch_count = PrefetchCount}) ->
    {reply, PrefetchCount, State};

handle_call({can_send, QPid, _AckRequired}, _From,
            State = #lim{blocked = true}) ->
    {reply, false, limit_queue(QPid, State)};
handle_call({can_send, QPid, AckRequired}, _From,
            State = #lim{volume = Volume}) ->
    case limit_reached(State) of
        true  -> {reply, false, limit_queue(QPid, State)};
        false -> {reply, true,  State#lim{volume = if AckRequired -> Volume + 1;
                                                      true        -> Volume
                                                   end}}
    end.

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
    {ok, State}.

%%----------------------------------------------------------------------------
%% Internal plumbing
%%----------------------------------------------------------------------------

maybe_notify(OldState, NewState) ->
    case (limit_reached(OldState) orelse blocked(OldState)) andalso
        not (limit_reached(NewState) orelse blocked(NewState)) of
        true  -> notify_queues(NewState);
        false -> NewState
    end.

limit_reached(#lim{prefetch_count = Limit, volume = Volume}) ->
    Limit =/= 0 andalso Volume >= Limit.

blocked(#lim{blocked = Blocked}) -> Blocked.

remember_queue(QPid, State = #lim{queues = Queues}) ->
    case orddict:is_key(QPid, Queues) of
        false -> MRef = erlang:monitor(process, QPid),
                 State#lim{queues = orddict:store(QPid, {MRef, false}, Queues)};
        true  -> State
    end.

forget_queue(QPid, State = #lim{queues = Queues}) ->
    case orddict:find(QPid, Queues) of
        {ok, {MRef, _}} -> true = erlang:demonitor(MRef),
                           State#lim{queues = orddict:erase(QPid, Queues)};
        error           -> State
    end.

limit_queue(QPid, State = #lim{queues = Queues}) ->
    UpdateFun = fun ({MRef, _}) -> {MRef, true} end,
    State#lim{queues = orddict:update(QPid, UpdateFun, Queues)}.

notify_queues(State = #lim{ch_pid = ChPid, queues = Queues}) ->
    {QList, NewQueues} =
        orddict:fold(fun (_QPid, {_, false}, Acc) -> Acc;
                         (QPid, {MRef, true}, {L, D}) ->
                             {[QPid | L], orddict:store(QPid, {MRef, false}, D)}
                     end, {[], Queues}, Queues),
    case length(QList) of
        0 -> ok;
        1 -> ok = rabbit_amqqueue:resume(hd(QList), ChPid); %% common case
        L ->
            %% We randomly vary the position of queues in the list,
            %% thus ensuring that each queue has an equal chance of
            %% being notified first.
            {L1, L2} = lists:split(random:uniform(L), QList),
            [[ok = rabbit_amqqueue:resume(Q, ChPid) || Q <- L3]
             || L3 <- [L2, L1]],
            ok
    end,
    State#lim{queues = NewQueues}.
