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
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_limiter).

-behaviour(gen_server2).

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2, prioritise_call/3]).
-export([start_link/0, make_token/0, make_token/1, is_enabled/1, enable/2,
         disable/1]).
-export([limit/2, can_send/3, ack/2, register/2, unregister/2]).
-export([get_limit/1, block/1, unblock/1, is_blocked/1]).

-export([start/0]).

%%----------------------------------------------------------------------------

-record(token, {pid, enabled}).

-ifdef(use_specs).

-export_type([token/0]).

-opaque(token() :: #token{}).

-spec(start_link/0 :: () -> rabbit_types:ok_pid_or_error()).
-spec(make_token/0 :: () -> token()).
-spec(make_token/1 :: ('undefined' | pid()) -> token()).
-spec(is_enabled/1 :: (token()) -> boolean()).
-spec(enable/2 :: (token(), non_neg_integer()) -> token()).
-spec(disable/1 :: (token()) -> token()).
-spec(limit/2 :: (token(), non_neg_integer()) -> 'ok' | {'disabled', token()}).
-spec(can_send/3 :: (token(), pid(), boolean()) -> boolean()).
-spec(ack/2 :: (token(), non_neg_integer()) -> 'ok').
-spec(register/2 :: (token(), pid()) -> 'ok').
-spec(unregister/2 :: (token(), pid()) -> 'ok').
-spec(get_limit/1 :: (token()) -> non_neg_integer()).
-spec(block/1 :: (token()) -> 'ok').
-spec(unblock/1 :: (token()) -> 'ok' | {'disabled', token()}).
-spec(is_blocked/1 :: (token()) -> boolean()).

-endif.

%%----------------------------------------------------------------------------

-record(lim, {ch_pid,
              queues = orddict:new()}). % QPid -> {MonitorRef, Notify}
%% 'Notify' is a boolean that indicates whether a queue should be
%% notified of a change in the limit or volume that may allow it to
%% deliver more messages via the limiter's channel.

-define(BLOCKED,  2).
-define(VOLUME,   3).
-define(PREFETCH, 4).
-define(LOCK,     5).

-define(TRUE, 1).
-define(FALSE, 0).
-define(ID, 0).

-define(IS_BLOCKED(X), X == ?TRUE).

-define(TABLE, ?MODULE).

%%----------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------

start() ->
    ets:new(?TABLE, [public, set, named_table]),
    ok.

start_link() -> gen_server2:start_link(?MODULE, [], []).

make_token() -> make_token(undefined).
make_token(Pid) -> #token{pid = Pid, enabled = false}.

is_enabled(#token{enabled = Enabled}) -> Enabled.

enable(#token{pid = Pid} = Token, Volume) ->
    gen_server2:call(Pid, {enable, Token, self(), Volume}, infinity).

disable(#token{pid = Pid} = Token) ->
    gen_server2:call(Pid, {disable, Token}, infinity).

limit(Limiter, PrefetchCount) ->
    maybe_call(Limiter, {limit, PrefetchCount, Limiter}, ok).

%% Ask the limiter whether the queue can deliver a message without
%% breaching a limit. Note that we don't use maybe_call here in order
%% to avoid always going through with_exit_handler/2, even when the
%% limiter is disabled.
can_send(#token{pid = Pid, enabled = true}, QPid, AckRequired)
  when node(Pid) =:= node() ->
    can_send2(Pid, QPid, AckRequired);
can_send(#token{pid = Pid, enabled = true}, QPid, AckRequired) ->
    rabbit_misc:with_exit_handler(
      fun () -> true end,
      fun () ->
              gen_server2:call(Pid, {can_send, QPid, AckRequired}, infinity)
      end);
can_send(_, _, _) ->
    true.

%% Let the limiter know that the channel has received some acks from a
%% consumer
ack(Limiter, Count) -> maybe_cast(Limiter, {ack, Count}).

register(Limiter, QPid) -> maybe_call(Limiter, {register, QPid}, ok).

unregister(Limiter, QPid) -> maybe_cast(Limiter, {unregister, QPid}).

get_limit(Limiter) ->
    rabbit_misc:with_exit_handler(
      fun () -> 0 end,
      fun () -> maybe_call(Limiter, get_limit, 0) end).

block(Limiter) ->
    maybe_call(Limiter, block, ok).

unblock(Limiter) ->
    maybe_call(Limiter, {unblock, Limiter}, ok).

is_blocked(Limiter) ->
    maybe_call(Limiter, is_blocked, false).

maybe_call(#token{pid = Pid, enabled = true}, Call, _Default) ->
    gen_server2:call(Pid, Call, infinity);
maybe_call(_, _Call, Default) ->
    Default.

maybe_cast(#token{pid = Pid, enabled = true}, Cast) ->
    gen_server2:cast(Pid, Cast);
maybe_cast(_, _Call) ->
    ok.

%%----------------------------------------------------------------------------
%% gen_server callbacks
%%----------------------------------------------------------------------------

init([]) ->
    true = ets:insert_new(?TABLE, {self(), ?FALSE, 0, 0, ?FALSE}),
    {ok, #lim{}}.

prioritise_call(get_limit, _From, _State) -> 9;
prioritise_call(_Msg,      _From, _State) -> 0.

can_send2(LimPid, QPid, true) ->
    with_lock(
      LimPid,
      fun () ->
              [Blocked, OldVolume, _NewVolume, Prefetch] =
                  ets:update_counter(?TABLE, LimPid,
                                     [{?BLOCKED, ?ID},
                                      {?VOLUME, 0},
                                      {?VOLUME, 1},
                                      {?PREFETCH, 0}]),
              case ?IS_BLOCKED(Blocked) orelse limit_reached(Prefetch, OldVolume) of
                  true ->
                      gen_server2:cast(LimPid, {limit_queue, QPid}),
                      ets:update_counter(?TABLE, LimPid, {?VOLUME, -1}),
                      false;
                  false ->
                      true
              end
      end);
can_send2(LimPid, QPid, false) ->
    [{Blocked, Volume, Prefetch}] =
        with_lock(LimPid, fun () -> ets:lookup(?TABLE, LimPid) end),
    case ?IS_BLOCKED(Blocked) orelse limit_reached(Prefetch, Volume) of
        true  -> gen_server2:cast(LimPid, {limit_queue, QPid}),
                 false;
        false -> true
    end.

can_send1(QPid, true, State) ->
    case with_lock(
           self(),
           fun () ->
                   [Blocked, OldVolume, _NewVolume, Prefetch] =
                       ets:update_counter(?TABLE, self(),
                                          [{?BLOCKED, ?ID},
                                           {?VOLUME, 0},
                                           {?VOLUME, 1},
                                           {?PREFETCH, 0}]),
                   case ?IS_BLOCKED(Blocked) orelse limit_reached(Prefetch, OldVolume) of
                       true  -> ets:update_counter(?TABLE, self(), {?VOLUME, -1}),
                                false;
                       false -> true
                   end
           end) of
        false -> {reply, false, limit_queue(QPid, State)};
        true  -> {reply, true, State}
    end;
can_send1(QPid, false, State) ->
    [{Blocked, Volume, Prefetch}] =
        with_lock(self(), fun () -> ets:lookup(?TABLE, self()) end),
    case ?IS_BLOCKED(Blocked) orelse limit_reached(Prefetch, Volume) of
        true  -> {reply, false, limit_queue(QPid, State)};
        false -> {reply, true, State}
    end.

handle_call({can_send, QPid, AckRequired}, _From, State) ->
    can_send1(QPid, AckRequired, State);

handle_call(get_limit, _From, State) ->
    [{_Blocked, _Volume, Prefetch}] =
        with_lock(self(), fun () -> ets:lookup(?TABLE, self()) end),
    {reply, Prefetch, State};

handle_call({limit, PrefetchCount, Token}, _From, State) ->
    [Blocked, Volume, OldPrefetch, PrefetchCount] =
        with_lock(self(),
                  fun () ->
                          ets:update_counter(?TABLE, self(),
                                             [{?BLOCKED, ?ID},
                                              {?VOLUME, 0},
                                              {?PREFETCH, 0},
                                              {?PREFETCH, 1, 0, PrefetchCount}])
                  end),
    case maybe_notify({Blocked, Volume, OldPrefetch},
                      {Blocked, Volume, PrefetchCount}, State) of
        {cont, State1} ->
            {reply, ok, State1};
        {stop, State1} ->
            {reply, {disabled, Token#token{enabled = false}}, State1}
    end;

handle_call(block, _From, State) ->
    true = with_lock(
             self(), fun () -> ets:update_element(?TABLE, self(), {?BLOCKED, ?TRUE}) end),
    {reply, ok, State};

handle_call({unblock, Token}, _From, State) ->
    [OldBlocked, NewBlocked, Volume, Prefetch] =
        with_lock(self(),
                  fun () ->
                          ets:update_counter(?TABLE, self(),
                                             [{?BLOCKED, ?ID},
                                              {?BLOCKED, -?TRUE, ?FALSE, ?FALSE},
                                              {?VOLUME, 0},
                                              {?PREFETCH, 0}])
                  end),
    case maybe_notify({OldBlocked, Volume, Prefetch},
                      {NewBlocked, Volume, Prefetch}, State) of
        {cont, State1} ->
            {reply, ok, State1};
        {stop, State1} ->
            {reply, {disabled, Token#token{enabled = false}}, State1}
    end;

handle_call(is_blocked, _From, State) ->
    [{Blocked, _Volume, _Prefetch}] =
        with_lock(self(), fun () -> ets:lookup(?TABLE, self()) end),
    {reply, ?IS_BLOCKED(Blocked), State};

handle_call({enable, Token, Channel, Volume}, _From, State) ->
    true = with_lock(
             self(), fun () -> ets:update_element(?TABLE, self(), {?VOLUME, Volume}) end),
    {reply, Token#token{enabled = true}, State#lim{ch_pid = Channel}};
handle_call({disable, Token}, _From, State) ->
    {reply, Token#token{enabled = false}, State};

handle_call({register, QPid}, _From, State) ->
    {reply, ok, remember_queue(QPid, State)}.

handle_cast({ack, Count}, State) ->
    [Blocked, OldVolume, NewVolume, Prefetch] = R =
        with_lock(self(), fun () -> ets:update_counter(?TABLE, self(),
                                                       [{?BLOCKED, ?ID},
                                                        {?VOLUME, 0},
                                                        {?VOLUME, -Count, 0, 0},
                                                        {?PREFETCH, 0}]) end),
    {cont, State1} = maybe_notify({Blocked, OldVolume, Prefetch},
                                  {Blocked, NewVolume, Prefetch},
                                  State),
    {noreply, State1};

handle_cast({limit_queue, QPid}, State) ->
    {noreply, limit_queue(QPid, State)};

handle_cast({unregister, QPid}, State) ->
    {noreply, forget_queue(QPid, State)}.

handle_info({'DOWN', _MonitorRef, _Type, QPid, _Info}, State) ->
    {noreply, forget_queue(QPid, State)}.

terminate(_, _) ->
    true = ets:delete(?TABLE, self()),
    ok.

code_change(_, State, _) ->
    State.

%%----------------------------------------------------------------------------
%% Internal plumbing
%%----------------------------------------------------------------------------

maybe_notify({OldBlocked, OldVolume, OldPrefetch},
             {NewBlocked, NewVolume, NewPrefetch}, State) ->
    case (limit_reached(OldPrefetch, OldVolume) orelse ?IS_BLOCKED(OldBlocked)) andalso
        not (limit_reached(NewPrefetch, NewVolume) orelse ?IS_BLOCKED(NewBlocked)) of
        true  -> {case NewPrefetch of
                      0 -> stop;
                      _ -> cont
                  end, notify_queues(State)};
        false -> {cont, State}
    end.

limit_reached(0, _Volume) ->
    false;
limit_reached(Prefetch, Volume) ->
    Volume >= Prefetch.

remember_queue(QPid, State = #lim{queues = Queues}) ->
    case orddict:is_key(QPid, Queues) of
        false -> MRef = erlang:monitor(process, QPid),
                 State#lim{queues = orddict:store(QPid, {MRef, false}, Queues)};
        true  -> State
    end.

forget_queue(QPid, State = #lim{ch_pid = ChPid, queues = Queues}) ->
    case orddict:find(QPid, Queues) of
        {ok, {MRef, _}} -> true = erlang:demonitor(MRef),
                           ok = rabbit_amqqueue:unblock(QPid, ChPid),
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
        L ->
            %% We randomly vary the position of queues in the list,
            %% thus ensuring that each queue has an equal chance of
            %% being notified first.
            {L1, L2} = lists:split(random:uniform(L), QList),
            [[ok = rabbit_amqqueue:unblock(Q, ChPid) || Q <- L3]
             || L3 <- [L2, L1]],
            ok
    end,
    State#lim{queues = NewQueues}.

with_lock(LimPid, Fun) ->
    case ets:update_counter(?TABLE, LimPid, {?LOCK, ?TRUE}) of
        ?TRUE -> R = Fun(),
                 ets:update_counter(?TABLE, LimPid, {?LOCK, -?TRUE}),
                 R;
        _     -> ets:update_counter(?TABLE, LimPid, {?LOCK, -?TRUE}),
                 with_lock(LimPid, Fun)
    end.
