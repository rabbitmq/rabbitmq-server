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

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2, prioritise_call/3]).
-export([start_link/0, make_token/0, make_token/1, is_enabled/1, enable/2,
         disable/1]).
-export([limit/2, can_send/4, ack/2, register/2, unregister/2]).
-export([get_limit/1, block/1, unblock/1, is_consumer_blocked/2, is_blocked/1]).
-export([credit/4, drained/1, forget_consumer/2, copy_queue_state/2]).

-import(rabbit_misc, [serial_add/2, serial_diff/2]).

%%----------------------------------------------------------------------------

-record(token, {pid, enabled, credits}).

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
-spec(can_send/4 :: (token(), pid(), boolean(), rabbit_types:ctag())
                    -> token() | 'consumer_blocked' | 'channel_blocked').
-spec(ack/2 :: (token(), non_neg_integer()) -> 'ok').
-spec(register/2 :: (token(), pid()) -> 'ok').
-spec(unregister/2 :: (token(), pid()) -> 'ok').
-spec(get_limit/1 :: (token()) -> non_neg_integer()).
-spec(block/1 :: (token()) -> 'ok').
-spec(unblock/1 :: (token()) -> 'ok' | {'disabled', token()}).
-spec(is_blocked/1 :: (token()) -> boolean()).
-spec(is_consumer_blocked/2 :: (token(), rabbit_types:ctag()) -> boolean()).
-spec(credit/4 :: (token(), rabbit_types:ctag(), non_neg_integer(), boolean())
                  -> token()).
-spec(drained/1 :: (token())
                   -> {[{rabbit_types:ctag(), non_neg_integer()}], token()}).
-spec(forget_consumer/2 :: (token(), rabbit_types:ctag()) -> token()).
-spec(copy_queue_state/2 :: (token(), token()) -> token()).

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

-record(credit, {credit = 0, drain = false}).

%%----------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------

start_link() -> gen_server2:start_link(?MODULE, [], []).

make_token() -> make_token(undefined).
make_token(Pid) -> #token{pid     = Pid,
                          enabled = false,
                          credits = gb_trees:empty()}.

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
can_send(Token = #token{pid = Pid, enabled = Enabled, credits = Credits},
         QPid, AckReq, CTag) ->
    case is_consumer_blocked(Token, CTag) of
        false -> case not Enabled orelse call_can_send(Pid, QPid, AckReq) of
                     true  -> Token#token{
                                credits = record_send_q(CTag, Credits)};
                     false -> channel_blocked
                 end;
        true  -> consumer_blocked
    end.

call_can_send(Pid, QPid, AckRequired) ->
    rabbit_misc:with_exit_handler(
      fun () -> true end,
      fun () ->
              gen_server2:call(Pid, {can_send, QPid, AckRequired}, infinity)
      end).

%% Let the limiter know that the channel has received some acks from a
%% consumer
ack(Limiter, Count) -> maybe_cast(Limiter, {ack, Count}).

register(Limiter, QPid) -> maybe_cast(Limiter, {register, QPid}).

unregister(Limiter, QPid) -> maybe_cast(Limiter, {unregister, QPid}).

get_limit(Limiter) ->
    rabbit_misc:with_exit_handler(
      fun () -> 0 end,
      fun () -> maybe_call(Limiter, get_limit, 0) end).

block(Limiter) ->
    maybe_call(Limiter, block, ok).

unblock(Limiter) ->
    maybe_call(Limiter, {unblock, Limiter}, ok).

is_consumer_blocked(#token{credits = Credits}, CTag) ->
    case gb_trees:lookup(CTag, Credits) of
        {value, #credit{credit = C}} when C > 0 -> false;
        {value, #credit{}}                      -> true;
        none                                    -> false
    end.

is_blocked(Limiter) ->
    maybe_call(Limiter, is_blocked, false).

credit(Limiter = #token{credits = Credits}, CTag, Credit, Drain) ->
    Limiter#token{credits = update_credit(CTag, Credit, Drain, Credits)}.

drained(Limiter = #token{credits = Credits}) ->
    {CTagCredits, Credits2} =
        rabbit_misc:gb_trees_fold(
          fun (CTag,  #credit{credit = C,  drain = true},  {Acc, Creds0}) ->
                  {[{CTag, C} | Acc], update_credit(CTag, 0, false, Creds0)};
              (_CTag, #credit{credit = _C, drain = false}, {Acc, Creds0}) ->
                  {Acc, Creds0}
          end, {[], Credits}, Credits),
    {CTagCredits, Limiter#token{credits = Credits2}}.

forget_consumer(Limiter = #token{credits = Credits}, CTag) ->
    Limiter#token{credits = gb_trees:delete_any(CTag, Credits)}.

copy_queue_state(#token{credits = Credits}, Token) ->
    Token#token{credits = Credits}.

%%----------------------------------------------------------------------------
%% Queue-local code
%%----------------------------------------------------------------------------

%% We want to do all the AMQP 1.0-ish link level credit calculations
%% in the queue (to do them elsewhere introduces a ton of
%% races). However, it's a big chunk of code that is conceptually very
%% linked to the limiter concept. So we get the queue to hold a bit of
%% state for us (#token.credits), and maintain a fiction that the
%% limiter is making the decisions...

record_send_q(CTag, Credits) ->
    case gb_trees:lookup(CTag, Credits) of
        {value, #credit{credit = Credit, drain = Drain}} ->
            update_credit(CTag, Credit, Drain, Credits);
        none ->
            Credits
    end.

update_credit(CTag, Credit, Drain, Credits) ->
    %% Using up all credit implies no need to send a 'drained' event
    Drain1 = Drain andalso Credit > 0,
    gb_trees:enter(CTag, #credit{credit = Credit, drain = Drain1}, Credits).

%%----------------------------------------------------------------------------
%% gen_server callbacks
%%----------------------------------------------------------------------------

init([]) ->
    {ok, #lim{}}.

prioritise_call(get_limit, _From, _State) -> 9;
prioritise_call(_Msg,      _From, _State) -> 0.

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
    end;

handle_call(get_limit, _From, State = #lim{prefetch_count = PrefetchCount}) ->
    {reply, PrefetchCount, State};

handle_call({limit, PrefetchCount, Token}, _From, State) ->
    case maybe_notify(State, State#lim{prefetch_count = PrefetchCount}) of
        {cont, State1} ->
            {reply, ok, State1};
        {stop, State1} ->
            {reply, {disabled, Token#token{enabled = false}}, State1}
    end;

handle_call(block, _From, State) ->
    {reply, ok, State#lim{blocked = true}};

handle_call({unblock, Token}, _From, State) ->
    case maybe_notify(State, State#lim{blocked = false}) of
        {cont, State1} ->
            {reply, ok, State1};
        {stop, State1} ->
            {reply, {disabled, Token#token{enabled = false}}, State1}
    end;

handle_call(is_blocked, _From, State) ->
    {reply, blocked(State), State};

handle_call({enable, Token, Channel, Volume}, _From, State) ->
    {reply, Token#token{enabled = true},
     State#lim{ch_pid = Channel, volume = Volume}};
handle_call({disable, Token}, _From, State) ->
    {reply, Token#token{enabled = false}, State}.

handle_cast({ack, Count}, State = #lim{volume = Volume}) ->
    NewVolume = if Volume == 0 -> 0;
                   true        -> Volume - Count
                end,
    {cont, State1} = maybe_notify(State, State#lim{volume = NewVolume}),
    {noreply, State1};

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
        true  -> NewState1 = notify_queues(NewState),
                 {case NewState1#lim.prefetch_count of
                      0 -> stop;
                      _ -> cont
                  end, NewState1};
        false -> {cont, NewState}
    end.

maybe_call(#token{pid = Pid, enabled = true}, Call, _Default) ->
    gen_server2:call(Pid, Call, infinity);
maybe_call(_, _Call, Default) ->
    Default.

maybe_cast(#token{pid = Pid, enabled = true}, Cast) ->
    gen_server2:cast(Pid, Cast);
maybe_cast(_, _Call) ->
    ok.

limit_reached(#lim{prefetch_count = Limit, volume = Volume}) ->
    Limit =/= 0 andalso Volume >= Limit.

blocked(#lim{blocked = Blocked}) -> Blocked.

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
        1 -> ok = rabbit_amqqueue:unblock(hd(QList), ChPid); %% common case
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
