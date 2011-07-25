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
-export([start_link/0, make_new_token/1, is_enabled/1, enable/2, disable/1]).
-export_type([limiter_token/0]).
-export([limit/2, can_send/3, ack/2, register/2, unregister/2]).
-export([get_limit/1, block/1, unblock/1, is_blocked/1]).

%%----------------------------------------------------------------------------

-record(limiter_token, {pid, enabled = false}).

-ifdef(use_specs).

-type(maybe_token() :: limiter_token() | 'undefined').
-opaque(limiter_token() :: #'limiter_token'{}).

-spec(start_link/0 :: () -> rabbit_types:ok_pid_or_error()).
-spec(make_new_token/1 :: (pid()) -> limiter_token()).
-spec(is_enabled/1 :: (limiter_token()) -> boolean()).
-spec(enable/2 :: (limiter_token(), non_neg_integer()) -> limiter_token()).
-spec(disable/1 :: (limiter_token()) -> limiter_token()).
-spec(limit/2 :: (maybe_token(), non_neg_integer()) -> 'ok' | 'stopped').
-spec(can_send/3 :: (maybe_token(), pid(), boolean()) -> boolean()).
-spec(ack/2 :: (maybe_token(), non_neg_integer()) -> 'ok').
-spec(register/2 :: (maybe_token(), pid()) -> 'ok').
-spec(unregister/2 :: (maybe_token(), pid()) -> 'ok').
-spec(get_limit/1 :: (maybe_token()) -> non_neg_integer()).
-spec(block/1 :: (maybe_token()) -> 'ok').
-spec(unblock/1 :: (maybe_token()) -> 'ok' | 'stopped').
-spec(is_blocked/1 :: (maybe_token()) -> boolean()).

-endif.

%%----------------------------------------------------------------------------

-record(lim, {prefetch_count = 0,
              ch_pid,
              blocked = false,
              queues = dict:new(), % QPid -> {MonitorRef, Notify}
              volume = 0}).
%% 'Notify' is a boolean that indicates whether a queue should be
%% notified of a change in the limit or volume that may allow it to
%% deliver more messages via the limiter's channel.

%%----------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------

start_link() ->
    gen_server2:start_link(?MODULE, [], []).

make_new_token(Pid) ->
    #limiter_token{pid = Pid}.

is_enabled(#limiter_token{enabled = Enabled}) ->
    Enabled.

enable(#limiter_token{pid = Pid} = Token, Volume) ->
    gen_server2:call(Pid, {enable, Token, self(), Volume}).

disable(#limiter_token{pid = Pid} = Token) ->
    gen_server2:call(Pid, {disable, Token}).

limit(LimiterToken, PrefetchCount) ->
    maybe_call(LimiterToken, {limit, PrefetchCount, LimiterToken}, ok).

%% Ask the limiter whether the queue can deliver a message without
%% breaching a limit
can_send(undefined, _QPid, _AckRequired) ->
    true;
can_send(#limiter_token{enabled = false}, _QPid, _AckRequired) ->
    true;
can_send(LimiterToken, QPid, AckRequired) ->
    rabbit_misc:with_exit_handler(
      fun () -> true end,
      fun () ->
              maybe_call(LimiterToken, {can_send, QPid, AckRequired}, ok)
      end).

%% Let the limiter know that the channel has received some acks from a
%% consumer
ack(LimiterToken, Count) -> maybe_cast(LimiterToken, {ack, Count}).

register(LimiterToken, QPid) -> maybe_cast(LimiterToken, {register, QPid}).

unregister(LimiterToken, QPid) -> maybe_cast(LimiterToken,
                                             {unregister, QPid}).

get_limit(LimiterToken) ->
    rabbit_misc:with_exit_handler(
      fun () -> 0 end,
      fun () -> maybe_call(LimiterToken, get_limit, ok) end).

block(LimiterToken) ->
    maybe_call(LimiterToken, block, ok).

unblock(LimiterToken) ->
    maybe_call(LimiterToken, {unblock, LimiterToken}, ok).

is_blocked(LimiterToken) ->
    maybe_call(LimiterToken, is_blocked, false).

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
            {reply, {disabled, Token#limiter_token{enabled = false}}, State1}
    end;

handle_call(block, _From, State) ->
    {reply, ok, State#lim{blocked = true}};

handle_call({unblock, Token}, _From, State) ->
    case maybe_notify(State, State#lim{blocked = false}) of
        {cont, State1} ->
            {reply, ok, State1};
        {stop, State1} ->
            {reply, {disabled, Token#limiter_token{enabled = false}}, State1}
    end;

handle_call(is_blocked, _From, State) ->
    {reply, blocked(State), State};

handle_call({enable, Token, Channel, Volume}, _From, State) ->
    {reply, Token#limiter_token{enabled = true},
     State#lim{ch_pid = Channel, volume = Volume}};
handle_call({disable, Token}, _From, State) ->
    {reply, Token#limiter_token{enabled = false}, State}.

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
    State.

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

maybe_call(undefined, _Call, Default) ->
    Default;
maybe_call(#limiter_token{enabled = false}, _Call, Default) ->
    Default;
maybe_call(#limiter_token{pid = Pid}, Call, _Default) ->
    gen_server2:call(Pid, Call, infinity).

maybe_cast(undefined, _Call) ->
    ok;
maybe_cast(#limiter_token{enabled = false}, _Cast) ->
    ok;
maybe_cast(#limiter_token{pid = Pid}, Cast) ->
    gen_server2:cast(Pid, Cast).

limit_reached(#lim{prefetch_count = Limit, volume = Volume}) ->
    Limit =/= 0 andalso Volume >= Limit.

blocked(#lim{blocked = Blocked}) -> Blocked.

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
