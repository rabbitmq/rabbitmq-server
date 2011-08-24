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
-include("rabbit_framing.hrl").

-behaviour(gen_server2).

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2, prioritise_call/3]).

-export([start_link/0, make_token/0, make_token/1, is_enabled/1, enable/2,
         disable/1]).
-export([limit/2, can_send/5, ack/2, register/2, unregister/2]).
-export([get_limit/1, block/1, unblock/1, is_blocked/1]).
-export([set_credit/5]).

-import(rabbit_misc, [serial_add/2, serial_diff/2]).

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
-spec(can_send/3 :: (token(), pid(), boolean(), ) -> boolean()).
-spec(ack/2 :: (token(), non_neg_integer()) -> 'ok').
-spec(register/2 :: (token(), pid()) -> 'ok').
-spec(unregister/2 :: (token(), pid()) -> 'ok').
-spec(get_limit/1 :: (token()) -> non_neg_integer()).
-spec(block/1 :: (token()) -> 'ok').
-spec(unblock/1 :: (token()) -> 'ok' | {'disabled', token()}).
-spec(is_blocked/1 :: (token()) -> boolean()).
%% Missing: set_credit

-endif.

%%----------------------------------------------------------------------------

-record(lim, {prefetch_count = 0,
              ch_pid,
              blocked = false,
              credits = dict:new(),
              queues = orddict:new(), % QPid -> {MonitorRef, Notify}
              volume = 0}).

-record(credit, {count = 0, credit = 0, drain = false}).

%%----------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------

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
can_send(#token{pid = Pid, enabled = true}, QPid, AckRequired, CTag, Len) ->
    rabbit_misc:with_exit_handler(
      fun () -> true end,
      fun () ->
              gen_server2:call(Pid, {can_send, QPid, AckRequired, CTag, Len},
                               infinity)
      end);
can_send(_, _, _, _, _) ->
    true.

%% Let the limiter know that the channel has received some acks from a
%% consumer
ack(Limiter, CTag) -> maybe_cast(Limiter, {ack, CTag}).

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

set_credit(Limiter, CTag, Credit, Count, Drain) ->
    maybe_call(Limiter, {set_credit, CTag, Credit, Count, Drain, Limiter}, ok).

is_blocked(Limiter) ->
    maybe_call(Limiter, is_blocked, false).

%%----------------------------------------------------------------------------
%% gen_server callbacks
%%----------------------------------------------------------------------------

init([]) ->
    {ok, #lim{}}.

prioritise_call(get_limit, _From, _State) -> 9;
prioritise_call(_Msg,      _From, _State) -> 0.

handle_call({can_send, QPid, _AckRequired, _CTag, _Len}, _From,
            State = #lim{blocked = true}) ->
    {reply, false, limit_queue(QPid, State)};
handle_call({can_send, QPid, AckRequired, CTag, Len}, _From,
            State = #lim{volume = Volume}) ->
    case limit_reached(CTag, State) of
        true  -> {reply, false, limit_queue(QPid, State)};
        false -> {reply, true,
                  decr_credit(CTag, Len,
                              State#lim{volume = if AckRequired -> Volume + 1;
                                                    true        -> Volume
                                                 end})}
    end;

handle_call(get_limit, _From, State = #lim{prefetch_count = PrefetchCount}) ->
    {reply, PrefetchCount, State};

handle_call({limit, PrefetchCount, Token}, _From, State) ->
    case maybe_notify(irrelevant,
                      State, State#lim{prefetch_count = PrefetchCount}) of
        {cont, State1} ->
            {reply, ok, State1};
        {stop, State1} ->
            {reply, {disabled, Token#token{enabled = false}}, State1}
    end;

handle_call(block, _From, State) ->
    {reply, ok, State#lim{blocked = true}};

handle_call({set_credit, CTag, Credit, Count, Drain, Token}, _From, State) ->
    case maybe_notify(CTag, State,
                      reset_credit(CTag, Credit, Count, Drain, State)) of
        {cont, State1} ->
            {reply, ok, State1};
        {stop, State1} ->
            {reply, {disabled, Token#token{enabled = false}}, State1}
    end;

handle_call({unblock, Token}, _From, State) ->
    case maybe_notify(irrelevant, State, State#lim{blocked = false}) of
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

handle_cast({ack, CTag}, State = #lim{volume = Volume}) ->
    NewVolume = if Volume == 0 -> 0;
                   true        -> Volume - 1
                end,
    {cont, State1} = maybe_notify(CTag, State, State#lim{volume = NewVolume}),
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

maybe_notify(CTag, OldState, NewState) ->
    case (limit_reached(CTag, OldState) orelse blocked(OldState)) andalso
        not (limit_reached(CTag, NewState) orelse blocked(NewState)) of
        true  -> NewState1 = notify_queues(NewState),
                 {case {NewState1#lim.prefetch_count,
                        dict:size(NewState1#lim.credits)} of
                      {0, 0} -> stop;
                      _      -> cont
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

limit_reached(irrelevant, _) ->
    false;
limit_reached(CTag, #lim{prefetch_count = Limit, volume = Volume,
                         credits = Credits}) ->
    case dict:find(CTag, Credits) of
        {ok, #credit{ credit = 0 }} -> true;
        _                           -> false
    end orelse (Limit =/= 0 andalso Volume >= Limit).

decr_credit(CTag, Len, State = #lim{ credits = Credits,
                                     ch_pid = ChPid } ) ->
    case dict:find(CTag, Credits) of
        {ok, #credit{ credit = Credit, count = Count, drain = Drain }} ->
            {NewCredit, NewCount} =
                case {Credit, Len, Drain} of
                    {1, _, _}    -> {0, serial_add(Count, 1)};
                    {_, 1, true} ->
                        %% Drain, so advance til credit = 0
                        NewCount0 = serial_add(Count, (Credit - 1)),
                        send_drained(ChPid, CTag, NewCount0),
                        {0, NewCount0}; %% Magic reduction to 0
                    {_, _, _}    -> {Credit - 1, serial_add(Count, 1)}
                end,
            update_credit(CTag, NewCredit, NewCount, Drain, State);
        error ->
            State
    end.

send_drained(ChPid, CTag, Count) ->
    rabbit_channel:send_command(ChPid,
                                #'basic.credit_state'{consumer_tag = CTag,
                                                      credit       = 0,
                                                      count        = Count,
                                                      available    = 0,
                                                      drain        = true}).

%% Assert the credit state.  The count may not match ours, in which
%% case we must rebase the credit.
%% TODO Edge case: if the queue has nothing in it, and drain is set,
%% we want to send a basic.credit back.
reset_credit(CTag, Credit0, Count0, Drain, State = #lim{credits = Credits}) ->
    Count =
        case dict:find(CTag, Credits) of
            {ok, #credit{ count = LocalCount }} ->
                LocalCount;
            _ -> Count0
        end,
    %% Our credit may have been reduced while messages are in flight,
    %% so we bottom out at 0.
    Credit = erlang:max(0, serial_diff(serial_add(Count0, Credit0), Count)),
    update_credit(CTag, Credit, Count, Drain, State).

%% Store the credit
update_credit(CTag, -1, _Count, _Drain, State = #lim{credits = Credits}) ->
    State#lim{credits = dict:erase(CTag, Credits)};

update_credit(CTag, Credit, Count, Drain, State = #lim{credits = Credits}) ->
    State#lim{credits = dict:store(CTag,
                                   #credit{credit = Credit,
                                           count = Count,
                                           drain = Drain}, Credits)}.

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
