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
-export([start_link/2]).
-export([limit/2, can_send/5, ack/2, register/2, unregister/2]).
-export([get_limit/1, block/1, unblock/1, set_credit/5, is_blocked/1]).

-import(rabbit_misc, [serial_add/2, serial_diff/2]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(maybe_pid() :: pid() | 'undefined').

-spec(start_link/2 :: (pid(), non_neg_integer()) ->
                           rabbit_types:ok_pid_or_error()).
-spec(limit/2 :: (maybe_pid(), non_neg_integer()) -> 'ok' | 'stopped').
-spec(can_send/5 :: (maybe_pid(), pid(), boolean(), binary(), non_neg_integer()) -> boolean()).
-spec(ack/2 :: (maybe_pid(), binary()) -> 'ok').
-spec(register/2 :: (maybe_pid(), pid()) -> 'ok').
-spec(unregister/2 :: (maybe_pid(), pid()) -> 'ok').
-spec(get_limit/1 :: (maybe_pid()) -> non_neg_integer()).
-spec(block/1 :: (maybe_pid()) -> 'ok').
-spec(unblock/1 :: (maybe_pid()) -> 'ok' | 'stopped').
-spec(is_blocked/1 :: (maybe_pid()) -> boolean()).

-endif.

%%----------------------------------------------------------------------------

-record(lim, {prefetch_count = 0,
              ch_pid,
              blocked = false,
              credits = dict:new(),
              queues = dict:new(), % QPid -> {MonitorRef, Notify}
              volume = 0}).

-record(credit, {count = 0, credit = 0, drain = false}).

%% 'Notify' is a boolean that indicates whether a queue should be
%% notified of a change in the limit or volume that may allow it to
%% deliver more messages via the limiter's channel.

%%----------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------

start_link(ChPid, UnackedMsgCount) ->
    gen_server2:start_link(?MODULE, [ChPid, UnackedMsgCount], []).

limit(undefined, 0) ->
    ok;
limit(LimiterPid, PrefetchCount) ->
    gen_server2:call(LimiterPid, {limit, PrefetchCount}, infinity).

%% Ask the limiter whether the queue can deliver a message without
%% breaching a limit
can_send(undefined, _QPid, _AckRequired, _CTag, _Len) ->
    true;
can_send(LimiterPid, QPid, AckRequired, CTag, Len) ->
    rabbit_misc:with_exit_handler(
      fun () -> true end,
      fun () -> gen_server2:call(LimiterPid,
                                 {can_send, QPid, AckRequired, CTag, Len},
                                 infinity) end).

%% Let the limiter know that the channel has received some acks from a
%% consumer
ack(undefined, _CTag) -> ok;
ack(LimiterPid, CTag) -> gen_server2:cast(LimiterPid, {ack, CTag}).

register(undefined, _QPid) -> ok;
register(LimiterPid, QPid) -> gen_server2:cast(LimiterPid, {register, QPid}).

unregister(undefined, _QPid) -> ok;
unregister(LimiterPid, QPid) -> gen_server2:cast(LimiterPid, {unregister, QPid}).

get_limit(undefined) ->
    0;
get_limit(Pid) ->
    rabbit_misc:with_exit_handler(
      fun () -> 0 end,
      fun () -> gen_server2:call(Pid, get_limit, infinity) end).

block(undefined) ->
    ok;
block(LimiterPid) ->
    gen_server2:call(LimiterPid, block, infinity).

unblock(undefined) ->
    ok;
unblock(LimiterPid) ->
    gen_server2:call(LimiterPid, unblock, infinity).

set_credit(undefined, _, _, _, _) ->
    ok;
set_credit(LimiterPid, CTag, Credit, Count, Drain) ->
    gen_server2:call(LimiterPid, {set_credit, CTag, Credit, Count, Drain}, infinity).

is_blocked(undefined) ->
    false;
is_blocked(LimiterPid) ->
    gen_server2:call(LimiterPid, is_blocked, infinity).

%%----------------------------------------------------------------------------
%% gen_server callbacks
%%----------------------------------------------------------------------------

init([ChPid, UnackedMsgCount]) ->
    {ok, #lim{ch_pid = ChPid, volume = UnackedMsgCount}}.

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

handle_call({limit, PrefetchCount}, _From, State) ->
    case maybe_notify(irrelevant,
                      State, State#lim{prefetch_count = PrefetchCount}) of
        {cont, State1} -> {reply, ok, State1};
        {stop, State1} -> {stop, normal, stopped, State1}
    end;

handle_call(block, _From, State) ->
    {reply, ok, State#lim{blocked = true}};

handle_call(unblock, _From, State) ->
    maybe_notify_reply(irrelevant, State, State#lim{blocked = false});

handle_call({set_credit, CTag, Credit, Count, Drain}, _From, State) ->
    maybe_notify_reply(CTag, State, reset_credit(CTag, Credit, Count, Drain, State));

handle_call(is_blocked, _From, State) ->
    {reply, blocked(State), State}.

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

maybe_notify_reply(CTag, OldState, NewState) ->
    case maybe_notify(CTag, OldState, NewState) of
        {cont, State} -> {reply, ok, State};
        {stop, State} -> {stop, normal, stopped, State}
    end.

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
