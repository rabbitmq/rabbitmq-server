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
-export([limit/2, can_send/4, ack/2, register/2, unregister/2]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(maybe_pid() :: pid() | 'undefined').

-spec(start_link/1 :: (pid()) -> pid()).
-spec(shutdown/1 :: (maybe_pid()) -> 'ok').
-spec(limit/2 :: (maybe_pid(), non_neg_integer()) -> 'ok').
-spec(can_send/4 :: (maybe_pid(), pid(), boolean(), non_neg_integer()) ->
             boolean()).
-spec(ack/2 :: (maybe_pid(), non_neg_integer()) -> 'ok').
-spec(register/2 :: (maybe_pid(), pid()) -> 'ok').
-spec(unregister/2 :: (maybe_pid(), pid()) -> 'ok').

-endif.

%%----------------------------------------------------------------------------

-record(lim, {prefetch_count = 0,
              ch_pid,
              limited   = dict:new(), % QPid -> {MonitorRef, Length}
              unlimited = dict:new(), % QPid -> {MonitorRef, Length}
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
can_send(undefined, _QPid, _AckRequired, _Length) ->
    true;
can_send(LimiterPid, QPid, AckRequired, Length) ->
    rabbit_misc:with_exit_handler(
      fun () -> true end,
      fun () -> gen_server2:call(LimiterPid, {can_send, QPid, AckRequired,
                                              Length}, infinity) end).

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

handle_call({can_send, QPid, AckRequired, Length}, _From,
            State = #lim{volume = Volume}) ->
    case limit_reached(State) of
        true ->
            {reply, false, limit_queue(QPid, Length, State)};
        false ->
            {reply, true,
             update_length(QPid, Length,
                           State#lim{volume = if AckRequired -> Volume + 1;
                                                 true        -> Volume
                                              end})}
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

remember_queue(QPid, State = #lim{limited = Limited, unlimited = Unlimited}) ->
    case dict:is_key(QPid, Limited) orelse dict:is_key(QPid, Unlimited) of
        false -> MRef = erlang:monitor(process, QPid),
                 State#lim{unlimited = dict:store(QPid, {MRef, 0}, Unlimited)};
        true  -> State
    end.

forget_queue(QPid, State = #lim{ch_pid = ChPid, limited = Limited,
                                unlimited = Unlimited}) ->
    Limited1 = forget_queue(ChPid, QPid, Limited, true),
    Unlimited1 = forget_queue(ChPid, QPid, Unlimited, false),
    State#lim{limited = Limited1, unlimited = Unlimited1}.

forget_queue(ChPid, QPid, Queues, NeedsUnblocking) ->
    case dict:find(QPid, Queues) of
        {ok, {MRef, _}} ->
            true = erlang:demonitor(MRef),
            (not NeedsUnblocking) orelse unblock(async, QPid, ChPid),
            dict:erase(QPid, Queues);
        error ->
            Queues
    end.

limit_queue(QPid, Length, State = #lim{unlimited = Unlimited,
                                       limited = Limited}) ->
    {MRef, _} = case dict:find(QPid, Unlimited) of
                    error        -> dict:fetch(QPid, Limited);
                    {ok, Result} -> Result
                end,
    Unlimited1 = dict:erase(QPid, Unlimited),
    Limited1 = dict:store(QPid, {MRef, Length}, Limited),
    State#lim{unlimited = Unlimited1, limited = Limited1}.

%% knows that the queue is unlimited
update_length(QPid, Length, State = #lim{unlimited = Unlimited,
                                         limited = Limited}) ->
    UpdateFun = fun ({MRef, _}) -> {MRef, Length} end,
    case dict:is_key(QPid, Unlimited) of
        true  -> State#lim{unlimited = dict:update(QPid, UpdateFun, Unlimited)};
        false -> State#lim{limited = dict:update(QPid, UpdateFun, Limited)}
    end.

is_zero_num(0) -> 0;
is_zero_num(_) -> 1.

notify_queues(#lim{ch_pid = ChPid, limited = Limited, unlimited = Unlimited,
                   prefetch_count = PrefetchCount, volume = Volume} = State) ->
    Capacity = PrefetchCount - Volume,
    {QDict, LengthSum, NonZeroQCount} =
        dict:fold(fun (QPid, {_MRef, Length}, {Dict, Sum, NZQCount}) ->
                          Sum1 = Sum + lists:max([1, Length]),
                          {orddict:append(Length, QPid, Dict), Sum1,
                           NZQCount + is_zero_num(Length)}
                  end, {orddict:new(), 0, 0}, Limited),
    {Unlimited1, Limited1} =
        case orddict:size(QDict) of
            0 -> {Unlimited, Limited};
            QCount ->
                QTree = gb_trees:from_orddict(QDict),
                case Capacity >= NonZeroQCount of
                    true ->
                        unblock_all(ChPid, QCount, QTree, Unlimited, Limited);
                    false ->
                        %% try to tell enough queues that we guarantee
                        %% we'll get blocked again
                        {Capacity1, Unlimited2, Limited2} =
                            unblock_queues(
                              sync, ChPid, LengthSum, Capacity, QTree,
                              Unlimited, Limited),
                        case 0 == Capacity1 of
                            true ->
                                {Unlimited2, Limited2};
                            false -> %% just tell everyone
                                unblock_all(ChPid, QCount, QTree, Unlimited2,
                                            Limited2)
                        end
                end
        end,
    State#lim{unlimited = Unlimited1, limited = Limited1}.

unblock_all(ChPid, QCount, QTree, Unlimited, Limited) ->
    {_Capacity2, Unlimited1, Limited1} =
        unblock_queues(async, ChPid, 1, QCount, QTree, Unlimited, Limited),
    {Unlimited1, Limited1}.

unblock_queues(_Mode, _ChPid, _L, 0, _QList, Unlimited, Limited) ->
    {0, Unlimited, Limited};
unblock_queues(Mode, ChPid, L, QueueCount, QList, Unlimited, Limited) ->
    {Length, QPids, QList1} = gb_trees:take_largest(QList),
    unblock_queues(Mode, ChPid, L, QueueCount, QList1, Unlimited, Limited,
                   Length, QPids).

unblock_queues(Mode, ChPid, L, QueueCount, QList, Unlimited, Limited, Length,
               []) ->
    case gb_trees:is_empty(QList) of
        true  -> {QueueCount, Unlimited, Limited};
        false -> unblock_queues(Mode, ChPid, L - Length, QueueCount, QList,
                                Unlimited, Limited)
    end;
unblock_queues(Mode, ChPid, L, QueueCount, QList, Unlimited, Limited, Length,
               [QPid|QPids]) ->
    case dict:find(QPid, Limited) of
        error ->
            %% We're reusing the gb_tree in multiple calls to
            %% unblock_queues and so we may well be trying to unblock
            %% already-unblocked queues. Just recurse
            unblock_queues(Mode, ChPid, L, QueueCount, QList, Unlimited,
                           Limited, Length, QPids);
        {ok, Value = {_MRef, Length}} ->
            case Length == 0 andalso Mode == sync of
                true -> {QueueCount, Unlimited, Limited};
                false ->
                    {QueueCount1, Unlimited1, Limited1} =
                        case 1 >= L orelse Length >= random:uniform(L) of
                            true ->
                                case unblock(Mode, QPid, ChPid) of
                                    true ->
                                        {QueueCount - 1,
                                         dict:store(QPid, Value, Unlimited),
                                         dict:erase(QPid, Limited)};
                                    false -> {QueueCount, Unlimited, Limited}
                                end;
                            false -> {QueueCount, Unlimited, Limited}
                        end,
                    unblock_queues(Mode, ChPid, L - Length, QueueCount1, QList,
                                   Unlimited1, Limited1, Length, QPids)
            end
    end.

unblock(sync, QPid, ChPid) -> rabbit_amqqueue:unblock_sync(QPid, ChPid);
unblock(async, QPid, ChPid) -> rabbit_amqqueue:unblock_async(QPid, ChPid).
