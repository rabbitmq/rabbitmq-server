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

-module(rabbit_amqqueue_process).
-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-behaviour(gen_server2).

-define(UNSENT_MESSAGE_LIMIT, 100).
-define(HIBERNATE_AFTER, 1000).

-export([start_link/1]).

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2, handle_info/2]).

-import(queue).
-import(erlang).
-import(lists).

% Queue's state
-record(q, {q,
            owner,
            exclusive_consumer,
            has_had_consumers,
            mixed_state,
            next_msg_id,
            round_robin}).

-record(consumer, {tag, ack_required}).

-record(tx, {ch_pid, is_persistent, pending_messages, pending_acks}).

%% These are held in our process dictionary
-record(cr, {consumers,
             ch_pid,
             limiter_pid,
             monitor_ref,
             unacked_messages,
             is_limit_active,
             txn,
             unsent_message_count}).

-define(INFO_KEYS,
        [name,
         durable,
         auto_delete,
         arguments,
         pid,
         messages_ready,
         messages_unacknowledged,
         messages_uncommitted,
         messages,
         acks_uncommitted,
         consumers,
         transactions,
         memory]).
         
%%----------------------------------------------------------------------------

start_link(Q) ->
    gen_server2:start_link(?MODULE, Q, []).

%%----------------------------------------------------------------------------

init(Q = #amqqueue { name = QName, durable = Durable }) ->
    ?LOGDEBUG("Queue starting - ~p~n", [Q]),
    {ok, MS} = rabbit_mixed_queue:start_link(QName, Durable, mixed), %% TODO, CHANGE ME
    {ok, #q{q = Q,
            owner = none,
            exclusive_consumer = none,
            has_had_consumers = false,
            mixed_state = MS,
            next_msg_id = 1,
            round_robin = queue:new()}, ?HIBERNATE_AFTER}.

terminate(_Reason, State) ->
    %% FIXME: How do we cancel active subscriptions?
    QName = qname(State),
    NewState =
        lists:foldl(fun (Txn, State1) ->
                            rollback_transaction(Txn, State1)
                    end, State, all_tx()),
    rabbit_mixed_queue:delete_queue(NewState #q.mixed_state),
    ok = rabbit_amqqueue:internal_delete(QName).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

reply(Reply, NewState) -> {reply, Reply, NewState, ?HIBERNATE_AFTER}.

noreply(NewState) -> {noreply, NewState, ?HIBERNATE_AFTER}.

lookup_ch(ChPid) ->
    case get({ch, ChPid}) of
        undefined -> not_found;
        C         -> C
    end.

ch_record(ChPid) ->
    Key = {ch, ChPid},
    case get(Key) of
        undefined ->
            MonitorRef = erlang:monitor(process, ChPid),
            C = #cr{consumers = [],
                    ch_pid = ChPid,
                    monitor_ref = MonitorRef,
                    unacked_messages = dict:new(),
                    is_limit_active = false,
                    txn = none,
                    unsent_message_count = 0},
            put(Key, C),
            C;
        C = #cr{} -> C
    end.

store_ch_record(C = #cr{ch_pid = ChPid}) ->
    put({ch, ChPid}, C).

all_ch_record() ->
    [C || {{ch, _}, C} <- get()].

is_ch_blocked(#cr{unsent_message_count = Count, is_limit_active = Limited}) ->
    Limited orelse Count > ?UNSENT_MESSAGE_LIMIT.

ch_record_state_transition(OldCR, NewCR) ->
    BlockedOld = is_ch_blocked(OldCR),
    BlockedNew = is_ch_blocked(NewCR),
    if BlockedOld andalso not(BlockedNew) -> unblock;
       BlockedNew andalso not(BlockedOld) -> block;
       true                               -> ok
    end.

record_current_channel_tx(ChPid, Txn) ->
    %% as a side effect this also starts monitoring the channel (if
    %% that wasn't happening already)
    store_ch_record((ch_record(ChPid))#cr{txn = Txn}).
    
deliver_queue(Fun, FunAcc0,
              State = #q{q = #amqqueue{name = QName},
                         round_robin = RoundRobin,
                         next_msg_id = NextId}) ->
    case queue:out(RoundRobin) of
        {{value, QEntry = {ChPid, #consumer{tag = ConsumerTag,
                                            ack_required = AckRequired}}},
         RoundRobinTail} ->
            C = #cr{limiter_pid = LimiterPid,
                    unsent_message_count = Count,
                    unacked_messages = UAM} = ch_record(ChPid),
            IsMsgReady = Fun(is_message_ready, FunAcc0, State),
            case IsMsgReady
                andalso
                ( (not AckRequired)
                  orelse
                  rabbit_limiter:can_send( LimiterPid, self() )
                ) of
                true ->
                    case Fun(AckRequired, FunAcc0, State) of
                        {empty, FunAcc1, State2} ->
                            {FunAcc1, State2};
                        {{Msg, IsDelivered, AckTag, Remaining}, FunAcc1, State2} ->
                            rabbit_channel:deliver(
                              ChPid, ConsumerTag, AckRequired,
                              {QName, self(), NextId, IsDelivered, Msg}),
                            NewUAM = case AckRequired of
                                         true  -> dict:store(NextId, {Msg, AckTag}, UAM);
                                         false -> UAM
                                     end,
                            NewC = C#cr{unsent_message_count = Count + 1,
                                        unacked_messages = NewUAM},
                            store_ch_record(NewC),
                            NewConsumers =
                                case ch_record_state_transition(C, NewC) of
                                    ok    -> queue:in(QEntry, RoundRobinTail);
                                    block -> block_consumers(ChPid, RoundRobinTail)
                                end,
                            State3 = State2 #q { round_robin = NewConsumers,
                                                 next_msg_id = NextId + 1
                                                },
                            if Remaining == 0 -> {FunAcc1, State3};
                               true -> deliver_queue(Fun, FunAcc1, State3)
                            end
                    end;
                %% if IsMsgReady then (AckRequired and we've hit the limiter)
                false when IsMsgReady ->
                    store_ch_record(C#cr{is_limit_active = true}),
                    NewConsumers = block_consumers(ChPid, RoundRobinTail),
                    deliver_queue(Fun, FunAcc0, State #q { round_robin = NewConsumers });
                false ->
                    %% no message was ready, so we don't need to block anyone
                    {FunAcc0, State}
            end;
        {empty, _} ->
            {FunAcc0, State}
    end.

deliver_from_queue(is_message_ready, undefined, #q { mixed_state = MS }) ->
    0 /= rabbit_mixed_queue:length(MS);
deliver_from_queue(AckRequired, Acc = undefined, State = #q { mixed_state = MS }) ->
    {Res, MS2} = rabbit_mixed_queue:deliver(MS),
    MS3 = case {Res, AckRequired} of
              {_, true} -> MS2;
              {empty, _} -> MS2;
              {{_Msg, _IsDelivered, AckTag, _Remaining}, false} ->
                  {ok, MS4} = rabbit_mixed_queue:ack([AckTag], MS2),
                  MS4
          end,
    {Res, Acc, State #q { mixed_state = MS3 }}.

run_message_queue(State) ->
    {undefined, State2} = deliver_queue(fun deliver_from_queue/3, undefined, State),
    State2.

attempt_immediate_delivery(none, _ChPid, Msg, State) ->
    Fun =
        fun (is_message_ready, false, _State) ->
                true;
            (AckRequired, false, State2) ->
                {AckTag, State3} =
                    if AckRequired ->
                            {ok, AckTag2, MS} = rabbit_mixed_queue:publish_delivered(Msg,
                                                                                     State2 #q.mixed_state),
                            {AckTag2, State2 #q { mixed_state = MS }};
                       true ->
                            {noack, State2}
                    end,
                {{Msg, false, AckTag, 0}, true, State3}
        end,
    deliver_queue(Fun, false, State);
attempt_immediate_delivery(Txn, ChPid, Msg, State) ->
    {ok, MS} = rabbit_mixed_queue:tx_publish(Msg, State #q.mixed_state),
    record_pending_message(Txn, ChPid, Msg),
    {true, State #q { mixed_state = MS }}.

deliver_or_enqueue(Txn, ChPid, Msg, State) ->
    case attempt_immediate_delivery(Txn, ChPid, Msg, State) of
        {true, NewState} ->
            {true, NewState};
        {false, NewState} ->
            %% Txn is none and no unblocked channels with consumers
            {ok, MS} = rabbit_mixed_queue:publish(Msg, State #q.mixed_state),
            {false, NewState #q { mixed_state = MS }}
    end.

%% all these messages have already been delivered at least once and
%% not ack'd, but need to be either redelivered or requeued
deliver_or_requeue_n([], State) ->
    run_message_queue(State);
deliver_or_requeue_n(MsgsWithAcks, State) ->
    {{_RemainingLengthMinusOne, AutoAcks, OutstandingMsgs}, NewState} =
        deliver_queue(fun deliver_or_requeue_msgs/3, {length(MsgsWithAcks) - 1, [], MsgsWithAcks}, State),
    {ok, MS} = rabbit_mixed_queue:ack(lists:reverse(AutoAcks), NewState #q.mixed_state),
    case OutstandingMsgs of
        [] -> run_message_queue(NewState #q { mixed_state = MS });
        _ -> {ok, MS2} = rabbit_mixed_queue:requeue(OutstandingMsgs, MS),
             NewState #q { mixed_state = MS2 }
    end.

deliver_or_requeue_msgs(is_message_ready, {Len, _AcksAcc, _MsgsWithAcks}, _State) ->
    -1 < Len;
deliver_or_requeue_msgs(false, {Len, AcksAcc, [{Msg, AckTag} | MsgsWithAcks]}, State) ->
    {{Msg, true, noack, Len}, {Len - 1, [AckTag|AcksAcc], MsgsWithAcks}, State};
deliver_or_requeue_msgs(true, {Len, AcksAcc, [{Msg, AckTag} | MsgsWithAcks]}, State) ->
    {{Msg, true, AckTag, Len}, {Len - 1, AcksAcc, MsgsWithAcks}, State}.

block_consumers(ChPid, RoundRobin) ->
    %%?LOGDEBUG("~p Blocking ~p from ~p~n", [self(), ChPid, queue:to_list(RoundRobin)]),
    queue:from_list(lists:filter(fun ({CP, _}) -> CP /= ChPid end,
                                 queue:to_list(RoundRobin))).

unblock_consumers(ChPid, Consumers, RoundRobin) ->
    %%?LOGDEBUG("Unblocking ~p ~p ~p~n", [ChPid, Consumers, queue:to_list(RoundRobin)]),
    queue:join(RoundRobin,
               queue:from_list([{ChPid, Con} || Con <- Consumers])).

block_consumer(ChPid, ConsumerTag, RoundRobin) ->
    %%?LOGDEBUG("~p Blocking ~p from ~p~n", [self(), ConsumerTag, queue:to_list(RoundRobin)]),
    queue:from_list(lists:filter(
                      fun ({CP, #consumer{tag = CT}}) ->
                              (CP /= ChPid) or (CT /= ConsumerTag)
                      end, queue:to_list(RoundRobin))).

possibly_unblock(State, ChPid, Update) ->
    case lookup_ch(ChPid) of
        not_found ->
            State;
        C ->
            NewC = Update(C),
            store_ch_record(NewC),
            case ch_record_state_transition(C, NewC) of
                ok      -> State;
                unblock -> NewRR = unblock_consumers(ChPid,
                                                     NewC#cr.consumers,
                                                     State#q.round_robin),
                           run_message_queue(State#q{round_robin = NewRR})
            end
    end.
    
check_auto_delete(State = #q{q = #amqqueue{auto_delete = false}}) ->
    {continue, State};
check_auto_delete(State = #q{has_had_consumers = false}) ->
    {continue, State};
check_auto_delete(State = #q{round_robin = RoundRobin}) ->
    % The clauses above rule out cases where no-one has consumed from
    % this queue yet, and cases where we are not an auto_delete queue
    % in any case. Thus it remains to check whether we have any active
    % listeners at this point.
    case queue:is_empty(RoundRobin) of
        true ->
            % There are no waiting listeners. It's possible that we're
            % completely unused. Check.
            case is_unused() of
                true ->
                    % There are no active consumers at this
                    % point. This is the signal to autodelete.
                    {stop, State};
                false ->
                    % There is at least one active consumer, so we
                    % shouldn't delete ourselves.
                    {continue, State}
            end;
        false ->
            % There are some waiting listeners, thus we are not
            % unused, so can continue life as normal without needing
            % to check the process dictionary.
            {continue, State}
    end.

handle_ch_down(DownPid, State = #q{exclusive_consumer = Holder,
                                   round_robin = ActiveConsumers}) ->
    case lookup_ch(DownPid) of
        not_found -> noreply(State);
        #cr{monitor_ref = MonitorRef, ch_pid = ChPid, txn = Txn,
            unacked_messages = UAM} ->
            NewActive = block_consumers(ChPid, ActiveConsumers),
            erlang:demonitor(MonitorRef),
            erase({ch, ChPid}),
            State1 =
                case Txn of
                    none -> State;
                    _    -> rollback_transaction(Txn, State)
                end,
            case check_auto_delete(
                   deliver_or_requeue_n(
                     [MsgWithAck ||
                         {_MsgId, MsgWithAck} <- dict:to_list(UAM)],
                     State1 # q {
                       exclusive_consumer = case Holder of
                                                {ChPid, _} -> none;
                                                Other -> Other
                                            end,
                       round_robin = NewActive})) of
                {continue, State2} ->
                    noreply(State2);
                {stop, State2} ->
                    {stop, normal, State2}
            end
    end.

cancel_holder(ChPid, ConsumerTag, {ChPid, ConsumerTag}) ->
    none;
cancel_holder(_ChPid, _ConsumerTag, Holder) ->
    Holder.

check_queue_owner(none,           _)         -> ok;
check_queue_owner({ReaderPid, _}, ReaderPid) -> ok;
check_queue_owner({_,         _}, _)         -> mismatch.

check_exclusive_access({_ChPid, _ConsumerTag}, _ExclusiveConsume) ->
    in_use;
check_exclusive_access(none, false) ->
    ok;
check_exclusive_access(none, true) ->
    case is_unused() of
        true  -> ok;
        false -> in_use
    end.

is_unused() ->
    is_unused1(get()).

is_unused1([]) ->
    true;
is_unused1([{{ch, _}, #cr{consumers = Consumers}} | _Rest])
  when Consumers /= [] ->
    false;
is_unused1([_ | Rest]) ->
    is_unused1(Rest).

maybe_send_reply(_ChPid, undefined) -> ok;
maybe_send_reply(ChPid, Msg) -> ok = rabbit_channel:send_command(ChPid, Msg).

qname(#q{q = #amqqueue{name = QName}}) -> QName.

lookup_tx(Txn) ->
    case get({txn, Txn}) of
        undefined -> #tx{ch_pid = none,
                         is_persistent = false,
                         pending_messages = [],
                         pending_acks = []};
        V -> V
    end.

store_tx(Txn, Tx) ->
    put({txn, Txn}, Tx).

erase_tx(Txn) ->
    erase({txn, Txn}).

all_tx_record() ->
    [T || {{txn, _}, T} <- get()].

all_tx() ->
    [Txn || {{txn, Txn}, _} <- get()].

record_pending_message(Txn, ChPid, Message = #basic_message { is_persistent = IsPersistent }) ->
    Tx = #tx{pending_messages = Pending, is_persistent = IsPersistentTxn } = lookup_tx(Txn),
    record_current_channel_tx(ChPid, Txn),
    store_tx(Txn, Tx #tx { pending_messages = [Message | Pending],
                           is_persistent = IsPersistentTxn orelse IsPersistent
                         }).

record_pending_acks(Txn, ChPid, MsgIds) ->
    Tx = #tx{pending_acks = Pending} = lookup_tx(Txn),
    record_current_channel_tx(ChPid, Txn),
    store_tx(Txn, Tx#tx{pending_acks = [MsgIds | Pending],
                        ch_pid = ChPid}).

commit_transaction(Txn, State) ->
    #tx { ch_pid = ChPid,
          pending_messages = PendingMessages,
          pending_acks = PendingAcks
        } = lookup_tx(Txn),
    PendingMessagesOrdered = lists:reverse(PendingMessages),
    PendingAcksOrdered = lists:append(lists:reverse(PendingAcks)),
    {ok, MS} =
        case lookup_ch(ChPid) of
            not_found ->
                rabbit_mixed_queue:tx_commit(
                  PendingMessagesOrdered, [], State #q.mixed_state);
            C = #cr { unacked_messages = UAM } ->
                {MsgWithAcks, Remaining} =
                    collect_messages(PendingAcksOrdered, UAM),
                store_ch_record(C#cr{unacked_messages = Remaining}),
                rabbit_mixed_queue:tx_commit(
                  PendingMessagesOrdered,
                  lists:map(fun ({_Msg, AckTag}) -> AckTag end, MsgWithAcks),
                  State #q.mixed_state)
        end,
    State #q { mixed_state = MS }.

rollback_transaction(Txn, State) ->
    #tx { pending_messages = PendingMessages
        } = lookup_tx(Txn),
    {ok, MS} = rabbit_mixed_queue:tx_cancel(lists:reverse(PendingMessages), State #q.mixed_state),
    erase_tx(Txn),
    State #q { mixed_state = MS }.

%% {A, B} = collect_messages(C, D) %% A = C `intersect` D; B = D \\ C
%% err, A = C `intersect` D , via projection through the dict that is C
collect_messages(MsgIds, UAM) ->
    lists:mapfoldl(
      fun (MsgId, D) -> {dict:fetch(MsgId, D), dict:erase(MsgId, D)} end,
      UAM, MsgIds).

infos(Items, State) -> [{Item, i(Item, State)} || Item <- Items].

i(name,        #q{q = #amqqueue{name        = Name}})       -> Name;
i(durable,     #q{q = #amqqueue{durable     = Durable}})    -> Durable;
i(auto_delete, #q{q = #amqqueue{auto_delete = AutoDelete}}) -> AutoDelete;
i(arguments,   #q{q = #amqqueue{arguments   = Arguments}})  -> Arguments;
i(pid, _) ->
    self();
i(messages_ready, #q { mixed_state = MS }) ->
    rabbit_mixed_queue:length(MS);
i(messages_unacknowledged, _) ->
    lists:sum([dict:size(UAM) ||
                  #cr{unacked_messages = UAM} <- all_ch_record()]);
i(messages_uncommitted, _) ->
    lists:sum([length(Pending) ||
                  #tx{pending_messages = Pending} <- all_tx_record()]);
i(messages, State) ->
    lists:sum([i(Item, State) || Item <- [messages_ready,
                                          messages_unacknowledged,
                                          messages_uncommitted]]);
i(acks_uncommitted, _) ->
    lists:sum([length(Pending) ||
                  #tx{pending_acks = Pending} <- all_tx_record()]);
i(consumers, _) ->
    lists:sum([length(Consumers) ||
                  #cr{consumers = Consumers} <- all_ch_record()]);
i(transactions, _) ->
    length(all_tx_record());
i(memory, _) ->
    {memory, M} = process_info(self(), memory),
    M;
i(Item, _) ->
    throw({bad_argument, Item}).

%---------------------------------------------------------------------------

handle_call(info, _From, State) ->
    reply(infos(?INFO_KEYS, State), State);

handle_call({info, Items}, _From, State) ->
    try
        reply({ok, infos(Items, State)}, State)
    catch Error -> reply({error, Error}, State)
    end;

handle_call({deliver_immediately, Txn, Message, ChPid}, _From, State) ->
    %% Synchronous, "immediate" delivery mode
    %%
    %% FIXME: Is this correct semantics?
    %%
    %% I'm worried in particular about the case where an exchange has
    %% two queues against a particular routing key, and a message is
    %% sent in immediate mode through the binding. In non-immediate
    %% mode, both queues get the message, saving it for later if
    %% there's noone ready to receive it just now. In immediate mode,
    %% should both queues still get the message, somehow, or should
    %% just all ready-to-consume queues get the message, with unready
    %% queues discarding the message?
    %%
    {Delivered, NewState} = attempt_immediate_delivery(Txn, ChPid, Message, State),
    reply(Delivered, NewState);

handle_call({deliver, Txn, Message, ChPid}, _From, State) ->
    %% Synchronous, "mandatory" delivery mode
    {Delivered, NewState} = deliver_or_enqueue(Txn, ChPid, Message, State),
    reply(Delivered, NewState);

handle_call({commit, Txn}, From, State) ->
    NewState = commit_transaction(Txn, State),
    %% optimisation: we reply straight away so the sender can continue
    gen_server2:reply(From, ok),
    erase_tx(Txn),
    noreply(run_message_queue(NewState));

handle_call({notify_down, ChPid}, From, State) ->
    %% optimisation: we reply straight away so the sender can continue
    gen_server2:reply(From, ok),
    handle_ch_down(ChPid, State);

handle_call({basic_get, ChPid, NoAck}, _From,
            State = #q{q = #amqqueue{name = QName},
                       next_msg_id = NextId,
                       mixed_state = MS
                       }) ->
    case rabbit_mixed_queue:deliver(MS) of
        {empty, MS2} -> reply(empty, State #q { mixed_state = MS2 });
        {{Msg, IsDelivered, AckTag, Remaining}, MS2} ->
            AckRequired = not(NoAck),
            {ok, MS3} =
                if AckRequired ->
                        C = #cr{unacked_messages = UAM} = ch_record(ChPid),
                        NewUAM = dict:store(NextId, {Msg, AckTag}, UAM),
                        store_ch_record(C#cr{unacked_messages = NewUAM}),
                        {ok, MS2};
                   true ->
                        rabbit_mixed_queue:ack([AckTag], MS2)
                end,
            Message = {QName, self(), NextId, IsDelivered, Msg},
            reply({ok, Remaining, Message},
                  State #q { next_msg_id = NextId + 1,
                             mixed_state = MS3
                           })
    end;

handle_call({basic_consume, NoAck, ReaderPid, ChPid, LimiterPid,
             ConsumerTag, ExclusiveConsume, OkMsg},
            _From, State = #q{owner = Owner,
                              exclusive_consumer = ExistingHolder,
                              round_robin = RoundRobin}) ->
    case check_queue_owner(Owner, ReaderPid) of
        mismatch ->
            reply({error, queue_owned_by_another_connection}, State);
        ok ->
            case check_exclusive_access(ExistingHolder, ExclusiveConsume) of
                in_use ->
                    reply({error, exclusive_consume_unavailable}, State);
                ok ->
                    C = #cr{consumers = Consumers} = ch_record(ChPid),
                    Consumer = #consumer{tag = ConsumerTag, ack_required = not(NoAck)},
                    store_ch_record(C#cr{consumers = [Consumer | Consumers],
                                         limiter_pid = LimiterPid}),
                    if Consumers == [] ->
                            ok = rabbit_limiter:register(LimiterPid, self());
                       true ->
                            ok
                    end,
                    State1 = State#q{has_had_consumers = true,
                                     exclusive_consumer =
                                       if
                                           ExclusiveConsume -> {ChPid, ConsumerTag};
                                           true -> ExistingHolder
                                       end,
                                     round_robin = queue:in({ChPid, Consumer}, RoundRobin)},
                    ok = maybe_send_reply(ChPid, OkMsg),
                    reply(ok, run_message_queue(State1))
            end
    end;

handle_call({basic_cancel, ChPid, ConsumerTag, OkMsg}, _From,
            State = #q{exclusive_consumer = Holder,
                       round_robin = RoundRobin}) ->
    case lookup_ch(ChPid) of
        not_found ->
            ok = maybe_send_reply(ChPid, OkMsg),
            reply(ok, State);
        C = #cr{consumers = Consumers, limiter_pid = LimiterPid} ->
            NewConsumers = lists:filter
                             (fun (#consumer{tag = CT}) -> CT /= ConsumerTag end,
                              Consumers),
            store_ch_record(C#cr{consumers = NewConsumers}),
            if NewConsumers == [] ->
                    ok = rabbit_limiter:unregister(LimiterPid, self());
               true ->
                    ok
            end,
            ok = maybe_send_reply(ChPid, OkMsg),
            case check_auto_delete(
                   State#q{exclusive_consumer = cancel_holder(ChPid,
                                                              ConsumerTag,
                                                              Holder),
                           round_robin = block_consumer(ChPid,
                                                        ConsumerTag,
                                                        RoundRobin)}) of
                {continue, State1} ->
                    reply(ok, State1);
                {stop, State1} ->
                    {stop, normal, ok, State1}
            end
    end;

handle_call(stat, _From, State = #q{q = #amqqueue{name = Name},
                                    mixed_state = MS,
                                    round_robin = RoundRobin}) ->
    Length = rabbit_mixed_queue:length(MS),
    reply({ok, Name, Length, queue:len(RoundRobin)}, State);

handle_call({delete, IfUnused, IfEmpty}, _From,
            State = #q { mixed_state = MS }) ->
    Length = rabbit_mixed_queue:length(MS),
    IsEmpty = Length == 0,
    IsUnused = is_unused(),
    if
        IfEmpty and not(IsEmpty) ->
            reply({error, not_empty}, State);
        IfUnused and not(IsUnused) ->
            reply({error, in_use}, State);
        true ->
            {stop, normal, {ok, Length}, State}
    end;

handle_call(purge, _From, State) ->
    {Count, MS} = rabbit_mixed_queue:purge(State #q.mixed_state),
    reply({ok, Count},
          State #q { mixed_state = MS });

handle_call({claim_queue, ReaderPid}, _From, State = #q{owner = Owner,
                                                        exclusive_consumer = Holder}) ->
    case Owner of
        none ->
            case check_exclusive_access(Holder, true) of
                in_use ->
                    %% FIXME: Is this really the right answer? What if
                    %% an active consumer's reader is actually the
                    %% claiming pid? Should that be allowed? In order
                    %% to check, we'd need to hold not just the ch
                    %% pid for each consumer, but also its reader
                    %% pid...
                    reply(locked, State);
                ok ->
                    reply(ok, State#q{owner = {ReaderPid, erlang:monitor(process, ReaderPid)}})
            end;
        {ReaderPid, _MonitorRef} ->
            reply(ok, State);
        _ ->
            reply(locked, State)
    end.

handle_cast({deliver, Txn, Message, ChPid}, State) ->
    %% Asynchronous, non-"mandatory", non-"immediate" deliver mode.
    {_Delivered, NewState} = deliver_or_enqueue(Txn, ChPid, Message, State),
    noreply(NewState);

handle_cast({ack, Txn, MsgIds, ChPid}, State) ->
    case lookup_ch(ChPid) of
        not_found ->
            noreply(State);
        C = #cr{unacked_messages = UAM} ->
            {MsgWithAcks, Remaining} = collect_messages(MsgIds, UAM),
            case Txn of
                none ->
                    Acks = lists:map(fun ({_Msg, AckTag}) -> AckTag end, MsgWithAcks),
                    {ok, MS} = rabbit_mixed_queue:ack(Acks, State #q.mixed_state),
                    store_ch_record(C#cr{unacked_messages = Remaining}),
                    noreply(State #q { mixed_state = MS });
                _  ->
                    record_pending_acks(Txn, ChPid, MsgIds),
                    noreply(State)
            end
    end;

handle_cast({rollback, Txn}, State) ->
    noreply(rollback_transaction(Txn, State));

handle_cast({requeue, MsgIds, ChPid}, State) ->
    case lookup_ch(ChPid) of
        not_found ->
            rabbit_log:warning("Ignoring requeue from unknown ch: ~p~n",
                               [ChPid]),
            noreply(State);
        C = #cr{unacked_messages = UAM} ->
            {MsgWithAcks, NewUAM} = collect_messages(MsgIds, UAM),
            store_ch_record(C#cr{unacked_messages = NewUAM}),
            noreply(deliver_or_requeue_n(MsgWithAcks, State))
    end;

handle_cast({unblock, ChPid}, State) ->
    noreply(
      possibly_unblock(State, ChPid,
                       fun (C) -> C#cr{is_limit_active = false} end));

handle_cast({notify_sent, ChPid}, State) ->
    noreply(
      possibly_unblock(State, ChPid,
                       fun (C = #cr{unsent_message_count = Count}) ->
                               C#cr{unsent_message_count = Count - 1}
                       end));

handle_cast({limit, ChPid, LimiterPid}, State) ->
    noreply(
      possibly_unblock(
        State, ChPid,
        fun (C = #cr{consumers = Consumers,
                     limiter_pid = OldLimiterPid,
                     is_limit_active = Limited}) ->
                if Consumers =/= [] andalso OldLimiterPid == undefined ->
                        ok = rabbit_limiter:register(LimiterPid, self());
                   true ->
                        ok
                end,
                NewLimited = Limited andalso LimiterPid =/= undefined,
                C#cr{limiter_pid = LimiterPid, is_limit_active = NewLimited}
        end)).

handle_info({'DOWN', MonitorRef, process, DownPid, _Reason},
            State = #q{owner = {DownPid, MonitorRef}}) ->
    %% We know here that there are no consumers on this queue that are
    %% owned by other pids than the one that just went down, so since
    %% exclusive in some sense implies autodelete, we delete the queue
    %% here. The other way of implementing the "exclusive implies
    %% autodelete" feature is to actually set autodelete when an
    %% exclusive declaration is seen, but this has the problem that
    %% the python tests rely on the queue not going away after a
    %% basic.cancel when the queue was declared exclusive and
    %% nonautodelete.
    NewState = State#q{owner = none},
    {stop, normal, NewState};
handle_info({'DOWN', _MonitorRef, process, DownPid, _Reason}, State) ->
    handle_ch_down(DownPid, State);

handle_info(timeout, State) ->
    %% TODO: Once we drop support for R11B-5, we can change this to
    %% {noreply, State, hibernate};
    proc_lib:hibernate(gen_server2, enter_loop, [?MODULE, [], State]);

handle_info(Info, State) ->
    ?LOGDEBUG("Info in queue: ~p~n", [Info]),
    {stop, {unhandled_info, Info}, State}.
