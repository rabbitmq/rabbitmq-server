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
-define(MEMORY_REPORT_INTERVAL, 500).

-export([start_link/1]).

-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

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
            active_consumers,
            blocked_consumers,
            memory_report_counter,
            old_memory_report
           }).

-record(consumer, {tag, ack_required}).

-record(tx, {ch_pid, is_persistent, pending_messages, pending_acks}).

%% These are held in our process dictionary
-record(cr, {consumer_count,
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
    {ok, Mode} = rabbit_queue_mode_manager:register(self()),
    {ok, MS} = rabbit_mixed_queue:init(QName, Durable, Mode),
    {ok, #q{q = Q,
            owner = none,
            exclusive_consumer = none,
            has_had_consumers = false,
            mixed_state = MS,
            next_msg_id = 1,
            active_consumers = queue:new(),
            blocked_consumers = queue:new(),
            memory_report_counter = ?MEMORY_REPORT_INTERVAL,
            old_memory_report = 1
           }, ?HIBERNATE_AFTER}.

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

reply(Reply, NewState = #q { memory_report_counter = 0 }) ->
    {reply, Reply, report_memory(NewState), ?HIBERNATE_AFTER};
reply(Reply, NewState = #q { memory_report_counter = C }) ->
    {reply, Reply, NewState #q { memory_report_counter = C - 1 },
     ?HIBERNATE_AFTER}.

noreply(NewState = #q { memory_report_counter = 0}) ->
    {noreply, report_memory(NewState), ?HIBERNATE_AFTER};
noreply(NewState = #q { memory_report_counter = C}) ->
    {noreply, NewState #q { memory_report_counter = C - 1 }, ?HIBERNATE_AFTER}.

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
            C = #cr{consumer_count = 0,
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
    Limited orelse Count >= ?UNSENT_MESSAGE_LIMIT.

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
    
deliver_queue(Funs = {PredFun, DeliverFun}, FunAcc,
              State = #q{q = #amqqueue{name = QName},
                         active_consumers = ActiveConsumers,
                         blocked_consumers = BlockedConsumers,
                         next_msg_id = NextId}) ->
    case queue:out(ActiveConsumers) of
        {{value, QEntry = {ChPid, #consumer{tag = ConsumerTag,
                                            ack_required = AckRequired}}},
         ActiveConsumersTail} ->
            C = #cr{limiter_pid = LimiterPid,
                    unsent_message_count = Count,
                    unacked_messages = UAM} = ch_record(ChPid),
            IsMsgReady = PredFun(FunAcc, State),
            case (IsMsgReady andalso
                  rabbit_limiter:can_send( LimiterPid, self(), AckRequired )) of
                true ->
                    {{Msg, IsDelivered, AckTag}, FunAcc1, State1} =
                        DeliverFun(AckRequired, FunAcc, State),
                    ?LOGDEBUG("AMQQUEUE ~p DELIVERY:~n~p~n", [QName, Msg]),
                    rabbit_channel:deliver(
                      ChPid, ConsumerTag, AckRequired,
                      {QName, self(), NextId, IsDelivered, Msg}),
                    NewUAM =
                        case AckRequired of
                            true  -> dict:store(NextId, {Msg, AckTag}, UAM);
                            false -> UAM
                        end,
                    NewC = C#cr{unsent_message_count = Count + 1,
                                unacked_messages = NewUAM},
                    store_ch_record(NewC),
                    {NewActiveConsumers, NewBlockedConsumers} =
                        case ch_record_state_transition(C, NewC) of
                            ok    -> {queue:in(QEntry, ActiveConsumersTail),
                                      BlockedConsumers};
                            block ->
                                {ActiveConsumers1, BlockedConsumers1} =
                                    move_consumers(ChPid,
                                                   ActiveConsumersTail,
                                                   BlockedConsumers),
                                {ActiveConsumers1,
                                 queue:in(QEntry, BlockedConsumers1)}
                        end,
                    State2 = State1 #q {
                               active_consumers = NewActiveConsumers,
                               blocked_consumers = NewBlockedConsumers,
                               next_msg_id = NextId + 1
                                       },
                    deliver_queue(Funs, FunAcc1, State2);
                %% if IsMsgReady then we've hit the limiter
                false when IsMsgReady ->
                    store_ch_record(C#cr{is_limit_active = true}),
                    {NewActiveConsumers, NewBlockedConsumers} =
                        move_consumers(ChPid,
                                       ActiveConsumers,
                                       BlockedConsumers),
                    deliver_queue(
                      Funs, FunAcc,
                      State#q{active_consumers = NewActiveConsumers,
                              blocked_consumers = NewBlockedConsumers});
                false ->
                    %% no message was ready, so we don't need to block anyone
                    {FunAcc, State}
            end;
        {empty, _} ->
            {FunAcc, State}
    end.

deliver_from_queue_pred({IsEmpty, _AutoAcks}, _State) ->
    not IsEmpty.
deliver_from_queue_deliver(AckRequired, {false, AutoAcks},
                           State = #q { mixed_state = MS }) ->
    {{Msg, IsDelivered, AckTag, Remaining}, MS1} =
        rabbit_mixed_queue:deliver(MS),
    AutoAcks1 =
        case AckRequired of
            true -> AutoAcks;
            false -> [AckTag | AutoAcks]
        end,
    {{Msg, IsDelivered, AckTag}, {0 == Remaining, AutoAcks1},
     State #q { mixed_state = MS1 }}.

run_message_queue(State = #q { mixed_state = MS }) ->
    Funs = { fun deliver_from_queue_pred/2,
             fun deliver_from_queue_deliver/3 },
    IsEmpty = rabbit_mixed_queue:is_empty(MS),
    {{_IsEmpty1, AutoAcks}, State1} =
        deliver_queue(Funs, {IsEmpty, []}, State),
    {ok, MS1} =
        rabbit_mixed_queue:ack(lists:reverse(AutoAcks), State1 #q.mixed_state),
    State1 #q { mixed_state = MS1 }.

attempt_immediate_delivery(none, _ChPid, Msg, State) ->
    PredFun = fun (IsEmpty, _State) -> not IsEmpty end,
    DeliverFun =
        fun (AckRequired, false, State1) ->
                {AckTag, State2} =
                    case AckRequired of
                        true ->
                            {ok, AckTag1, MS} =
                                rabbit_mixed_queue:publish_delivered(
                                  Msg, State1 #q.mixed_state),
                            {AckTag1, State1 #q { mixed_state = MS }};
                        false ->
                            {noack, State1}
                    end,
                {{Msg, false, AckTag}, true, State2}
        end,
    deliver_queue({ PredFun, DeliverFun }, false, State);
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
    Funs = { fun deliver_or_requeue_msgs_pred/2,
             fun deliver_or_requeue_msgs_deliver/3 },
    {{_RemainingLengthMinusOne, AutoAcks, OutstandingMsgs}, NewState} =
        deliver_queue(Funs, {length(MsgsWithAcks) - 1, [], MsgsWithAcks},
                      State),
    {ok, MS} = rabbit_mixed_queue:ack(lists:reverse(AutoAcks),
                                      NewState #q.mixed_state),
    case OutstandingMsgs of
        [] -> run_message_queue(NewState #q { mixed_state = MS });
        _ -> {ok, MS1} = rabbit_mixed_queue:requeue(OutstandingMsgs, MS),
             NewState #q { mixed_state = MS1 }
    end.

deliver_or_requeue_msgs_pred({Len, _AcksAcc, _MsgsWithAcks}, _State) ->
    -1 < Len.
deliver_or_requeue_msgs_deliver(
  false, {Len, AcksAcc, [{Msg, AckTag} | MsgsWithAcks]}, State) ->
    {{Msg, true, noack}, {Len - 1, [AckTag|AcksAcc], MsgsWithAcks}, State};
deliver_or_requeue_msgs_deliver(
  true, {Len, AcksAcc, [{Msg, AckTag} | MsgsWithAcks]}, State) ->
    {{Msg, true, AckTag}, {Len - 1, AcksAcc, MsgsWithAcks}, State}.

add_consumer(ChPid, Consumer, Queue) -> queue:in({ChPid, Consumer}, Queue).

remove_consumer(ChPid, ConsumerTag, Queue) ->
    %% TODO: replace this with queue:filter/2 once we move to R12
    queue:from_list(lists:filter(
                      fun ({CP, #consumer{tag = CT}}) ->
                              (CP /= ChPid) or (CT /= ConsumerTag)
                      end, queue:to_list(Queue))).

remove_consumers(ChPid, Queue) ->
    %% TODO: replace this with queue:filter/2 once we move to R12
    queue:from_list(lists:filter(fun ({CP, _}) -> CP /= ChPid end,
                                 queue:to_list(Queue))).

move_consumers(ChPid, From, To) ->
    {Kept, Removed} = lists:partition(fun ({CP, _}) -> CP /= ChPid end,
                                      queue:to_list(From)),
    {queue:from_list(Kept), queue:join(To, queue:from_list(Removed))}.

possibly_unblock(State, ChPid, Update) ->
    case lookup_ch(ChPid) of
        not_found ->
            State;
        C ->
            NewC = Update(C),
            store_ch_record(NewC),
            case ch_record_state_transition(C, NewC) of
                ok      -> State;
                unblock -> {NewBlockedeConsumers, NewActiveConsumers} =
                               move_consumers(ChPid,
                                              State#q.blocked_consumers,
                                              State#q.active_consumers),
                           run_message_queue(
                             State#q{active_consumers = NewActiveConsumers,
                                     blocked_consumers = NewBlockedeConsumers})
            end
    end.
    
should_auto_delete(#q{q = #amqqueue{auto_delete = false}}) -> false;
should_auto_delete(#q{has_had_consumers = false}) -> false;
should_auto_delete(State) -> is_unused(State).

handle_ch_down(DownPid, State = #q{exclusive_consumer = Holder}) ->
    case lookup_ch(DownPid) of
        not_found -> noreply(State);
        #cr{monitor_ref = MonitorRef, ch_pid = ChPid, txn = Txn,
            unacked_messages = UAM} ->
            erlang:demonitor(MonitorRef),
            erase({ch, ChPid}),
            State1 =
                case Txn of
                    none -> State;
                    _    -> rollback_transaction(Txn, State)
                end,
            State2 =
                deliver_or_requeue_n(
                  [MsgWithAck ||
                      {_MsgId, MsgWithAck} <- dict:to_list(UAM)],
                  State1 #q {
                    exclusive_consumer = case Holder of
                                             {ChPid, _} -> none;
                                             Other -> Other
                                         end,
                    active_consumers = remove_consumers(
                                         ChPid, State1#q.active_consumers),
                    blocked_consumers = remove_consumers(
                                          ChPid, State1#q.blocked_consumers)}),
            case should_auto_delete(State2) of
                false -> noreply(State2);
                true  -> {stop, normal, State2}
            end
    end.

cancel_holder(ChPid, ConsumerTag, {ChPid, ConsumerTag}) ->
    none;
cancel_holder(_ChPid, _ConsumerTag, Holder) ->
    Holder.

check_queue_owner(none,           _)         -> ok;
check_queue_owner({ReaderPid, _}, ReaderPid) -> ok;
check_queue_owner({_,         _}, _)         -> mismatch.

check_exclusive_access({_ChPid, _ConsumerTag}, _ExclusiveConsume, _State) ->
    in_use;
check_exclusive_access(none, false, _State) ->
    ok;
check_exclusive_access(none, true, State) ->
    case is_unused(State) of
        true  -> ok;
        false -> in_use
    end.

is_unused(State) -> queue:is_empty(State#q.active_consumers) andalso
                        queue:is_empty(State#q.blocked_consumers).

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

record_pending_message(Txn, ChPid, Message =
                       #basic_message { is_persistent = IsPersistent }) ->
    Tx = #tx{pending_messages = Pending, is_persistent = IsPersistentTxn } =
        lookup_tx(Txn),
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
    Acks =
        case lookup_ch(ChPid) of
            not_found -> [];
            C = #cr { unacked_messages = UAM } ->
                {MsgWithAcks, Remaining} =
                    collect_messages(PendingAcksOrdered, UAM),
                store_ch_record(C#cr{unacked_messages = Remaining}),
                [ AckTag || {_Msg, AckTag} <- MsgWithAcks ]              
        end,
    {ok, MS} = rabbit_mixed_queue:tx_commit(
                 PendingMessagesOrdered, Acks, State #q.mixed_state),
    State #q { mixed_state = MS }.

rollback_transaction(Txn, State) ->
    #tx { pending_messages = PendingMessages
        } = lookup_tx(Txn),
    {ok, MS} = rabbit_mixed_queue:tx_cancel(lists:reverse(PendingMessages),
                                            State #q.mixed_state),
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
i(consumers, State) ->
    queue:len(State#q.active_consumers) + queue:len(State#q.blocked_consumers);
i(transactions, _) ->
    length(all_tx_record());
i(memory, _) ->
    {memory, M} = process_info(self(), memory),
    M;
i(Item, _) ->
    throw({bad_argument, Item}).

report_memory(State = #q { old_memory_report = OldMem,
                           mixed_state = MS }) ->
    MSize = rabbit_mixed_queue:estimate_extra_memory(MS),
    {memory, PSize} = process_info(self(), memory),
    NewMem = case MSize + PSize of
                 0 -> 1; %% avoid / 0
                 N -> N
             end,
    State1 = State #q { memory_report_counter = ?MEMORY_REPORT_INTERVAL },
    case (NewMem / OldMem) > 1.1 orelse (OldMem / NewMem) > 1.1 of
        true ->
            rabbit_queue_mode_manager:report_memory(self(), NewMem),
            State1 #q { old_memory_report = NewMem };
        false -> State1
    end.

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
    {Delivered, NewState} =
        attempt_immediate_delivery(Txn, ChPid, Message, State),
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
        {empty, MS1} -> reply(empty, State #q { mixed_state = MS1 });
        {{Msg, IsDelivered, AckTag, Remaining}, MS1} ->
            AckRequired = not(NoAck),
            {ok, MS3} =
                case AckRequired of
                    true ->
                        C = #cr{unacked_messages = UAM} = ch_record(ChPid),
                        NewUAM = dict:store(NextId, {Msg, AckTag}, UAM),
                        store_ch_record(C#cr{unacked_messages = NewUAM}),
                        {ok, MS1};
                    false ->
                        rabbit_mixed_queue:ack([AckTag], MS1)
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
                              exclusive_consumer = ExistingHolder}) ->
    case check_queue_owner(Owner, ReaderPid) of
        mismatch ->
            reply({error, queue_owned_by_another_connection}, State);
        ok ->
            case check_exclusive_access(ExistingHolder, ExclusiveConsume,
                                        State) of
                in_use ->
                    reply({error, exclusive_consume_unavailable}, State);
                ok ->
                    C = #cr{consumer_count = ConsumerCount} = ch_record(ChPid),
                    Consumer = #consumer{tag = ConsumerTag,
                                         ack_required = not(NoAck)},
                    store_ch_record(C#cr{consumer_count = ConsumerCount +1,
                                         limiter_pid = LimiterPid}),
                    case ConsumerCount of
                        0 -> ok = rabbit_limiter:register(LimiterPid, self());
                        _ -> ok
                    end,
                    ExclusiveConsumer = case ExclusiveConsume of
                                            true  -> {ChPid, ConsumerTag};
                                            false -> ExistingHolder
                                        end,
                    State1 = State#q{has_had_consumers = true,
                                     exclusive_consumer = ExclusiveConsumer},
                    ok = maybe_send_reply(ChPid, OkMsg),
                    State2 =
                        case is_ch_blocked(C) of
                            true  -> State1#q{
                                       blocked_consumers =
                                       add_consumer(
                                         ChPid, Consumer,
                                         State1#q.blocked_consumers)};
                            false -> run_message_queue(
                                       State1#q{
                                         active_consumers =
                                         add_consumer(
                                           ChPid, Consumer,
                                           State1#q.active_consumers)})
                        end,
                    reply(ok, State2)
            end
    end;

handle_call({basic_cancel, ChPid, ConsumerTag, OkMsg}, _From,
            State = #q{exclusive_consumer = Holder}) ->
    case lookup_ch(ChPid) of
        not_found ->
            ok = maybe_send_reply(ChPid, OkMsg),
            reply(ok, State);
        C = #cr{consumer_count = ConsumerCount, limiter_pid = LimiterPid} ->
            store_ch_record(C#cr{consumer_count = ConsumerCount - 1}),
            ok = case ConsumerCount of
                     1 -> rabbit_limiter:unregister(LimiterPid, self());
                     _ -> ok
                 end,
            ok = maybe_send_reply(ChPid, OkMsg),
            NewState =
                State#q{exclusive_consumer = cancel_holder(ChPid,
                                                           ConsumerTag,
                                                           Holder),
                        active_consumers = remove_consumer(
                                             ChPid, ConsumerTag,
                                             State#q.active_consumers),
                        blocked_consumers = remove_consumer(
                                              ChPid, ConsumerTag,
                                              State#q.blocked_consumers)},
            case should_auto_delete(NewState) of
                false -> reply(ok, NewState);
                true  -> {stop, normal, ok, NewState}
            end
    end;

handle_call(stat, _From, State = #q{q = #amqqueue{name = Name},
                                    mixed_state = MS,
                                    active_consumers = ActiveConsumers}) ->
    Length = rabbit_mixed_queue:length(MS),
    reply({ok, Name, Length, queue:len(ActiveConsumers)}, State);

handle_call({delete, IfUnused, IfEmpty}, _From,
            State = #q { mixed_state = MS }) ->
    Length = rabbit_mixed_queue:length(MS),
    IsEmpty = Length == 0,
    IsUnused = is_unused(State),
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

handle_call({claim_queue, ReaderPid}, _From,
            State = #q{owner = Owner, exclusive_consumer = Holder}) ->
    case Owner of
        none ->
            case check_exclusive_access(Holder, true, State) of
                in_use ->
                    %% FIXME: Is this really the right answer? What if
                    %% an active consumer's reader is actually the
                    %% claiming pid? Should that be allowed? In order
                    %% to check, we'd need to hold not just the ch
                    %% pid for each consumer, but also its reader
                    %% pid...
                    reply(locked, State);
                ok ->
                    reply(ok, State #q { owner =
                                         {ReaderPid,
                                          erlang:monitor(process, ReaderPid)} })
                                                 
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
                    Acks = [ AckTag || {_Msg, AckTag} <- MsgWithAcks ],
                    {ok, MS} =
                        rabbit_mixed_queue:ack(Acks, State #q.mixed_state),
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
        fun (C = #cr{consumer_count = ConsumerCount,
                     limiter_pid = OldLimiterPid,
                     is_limit_active = Limited}) ->
                if ConsumerCount =/= 0 andalso OldLimiterPid == undefined ->
                        ok = rabbit_limiter:register(LimiterPid, self());
                   true ->
                        ok
                end,
                NewLimited = Limited andalso LimiterPid =/= undefined,
                C#cr{limiter_pid = LimiterPid, is_limit_active = NewLimited}
        end));

handle_cast({constrain, Constrain}, State = #q { mixed_state = MS }) ->
    {ok, MS1} = (case Constrain of
                    true  -> fun rabbit_mixed_queue:to_disk_only_mode/1;
                    false -> fun rabbit_mixed_queue:to_mixed_mode/1
                 end)(MS),
    noreply(State #q { mixed_state = MS1 }).

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

handle_info(timeout, State = #q { memory_report_counter = Count }) 
  when Count == ?MEMORY_REPORT_INTERVAL ->
    %% Have to do the +1 because the timeout below, with noreply, will -1
    %% TODO: Once we drop support for R11B-5, we can change this to
    %% {noreply, State, hibernate};
    proc_lib:hibernate(gen_server2, enter_loop, [?MODULE, [], State]);

handle_info(timeout, State) ->
    State1 = report_memory(State),
    noreply(State1 #q { memory_report_counter = 1 + ?MEMORY_REPORT_INTERVAL });

handle_info(Info, State) ->
    ?LOGDEBUG("Info in queue: ~p~n", [Info]),
    {stop, {unhandled_info, Info}, State}.
