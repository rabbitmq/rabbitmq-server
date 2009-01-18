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
            next_msg_id,
            message_buffer,
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

init(Q) ->
    ?LOGDEBUG("Queue starting - ~p~n", [Q]),
    {ok, #q{q = Q,
            owner = none,
            exclusive_consumer = none,
            has_had_consumers = false,
            next_msg_id = 1,
            message_buffer = queue:new(),
            round_robin = queue:new()}, ?HIBERNATE_AFTER}.

terminate(_Reason, State) ->
    %% FIXME: How do we cancel active subscriptions?
    QName = qname(State),
    lists:foreach(fun (Txn) -> ok = rollback_work(Txn, QName) end,
                  all_tx()),
    ok = purge_message_buffer(QName, State#q.message_buffer),
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

deliver_immediately(Message, Delivered,
                    State = #q{q = #amqqueue{name = QName},
                               round_robin = RoundRobin,
                               next_msg_id = NextId}) ->
    ?LOGDEBUG("AMQQUEUE ~p DELIVERY:~n~p~n", [QName, Message]),
    case queue:out(RoundRobin) of
        {{value, QEntry = {ChPid, #consumer{tag = ConsumerTag,
                                            ack_required = AckRequired}}},
         RoundRobinTail} ->
            C = #cr{limiter_pid = LimiterPid,
                    unsent_message_count = Count,
                    unacked_messages = UAM} = ch_record(ChPid),
            case not(AckRequired) orelse rabbit_limiter:can_send(
                                           LimiterPid, self()) of
                true ->
                    rabbit_channel:deliver(
                      ChPid, ConsumerTag, AckRequired,
                      {QName, self(), NextId, Delivered, Message}),
                    NewUAM = case AckRequired of
                                 true  -> dict:store(NextId, Message, UAM);
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
                    {offered, AckRequired, State#q{round_robin = NewConsumers,
                                                   next_msg_id = NextId + 1}};
                false ->
                    store_ch_record(C#cr{is_limit_active = true}),
                    NewConsumers = block_consumers(ChPid, RoundRobinTail),
                    deliver_immediately(Message, Delivered,
                                        State#q{round_robin = NewConsumers})
            end;
        {empty, _} ->
            {not_offered, State}
    end.

attempt_delivery(none, Message, State) ->
    case deliver_immediately(Message, false, State) of
        {offered, false, State1} ->
            {true, State1};
        {offered, true, State1} ->
            persist_message(none, qname(State), Message),
            persist_delivery(qname(State), Message, false),
            {true, State1};
        {not_offered, State1} ->
            {false, State1}
    end;
attempt_delivery(Txn, Message, State) ->
    persist_message(Txn, qname(State), Message),
    record_pending_message(Txn, Message),
    {true, State}.

deliver_or_enqueue(Txn, Message, State) ->
    case attempt_delivery(Txn, Message, State) of
        {true, NewState} ->
            {true, NewState};
        {false, NewState} ->
            persist_message(Txn, qname(State), Message),
            NewMB = queue:in({Message, false}, NewState#q.message_buffer),
            {false, NewState#q{message_buffer = NewMB}}
    end.

deliver_or_enqueue_n(Messages, State = #q{message_buffer = MessageBuffer}) ->
    run_poke_burst(queue:join(MessageBuffer, queue:from_list(Messages)),
                   State).

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
                           run_poke_burst(State#q{round_robin = NewRR})
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
        #cr{monitor_ref = MonitorRef, ch_pid = ChPid, unacked_messages = UAM} ->
            NewActive = block_consumers(ChPid, ActiveConsumers),
            erlang:demonitor(MonitorRef),
            erase({ch, ChPid}),
            case check_auto_delete(
                   deliver_or_enqueue_n(
                     [{Message, true} ||
                         {_Messsage_id, Message} <- dict:to_list(UAM)],
                     State#q{
                       exclusive_consumer = case Holder of
                                                {ChPid, _} -> none;
                                                Other -> Other
                                            end,
                       round_robin = NewActive})) of
                {continue, NewState} ->
                    noreply(NewState);
                {stop, NewState} ->
                    {stop, normal, NewState}
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

run_poke_burst(State = #q{message_buffer = MessageBuffer}) ->
    run_poke_burst(MessageBuffer, State).

run_poke_burst(MessageBuffer, State) ->
    case queue:out(MessageBuffer) of
        {{value, {Message, Delivered}}, BufferTail} ->
            case deliver_immediately(Message, Delivered, State) of
                {offered, true, NewState} ->
                    persist_delivery(qname(State), Message, Delivered),
                    run_poke_burst(BufferTail, NewState);
                {offered, false, NewState} ->
                    persist_auto_ack(qname(State), Message),
                    run_poke_burst(BufferTail, NewState);
                {not_offered, NewState} ->
                    NewState#q{message_buffer = MessageBuffer}
            end;
        {empty, _} ->
            State#q{message_buffer = MessageBuffer}
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

persist_message(_Txn, _QName, #basic_message{persistent_key = none}) ->
    ok;
persist_message(Txn, QName, Message) ->
    M = Message#basic_message{
          %% don't persist any recoverable decoded properties, rebuild from properties_bin on restore
          content = rabbit_binary_parser:clear_decoded_content(
                      Message#basic_message.content)},
    persist_work(Txn, QName,
                 [{publish, M, {QName, M#basic_message.persistent_key}}]).

persist_delivery(_QName, _Message,
                 true) ->
    ok;
persist_delivery(_QName, #basic_message{persistent_key = none},
                 _Delivered) ->
    ok;
persist_delivery(QName, #basic_message{persistent_key = PKey},
                 _Delivered) ->
    persist_work(none, QName, [{deliver, {QName, PKey}}]).

persist_acks(Txn, QName, Messages) ->
    persist_work(Txn, QName,
                 [{ack, {QName, PKey}} ||
                     #basic_message{persistent_key = PKey} <- Messages,
                     PKey =/= none]).

persist_auto_ack(_QName, #basic_message{persistent_key = none}) ->
    ok;
persist_auto_ack(QName, #basic_message{persistent_key = PKey}) ->
    %% auto-acks are always non-transactional
    rabbit_persister:dirty_work([{ack, {QName, PKey}}]).

persist_work(_Txn,_QName, []) ->
    ok;
persist_work(none, _QName, WorkList) ->
    rabbit_persister:dirty_work(WorkList);
persist_work(Txn, QName, WorkList) ->
    mark_tx_persistent(Txn),
    rabbit_persister:extend_transaction({Txn, QName}, WorkList).

commit_work(Txn, QName) ->
    do_if_persistent(fun rabbit_persister:commit_transaction/1,
                     Txn, QName).

rollback_work(Txn, QName) ->
    do_if_persistent(fun rabbit_persister:rollback_transaction/1,
                     Txn, QName).

%% optimisation: don't do unnecessary work
%% it would be nice if this was handled by the persister
do_if_persistent(F, Txn, QName) ->
    case is_tx_persistent(Txn) of
        false -> ok;
        true  -> ok = F({Txn, QName})
    end.

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

mark_tx_persistent(Txn) ->
    Tx = lookup_tx(Txn),
    store_tx(Txn, Tx#tx{is_persistent = true}).
    
is_tx_persistent(Txn) ->
    #tx{is_persistent = Res} = lookup_tx(Txn),
    Res.

record_pending_message(Txn, Message) ->
    Tx = #tx{pending_messages = Pending} = lookup_tx(Txn),
    store_tx(Txn, Tx#tx{pending_messages = [{Message, false} | Pending]}).

record_pending_acks(Txn, ChPid, MsgIds) ->
    Tx = #tx{pending_acks = Pending} = lookup_tx(Txn),
    store_tx(Txn, Tx#tx{pending_acks = [MsgIds | Pending], ch_pid = ChPid}).

process_pending(Txn, State) ->
    #tx{ch_pid = ChPid,
        pending_messages = PendingMessages,
        pending_acks = PendingAcks} = lookup_tx(Txn),
    case lookup_ch(ChPid) of
        not_found -> ok;
        C = #cr{unacked_messages = UAM} ->
            {_Acked, Remaining} =
                collect_messages(lists:append(PendingAcks), UAM),
            store_ch_record(C#cr{unacked_messages = Remaining})
    end,
    deliver_or_enqueue_n(lists:reverse(PendingMessages), State).

collect_messages(MsgIds, UAM) ->
    lists:mapfoldl(
      fun (MsgId, D) -> {dict:fetch(MsgId, D), dict:erase(MsgId, D)} end,
      UAM, MsgIds).

purge_message_buffer(QName, MessageBuffer) ->
    Messages =
        [[Message || {Message, _Delivered} <-
                         queue:to_list(MessageBuffer)] |
         lists:map(
           fun (#cr{unacked_messages = UAM}) ->
                   [Message || {_MessageId, Message} <- dict:to_list(UAM)]
           end,
           all_ch_record())],
    %% the simplest, though certainly not the most obvious or
    %% efficient, way to purge messages from the persister is to
    %% artifically ack them.
    persist_acks(none, QName, lists:append(Messages)).

infos(Items, State) -> [{Item, i(Item, State)} || Item <- Items].

i(name,        #q{q = #amqqueue{name        = Name}})       -> Name;
i(durable,     #q{q = #amqqueue{durable     = Durable}})    -> Durable;
i(auto_delete, #q{q = #amqqueue{auto_delete = AutoDelete}}) -> AutoDelete;
i(arguments,   #q{q = #amqqueue{arguments   = Arguments}})  -> Arguments;
i(pid, _) ->
    self();
i(messages_ready, #q{message_buffer = MessageBuffer}) ->
    queue:len(MessageBuffer);
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

handle_call({deliver_immediately, Txn, Message}, _From, State) ->
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
    {Delivered, NewState} = attempt_delivery(Txn, Message, State),
    reply(Delivered, NewState);

handle_call({deliver, Txn, Message}, _From, State) ->
    %% Synchronous, "mandatory" delivery mode
    {Delivered, NewState} = deliver_or_enqueue(Txn, Message, State),
    reply(Delivered, NewState);

handle_call({commit, Txn}, From, State) ->
    ok = commit_work(Txn, qname(State)),
    %% optimisation: we reply straight away so the sender can continue
    gen_server2:reply(From, ok),
    NewState = process_pending(Txn, State),
    erase_tx(Txn),
    noreply(NewState);

handle_call({notify_down, ChPid}, From, State) ->
    %% optimisation: we reply straight away so the sender can continue
    gen_server2:reply(From, ok),
    handle_ch_down(ChPid, State);

handle_call({basic_get, ChPid, NoAck}, _From,
            State = #q{q = #amqqueue{name = QName},
                       next_msg_id = NextId,
                       message_buffer = MessageBuffer}) ->
    case queue:out(MessageBuffer) of
        {{value, {Message, Delivered}}, BufferTail} ->
            AckRequired = not(NoAck),
            case AckRequired of
                true  ->
                    persist_delivery(QName, Message, Delivered),
                    C = #cr{unacked_messages = UAM} = ch_record(ChPid),
                    NewUAM = dict:store(NextId, Message, UAM),
                    store_ch_record(C#cr{unacked_messages = NewUAM});
                false ->
                    persist_auto_ack(QName, Message)
            end,
            Msg = {QName, self(), NextId, Delivered, Message},
            reply({ok, queue:len(BufferTail), Msg},
                  State#q{message_buffer = BufferTail,
                          next_msg_id = NextId + 1});
        {empty, _} ->
            reply(empty, State)
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
                    reply(ok, run_poke_burst(State1))
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
                                    message_buffer = MessageBuffer,
                                    round_robin = RoundRobin}) ->
    reply({ok, Name, queue:len(MessageBuffer), queue:len(RoundRobin)}, State);

handle_call({delete, IfUnused, IfEmpty}, _From,
            State = #q{message_buffer = MessageBuffer}) ->
    IsEmpty = queue:is_empty(MessageBuffer),
    IsUnused = is_unused(),
    if
        IfEmpty and not(IsEmpty) ->
            reply({error, not_empty}, State);
        IfUnused and not(IsUnused) ->
            reply({error, in_use}, State);
        true ->
            {stop, normal, {ok, queue:len(MessageBuffer)}, State}
    end;

handle_call(purge, _From, State = #q{message_buffer = MessageBuffer}) ->
    ok = purge_message_buffer(qname(State), MessageBuffer),
    reply({ok, queue:len(MessageBuffer)},
          State#q{message_buffer = queue:new()});

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

handle_cast({deliver, Txn, Message}, State) ->
    %% Asynchronous, non-"mandatory", non-"immediate" deliver mode.
    {_Delivered, NewState} = deliver_or_enqueue(Txn, Message, State),
    noreply(NewState);

handle_cast({ack, Txn, MsgIds, ChPid}, State) ->
    case lookup_ch(ChPid) of
        not_found ->
            noreply(State);
        C = #cr{unacked_messages = UAM} ->
            {Acked, Remaining} = collect_messages(MsgIds, UAM),
            persist_acks(Txn, qname(State), Acked),
            case Txn of
                none ->
                    store_ch_record(C#cr{unacked_messages = Remaining});
                _  ->
                    record_pending_acks(Txn, ChPid, MsgIds)
            end,
            noreply(State)
    end;

handle_cast({rollback, Txn}, State) ->
    ok = rollback_work(Txn, qname(State)),
    erase_tx(Txn),
    noreply(State);

handle_cast({redeliver, Messages}, State) ->
    noreply(deliver_or_enqueue_n(Messages, State));

handle_cast({requeue, MsgIds, ChPid}, State) ->
    case lookup_ch(ChPid) of
        not_found ->
            rabbit_log:warning("Ignoring requeue from unknown ch: ~p~n",
                               [ChPid]),
            noreply(State);
        C = #cr{unacked_messages = UAM} ->
            {Messages, NewUAM} = collect_messages(MsgIds, UAM),
            store_ch_record(C#cr{unacked_messages = NewUAM}),
            noreply(deliver_or_enqueue_n(
                      [{Message, true} || Message <- Messages], State))
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
