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
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_amqqueue_process).
-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-behaviour(gen_server2).

-define(UNSENT_MESSAGE_LIMIT,          100).
-define(SYNC_INTERVAL,                 5). %% milliseconds
-define(RAM_DURATION_UPDATE_INTERVAL,  5000).

-export([start_link/1, info_keys/0]).

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2, handle_pre_hibernate/1]).

-import(queue).
-import(erlang).
-import(lists).

% Queue's state
-record(q, {q,
            exclusive_consumer,
            has_had_consumers,
            backing_queue,
            backing_queue_state,
            active_consumers,
            blocked_consumers,
            expires,
            sync_timer_ref,
            rate_timer_ref,
            expiry_timer_ref,
            stats_timer
           }).

-record(consumer, {tag, ack_required}).

%% These are held in our process dictionary
-record(cr, {consumer_count,
             ch_pid,
             limiter_pid,
             monitor_ref,
             acktags,
             is_limit_active,
             txn,
             unsent_message_count}).

-define(STATISTICS_KEYS,
        [pid,
         exclusive_consumer_pid,
         exclusive_consumer_tag,
         messages_ready,
         messages_unacknowledged,
         messages,
         consumers,
         memory,
         backing_queue_status
        ]).

-define(CREATION_EVENT_KEYS,
        [pid,
         name,
         durable,
         auto_delete,
         arguments,
         owner_pid
        ]).

-define(INFO_KEYS, ?CREATION_EVENT_KEYS ++ ?STATISTICS_KEYS -- [pid]).

%%----------------------------------------------------------------------------

start_link(Q) -> gen_server2:start_link(?MODULE, Q, []).

info_keys() -> ?INFO_KEYS.

%%----------------------------------------------------------------------------

init(Q) ->
    ?LOGDEBUG("Queue starting - ~p~n", [Q]),
    process_flag(trap_exit, true),
    {ok, BQ} = application:get_env(backing_queue_module),

    {ok, #q{q                   = Q#amqqueue{pid = self()},
            exclusive_consumer  = none,
            has_had_consumers   = false,
            backing_queue       = BQ,
            backing_queue_state = undefined,
            active_consumers    = queue:new(),
            blocked_consumers   = queue:new(),
            expires             = undefined,
            sync_timer_ref      = undefined,
            rate_timer_ref      = undefined,
            expiry_timer_ref    = undefined,
            stats_timer         = rabbit_event:init_stats_timer()}, hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

terminate(shutdown,      State = #q{backing_queue = BQ}) ->
    terminate_shutdown(fun (BQS) -> BQ:terminate(BQS) end, State);
terminate({shutdown, _}, State = #q{backing_queue = BQ}) ->
    terminate_shutdown(fun (BQS) -> BQ:terminate(BQS) end, State);
terminate(_Reason,       State = #q{backing_queue = BQ}) ->
    %% FIXME: How do we cancel active subscriptions?
    terminate_shutdown(fun (BQS) ->
                               BQS1 = BQ:delete_and_terminate(BQS),
                               %% don't care if the internal delete
                               %% doesn't return 'ok'.
                               rabbit_amqqueue:internal_delete(qname(State)),
                               BQS1
                       end, State).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

init_expires(State = #q{q = #amqqueue{arguments = Arguments}}) ->
    case rabbit_misc:table_lookup(Arguments, <<"x-expires">>) of
        {_Type, Expires} -> ensure_expiry_timer(State#q{expires = Expires});
        undefined        -> State
    end.

declare(Recover, From,
        State = #q{q = Q = #amqqueue{name = QName, durable = IsDurable},
                   backing_queue = BQ, backing_queue_state = undefined,
                   stats_timer = StatsTimer}) ->
    case rabbit_amqqueue:internal_declare(Q, Recover) of
        not_found -> {stop, normal, not_found, State};
        Q         -> gen_server2:reply(From, {new, Q}),
                     ok = file_handle_cache:register_callback(
                            rabbit_amqqueue, set_maximum_since_use,
                            [self()]),
                     ok = rabbit_memory_monitor:register(
                            self(), {rabbit_amqqueue,
                                     set_ram_duration_target, [self()]}),
                     BQS = BQ:init(QName, IsDurable, Recover),
                     State1 = init_expires(State#q{backing_queue_state = BQS}),
                     rabbit_event:notify(queue_created,
                                         infos(?CREATION_EVENT_KEYS, State1)),
                     rabbit_event:if_enabled(StatsTimer,
                                             fun() -> emit_stats(State1) end),
                     noreply(State1);
        Q1        -> {stop, normal, {existing, Q1}, State}
    end.

terminate_shutdown(Fun, State) ->
    State1 = #q{backing_queue = BQ, backing_queue_state = BQS} =
        stop_sync_timer(stop_rate_timer(State)),
    case BQS of
        undefined -> State;
        _         -> ok = rabbit_memory_monitor:deregister(self()),
                     BQS1 = lists:foldl(
                              fun (#cr{txn = none}, BQSN) ->
                                      BQSN;
                                  (#cr{txn = Txn}, BQSN) ->
                                      {_AckTags, BQSN1} =
                                          BQ:tx_rollback(Txn, BQSN),
                                      BQSN1
                              end, BQS, all_ch_record()),
                     rabbit_event:notify(queue_deleted, [{pid, self()}]),
                     State1#q{backing_queue_state = Fun(BQS1)}
    end.

reply(Reply, NewState) ->
    assert_invariant(NewState),
    {NewState1, Timeout} = next_state(NewState),
    {reply, Reply, NewState1, Timeout}.

noreply(NewState) ->
    assert_invariant(NewState),
    {NewState1, Timeout} = next_state(NewState),
    {noreply, NewState1, Timeout}.

next_state(State) ->
    State1 = #q{backing_queue = BQ, backing_queue_state = BQS} =
        ensure_rate_timer(State),
    State2 = ensure_stats_timer(State1),
    case BQ:needs_idle_timeout(BQS)of
        true  -> {ensure_sync_timer(State2), 0};
        false -> {stop_sync_timer(State2), hibernate}
    end.

ensure_sync_timer(State = #q{sync_timer_ref = undefined, backing_queue = BQ}) ->
    {ok, TRef} = timer:apply_after(
                   ?SYNC_INTERVAL,
                   rabbit_amqqueue, maybe_run_queue_via_backing_queue,
                   [self(), fun (BQS) -> BQ:idle_timeout(BQS) end]),
    State#q{sync_timer_ref = TRef};
ensure_sync_timer(State) ->
    State.

stop_sync_timer(State = #q{sync_timer_ref = undefined}) ->
    State;
stop_sync_timer(State = #q{sync_timer_ref = TRef}) ->
    {ok, cancel} = timer:cancel(TRef),
    State#q{sync_timer_ref = undefined}.

ensure_rate_timer(State = #q{rate_timer_ref = undefined}) ->
    {ok, TRef} = timer:apply_after(
                   ?RAM_DURATION_UPDATE_INTERVAL,
                   rabbit_amqqueue, update_ram_duration,
                   [self()]),
    State#q{rate_timer_ref = TRef};
ensure_rate_timer(State = #q{rate_timer_ref = just_measured}) ->
    State#q{rate_timer_ref = undefined};
ensure_rate_timer(State) ->
    State.

stop_rate_timer(State = #q{rate_timer_ref = undefined}) ->
    State;
stop_rate_timer(State = #q{rate_timer_ref = just_measured}) ->
    State#q{rate_timer_ref = undefined};
stop_rate_timer(State = #q{rate_timer_ref = TRef}) ->
    {ok, cancel} = timer:cancel(TRef),
    State#q{rate_timer_ref = undefined}.

stop_expiry_timer(State = #q{expiry_timer_ref = undefined}) ->
    State;
stop_expiry_timer(State = #q{expiry_timer_ref = TRef}) ->
    {ok, cancel} = timer:cancel(TRef),
    State#q{expiry_timer_ref = undefined}.

%% We only wish to expire where there are no consumers *and* when
%% basic.get hasn't been called for the configured period.
ensure_expiry_timer(State = #q{expires = undefined}) ->
    State;
ensure_expiry_timer(State = #q{expires = Expires}) ->
    case is_unused(State) of
        true ->
            NewState = stop_expiry_timer(State),
            {ok, TRef} = timer:apply_after(
                           Expires, rabbit_amqqueue, maybe_expire, [self()]),
            NewState#q{expiry_timer_ref = TRef};
        false ->
            State
    end.

ensure_stats_timer(State = #q{stats_timer = StatsTimer,
                              q = Q}) ->
    State#q{stats_timer = rabbit_event:ensure_stats_timer(
                            StatsTimer,
                            fun() -> rabbit_amqqueue:emit_stats(Q) end)}.

assert_invariant(#q{active_consumers = AC,
                    backing_queue = BQ, backing_queue_state = BQS}) ->
    true = (queue:is_empty(AC) orelse BQ:is_empty(BQS)).

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
                    acktags = sets:new(),
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

deliver_msgs_to_consumers(Funs = {PredFun, DeliverFun}, FunAcc,
                          State = #q{q = #amqqueue{name = QName},
                                     active_consumers = ActiveConsumers,
                                     blocked_consumers = BlockedConsumers}) ->
    case queue:out(ActiveConsumers) of
        {{value, QEntry = {ChPid, #consumer{tag = ConsumerTag,
                                            ack_required = AckRequired}}},
         ActiveConsumersTail} ->
            C = #cr{limiter_pid = LimiterPid,
                    unsent_message_count = Count,
                    acktags = ChAckTags} = ch_record(ChPid),
            IsMsgReady = PredFun(FunAcc, State),
            case (IsMsgReady andalso
                  rabbit_limiter:can_send( LimiterPid, self(), AckRequired )) of
                true ->
                    {{Message, IsDelivered, AckTag}, FunAcc1, State1} =
                        DeliverFun(AckRequired, FunAcc, State),
                    rabbit_channel:deliver(
                      ChPid, ConsumerTag, AckRequired,
                      {QName, self(), AckTag, IsDelivered, Message}),
                    ChAckTags1 = case AckRequired of
                                     true  -> sets:add_element(
                                                AckTag, ChAckTags);
                                     false -> ChAckTags
                                 end,
                    NewC = C#cr{unsent_message_count = Count + 1,
                                acktags = ChAckTags1},
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
                    State2 = State1#q{
                               active_consumers = NewActiveConsumers,
                               blocked_consumers = NewBlockedConsumers},
                    deliver_msgs_to_consumers(Funs, FunAcc1, State2);
                %% if IsMsgReady then we've hit the limiter
                false when IsMsgReady ->
                    store_ch_record(C#cr{is_limit_active = true}),
                    {NewActiveConsumers, NewBlockedConsumers} =
                        move_consumers(ChPid,
                                       ActiveConsumers,
                                       BlockedConsumers),
                    deliver_msgs_to_consumers(
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

deliver_from_queue_pred(IsEmpty, _State) ->
    not IsEmpty.

deliver_from_queue_deliver(AckRequired, false,
                           State = #q{backing_queue = BQ,
                                      backing_queue_state = BQS}) ->
    {{Message, IsDelivered, AckTag, Remaining}, BQS1} =
        BQ:fetch(AckRequired, BQS),
    {{Message, IsDelivered, AckTag}, 0 == Remaining,
     State #q { backing_queue_state = BQS1 }}.

run_message_queue(State = #q{backing_queue = BQ, backing_queue_state = BQS}) ->
    Funs = {fun deliver_from_queue_pred/2,
            fun deliver_from_queue_deliver/3},
    IsEmpty = BQ:is_empty(BQS),
    {_IsEmpty1, State1} = deliver_msgs_to_consumers(Funs, IsEmpty, State),
    State1.

attempt_delivery(none, _ChPid, Message, State = #q{backing_queue = BQ}) ->
    PredFun = fun (IsEmpty, _State) -> not IsEmpty end,
    DeliverFun =
        fun (AckRequired, false, State1 = #q{backing_queue_state = BQS}) ->
                {AckTag, BQS1} =
                    BQ:publish_delivered(AckRequired, Message, BQS),
                {{Message, false, AckTag}, true,
                 State1#q{backing_queue_state = BQS1}}
        end,
    deliver_msgs_to_consumers({ PredFun, DeliverFun }, false, State);
attempt_delivery(Txn, ChPid, Message, State = #q{backing_queue = BQ,
                                                 backing_queue_state = BQS}) ->
    record_current_channel_tx(ChPid, Txn),
    {true, State#q{backing_queue_state = BQ:tx_publish(Txn, Message, BQS)}}.

deliver_or_enqueue(Txn, ChPid, Message, State = #q{backing_queue = BQ}) ->
    case attempt_delivery(Txn, ChPid, Message, State) of
        {true, NewState} ->
            {true, NewState};
        {false, NewState} ->
            %% Txn is none and no unblocked channels with consumers
            BQS = BQ:publish(Message, State #q.backing_queue_state),
            {false, NewState#q{backing_queue_state = BQS}}
    end.

requeue_and_run(AckTags, State = #q{backing_queue = BQ}) ->
    maybe_run_queue_via_backing_queue(
      fun (BQS) -> BQ:requeue(AckTags, BQS) end, State).

add_consumer(ChPid, Consumer, Queue) -> queue:in({ChPid, Consumer}, Queue).

remove_consumer(ChPid, ConsumerTag, Queue) ->
    queue:filter(fun ({CP, #consumer{tag = CT}}) ->
                         (CP /= ChPid) or (CT /= ConsumerTag)
                 end, Queue).

remove_consumers(ChPid, Queue) ->
    queue:filter(fun ({CP, _}) -> CP /= ChPid end, Queue).

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
                unblock -> {NewBlockedConsumers, NewActiveConsumers} =
                               move_consumers(ChPid,
                                              State#q.blocked_consumers,
                                              State#q.active_consumers),
                           run_message_queue(
                             State#q{active_consumers = NewActiveConsumers,
                                     blocked_consumers = NewBlockedConsumers})
            end
    end.

should_auto_delete(#q{q = #amqqueue{auto_delete = false}}) -> false;
should_auto_delete(#q{has_had_consumers = false}) -> false;
should_auto_delete(State) -> is_unused(State).

handle_ch_down(DownPid, State = #q{exclusive_consumer = Holder}) ->
    case lookup_ch(DownPid) of
        not_found ->
            {ok, State};
        #cr{monitor_ref = MonitorRef, ch_pid = ChPid, txn = Txn,
            acktags = ChAckTags} ->
            erlang:demonitor(MonitorRef),
            erase({ch, ChPid}),
            State1 = State#q{
                       exclusive_consumer = case Holder of
                                                {ChPid, _} -> none;
                                                Other      -> Other
                                            end,
                       active_consumers = remove_consumers(
                                            ChPid, State#q.active_consumers),
                       blocked_consumers = remove_consumers(
                                             ChPid, State#q.blocked_consumers)},
            case should_auto_delete(State1) of
                true  -> {stop, State1};
                false -> State2 = case Txn of
                                      none -> State1;
                                      _    -> rollback_transaction(Txn, ChPid,
                                                                   State1)
                                  end,
                         {ok, requeue_and_run(sets:to_list(ChAckTags),
                                              ensure_expiry_timer(State2))}
            end
    end.

cancel_holder(ChPid, ConsumerTag, {ChPid, ConsumerTag}) ->
    none;
cancel_holder(_ChPid, _ConsumerTag, Holder) ->
    Holder.

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

maybe_run_queue_via_backing_queue(Fun, State = #q{backing_queue_state = BQS}) ->
    run_message_queue(State#q{backing_queue_state = Fun(BQS)}).

commit_transaction(Txn, From, ChPid, State = #q{backing_queue = BQ,
                                                backing_queue_state = BQS}) ->
    {AckTags, BQS1} =
        BQ:tx_commit(Txn, fun () -> gen_server2:reply(From, ok) end, BQS),
    %% ChPid must be known here because of the participant management
    %% by the channel.
    C = #cr{acktags = ChAckTags} = lookup_ch(ChPid),
    ChAckTags1 = subtract_acks(ChAckTags, AckTags),
    store_ch_record(C#cr{acktags = ChAckTags1, txn = none}),
    State#q{backing_queue_state = BQS1}.

rollback_transaction(Txn, ChPid, State = #q{backing_queue = BQ,
                                            backing_queue_state = BQS}) ->
    {_AckTags, BQS1} = BQ:tx_rollback(Txn, BQS),
    %% Iff we removed acktags from the channel record on ack+txn then
    %% we would add them back in here (would also require ChPid)
    record_current_channel_tx(ChPid, none),
    State#q{backing_queue_state = BQS1}.

subtract_acks(A, B) when is_list(B) ->
    lists:foldl(fun sets:del_element/2, A, B).

infos(Items, State) -> [{Item, i(Item, State)} || Item <- Items].

i(name,        #q{q = #amqqueue{name        = Name}})       -> Name;
i(durable,     #q{q = #amqqueue{durable     = Durable}})    -> Durable;
i(auto_delete, #q{q = #amqqueue{auto_delete = AutoDelete}}) -> AutoDelete;
i(arguments,   #q{q = #amqqueue{arguments   = Arguments}})  -> Arguments;
i(pid, _) ->
    self();
i(owner_pid, #q{q = #amqqueue{exclusive_owner = none}}) ->
    '';
i(owner_pid, #q{q = #amqqueue{exclusive_owner = ExclusiveOwner}}) ->
    ExclusiveOwner;
i(exclusive_consumer_pid, #q{exclusive_consumer = none}) ->
    '';
i(exclusive_consumer_pid, #q{exclusive_consumer = {ChPid, _ConsumerTag}}) ->
    ChPid;
i(exclusive_consumer_tag, #q{exclusive_consumer = none}) ->
    '';
i(exclusive_consumer_tag, #q{exclusive_consumer = {_ChPid, ConsumerTag}}) ->
    ConsumerTag;
i(messages_ready, #q{backing_queue_state = BQS, backing_queue = BQ}) ->
    BQ:len(BQS);
i(messages_unacknowledged, _) ->
    lists:sum([sets:size(C#cr.acktags) || C <- all_ch_record()]);
i(messages, State) ->
    lists:sum([i(Item, State) || Item <- [messages_ready,
                                          messages_unacknowledged]]);
i(consumers, State) ->
    queue:len(State#q.active_consumers) + queue:len(State#q.blocked_consumers);
i(memory, _) ->
    {memory, M} = process_info(self(), memory),
    M;
i(backing_queue_status, #q{backing_queue_state = BQS, backing_queue = BQ}) ->
    BQ:status(BQS);
i(Item, _) ->
    throw({bad_argument, Item}).

emit_stats(State) ->
    rabbit_event:notify(queue_stats, infos(?STATISTICS_KEYS, State)).

%---------------------------------------------------------------------------

handle_call({init, Recover}, From,
            State = #q{q = #amqqueue{exclusive_owner = none}}) ->
    declare(Recover, From, State);

handle_call({init, Recover}, From,
            State = #q{q = #amqqueue{exclusive_owner = Owner}}) ->
    case rpc:call(node(Owner), erlang, is_process_alive, [Owner]) of
        true -> erlang:monitor(process, Owner),
                declare(Recover, From, State);
        _    -> #q{q = #amqqueue{name = QName, durable = IsDurable},
                   backing_queue = BQ, backing_queue_state = undefined} = State,
                gen_server2:reply(From, not_found),
                case Recover of
                    true -> ok;
                    _    -> rabbit_log:warning(
                              "Queue ~p exclusive owner went away~n", [QName])
                end,
                BQS = BQ:init(QName, IsDurable, Recover),
                %% Rely on terminate to delete the queue.
                {stop, normal, State#q{backing_queue_state = BQS}}
    end;

handle_call(info, _From, State) ->
    reply(infos(?INFO_KEYS, State), State);

handle_call({info, Items}, _From, State) ->
    try
        reply({ok, infos(Items, State)}, State)
    catch Error -> reply({error, Error}, State)
    end;

handle_call(consumers, _From,
            State = #q{active_consumers = ActiveConsumers,
                       blocked_consumers = BlockedConsumers}) ->
    reply(rabbit_misc:queue_fold(
            fun ({ChPid, #consumer{tag = ConsumerTag,
                                   ack_required = AckRequired}}, Acc) ->
                    [{ChPid, ConsumerTag, AckRequired} | Acc]
            end, [], queue:join(ActiveConsumers, BlockedConsumers)), State);

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
    {Delivered, NewState} = attempt_delivery(Txn, ChPid, Message, State),
    reply(Delivered, NewState);

handle_call({deliver, Txn, Message, ChPid}, _From, State) ->
    %% Synchronous, "mandatory" delivery mode
    {Delivered, NewState} = deliver_or_enqueue(Txn, ChPid, Message, State),
    reply(Delivered, NewState);

handle_call({commit, Txn, ChPid}, From, State) ->
    NewState = commit_transaction(Txn, From, ChPid, State),
    noreply(run_message_queue(NewState));

handle_call({notify_down, ChPid}, _From, State) ->
    %% we want to do this synchronously, so that auto_deleted queues
    %% are no longer visible by the time we send a response to the
    %% client.  The queue is ultimately deleted in terminate/2; if we
    %% return stop with a reply, terminate/2 will be called by
    %% gen_server2 *before* the reply is sent.
    case handle_ch_down(ChPid, State) of
        {ok, NewState}   -> reply(ok, NewState);
        {stop, NewState} -> {stop, normal, ok, NewState}
    end;

handle_call({basic_get, ChPid, NoAck}, _From,
            State = #q{q = #amqqueue{name = QName},
                       backing_queue_state = BQS, backing_queue = BQ}) ->
    AckRequired = not NoAck,
    State1 = ensure_expiry_timer(State),
    case BQ:fetch(AckRequired, BQS) of
        {empty, BQS1} -> reply(empty, State1#q{backing_queue_state = BQS1});
        {{Message, IsDelivered, AckTag, Remaining}, BQS1} ->
            case AckRequired of
                true ->  C = #cr{acktags = ChAckTags} = ch_record(ChPid),
                         store_ch_record(
                           C#cr{acktags = sets:add_element(AckTag, ChAckTags)});
                false -> ok
            end,
            Msg = {QName, self(), AckTag, IsDelivered, Message},
            reply({ok, Remaining, Msg}, State1#q{backing_queue_state = BQS1})
    end;

handle_call({basic_consume, NoAck, ChPid, LimiterPid,
             ConsumerTag, ExclusiveConsume, OkMsg},
            _From, State = #q{exclusive_consumer = ExistingHolder}) ->
    case check_exclusive_access(ExistingHolder, ExclusiveConsume,
                                State) of
        in_use ->
            reply({error, exclusive_consume_unavailable}, State);
        ok ->
            C = #cr{consumer_count = ConsumerCount} = ch_record(ChPid),
            Consumer = #consumer{tag = ConsumerTag,
                                 ack_required = not NoAck},
            store_ch_record(C#cr{consumer_count = ConsumerCount +1,
                                 limiter_pid = LimiterPid}),
            ok = case ConsumerCount of
                     0 -> rabbit_limiter:register(LimiterPid, self());
                     _ -> ok
                 end,
            ExclusiveConsumer = if ExclusiveConsume -> {ChPid, ConsumerTag};
                                   true             -> ExistingHolder
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
    end;

handle_call({basic_cancel, ChPid, ConsumerTag, OkMsg}, _From,
            State = #q{exclusive_consumer = Holder}) ->
    case lookup_ch(ChPid) of
        not_found ->
            ok = maybe_send_reply(ChPid, OkMsg),
            reply(ok, State);
        C = #cr{consumer_count = ConsumerCount, limiter_pid = LimiterPid} ->
            store_ch_record(C#cr{consumer_count = ConsumerCount - 1}),
            case ConsumerCount of
                1 -> ok = rabbit_limiter:unregister(LimiterPid, self());
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
                false -> reply(ok, ensure_expiry_timer(NewState));
                true  -> {stop, normal, ok, NewState}
            end
    end;

handle_call(stat, _From, State = #q{backing_queue = BQ,
                                    backing_queue_state = BQS,
                                    active_consumers = ActiveConsumers}) ->
    reply({ok, BQ:len(BQS), queue:len(ActiveConsumers)}, State);

handle_call({delete, IfUnused, IfEmpty}, _From,
            State = #q{backing_queue_state = BQS, backing_queue = BQ}) ->
    IsEmpty = BQ:is_empty(BQS),
    IsUnused = is_unused(State),
    if
        IfEmpty and not(IsEmpty) ->
            reply({error, not_empty}, State);
        IfUnused and not(IsUnused) ->
            reply({error, in_use}, State);
        true ->
            {stop, normal, {ok, BQ:len(BQS)}, State}
    end;

handle_call(purge, _From, State = #q{backing_queue = BQ,
                                     backing_queue_state = BQS}) ->
    {Count, BQS1} = BQ:purge(BQS),
    reply({ok, Count}, State#q{backing_queue_state = BQS1});

handle_call({requeue, AckTags, ChPid}, From, State) ->
    gen_server2:reply(From, ok),
    case lookup_ch(ChPid) of
        not_found ->
            noreply(State);
        C = #cr{acktags = ChAckTags} ->
            ChAckTags1 = subtract_acks(ChAckTags, AckTags),
            store_ch_record(C#cr{acktags = ChAckTags1}),
            noreply(requeue_and_run(AckTags, State))
    end;

handle_call({maybe_run_queue_via_backing_queue, Fun}, _From, State) ->
    reply(ok, maybe_run_queue_via_backing_queue(Fun, State)).

handle_cast({deliver, Txn, Message, ChPid}, State) ->
    %% Asynchronous, non-"mandatory", non-"immediate" deliver mode.
    {_Delivered, NewState} = deliver_or_enqueue(Txn, ChPid, Message, State),
    noreply(NewState);

handle_cast({ack, Txn, AckTags, ChPid},
            State = #q{backing_queue = BQ, backing_queue_state = BQS}) ->
    case lookup_ch(ChPid) of
        not_found ->
            noreply(State);
        C = #cr{acktags = ChAckTags} ->
            {C1, BQS1} =
                case Txn of
                    none -> ChAckTags1 = subtract_acks(ChAckTags, AckTags),
                            {C#cr{acktags = ChAckTags1}, BQ:ack(AckTags, BQS)};
                    _    -> {C#cr{txn = Txn}, BQ:tx_ack(Txn, AckTags, BQS)}
                end,
            store_ch_record(C1),
            noreply(State#q{backing_queue_state = BQS1})
    end;

handle_cast({reject, AckTags, Requeue, ChPid},
            State = #q{backing_queue = BQ, backing_queue_state = BQS}) ->
    case lookup_ch(ChPid) of
        not_found ->
            noreply(State);
        C = #cr{acktags = ChAckTags} ->
            ChAckTags1 = subtract_acks(ChAckTags, AckTags),
            store_ch_record(C#cr{acktags = ChAckTags1}),
            noreply(case Requeue of
                        true  -> requeue_and_run(AckTags, State);
                        false -> BQS1 = BQ:ack(AckTags, BQS),
                                 State #q { backing_queue_state = BQS1 }
                    end)
    end;

handle_cast({rollback, Txn, ChPid}, State) ->
    noreply(rollback_transaction(Txn, ChPid, State));

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

handle_cast({flush, ChPid}, State) ->
    ok = rabbit_channel:flushed(ChPid, self()),
    noreply(State);

handle_cast(update_ram_duration, State = #q{backing_queue = BQ,
                                            backing_queue_state = BQS}) ->
    {RamDuration, BQS1} = BQ:ram_duration(BQS),
    DesiredDuration =
        rabbit_memory_monitor:report_ram_duration(self(), RamDuration),
    BQS2 = BQ:set_ram_duration_target(DesiredDuration, BQS1),
    noreply(State#q{rate_timer_ref = just_measured,
                    backing_queue_state = BQS2});

handle_cast({set_ram_duration_target, Duration},
            State = #q{backing_queue = BQ, backing_queue_state = BQS}) ->
    BQS1 = BQ:set_ram_duration_target(Duration, BQS),
    noreply(State#q{backing_queue_state = BQS1});

handle_cast({set_maximum_since_use, Age}, State) ->
    ok = file_handle_cache:set_maximum_since_use(Age),
    noreply(State);

handle_cast(maybe_expire, State) ->
    case is_unused(State) of
        true  -> ?LOGDEBUG("Queue lease expired for ~p~n", [State#q.q]),
                 {stop, normal, State};
        false -> noreply(ensure_expiry_timer(State))
    end;

handle_cast(emit_stats, State = #q{stats_timer = StatsTimer}) ->
    %% Do not invoke noreply as it would see no timer and create a new one.
    emit_stats(State),
    State1 = State#q{stats_timer = rabbit_event:reset_stats_timer(StatsTimer)},
    assert_invariant(State1),
    {noreply, State1}.

handle_info({'DOWN', _MonitorRef, process, DownPid, _Reason},
            State = #q{q = #amqqueue{exclusive_owner = DownPid}}) ->
    %% Exclusively owned queues must disappear with their owner.  In
    %% the case of clean shutdown we delete the queue synchronously in
    %% the reader - although not required by the spec this seems to
    %% match what people expect (see bug 21824). However we need this
    %% monitor-and-async- delete in case the connection goes away
    %% unexpectedly.
    {stop, normal, State};
handle_info({'DOWN', _MonitorRef, process, DownPid, _Reason}, State) ->
    case handle_ch_down(DownPid, State) of
        {ok, NewState}   -> noreply(NewState);
        {stop, NewState} -> {stop, normal, NewState}
    end;

handle_info(timeout, State = #q{backing_queue = BQ}) ->
    noreply(maybe_run_queue_via_backing_queue(
              fun (BQS) -> BQ:idle_timeout(BQS) end, State));

handle_info({'EXIT', _Pid, Reason}, State) ->
    {stop, Reason, State};

handle_info(Info, State) ->
    ?LOGDEBUG("Info in queue: ~p~n", [Info]),
    {stop, {unhandled_info, Info}, State}.

handle_pre_hibernate(State = #q{backing_queue_state = undefined}) ->
    {hibernate, State};
handle_pre_hibernate(State = #q{backing_queue = BQ,
                                backing_queue_state = BQS,
                                stats_timer = StatsTimer}) ->
    BQS1 = BQ:handle_pre_hibernate(BQS),
    %% no activity for a while == 0 egress and ingress rates
    DesiredDuration =
        rabbit_memory_monitor:report_ram_duration(self(), infinity),
    BQS2 = BQ:set_ram_duration_target(DesiredDuration, BQS1),
    rabbit_event:if_enabled(StatsTimer, fun () -> emit_stats(State) end),
    State1 = State#q{stats_timer = rabbit_event:stop_stats_timer(StatsTimer),
                     backing_queue_state = BQS2},
    {hibernate, stop_rate_timer(State1)}.
