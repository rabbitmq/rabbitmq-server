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

-module(rabbit_amqqueue_process).
-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-behaviour(gen_server2).

-define(UNSENT_MESSAGE_LIMIT,          200).
-define(SYNC_INTERVAL,                 25). %% milliseconds
-define(RAM_DURATION_UPDATE_INTERVAL,  5000).

-export([start_link/1, info_keys/0]).

-export([init_with_backing_queue_state/7]).

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2, handle_pre_hibernate/1, prioritise_call/3,
         prioritise_cast/2, prioritise_info/2, format_message_queue/2]).

%% Queue's state
-record(q, {q,
            exclusive_consumer,
            has_had_consumers,
            backing_queue,
            backing_queue_state,
            active_consumers,
            expires,
            sync_timer_ref,
            rate_timer_ref,
            expiry_timer_ref,
            stats_timer,
            msg_id_to_channel,
            ttl,
            ttl_timer_ref,
            ttl_timer_expiry,
            senders,
            publish_seqno,
            unconfirmed,
            delayed_stop,
            queue_monitors,
            dlx,
            dlx_routing_key,
            status
           }).

-record(consumer, {tag, ack_required}).

%% These are held in our process dictionary
-record(cr, {ch_pid,
             monitor_ref,
             acktags,
             consumer_count,
             blocked_consumers,
             limiter,
             is_limit_active,
             unsent_message_count}).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/1 ::
        (rabbit_types:amqqueue()) -> rabbit_types:ok_pid_or_error()).
-spec(info_keys/0 :: () -> rabbit_types:info_keys()).
-spec(init_with_backing_queue_state/7 ::
        (rabbit_types:amqqueue(), atom(), tuple(), any(),
         [rabbit_types:delivery()], pmon:pmon(), dict()) -> #q{}).

-endif.

%%----------------------------------------------------------------------------

-define(STATISTICS_KEYS,
        [name,
         policy,
         exclusive_consumer_pid,
         exclusive_consumer_tag,
         messages_ready,
         messages_unacknowledged,
         messages,
         consumers,
         memory,
         slave_pids,
         synchronised_slave_pids,
         backing_queue_status,
         status
        ]).

-define(CREATION_EVENT_KEYS,
        [name,
         durable,
         auto_delete,
         arguments,
         owner_pid
        ]).

-define(INFO_KEYS, [pid | ?CREATION_EVENT_KEYS ++ ?STATISTICS_KEYS -- [name]]).

%%----------------------------------------------------------------------------

start_link(Q) -> gen_server2:start_link(?MODULE, Q, []).

info_keys() -> ?INFO_KEYS.

%%----------------------------------------------------------------------------

init(Q) ->
    process_flag(trap_exit, true),
    {ok, init_state(Q#amqqueue{pid = self()}), hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

init_with_backing_queue_state(Q = #amqqueue{exclusive_owner = Owner}, BQ, BQS,
                              RateTRef, Deliveries, Senders, MTC) ->
    case Owner of
        none -> ok;
        _    -> erlang:monitor(process, Owner)
    end,
    State = init_state(Q),
    State1 = State#q{backing_queue       = BQ,
                     backing_queue_state = BQS,
                     rate_timer_ref      = RateTRef,
                     senders             = Senders,
                     msg_id_to_channel   = MTC},
    State2 = process_args(State1),
    lists:foldl(fun (Delivery, StateN) ->
                        deliver_or_enqueue(Delivery, true, StateN)
                end, State2, Deliveries).

init_state(Q) ->
    State = #q{q                   = Q,
               exclusive_consumer  = none,
               has_had_consumers   = false,
               active_consumers    = queue:new(),
               senders             = pmon:new(),
               publish_seqno       = 1,
               unconfirmed         = dtree:empty(),
               queue_monitors      = pmon:new(),
               msg_id_to_channel   = gb_trees:empty(),
               status              = running},
    rabbit_event:init_stats_timer(State, #q.stats_timer).

terminate(shutdown = R,      State = #q{backing_queue = BQ}) ->
    terminate_shutdown(fun (BQS) -> BQ:terminate(R, BQS) end, State);
terminate({shutdown, _} = R, State = #q{backing_queue = BQ}) ->
    terminate_shutdown(fun (BQS) -> BQ:terminate(R, BQS) end, State);
terminate(Reason,            State = #q{q             = #amqqueue{name = QName},
                                        backing_queue = BQ}) ->
    terminate_shutdown(
      fun (BQS) ->
              BQS1 = BQ:delete_and_terminate(Reason, BQS),
              %% don't care if the internal delete doesn't return 'ok'.
              rabbit_amqqueue:internal_delete(QName),
              BQS1
      end, State).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

declare(Recover, From, State = #q{q                   = Q,
                                  backing_queue       = undefined,
                                  backing_queue_state = undefined}) ->
    case rabbit_amqqueue:internal_declare(Q, Recover =/= new) of
        #amqqueue{} = Q1 ->
            case matches(Recover, Q, Q1) of
                true ->
                    gen_server2:reply(From, {new, Q}),
                    ok = file_handle_cache:register_callback(
                           rabbit_amqqueue, set_maximum_since_use, [self()]),
                    ok = rabbit_memory_monitor:register(
                           self(), {rabbit_amqqueue,
                                    set_ram_duration_target, [self()]}),
                    BQ = backing_queue_module(Q1),
                    BQS = bq_init(BQ, Q, Recover),
                    recovery_barrier(Recover),
                    State1 = process_args(State#q{backing_queue       = BQ,
                                                  backing_queue_state = BQS}),
                    rabbit_event:notify(queue_created,
                                        infos(?CREATION_EVENT_KEYS, State1)),
                    rabbit_event:if_enabled(State1, #q.stats_timer,
                                            fun() -> emit_stats(State1) end),
                    noreply(State1);
                false ->
                    {stop, normal, {existing, Q1}, State}
            end;
        Err ->
            {stop, normal, Err, State}
    end.

matches(new, Q1, Q2) ->
    %% i.e. not policy
    Q1#amqqueue.name            =:= Q2#amqqueue.name            andalso
    Q1#amqqueue.durable         =:= Q2#amqqueue.durable         andalso
    Q1#amqqueue.auto_delete     =:= Q2#amqqueue.auto_delete     andalso
    Q1#amqqueue.exclusive_owner =:= Q2#amqqueue.exclusive_owner andalso
    Q1#amqqueue.arguments       =:= Q2#amqqueue.arguments       andalso
    Q1#amqqueue.pid             =:= Q2#amqqueue.pid             andalso
    Q1#amqqueue.slave_pids      =:= Q2#amqqueue.slave_pids;
matches(_,  Q,   Q) -> true;
matches(_, _Q, _Q1) -> false.

bq_init(BQ, Q, Recover) ->
    Self = self(),
    BQ:init(Q, Recover =/= new,
            fun (Mod, Fun) ->
                    rabbit_amqqueue:run_backing_queue(Self, Mod, Fun)
            end).

recovery_barrier(new) ->
    ok;
recovery_barrier(BarrierPid) ->
    MRef = erlang:monitor(process, BarrierPid),
    receive
        {BarrierPid, go}              -> erlang:demonitor(MRef, [flush]);
        {'DOWN', MRef, process, _, _} -> ok
    end.

process_args(State = #q{q = #amqqueue{arguments = Arguments}}) ->
    lists:foldl(
      fun({Arg, Fun}, State1) ->
              case rabbit_misc:table_lookup(Arguments, Arg) of
                  {_Type, Val} -> Fun(Val, State1);
                  undefined    -> State1
              end
      end, State,
      [{<<"x-expires">>,                 fun init_expires/2},
       {<<"x-dead-letter-exchange">>,    fun init_dlx/2},
       {<<"x-dead-letter-routing-key">>, fun init_dlx_routing_key/2},
       {<<"x-message-ttl">>,             fun init_ttl/2}]).

init_expires(Expires, State) -> ensure_expiry_timer(State#q{expires = Expires}).

init_ttl(TTL, State) -> drop_expired_msgs(State#q{ttl = TTL}).

init_dlx(DLX, State = #q{q = #amqqueue{name = QName}}) ->
    State#q{dlx = rabbit_misc:r(QName, exchange, DLX)}.

init_dlx_routing_key(RoutingKey, State) ->
    State#q{dlx_routing_key = RoutingKey}.

terminate_shutdown(Fun, State) ->
    State1 = #q{backing_queue_state = BQS} =
        lists:foldl(fun (F, S) -> F(S) end, State,
                    [fun stop_sync_timer/1,
                     fun stop_rate_timer/1,
                     fun stop_expiry_timer/1,
                     fun stop_ttl_timer/1]),
    case BQS of
        undefined -> State1;
        _         -> ok = rabbit_memory_monitor:deregister(self()),
                     QName = qname(State),
                     [emit_consumer_deleted(Ch, CTag, QName)
                      || {Ch, CTag, _} <- consumers(State1)],
                     State1#q{backing_queue_state = Fun(BQS)}
    end.

reply(Reply, NewState) ->
    {NewState1, Timeout} = next_state(NewState),
    {reply, Reply, ensure_stats_timer(ensure_rate_timer(NewState1)), Timeout}.

noreply(NewState) ->
    {NewState1, Timeout} = next_state(NewState),
    {noreply, ensure_stats_timer(ensure_rate_timer(NewState1)), Timeout}.

next_state(State = #q{backing_queue = BQ, backing_queue_state = BQS}) ->
    assert_invariant(State),
    {MsgIds, BQS1} = BQ:drain_confirmed(BQS),
    State1 = confirm_messages(MsgIds, State#q{backing_queue_state = BQS1}),
    case BQ:needs_timeout(BQS1) of
        false -> {stop_sync_timer(State1),   hibernate     };
        idle  -> {stop_sync_timer(State1),   ?SYNC_INTERVAL};
        timed -> {ensure_sync_timer(State1), 0             }
    end.

backing_queue_module(Q) ->
    case rabbit_mirror_queue_misc:is_mirrored(Q) of
        false -> {ok, BQM} = application:get_env(backing_queue_module),
                 BQM;
        true  -> rabbit_mirror_queue_master
    end.

ensure_sync_timer(State) ->
    rabbit_misc:ensure_timer(State, #q.sync_timer_ref,
                             ?SYNC_INTERVAL, sync_timeout).

stop_sync_timer(State) -> rabbit_misc:stop_timer(State, #q.sync_timer_ref).

ensure_rate_timer(State) ->
    rabbit_misc:ensure_timer(State, #q.rate_timer_ref,
                             ?RAM_DURATION_UPDATE_INTERVAL,
                             update_ram_duration).

stop_rate_timer(State) -> rabbit_misc:stop_timer(State, #q.rate_timer_ref).

%% We wish to expire only when there are no consumers *and* the expiry
%% hasn't been refreshed (by queue.declare or basic.get) for the
%% configured period.
ensure_expiry_timer(State = #q{expires = undefined}) ->
    State;
ensure_expiry_timer(State = #q{expires = Expires}) ->
    case is_unused(State) of
        true  -> NewState = stop_expiry_timer(State),
                 rabbit_misc:ensure_timer(NewState, #q.expiry_timer_ref,
                                          Expires, maybe_expire);
        false -> State
    end.

stop_expiry_timer(State) -> rabbit_misc:stop_timer(State, #q.expiry_timer_ref).

ensure_ttl_timer(undefined, State) ->
    State;
ensure_ttl_timer(Expiry, State = #q{ttl_timer_ref = undefined}) ->
    After = (case Expiry - now_micros() of
                 V when V > 0 -> V + 999; %% always fire later
                 _            -> 0
             end) div 1000,
    TRef = erlang:send_after(After, self(), drop_expired),
    State#q{ttl_timer_ref = TRef, ttl_timer_expiry = Expiry};
ensure_ttl_timer(Expiry, State = #q{ttl_timer_ref    = TRef,
                                    ttl_timer_expiry = TExpiry})
  when Expiry + 1000 < TExpiry ->
    case erlang:cancel_timer(TRef) of
        false -> State;
        _     -> ensure_ttl_timer(Expiry, State#q{ttl_timer_ref = undefined})
    end;
ensure_ttl_timer(_Expiry, State) ->
    State.

stop_ttl_timer(State) -> rabbit_misc:stop_timer(State, #q.ttl_timer_ref).

ensure_stats_timer(State) ->
    rabbit_event:ensure_stats_timer(State, #q.stats_timer, emit_stats).

assert_invariant(State = #q{active_consumers = AC}) ->
    true = (queue:is_empty(AC) orelse is_empty(State)).

is_empty(#q{backing_queue = BQ, backing_queue_state = BQS}) -> BQ:is_empty(BQS).

lookup_ch(ChPid) ->
    case get({ch, ChPid}) of
        undefined -> not_found;
        C         -> C
    end.

ch_record(ChPid) ->
    Key = {ch, ChPid},
    case get(Key) of
        undefined -> MonitorRef = erlang:monitor(process, ChPid),
                     C = #cr{ch_pid               = ChPid,
                             monitor_ref          = MonitorRef,
                             acktags              = queue:new(),
                             consumer_count       = 0,
                             blocked_consumers    = queue:new(),
                             is_limit_active      = false,
                             limiter              = rabbit_limiter:make_token(),
                             unsent_message_count = 0},
                     put(Key, C),
                     C;
        C = #cr{} -> C
    end.

update_ch_record(C = #cr{consumer_count       = ConsumerCount,
                         acktags              = ChAckTags,
                         unsent_message_count = UnsentMessageCount}) ->
    case {queue:is_empty(ChAckTags), ConsumerCount, UnsentMessageCount} of
        {true, 0, 0} -> ok = erase_ch_record(C);
        _            -> ok = store_ch_record(C)
    end,
    C.

store_ch_record(C = #cr{ch_pid = ChPid}) ->
    put({ch, ChPid}, C),
    ok.

erase_ch_record(#cr{ch_pid      = ChPid,
                    limiter     = Limiter,
                    monitor_ref = MonitorRef}) ->
    ok = rabbit_limiter:unregister(Limiter, self()),
    erlang:demonitor(MonitorRef),
    erase({ch, ChPid}),
    ok.

update_consumer_count(C = #cr{consumer_count = 0, limiter = Limiter}, +1) ->
    ok = rabbit_limiter:register(Limiter, self()),
    update_ch_record(C#cr{consumer_count = 1});
update_consumer_count(C = #cr{consumer_count = 1, limiter = Limiter}, -1) ->
    ok = rabbit_limiter:unregister(Limiter, self()),
    update_ch_record(C#cr{consumer_count = 0,
                          limiter = rabbit_limiter:make_token()});
update_consumer_count(C = #cr{consumer_count = Count}, Delta) ->
    update_ch_record(C#cr{consumer_count = Count + Delta}).

all_ch_record() -> [C || {{ch, _}, C} <- get()].

block_consumer(C = #cr{blocked_consumers = Blocked}, QEntry) ->
    update_ch_record(C#cr{blocked_consumers = queue:in(QEntry, Blocked)}).

is_ch_blocked(#cr{unsent_message_count = Count, is_limit_active = Limited}) ->
    Limited orelse Count >= ?UNSENT_MESSAGE_LIMIT.

ch_record_state_transition(OldCR, NewCR) ->
    case {is_ch_blocked(OldCR), is_ch_blocked(NewCR)} of
        {true, false} -> unblock;
        {false, true} -> block;
        {_, _}        -> ok
    end.

deliver_msgs_to_consumers(_DeliverFun, true, State) ->
    {true, State};
deliver_msgs_to_consumers(DeliverFun, false,
                          State = #q{active_consumers = ActiveConsumers}) ->
    case queue:out(ActiveConsumers) of
        {empty, _} ->
            {false, State};
        {{value, QEntry}, Tail} ->
            {Stop, State1} = deliver_msg_to_consumer(
                               DeliverFun, QEntry,
                               State#q{active_consumers = Tail}),
            deliver_msgs_to_consumers(DeliverFun, Stop, State1)
    end.

deliver_msg_to_consumer(DeliverFun, E = {ChPid, Consumer}, State) ->
    C = ch_record(ChPid),
    case is_ch_blocked(C) of
        true  -> block_consumer(C, E),
                 {false, State};
        false -> case rabbit_limiter:can_send(C#cr.limiter, self(),
                                              Consumer#consumer.ack_required) of
                     false -> block_consumer(C#cr{is_limit_active = true}, E),
                              {false, State};
                     true  -> AC1 = queue:in(E, State#q.active_consumers),
                              deliver_msg_to_consumer(
                                DeliverFun, Consumer, C,
                                State#q{active_consumers = AC1})
                 end
    end.

deliver_msg_to_consumer(DeliverFun,
                        #consumer{tag          = ConsumerTag,
                                  ack_required = AckRequired},
                        C = #cr{ch_pid               = ChPid,
                                acktags              = ChAckTags,
                                unsent_message_count = Count},
                        State = #q{q = #amqqueue{name = QName}}) ->
    {{Message, IsDelivered, AckTag}, Stop, State1} =
        DeliverFun(AckRequired, State),
    rabbit_channel:deliver(ChPid, ConsumerTag, AckRequired,
                           {QName, self(), AckTag, IsDelivered, Message}),
    ChAckTags1 = case AckRequired of
                     true  -> queue:in(AckTag, ChAckTags);
                     false -> ChAckTags
                 end,
    update_ch_record(C#cr{acktags              = ChAckTags1,
                          unsent_message_count = Count + 1}),
    {Stop, State1}.

deliver_from_queue_deliver(AckRequired, State) ->
    {Result, State1} = fetch(AckRequired, State),
    {Result, is_empty(State1), State1}.

confirm_messages([], State) ->
    State;
confirm_messages(MsgIds, State = #q{msg_id_to_channel = MTC}) ->
    {CMs, MTC1} =
        lists:foldl(
          fun(MsgId, {CMs, MTC0}) ->
                  case gb_trees:lookup(MsgId, MTC0) of
                      {value, {SenderPid, MsgSeqNo}} ->
                          {rabbit_misc:gb_trees_cons(SenderPid,
                                                     MsgSeqNo, CMs),
                           gb_trees:delete(MsgId, MTC0)};
                      none ->
                          {CMs, MTC0}
                  end
          end, {gb_trees:empty(), MTC}, MsgIds),
    rabbit_misc:gb_trees_foreach(fun rabbit_misc:confirm_to_sender/2, CMs),
    State#q{msg_id_to_channel = MTC1}.

send_or_record_confirm(#delivery{msg_seq_no = undefined}, State) ->
    {never, State};
send_or_record_confirm(#delivery{sender     = SenderPid,
                                 msg_seq_no = MsgSeqNo,
                                 message    = #basic_message {
                                   is_persistent = true,
                                   id            = MsgId}},
                       State = #q{q                 = #amqqueue{durable = true},
                                  msg_id_to_channel = MTC}) ->
    MTC1 = gb_trees:insert(MsgId, {SenderPid, MsgSeqNo}, MTC),
    {eventually, State#q{msg_id_to_channel = MTC1}};
send_or_record_confirm(#delivery{sender     = SenderPid,
                                 msg_seq_no = MsgSeqNo}, State) ->
    rabbit_misc:confirm_to_sender(SenderPid, [MsgSeqNo]),
    {immediately, State}.

discard(#delivery{sender     = SenderPid,
                  msg_seq_no = MsgSeqNo,
                  message    = #basic_message{id = MsgId}}, State) ->
    State1 = #q{backing_queue = BQ, backing_queue_state = BQS} =
        case MsgSeqNo of
            undefined -> State;
            _         -> confirm_messages([MsgId], State)
        end,
    BQS1 = BQ:discard(MsgId, SenderPid, BQS),
    State1#q{backing_queue_state = BQS1}.

run_message_queue(State) ->
    {_IsEmpty1, State1} = deliver_msgs_to_consumers(
                            fun deliver_from_queue_deliver/2,
                            is_empty(State), State),
    State1.

attempt_delivery(Delivery = #delivery{sender = SenderPid, message = Message},
                 Props, Delivered, State = #q{backing_queue       = BQ,
                                              backing_queue_state = BQS}) ->
    case BQ:is_duplicate(Message, BQS) of
        {false, BQS1} ->
            deliver_msgs_to_consumers(
              fun (true, State1 = #q{backing_queue_state = BQS2}) ->
                      {AckTag, BQS3} = BQ:publish_delivered(
                                         Message, Props, SenderPid, BQS2),
                      {{Message, Delivered, AckTag},
                       true, State1#q{backing_queue_state = BQS3}};
                  (false, State1) ->
                      {{Message, Delivered, undefined},
                       true, discard(Delivery, State1)}
              end, false, State#q{backing_queue_state = BQS1});
        {published, BQS1} ->
            {true,  State#q{backing_queue_state = BQS1}};
        {discarded, BQS1} ->
            {false, State#q{backing_queue_state = BQS1}}
    end.

deliver_or_enqueue(Delivery = #delivery{message = Message, sender = SenderPid},
                   Delivered, State) ->
    {Confirm, State1} = send_or_record_confirm(Delivery, State),
    Props = message_properties(Message, Confirm, State),
    case attempt_delivery(Delivery, Props, Delivered, State1) of
        {true, State2} ->
            State2;
        %% The next one is an optimisation
        {false, State2 = #q{ttl = 0, dlx = undefined}} ->
            discard(Delivery, State2);
        {false, State2 = #q{backing_queue = BQ, backing_queue_state = BQS}} ->
            IsEmpty = BQ:is_empty(BQS),
            BQS1 = BQ:publish(Message, Props, Delivered, SenderPid, BQS),
            State3 = State2#q{backing_queue_state = BQS1},
            %% optimisation: it would be perfectly safe to always
            %% invoke drop_expired_msgs here, but that is expensive so
            %% we only do that IFF the new message ends up at the head
            %% of the queue (because the queue was empty) and has an
            %% expiry. Only then may it need expiring straight away,
            %% or, if expiry is not due yet, the expiry timer may need
            %% (re)scheduling.
            case {IsEmpty, Props#message_properties.expiry} of
                {false,         _} -> State3;
                {true,  undefined} -> State3;
                {true,          _} -> drop_expired_msgs(State3)
            end
    end.

requeue_and_run(AckTags, State = #q{backing_queue       = BQ,
                                    backing_queue_state = BQS}) ->
    {_MsgIds, BQS1} = BQ:requeue(AckTags, BQS),
    run_message_queue(drop_expired_msgs(State#q{backing_queue_state = BQS1})).

fetch(AckRequired, State = #q{backing_queue       = BQ,
                              backing_queue_state = BQS}) ->
    {Result, BQS1} = BQ:fetch(AckRequired, BQS),
    {Result, drop_expired_msgs(State#q{backing_queue_state = BQS1})}.

ack(AckTags, ChPid, State) ->
    subtract_acks(ChPid, AckTags, State,
                  fun (State1 = #q{backing_queue       = BQ,
                                   backing_queue_state = BQS}) ->
                          {_Guids, BQS1} = BQ:ack(AckTags, BQS),
                          State1#q{backing_queue_state = BQS1}
                  end).

requeue(AckTags, ChPid, State) ->
    subtract_acks(ChPid, AckTags, State,
                  fun (State1) -> requeue_and_run(AckTags, State1) end).

remove_consumer(ChPid, ConsumerTag, Queue) ->
    queue:filter(fun ({CP, #consumer{tag = CTag}}) ->
                         (CP /= ChPid) or (CTag /= ConsumerTag)
                 end, Queue).

remove_consumers(ChPid, Queue, QName) ->
    queue:filter(fun ({CP, #consumer{tag = CTag}}) when CP =:= ChPid ->
                         emit_consumer_deleted(ChPid, CTag, QName),
                         false;
                     (_) ->
                         true
                 end, Queue).

possibly_unblock(State, ChPid, Update) ->
    case lookup_ch(ChPid) of
        not_found ->
            State;
        C ->
            C1 = Update(C),
            case ch_record_state_transition(C, C1) of
                ok      ->  update_ch_record(C1),
                            State;
                unblock -> #cr{blocked_consumers = Consumers} = C1,
                           update_ch_record(
                             C1#cr{blocked_consumers = queue:new()}),
                           AC1 = queue:join(State#q.active_consumers,
                                            Consumers),
                           run_message_queue(State#q{active_consumers = AC1})
            end
    end.

should_auto_delete(#q{q = #amqqueue{auto_delete = false}}) -> false;
should_auto_delete(#q{has_had_consumers = false}) -> false;
should_auto_delete(State) -> is_unused(State).

handle_ch_down(DownPid, State = #q{exclusive_consumer = Holder,
                                   senders            = Senders}) ->
    Senders1 = case pmon:is_monitored(DownPid, Senders) of
                   false -> Senders;
                   true  -> credit_flow:peer_down(DownPid),
                            pmon:demonitor(DownPid, Senders)
               end,
    case lookup_ch(DownPid) of
        not_found ->
            {ok, State#q{senders = Senders1}};
        C = #cr{ch_pid            = ChPid,
                acktags           = ChAckTags,
                blocked_consumers = Blocked} ->
            QName = qname(State),
            _ = remove_consumers(ChPid, Blocked, QName), %% for stats emission
            ok = erase_ch_record(C),
            State1 = State#q{
                       exclusive_consumer = case Holder of
                                                {ChPid, _} -> none;
                                                Other      -> Other
                                            end,
                       active_consumers = remove_consumers(
                                            ChPid, State#q.active_consumers,
                                            QName),
                       senders          = Senders1},
            case should_auto_delete(State1) of
                true  -> {stop, State1};
                false -> {ok, requeue_and_run(queue:to_list(ChAckTags),
                                              ensure_expiry_timer(State1))}
            end
    end.

check_exclusive_access({_ChPid, _ConsumerTag}, _ExclusiveConsume, _State) ->
    in_use;
check_exclusive_access(none, false, _State) ->
    ok;
check_exclusive_access(none, true, State) ->
    case is_unused(State) of
        true  -> ok;
        false -> in_use
    end.

consumer_count() ->
    lists:sum([Count || #cr{consumer_count = Count} <- all_ch_record()]).

is_unused(_State) -> consumer_count() == 0.

maybe_send_reply(_ChPid, undefined) -> ok;
maybe_send_reply(ChPid, Msg) -> ok = rabbit_channel:send_command(ChPid, Msg).

qname(#q{q = #amqqueue{name = QName}}) -> QName.

backing_queue_timeout(State = #q{backing_queue       = BQ,
                                 backing_queue_state = BQS}) ->
    State#q{backing_queue_state = BQ:timeout(BQS)}.

subtract_acks(ChPid, AckTags, State, Fun) ->
    case lookup_ch(ChPid) of
        not_found ->
            State;
        C = #cr{acktags = ChAckTags} ->
            update_ch_record(
              C#cr{acktags = subtract_acks(AckTags, [], ChAckTags)}),
            Fun(State)
    end.

subtract_acks([], [], AckQ) ->
    AckQ;
subtract_acks([], Prefix, AckQ) ->
    queue:join(queue:from_list(lists:reverse(Prefix)), AckQ);
subtract_acks([T | TL] = AckTags, Prefix, AckQ) ->
    case queue:out(AckQ) of
        {{value,  T}, QTail} -> subtract_acks(TL,             Prefix, QTail);
        {{value, AT}, QTail} -> subtract_acks(AckTags, [AT | Prefix], QTail)
    end.

message_properties(Message, Confirm, #q{ttl = TTL}) ->
    #message_properties{expiry           = calculate_msg_expiry(Message, TTL),
                        needs_confirming = Confirm == eventually}.

calculate_msg_expiry(#basic_message{content = Content}, TTL) ->
    #content{properties = Props} =
        rabbit_binary_parser:ensure_content_decoded(Content),
    %% We assert that the expiration must be valid - we check in the channel.
    {ok, MsgTTL} = rabbit_basic:parse_expiration(Props),
    case lists:min([TTL, MsgTTL]) of
        undefined -> undefined;
        T         -> now_micros() + T * 1000
    end.

drop_expired_msgs(State) ->
    case is_empty(State) of
        true  -> State;
        false -> drop_expired_msgs(now_micros(), State)
    end.

drop_expired_msgs(Now, State = #q{backing_queue_state = BQS,
                                  backing_queue       = BQ }) ->
    ExpirePred = fun (#message_properties{expiry = Exp}) -> Now >= Exp end,
    {Props, State1} =
        with_dlx(
          State#q.dlx,
          fun (X) -> dead_letter_expired_msgs(ExpirePred, X, State) end,
          fun () -> {Next, BQS1} = BQ:dropwhile(ExpirePred, BQS),
                    {Next, State#q{backing_queue_state = BQS1}} end),
    ensure_ttl_timer(case Props of
                         undefined                         -> undefined;
                         #message_properties{expiry = Exp} -> Exp
                     end, State1).

with_dlx(undefined, _With,  Without) -> Without();
with_dlx(DLX,        With,  Without) -> case rabbit_exchange:lookup(DLX) of
                                            {ok, X}            -> With(X);
                                            {error, not_found} -> Without()
                                        end.

dead_letter_expired_msgs(ExpirePred, X, State = #q{backing_queue = BQ}) ->
    dead_letter_msgs(fun (DLFun, Acc, BQS1) ->
                             BQ:fetchwhile(ExpirePred, DLFun, Acc, BQS1)
                     end, expired, X, State).

dead_letter_rejected_msgs(AckTags, X,  State = #q{backing_queue = BQ}) ->
    {ok, State1} =
        dead_letter_msgs(
          fun (DLFun, Acc, BQS) ->
                  {Acc1, BQS1} = BQ:ackfold(DLFun, Acc, BQS, AckTags),
                  {ok, Acc1, BQS1}
          end, rejected, X, State),
    State1.

dead_letter_msgs(Fun, Reason, X, State = #q{dlx_routing_key     = RK,
                                            publish_seqno       = SeqNo0,
                                            unconfirmed         = UC0,
                                            queue_monitors      = QMons0,
                                            backing_queue_state = BQS,
                                            backing_queue       = BQ}) ->
    QName = qname(State),
    {Res, {AckImm1, SeqNo1, UC1, QMons1}, BQS1} =
        Fun(fun (Msg, AckTag, {AckImm, SeqNo, UC, QMons}) ->
                    case dead_letter_publish(Msg, Reason,
                                             X, RK, SeqNo, QName) of
                        []    -> {[AckTag | AckImm], SeqNo, UC, QMons};
                        QPids -> {AckImm, SeqNo + 1,
                                  dtree:insert(SeqNo, QPids, AckTag, UC),
                                  pmon:monitor_all(QPids, QMons)}
                    end
            end, {[], SeqNo0, UC0, QMons0}, BQS),
    {_Guids, BQS2} = BQ:ack(AckImm1, BQS1),
    {Res, State#q{publish_seqno       = SeqNo1,
                  unconfirmed         = UC1,
                  queue_monitors      = QMons1,
                  backing_queue_state = BQS2}}.

dead_letter_publish(Msg, Reason, X, RK, MsgSeqNo, QName) ->
    DLMsg = make_dead_letter_msg(Msg, Reason, X#exchange.name, RK, QName),
    Delivery = rabbit_basic:delivery(false, DLMsg, MsgSeqNo),
    {Queues, Cycles} = detect_dead_letter_cycles(
                         DLMsg, rabbit_exchange:route(X, Delivery)),
    lists:foreach(fun log_cycle_once/1, Cycles),
    {_, DeliveredQPids} = rabbit_amqqueue:deliver(
                            rabbit_amqqueue:lookup(Queues), Delivery),
    DeliveredQPids.

handle_queue_down(QPid, Reason, State = #q{queue_monitors = QMons,
                                           unconfirmed    = UC}) ->
    case pmon:is_monitored(QPid, QMons) of
        false -> noreply(State);
        true  -> case rabbit_misc:is_abnormal_exit(Reason) of
                     true  -> {Lost, _UC1} = dtree:take_all(QPid, UC),
                              QNameS = rabbit_misc:rs(qname(State)),
                              rabbit_log:warning("DLQ ~p for ~s died with "
                                                 "~p unconfirmed messages~n",
                                                 [QPid, QNameS, length(Lost)]);
                     false -> ok
                 end,
                 {MsgSeqNoAckTags, UC1} = dtree:take(QPid, UC),
                 cleanup_after_confirm(
                   [AckTag || {_MsgSeqNo, AckTag} <- MsgSeqNoAckTags],
                   State#q{queue_monitors = pmon:erase(QPid, QMons),
                           unconfirmed    = UC1})
    end.

stop(State) -> stop(undefined, noreply, State).

stop(From, Reply, State = #q{unconfirmed = UC}) ->
    case {dtree:is_empty(UC), Reply} of
        {true, noreply} -> {stop, normal, State};
        {true,       _} -> {stop, normal, Reply, State};
        {false,      _} -> noreply(State#q{delayed_stop = {From, Reply}})
    end.

cleanup_after_confirm(AckTags, State = #q{delayed_stop        = DS,
                                          unconfirmed         = UC,
                                          backing_queue       = BQ,
                                          backing_queue_state = BQS}) ->
    {_Guids, BQS1} = BQ:ack(AckTags, BQS),
    State1 = State#q{backing_queue_state = BQS1},
    case dtree:is_empty(UC) andalso DS =/= undefined of
        true  -> case DS of
                     {_,  noreply} -> ok;
                     {From, Reply} -> gen_server2:reply(From, Reply)
                 end,
                 {stop, normal, State1};
        false -> noreply(State1)
    end.

detect_dead_letter_cycles(#basic_message{content = Content}, Queues) ->
    #content{properties = #'P_basic'{headers = Headers}} =
        rabbit_binary_parser:ensure_content_decoded(Content),
    NoCycles = {Queues, []},
    case Headers of
        undefined ->
            NoCycles;
        _ ->
            case rabbit_misc:table_lookup(Headers, <<"x-death">>) of
                {array, DeathTables} ->
                    OldQueues = [rabbit_misc:table_lookup(D, <<"queue">>) ||
                                    {table, D} <- DeathTables],
                    OldQueues1 = [QName || {longstr, QName} <- OldQueues],
                    OldQueuesSet = ordsets:from_list(OldQueues1),
                    {Cycling, NotCycling} =
                        lists:partition(
                          fun(Queue) ->
                                  ordsets:is_element(Queue#resource.name,
                                                     OldQueuesSet)
                          end, Queues),
                    {NotCycling, [[QName | OldQueues1] ||
                                     #resource{name = QName} <- Cycling]};
                _ ->
                    NoCycles
            end
    end.

make_dead_letter_msg(Msg = #basic_message{content       = Content,
                                          exchange_name = Exchange,
                                          routing_keys  = RoutingKeys},
                     Reason, DLX, RK, #resource{name = QName}) ->
    {DeathRoutingKeys, HeadersFun1} =
        case RK of
            undefined -> {RoutingKeys, fun (H) -> H end};
            _         -> {[RK], fun (H) -> lists:keydelete(<<"CC">>, 1, H) end}
        end,
    ReasonBin = list_to_binary(atom_to_list(Reason)),
    TimeSec = rabbit_misc:now_ms() div 1000,
    HeadersFun2 =
        fun (Headers) ->
                %% The first routing key is the one specified in the
                %% basic.publish; all others are CC or BCC keys.
                RKs  = [hd(RoutingKeys) | rabbit_basic:header_routes(Headers)],
                RKs1 = [{longstr, Key} || Key <- RKs],
                Info = [{<<"reason">>,       longstr,   ReasonBin},
                        {<<"queue">>,        longstr,   QName},
                        {<<"time">>,         timestamp, TimeSec},
                        {<<"exchange">>,     longstr,   Exchange#resource.name},
                        {<<"routing-keys">>, array,     RKs1}],
                HeadersFun1(rabbit_basic:prepend_table_header(<<"x-death">>,
                                                              Info, Headers))
        end,
    Content1 = rabbit_basic:map_headers(HeadersFun2, Content),
    Msg#basic_message{exchange_name = DLX, id = rabbit_guid:gen(),
                      routing_keys = DeathRoutingKeys, content = Content1}.

now_micros() -> timer:now_diff(now(), {0,0,0}).

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
i(policy,    #q{q = #amqqueue{name = Name}}) ->
    {ok, Q} = rabbit_amqqueue:lookup(Name),
    case rabbit_policy:name(Q) of
        none   -> '';
        Policy -> Policy
    end;
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
    lists:sum([queue:len(C#cr.acktags) || C <- all_ch_record()]);
i(messages, State) ->
    lists:sum([i(Item, State) || Item <- [messages_ready,
                                          messages_unacknowledged]]);
i(consumers, _) ->
    consumer_count();
i(memory, _) ->
    {memory, M} = process_info(self(), memory),
    M;
i(slave_pids, #q{q = #amqqueue{name = Name}}) ->
    {ok, Q = #amqqueue{slave_pids = SPids}} =
        rabbit_amqqueue:lookup(Name),
    case rabbit_mirror_queue_misc:is_mirrored(Q) of
        false -> '';
        true  -> SPids
    end;
i(synchronised_slave_pids, #q{q = #amqqueue{name = Name}}) ->
    {ok, Q = #amqqueue{sync_slave_pids = SSPids}} =
        rabbit_amqqueue:lookup(Name),
    case rabbit_mirror_queue_misc:is_mirrored(Q) of
        false -> '';
        true  -> SSPids
    end;
i(status, #q{status = Status}) ->
    Status;
i(backing_queue_status, #q{backing_queue_state = BQS, backing_queue = BQ}) ->
    BQ:status(BQS);
i(Item, _) ->
    throw({bad_argument, Item}).

consumers(#q{active_consumers = ActiveConsumers}) ->
    lists:foldl(fun (C, Acc) -> consumers(C#cr.blocked_consumers, Acc) end,
                consumers(ActiveConsumers, []), all_ch_record()).

consumers(Consumers, Acc) ->
    rabbit_misc:queue_fold(
      fun ({ChPid, #consumer{tag = CTag, ack_required = AckRequired}}, Acc1) ->
              [{ChPid, CTag, AckRequired} | Acc1]
      end, Acc, Consumers).

emit_stats(State) ->
    emit_stats(State, []).

emit_stats(State, Extra) ->
    rabbit_event:notify(queue_stats, Extra ++ infos(?STATISTICS_KEYS, State)).

emit_consumer_created(ChPid, ConsumerTag, Exclusive, AckRequired, QName) ->
    rabbit_event:notify(consumer_created,
                        [{consumer_tag, ConsumerTag},
                         {exclusive,    Exclusive},
                         {ack_required, AckRequired},
                         {channel,      ChPid},
                         {queue,        QName}]).

emit_consumer_deleted(ChPid, ConsumerTag, QName) ->
    rabbit_event:notify(consumer_deleted,
                        [{consumer_tag, ConsumerTag},
                         {channel,      ChPid},
                         {queue,        QName}]).

%%----------------------------------------------------------------------------

prioritise_call(Msg, _From, _State) ->
    case Msg of
        info                                 -> 9;
        {info, _Items}                       -> 9;
        consumers                            -> 9;
        stat                                 -> 7;
        _                                    -> 0
    end.

prioritise_cast(Msg, _State) ->
    case Msg of
        delete_immediately                   -> 8;
        {set_ram_duration_target, _Duration} -> 8;
        {set_maximum_since_use, _Age}        -> 8;
        {run_backing_queue, _Mod, _Fun}      -> 6;
        _                                    -> 0
    end.

prioritise_info(Msg, #q{q = #amqqueue{exclusive_owner = DownPid}}) ->
    case Msg of
        {'DOWN', _, process, DownPid, _}     -> 8;
        update_ram_duration                  -> 8;
        maybe_expire                         -> 8;
        drop_expired                         -> 8;
        emit_stats                           -> 7;
        sync_timeout                         -> 6;
        _                                    -> 0
    end.

handle_call(_, _, State = #q{delayed_stop = DS}) when DS =/= undefined ->
    noreply(State);

handle_call({init, Recover}, From,
            State = #q{q = #amqqueue{exclusive_owner = none}}) ->
    declare(Recover, From, State);

handle_call({init, Recover}, From,
            State = #q{q = #amqqueue{exclusive_owner = Owner}}) ->
    case rabbit_misc:is_process_alive(Owner) of
        true  -> erlang:monitor(process, Owner),
                 declare(Recover, From, State);
        false -> #q{backing_queue = BQ, backing_queue_state = undefined,
                    q = #amqqueue{name = QName} = Q} = State,
                 gen_server2:reply(From, not_found),
                 case Recover of
                     new -> rabbit_log:warning(
                              "Queue ~p exclusive owner went away~n", [QName]);
                     _   -> ok
                 end,
                 BQS = bq_init(BQ, Q, Recover),
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

handle_call(consumers, _From, State) ->
    reply(consumers(State), State);

handle_call({deliver, Delivery, Delivered}, From, State) ->
    %% Synchronous, "mandatory" deliver mode.
    gen_server2:reply(From, ok),
    noreply(deliver_or_enqueue(Delivery, Delivered, State));

handle_call({notify_down, ChPid}, From, State) ->
    %% we want to do this synchronously, so that auto_deleted queues
    %% are no longer visible by the time we send a response to the
    %% client.  The queue is ultimately deleted in terminate/2; if we
    %% return stop with a reply, terminate/2 will be called by
    %% gen_server2 *before* the reply is sent. FIXME: in case of a
    %% delayed stop the reply is sent earlier.
    case handle_ch_down(ChPid, State) of
        {ok, State1}   -> reply(ok, State1);
        {stop, State1} -> stop(From, ok, State1)
    end;

handle_call({basic_get, ChPid, NoAck}, _From,
            State = #q{q = #amqqueue{name = QName}}) ->
    AckRequired = not NoAck,
    State1 = ensure_expiry_timer(State),
    case fetch(AckRequired, State1) of
        {empty, State2} ->
            reply(empty, State2);
        {{Message, IsDelivered, AckTag}, State2} ->
            State3 = #q{backing_queue = BQ, backing_queue_state = BQS} =
                case AckRequired of
                    true  -> C = #cr{acktags = ChAckTags} = ch_record(ChPid),
                             ChAckTags1 = queue:in(AckTag, ChAckTags),
                             update_ch_record(C#cr{acktags = ChAckTags1}),
                             State2;
                    false -> State2
                end,
            Msg = {QName, self(), AckTag, IsDelivered, Message},
            reply({ok, BQ:len(BQS), Msg}, State3)
    end;

handle_call({basic_consume, NoAck, ChPid, Limiter,
             ConsumerTag, ExclusiveConsume, OkMsg},
            _From, State = #q{exclusive_consumer = Holder}) ->
    case check_exclusive_access(Holder, ExclusiveConsume, State) of
        in_use ->
            reply({error, exclusive_consume_unavailable}, State);
        ok ->
            C = ch_record(ChPid),
            update_consumer_count(C#cr{limiter = Limiter}, +1),
            Consumer = #consumer{tag = ConsumerTag,
                                 ack_required = not NoAck},
            ExclusiveConsumer = if ExclusiveConsume -> {ChPid, ConsumerTag};
                                   true             -> Holder
                                end,
            State1 = State#q{has_had_consumers = true,
                             exclusive_consumer = ExclusiveConsumer},
            ok = maybe_send_reply(ChPid, OkMsg),
            emit_consumer_created(ChPid, ConsumerTag, ExclusiveConsume,
                                  not NoAck, qname(State1)),
            AC1 = queue:in({ChPid, Consumer}, State1#q.active_consumers),
            reply(ok, run_message_queue(State1#q{active_consumers = AC1}))
    end;

handle_call({basic_cancel, ChPid, ConsumerTag, OkMsg}, From,
            State = #q{exclusive_consumer = Holder}) ->
    ok = maybe_send_reply(ChPid, OkMsg),
    case lookup_ch(ChPid) of
        not_found ->
            reply(ok, State);
        C = #cr{blocked_consumers = Blocked} ->
            emit_consumer_deleted(ChPid, ConsumerTag, qname(State)),
            Blocked1 = remove_consumer(ChPid, ConsumerTag, Blocked),
            update_consumer_count(C#cr{blocked_consumers = Blocked1}, -1),
            State1 = State#q{
                       exclusive_consumer = case Holder of
                                                {ChPid, ConsumerTag} -> none;
                                                _                    -> Holder
                                            end,
                       active_consumers   = remove_consumer(
                                              ChPid, ConsumerTag,
                                              State#q.active_consumers)},
            case should_auto_delete(State1) of
                false -> reply(ok, ensure_expiry_timer(State1));
                true  -> stop(From, ok, State1)
            end
    end;

handle_call(stat, _From, State) ->
    State1 = #q{backing_queue = BQ, backing_queue_state = BQS} =
        ensure_expiry_timer(State),
    reply({ok, BQ:len(BQS), consumer_count()}, State1);

handle_call({delete, IfUnused, IfEmpty}, From,
            State = #q{backing_queue_state = BQS, backing_queue = BQ}) ->
    IsEmpty  = BQ:is_empty(BQS),
    IsUnused = is_unused(State),
    if
        IfEmpty  and not(IsEmpty)  -> reply({error, not_empty}, State);
        IfUnused and not(IsUnused) -> reply({error,    in_use}, State);
        true                       -> stop(From, {ok, BQ:len(BQS)}, State)
    end;

handle_call(purge, _From, State = #q{backing_queue       = BQ,
                                     backing_queue_state = BQS}) ->
    {Count, BQS1} = BQ:purge(BQS),
    reply({ok, Count}, State#q{backing_queue_state = BQS1});

handle_call({requeue, AckTags, ChPid}, From, State) ->
    gen_server2:reply(From, ok),
    noreply(requeue(AckTags, ChPid, State));

handle_call(sync_mirrors, _From,
            State = #q{backing_queue       = rabbit_mirror_queue_master,
                       backing_queue_state = BQS}) ->
    S = fun(BQSN) -> State#q{backing_queue_state = BQSN} end,
    HandleInfo = fun (Status) ->
                         receive {'$gen_call', From, {info, Items}} ->
                                 Infos = infos(Items, State#q{status = Status}),
                                 gen_server2:reply(From, {ok, Infos})
                         after 0 ->
                                 ok
                         end
                 end,
    EmitStats = fun (Status) ->
                        rabbit_event:if_enabled(
                          State, #q.stats_timer,
                          fun() -> emit_stats(State#q{status = Status}) end)
                end,
    case rabbit_mirror_queue_master:sync_mirrors(HandleInfo, EmitStats, BQS) of
        {ok, BQS1}           -> reply(ok, S(BQS1));
        {stop, Reason, BQS1} -> {stop, Reason, S(BQS1)}
    end;

handle_call(sync_mirrors, _From, State) ->
    reply({error, not_mirrored}, State);

%% By definition if we get this message here we do not have to do anything.
handle_call(cancel_sync_mirrors, _From, State) ->
    reply({ok, not_syncing}, State);

handle_call(force_event_refresh, _From,
            State = #q{exclusive_consumer = Exclusive}) ->
    rabbit_event:notify(queue_created, infos(?CREATION_EVENT_KEYS, State)),
    QName = qname(State),
    case Exclusive of
        none       -> [emit_consumer_created(
                         Ch, CTag, false, AckRequired, QName) ||
                          {Ch, CTag, AckRequired} <- consumers(State)];
        {Ch, CTag} -> [{Ch, CTag, AckRequired}] = consumers(State),
                      emit_consumer_created(Ch, CTag, true, AckRequired, QName)
    end,
    reply(ok, State).

handle_cast({confirm, MsgSeqNos, QPid}, State = #q{unconfirmed = UC}) ->
    {MsgSeqNoAckTags, UC1} = dtree:take(MsgSeqNos, QPid, UC),
    State1 = case dtree:is_defined(QPid, UC1) of
                 false -> QMons = State#q.queue_monitors,
                          State#q{queue_monitors = pmon:demonitor(QPid, QMons)};
                 true  -> State
             end,
    cleanup_after_confirm([AckTag || {_MsgSeqNo, AckTag} <- MsgSeqNoAckTags],
                          State1#q{unconfirmed = UC1});

handle_cast(_, State = #q{delayed_stop = DS}) when DS =/= undefined ->
    noreply(State);

handle_cast({run_backing_queue, Mod, Fun},
            State = #q{backing_queue = BQ, backing_queue_state = BQS}) ->
    noreply(State#q{backing_queue_state = BQ:invoke(Mod, Fun, BQS)});

handle_cast({deliver, Delivery = #delivery{sender = Sender}, Delivered, Flow},
            State = #q{senders = Senders}) ->
    %% Asynchronous, non-"mandatory" deliver mode.
    Senders1 = case Flow of
                   flow   -> credit_flow:ack(Sender),
                             pmon:monitor(Sender, Senders);
                   noflow -> Senders
               end,
    State1 = State#q{senders = Senders1},
    noreply(deliver_or_enqueue(Delivery, Delivered, State1));

handle_cast({ack, AckTags, ChPid}, State) ->
    noreply(ack(AckTags, ChPid, State));

handle_cast({reject, AckTags, true, ChPid}, State) ->
    noreply(requeue(AckTags, ChPid, State));

handle_cast({reject, AckTags, false, ChPid}, State) ->
    noreply(with_dlx(
              State#q.dlx,
              fun (X) -> subtract_acks(ChPid, AckTags, State,
                                       fun (State1) ->
                                               dead_letter_rejected_msgs(
                                                 AckTags, X, State1)
                                       end) end,
              fun () -> ack(AckTags, ChPid, State) end));

handle_cast(delete_immediately, State) ->
    stop(State);

handle_cast({unblock, ChPid}, State) ->
    noreply(
      possibly_unblock(State, ChPid,
                       fun (C) -> C#cr{is_limit_active = false} end));

handle_cast({notify_sent, ChPid, Credit}, State) ->
    noreply(
      possibly_unblock(State, ChPid,
                       fun (C = #cr{unsent_message_count = Count}) ->
                               C#cr{unsent_message_count = Count - Credit}
                       end));

handle_cast({limit, ChPid, Limiter}, State) ->
    noreply(
      possibly_unblock(
        State, ChPid,
        fun (C = #cr{consumer_count  = ConsumerCount,
                     limiter         = OldLimiter,
                     is_limit_active = OldLimited}) ->
                case (ConsumerCount =/= 0 andalso
                      not rabbit_limiter:is_enabled(OldLimiter)) of
                    true  -> ok = rabbit_limiter:register(Limiter, self());
                    false -> ok
                end,
                Limited = OldLimited andalso rabbit_limiter:is_enabled(Limiter),
                C#cr{limiter = Limiter, is_limit_active = Limited}
        end));

handle_cast({flush, ChPid}, State) ->
    ok = rabbit_channel:flushed(ChPid, self()),
    noreply(State);

handle_cast({set_ram_duration_target, Duration},
            State = #q{backing_queue = BQ, backing_queue_state = BQS}) ->
    BQS1 = BQ:set_ram_duration_target(Duration, BQS),
    noreply(State#q{backing_queue_state = BQS1});

handle_cast({set_maximum_since_use, Age}, State) ->
    ok = file_handle_cache:set_maximum_since_use(Age),
    noreply(State);

handle_cast(start_mirroring, State = #q{backing_queue       = BQ,
					backing_queue_state = BQS}) ->
    %% lookup again to get policy for init_with_existing_bq
    {ok, Q} = rabbit_amqqueue:lookup(qname(State)),
    true = BQ =/= rabbit_mirror_queue_master, %% assertion
    BQ1 = rabbit_mirror_queue_master,
    BQS1 = BQ1:init_with_existing_bq(Q, BQ, BQS),
    noreply(State#q{backing_queue       = BQ1,
		    backing_queue_state = BQS1});

handle_cast(stop_mirroring, State = #q{backing_queue       = BQ,
				       backing_queue_state = BQS}) ->
    BQ = rabbit_mirror_queue_master, %% assertion
    {BQ1, BQS1} = BQ:stop_mirroring(BQS),
    noreply(State#q{backing_queue       = BQ1,
		    backing_queue_state = BQS1});

handle_cast(wake_up, State) ->
    noreply(State).

%% We need to not ignore this as we need to remove outstanding
%% confirms due to queue death.
handle_info({'DOWN', _MonitorRef, process, DownPid, Reason},
            State = #q{delayed_stop = DS}) when DS =/= undefined ->
    handle_queue_down(DownPid, Reason, State);

handle_info(_, State = #q{delayed_stop = DS}) when DS =/= undefined ->
    noreply(State);

handle_info(maybe_expire, State) ->
    case is_unused(State) of
        true  -> stop(State);
        false -> noreply(ensure_expiry_timer(State))
    end;

handle_info(drop_expired, State) ->
    noreply(drop_expired_msgs(State#q{ttl_timer_ref = undefined}));

handle_info(emit_stats, State) ->
    emit_stats(State),
    %% Don't call noreply/1, we don't want to set timers
    {State1, Timeout} = next_state(rabbit_event:reset_stats_timer(
                                     State, #q.stats_timer)),
    {noreply, State1, Timeout};

handle_info({'DOWN', _MonitorRef, process, DownPid, _Reason},
            State = #q{q = #amqqueue{exclusive_owner = DownPid}}) ->
    %% Exclusively owned queues must disappear with their owner.  In
    %% the case of clean shutdown we delete the queue synchronously in
    %% the reader - although not required by the spec this seems to
    %% match what people expect (see bug 21824). However we need this
    %% monitor-and-async- delete in case the connection goes away
    %% unexpectedly.
    stop(State);

handle_info({'DOWN', _MonitorRef, process, DownPid, Reason}, State) ->
    case handle_ch_down(DownPid, State) of
        {ok, State1}   -> handle_queue_down(DownPid, Reason, State1);
        {stop, State1} -> stop(State1)
    end;

handle_info(update_ram_duration, State = #q{backing_queue = BQ,
                                            backing_queue_state = BQS}) ->
    {RamDuration, BQS1} = BQ:ram_duration(BQS),
    DesiredDuration =
        rabbit_memory_monitor:report_ram_duration(self(), RamDuration),
    BQS2 = BQ:set_ram_duration_target(DesiredDuration, BQS1),
    %% Don't call noreply/1, we don't want to set timers
    {State1, Timeout} = next_state(State#q{rate_timer_ref      = undefined,
                                           backing_queue_state = BQS2}),
    {noreply, State1, Timeout};

handle_info(sync_timeout, State) ->
    noreply(backing_queue_timeout(State#q{sync_timer_ref = undefined}));

handle_info(timeout, State) ->
    noreply(backing_queue_timeout(State));

handle_info({'EXIT', _Pid, Reason}, State) ->
    {stop, Reason, State};

handle_info({bump_credit, Msg}, State) ->
    credit_flow:handle_bump_msg(Msg),
    noreply(State);

handle_info(Info, State) ->
    {stop, {unhandled_info, Info}, State}.

handle_pre_hibernate(State = #q{backing_queue_state = undefined}) ->
    {hibernate, State};
handle_pre_hibernate(State = #q{backing_queue = BQ,
                                backing_queue_state = BQS}) ->
    {RamDuration, BQS1} = BQ:ram_duration(BQS),
    DesiredDuration =
        rabbit_memory_monitor:report_ram_duration(self(), RamDuration),
    BQS2 = BQ:set_ram_duration_target(DesiredDuration, BQS1),
    BQS3 = BQ:handle_pre_hibernate(BQS2),
    rabbit_event:if_enabled(
      State, #q.stats_timer,
      fun () -> emit_stats(State, [{idle_since, now()}]) end),
    State1 = rabbit_event:stop_stats_timer(State#q{backing_queue_state = BQS3},
                                           #q.stats_timer),
    {hibernate, stop_rate_timer(State1)}.

format_message_queue(Opt, MQ) -> rabbit_misc:format_message_queue(Opt, MQ).

log_cycle_once(Queues) ->
    Key = {queue_cycle, Queues},
    case get(Key) of
        true      -> ok;
        undefined -> rabbit_log:warning(
                       "Message dropped. Dead-letter queues cycle detected" ++
                       ": ~p~nThis cycle will NOT be reported again.~n",
                       [Queues]),
                     put(Key, true)
    end.
