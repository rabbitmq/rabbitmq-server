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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
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
         handle_info/2, handle_pre_hibernate/1, prioritise_call/4,
         prioritise_cast/3, prioritise_info/3, format_message_queue/2]).

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
            dlx,
            dlx_routing_key,
            max_length,
            args_policy_version,
            status
           }).

-record(consumer, {tag, ack_required, args}).

%% These are held in our process dictionary
-record(cr, {ch_pid,
             monitor_ref,
             acktags,
             consumer_count,
             %% Queue of {ChPid, #consumer{}} for consumers which have
             %% been blocked for any reason
             blocked_consumers,
             %% The limiter itself
             limiter,
             %% Internal flow control for queue -> writer
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
    State2 = process_args_policy(State1),
    State3 = lists:foldl(fun (Delivery, StateN) ->
                                 deliver_or_enqueue(Delivery, true, StateN)
                         end, State2, Deliveries),
    notify_decorators(startup, [], State3),
    State3.

init_state(Q) ->
    State = #q{q                   = Q,
               exclusive_consumer  = none,
               has_had_consumers   = false,
               active_consumers    = priority_queue:new(),
               senders             = pmon:new(delegate),
               msg_id_to_channel   = gb_trees:empty(),
               status              = running,
               args_policy_version = 0},
    rabbit_event:init_stats_timer(State, #q.stats_timer).

terminate(shutdown = R,      State = #q{backing_queue = BQ}) ->
    terminate_shutdown(fun (BQS) -> BQ:terminate(R, BQS) end, State);
terminate({shutdown, missing_owner} = Reason, State) ->
    %% if the owner was missing then there will be no queue, so don't emit stats
    terminate_shutdown(terminate_delete(false, Reason, State), State);
terminate({shutdown, _} = R, State = #q{backing_queue = BQ}) ->
    terminate_shutdown(fun (BQS) -> BQ:terminate(R, BQS) end, State);
terminate(Reason,            State) ->
    terminate_shutdown(terminate_delete(true, Reason, State), State).

terminate_delete(EmitStats, Reason,
                 State = #q{q = #amqqueue{name          = QName},
                                          backing_queue = BQ}) ->
    fun (BQS) ->
        BQS1 = BQ:delete_and_terminate(Reason, BQS),
        if EmitStats -> rabbit_event:if_enabled(State, #q.stats_timer,
                                                fun() -> emit_stats(State) end);
           true      -> ok
        end,
        %% don't care if the internal delete doesn't return 'ok'.
        rabbit_amqqueue:internal_delete(QName),
        BQS1
    end.

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
                    State1 = process_args_policy(
                               State#q{backing_queue       = BQ,
                                       backing_queue_state = BQS}),
                    notify_decorators(startup, [], State),
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

notify_decorators(Event, Props, State) when Event =:= startup;
                                            Event =:= shutdown ->
    decorator_callback(qname(State), Event, Props);

notify_decorators(Event, Props, State = #q{active_consumers    = ACs,
                                           backing_queue       = BQ,
                                           backing_queue_state = BQS}) ->
    decorator_callback(
      qname(State), notify,
      [Event, [{max_active_consumer_priority, priority_queue:highest(ACs)},
               {is_empty,                     BQ:is_empty(BQS)} | Props]]).

decorator_callback(QName, F, A) ->
    %% Look up again in case policy and hence decorators have changed
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q = #amqqueue{decorators = Ds}} ->
            [ok = apply(M, F, [Q|A]) || M <- rabbit_queue_decorator:select(Ds)];
        {error, not_found} ->
            ok
    end.

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

process_args_policy(State = #q{q                   = Q,
                               args_policy_version = N}) ->
      ArgsTable =
        [{<<"expires">>,                 fun res_min/2, fun init_exp/2},
         {<<"dead-letter-exchange">>,    fun res_arg/2, fun init_dlx/2},
         {<<"dead-letter-routing-key">>, fun res_arg/2, fun init_dlx_rkey/2},
         {<<"message-ttl">>,             fun res_min/2, fun init_ttl/2},
         {<<"max-length">>,              fun res_min/2, fun init_max_length/2}],
      drop_expired_msgs(
         lists:foldl(fun({Name, Resolve, Fun}, StateN) ->
                             Fun(args_policy_lookup(Name, Resolve, Q), StateN)
                     end, State#q{args_policy_version = N + 1}, ArgsTable)).

args_policy_lookup(Name, Resolve, Q = #amqqueue{arguments = Args}) ->
    AName = <<"x-", Name/binary>>,
    case {rabbit_policy:get(Name, Q), rabbit_misc:table_lookup(Args, AName)} of
        {undefined, undefined}       -> undefined;
        {undefined, {_Type, Val}}    -> Val;
        {Val,       undefined}       -> Val;
        {PolVal,    {_Type, ArgVal}} -> Resolve(PolVal, ArgVal)
    end.

res_arg(_PolVal, ArgVal) -> ArgVal.
res_min(PolVal, ArgVal)  -> erlang:min(PolVal, ArgVal).

%% In both these we init with the undefined variant first to stop any
%% existing timer, then start a new one which may fire after a
%% different time.
init_exp(undefined, State) -> stop_expiry_timer(State#q{expires = undefined});
init_exp(Expires,   State) -> State1 = init_exp(undefined, State),
                              ensure_expiry_timer(State1#q{expires = Expires}).

init_ttl(undefined, State) -> stop_ttl_timer(State#q{ttl = undefined});
init_ttl(TTL,       State) -> (init_ttl(undefined, State))#q{ttl = TTL}.

init_dlx(undefined, State) ->
    State#q{dlx = undefined};
init_dlx(DLX, State = #q{q = #amqqueue{name = QName}}) ->
    State#q{dlx = rabbit_misc:r(QName, exchange, DLX)}.

init_dlx_rkey(RoutingKey, State) -> State#q{dlx_routing_key = RoutingKey}.

init_max_length(MaxLen, State) ->
    {_Dropped, State1} = maybe_drop_head(State#q{max_length = MaxLen}),
    State1.

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
                     notify_decorators(shutdown, [], State),
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
ensure_expiry_timer(State = #q{expires             = Expires,
                               args_policy_version = Version}) ->
    case is_unused(State) of
        true  -> NewState = stop_expiry_timer(State),
                 rabbit_misc:ensure_timer(NewState, #q.expiry_timer_ref,
                                          Expires, {maybe_expire, Version});
        false -> State
    end.

stop_expiry_timer(State) -> rabbit_misc:stop_timer(State, #q.expiry_timer_ref).

ensure_ttl_timer(undefined, State) ->
    State;
ensure_ttl_timer(Expiry, State = #q{ttl_timer_ref       = undefined,
                                    args_policy_version = Version}) ->
    After = (case Expiry - now_micros() of
                 V when V > 0 -> V + 999; %% always fire later
                 _            -> 0
             end) div 1000,
    TRef = erlang:send_after(After, self(), {drop_expired, Version}),
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
    true = (priority_queue:is_empty(AC) orelse is_empty(State)).

is_empty(#q{backing_queue = BQ, backing_queue_state = BQS}) -> BQ:is_empty(BQS).

lookup_ch(ChPid) ->
    case get({ch, ChPid}) of
        undefined -> not_found;
        C         -> C
    end.

ch_record(ChPid, LimiterPid) ->
    Key = {ch, ChPid},
    case get(Key) of
        undefined -> MonitorRef = erlang:monitor(process, ChPid),
                     Limiter = rabbit_limiter:client(LimiterPid),
                     C = #cr{ch_pid               = ChPid,
                             monitor_ref          = MonitorRef,
                             acktags              = queue:new(),
                             consumer_count       = 0,
                             blocked_consumers    = priority_queue:new(),
                             limiter              = Limiter,
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

erase_ch_record(#cr{ch_pid = ChPid, monitor_ref = MonitorRef}) ->
    erlang:demonitor(MonitorRef),
    erase({ch, ChPid}),
    ok.

all_ch_record() -> [C || {{ch, _}, C} <- get()].

block_consumer(C = #cr{blocked_consumers = Blocked},
               {_ChPid, #consumer{tag = CTag}} = QEntry, State) ->
    update_ch_record(C#cr{blocked_consumers = add_consumer(QEntry, Blocked)}),
    notify_decorators(consumer_blocked, [{consumer_tag, CTag}], State).

is_ch_blocked(#cr{unsent_message_count = Count, limiter = Limiter}) ->
    Count >= ?UNSENT_MESSAGE_LIMIT orelse rabbit_limiter:is_suspended(Limiter).

maybe_send_drained(WasEmpty, State) ->
    case (not WasEmpty) andalso is_empty(State) of
        true  -> notify_decorators(queue_empty, [], State),
                 [send_drained(C) || C <- all_ch_record()];
        false -> ok
    end,
    State.

send_drained(C = #cr{ch_pid = ChPid, limiter = Limiter}) ->
    case rabbit_limiter:drained(Limiter) of
        {[], Limiter}          -> ok;
        {CTagCredit, Limiter2} -> rabbit_channel:send_drained(
                                    ChPid, CTagCredit),
                                  update_ch_record(C#cr{limiter = Limiter2})
    end.

deliver_msgs_to_consumers(_DeliverFun, true, State) ->
    {true, State};
deliver_msgs_to_consumers(DeliverFun, false,
                          State = #q{active_consumers = ActiveConsumers}) ->
    case priority_queue:out_p(ActiveConsumers) of
        {empty, _} ->
            {false, State};
        {{value, QEntry, Priority}, Tail} ->
            {Stop, State1} = deliver_msg_to_consumer(
                               DeliverFun, QEntry, Priority,
                               State#q{active_consumers = Tail}),
            deliver_msgs_to_consumers(DeliverFun, Stop, State1)
    end.

deliver_msg_to_consumer(DeliverFun, E = {ChPid, Consumer}, Priority, State) ->
    C = lookup_ch(ChPid),
    case is_ch_blocked(C) of
        true  -> block_consumer(C, E, State),
                 {false, State};
        false -> case rabbit_limiter:can_send(C#cr.limiter,
                                              Consumer#consumer.ack_required,
                                              Consumer#consumer.tag) of
                     {suspend, Limiter} ->
                         block_consumer(C#cr{limiter = Limiter}, E, State),
                         {false, State};
                     {continue, Limiter} ->
                         AC1 = priority_queue:in(E, Priority,
                                                 State#q.active_consumers),
                         deliver_msg_to_consumer0(
                           DeliverFun, Consumer, C#cr{limiter = Limiter},
                           State#q{active_consumers = AC1})
                 end
    end.

deliver_msg_to_consumer0(DeliverFun,
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

add_consumer({ChPid, Consumer = #consumer{args = Args}}, ActiveConsumers) ->
    Priority = case rabbit_misc:table_lookup(Args, <<"x-priority">>) of
                   {_, P} -> P;
                   _      -> 0
               end,
    priority_queue:in({ChPid, Consumer}, Priority, ActiveConsumers).

attempt_delivery(Delivery = #delivery{sender = SenderPid, message = Message},
                 Props, Delivered, State = #q{backing_queue       = BQ,
                                              backing_queue_state = BQS}) ->
    case BQ:is_duplicate(Message, BQS) of
        {false, BQS1} ->
            deliver_msgs_to_consumers(
              fun (true, State1 = #q{backing_queue_state = BQS2}) ->
                      true = BQ:is_empty(BQS2),
                      {AckTag, BQS3} = BQ:publish_delivered(
                                         Message, Props, SenderPid, BQS2),
                      {{Message, Delivered, AckTag},
                       true, State1#q{backing_queue_state = BQS3}};
                  (false, State1) ->
                      {{Message, Delivered, undefined},
                       true, discard(Delivery, State1)}
              end, false, State#q{backing_queue_state = BQS1});
        {true, BQS1} ->
            {true, State#q{backing_queue_state = BQS1}}
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
            BQS1 = BQ:publish(Message, Props, Delivered, SenderPid, BQS),
            {Dropped, State3 = #q{backing_queue_state = BQS2}} =
              maybe_drop_head(State2#q{backing_queue_state = BQS1}),
            QLen = BQ:len(BQS2),
            %% optimisation: it would be perfectly safe to always
            %% invoke drop_expired_msgs here, but that is expensive so
            %% we only do that if a new message that might have an
            %% expiry ends up at the head of the queue. If the head
            %% remains unchanged, or if the newly published message
            %% has no expiry and becomes the head of the queue then
            %% the call is unnecessary.
            case {Dropped > 0, QLen =:= 1, Props#message_properties.expiry} of
                {false, false,         _} -> State3;
                {true,  true,  undefined} -> State3;
                {_,     _,             _} -> drop_expired_msgs(State3)
            end
    end.

maybe_drop_head(State = #q{max_length = undefined}) ->
    {0, State};
maybe_drop_head(State = #q{max_length          = MaxLen,
                           backing_queue       = BQ,
                           backing_queue_state = BQS}) ->
    case BQ:len(BQS) - MaxLen of
        Excess when Excess > 0 ->
            {Excess,
             with_dlx(
               State#q.dlx,
               fun (X) -> dead_letter_maxlen_msgs(X, Excess, State) end,
               fun () ->
                       {_, BQS1} = lists:foldl(fun (_, {_, BQS0}) ->
                                                       BQ:drop(false, BQS0)
                                               end, {ok, BQS},
                                               lists:seq(1, Excess)),
                       State#q{backing_queue_state = BQS1}
               end)};
        _ -> {0, State}
    end.

requeue_and_run(AckTags, State = #q{backing_queue       = BQ,
                                    backing_queue_state = BQS}) ->
    WasEmpty = BQ:is_empty(BQS),
    {_MsgIds, BQS1} = BQ:requeue(AckTags, BQS),
    {_Dropped, State1} = maybe_drop_head(State#q{backing_queue_state = BQS1}),
    run_message_queue(maybe_send_drained(WasEmpty, drop_expired_msgs(State1))).

fetch(AckRequired, State = #q{backing_queue       = BQ,
                              backing_queue_state = BQS}) ->
    {Result, BQS1} = BQ:fetch(AckRequired, BQS),
    State1 = drop_expired_msgs(State#q{backing_queue_state = BQS1}),
    {Result, maybe_send_drained(Result =:= empty, State1)}.

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
    priority_queue:filter(fun ({CP, #consumer{tag = CTag}}) ->
                                  (CP /= ChPid) or (CTag /= ConsumerTag)
                          end, Queue).

remove_consumers(ChPid, Queue, QName) ->
    priority_queue:filter(fun ({CP, #consumer{tag = CTag}}) when CP =:= ChPid ->
                                  emit_consumer_deleted(ChPid, CTag, QName),
                                  false;
                              (_) ->
                                  true
                          end, Queue).

possibly_unblock(State, ChPid, Update) ->
    case lookup_ch(ChPid) of
        not_found -> State;
        C         -> C1 = Update(C),
                     case is_ch_blocked(C) andalso not is_ch_blocked(C1) of
                         false -> update_ch_record(C1),
                                  State;
                         true  -> unblock(State, C1)
                     end
    end.

unblock(State, C = #cr{limiter = Limiter}) ->
    case lists:partition(
           fun({_P, {_ChPid, #consumer{tag = CTag}}}) ->
                   rabbit_limiter:is_consumer_blocked(Limiter, CTag)
           end, priority_queue:to_list(C#cr.blocked_consumers)) of
        {_, []} ->
            update_ch_record(C),
            State;
        {Blocked, Unblocked} ->
            BlockedQ   = priority_queue:from_list(Blocked),
            UnblockedQ = priority_queue:from_list(Unblocked),
            update_ch_record(C#cr{blocked_consumers = BlockedQ}),
            AC1 = priority_queue:join(State#q.active_consumers, UnblockedQ),
            State1 = State#q{active_consumers = AC1},
            [notify_decorators(
               consumer_unblocked, [{consumer_tag, CTag}], State1) ||
                {_P, {_ChPid, #consumer{tag = CTag}}} <- Unblocked],
            run_message_queue(State1)
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

%% Logically this function should invoke maybe_send_drained/2.
%% However, that is expensive. Since some frequent callers of
%% drop_expired_msgs/1, in particular deliver_or_enqueue/3, cannot
%% possibly cause the queue to become empty, we push the
%% responsibility to the callers. So be cautious when adding new ones.
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

dead_letter_maxlen_msgs(X, Excess, State = #q{backing_queue = BQ}) ->
    {ok, State1} =
        dead_letter_msgs(
          fun (DLFun, Acc, BQS) ->
                  lists:foldl(fun (_, {ok, Acc0, BQS0}) ->
                                      {{Msg, _, AckTag}, BQS1} =
                                        BQ:fetch(true, BQS0),
                                      {ok, DLFun(Msg, AckTag, Acc0), BQS1}
                              end, {ok, Acc, BQS}, lists:seq(1, Excess))
          end, maxlen, X, State),
    State1.

dead_letter_msgs(Fun, Reason, X, State = #q{dlx_routing_key     = RK,
                                            backing_queue_state = BQS,
                                            backing_queue       = BQ}) ->
    QName = qname(State),
    {Res, Acks1, BQS1} =
        Fun(fun (Msg, AckTag, Acks) ->
                    dead_letter_publish(Msg, Reason, X, RK, QName),
                    [AckTag | Acks]
            end, [], BQS),
    {_Guids, BQS2} = BQ:ack(Acks1, BQS1),
    {Res, State#q{backing_queue_state = BQS2}}.

dead_letter_publish(Msg, Reason, X, RK, QName) ->
    DLMsg = make_dead_letter_msg(Msg, Reason, X#exchange.name, RK, QName),
    Delivery = rabbit_basic:delivery(false, DLMsg, undefined),
    {Queues, Cycles} = detect_dead_letter_cycles(
                         Reason, DLMsg, rabbit_exchange:route(X, Delivery)),
    lists:foreach(fun log_cycle_once/1, Cycles),
    rabbit_amqqueue:deliver( rabbit_amqqueue:lookup(Queues), Delivery),
    ok.

stop(State) -> stop(noreply, State).

stop(noreply, State) -> {stop, normal, State};
stop(Reply,   State) -> {stop, normal, Reply, State}.


detect_dead_letter_cycles(expired,
                          #basic_message{content = Content}, Queues) ->
    #content{properties = #'P_basic'{headers = Headers}} =
        rabbit_binary_parser:ensure_content_decoded(Content),
    NoCycles = {Queues, []},
    case Headers of
        undefined ->
            NoCycles;
        _ ->
            case rabbit_misc:table_lookup(Headers, <<"x-death">>) of
                {array, Deaths} ->
                    {Cycling, NotCycling} =
                        lists:partition(
                          fun (#resource{name = Queue}) ->
                                  is_dead_letter_cycle(Queue, Deaths)
                          end, Queues),
                    OldQueues = [rabbit_misc:table_lookup(D, <<"queue">>) ||
                                    {table, D} <- Deaths],
                    OldQueues1 = [QName || {longstr, QName} <- OldQueues],
                    {NotCycling, [[QName | OldQueues1] ||
                                     #resource{name = QName} <- Cycling]};
                _ ->
                    NoCycles
            end
    end;
detect_dead_letter_cycles(_Reason, _Msg, Queues) ->
    {Queues, []}.

is_dead_letter_cycle(Queue, Deaths) ->
    {Cycle, Rest} =
        lists:splitwith(
          fun ({table, D}) ->
                  {longstr, Queue} =/= rabbit_misc:table_lookup(D, <<"queue">>);
              (_) ->
                  true
          end, Deaths),
    %% Is there a cycle, and if so, is it entirely due to expiry?
    case Rest of
        []    -> false;
        [H|_] -> lists:all(
                   fun ({table, D}) ->
                           {longstr, <<"expired">>} =:=
                               rabbit_misc:table_lookup(D, <<"reason">>);
                       (_) ->
                           false
                   end, Cycle ++ [H])
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
    PerMsgTTL = per_msg_ttl_header(Content#content.properties),
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
                        {<<"routing-keys">>, array,     RKs1}] ++ PerMsgTTL,
                HeadersFun1(rabbit_basic:prepend_table_header(<<"x-death">>,
                                                              Info, Headers))
        end,
    Content1 = #content{properties = Props} =
        rabbit_basic:map_headers(HeadersFun2, Content),
    Content2 = Content1#content{properties =
                                    Props#'P_basic'{expiration = undefined}},
    Msg#basic_message{exchange_name = DLX,
                      id            = rabbit_guid:gen(),
                      routing_keys  = DeathRoutingKeys,
                      content       = Content2}.

per_msg_ttl_header(#'P_basic'{expiration = undefined}) ->
    [];
per_msg_ttl_header(#'P_basic'{expiration = Expiration}) ->
    [{<<"original-expiration">>, longstr, Expiration}];
per_msg_ttl_header(_) ->
    [].

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
i(policy,    #q{q = Q}) ->
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
    priority_queue:fold(
      fun ({ChPid, Consumer}, _P, Acc1) ->
              #consumer{tag = CTag, ack_required = Ack, args = Args} = Consumer,
              [{ChPid, CTag, Ack, Args} | Acc1]
      end, Acc, Consumers).

emit_stats(State) ->
    emit_stats(State, []).

emit_stats(State, Extra) ->
    rabbit_event:notify(queue_stats, Extra ++ infos(?STATISTICS_KEYS, State)).

emit_consumer_created(ChPid, CTag, Exclusive, AckRequired, QName, Args) ->
    rabbit_event:notify(consumer_created,
                        [{consumer_tag, CTag},
                         {exclusive,    Exclusive},
                         {ack_required, AckRequired},
                         {channel,      ChPid},
                         {queue,        QName},
                         {arguments,    Args}]).

emit_consumer_deleted(ChPid, ConsumerTag, QName) ->
    rabbit_event:notify(consumer_deleted,
                        [{consumer_tag, ConsumerTag},
                         {channel,      ChPid},
                         {queue,        QName}]).

%%----------------------------------------------------------------------------

prioritise_call(Msg, _From, _Len, _State) ->
    case Msg of
        info                                 -> 9;
        {info, _Items}                       -> 9;
        consumers                            -> 9;
        stat                                 -> 7;
        _                                    -> 0
    end.

prioritise_cast(Msg, _Len, _State) ->
    case Msg of
        delete_immediately                   -> 8;
        {set_ram_duration_target, _Duration} -> 8;
        {set_maximum_since_use, _Age}        -> 8;
        {run_backing_queue, _Mod, _Fun}      -> 6;
        _                                    -> 0
    end.

prioritise_info(Msg, _Len, #q{q = #amqqueue{exclusive_owner = DownPid}}) ->
    case Msg of
        {'DOWN', _, process, DownPid, _}     -> 8;
        update_ram_duration                  -> 8;
        {maybe_expire, _Version}             -> 8;
        {drop_expired, _Version}             -> 8;
        emit_stats                           -> 7;
        sync_timeout                         -> 6;
        _                                    -> 0
    end.

handle_call({init, Recover}, From,
            State = #q{q = #amqqueue{exclusive_owner = none}}) ->
    declare(Recover, From, State);

%% You used to be able to declare an exclusive durable queue. Sadly we
%% need to still tidy up after that case, there could be the remnants
%% of one left over from an upgrade. So that's why we don't enforce
%% Recover = false here.
handle_call({init, Recover}, From,
            State = #q{q = #amqqueue{exclusive_owner = Owner}}) ->
    case rabbit_misc:is_process_alive(Owner) of
        true  -> erlang:monitor(process, Owner),
                 declare(Recover, From, State);
        false -> #q{backing_queue       = undefined,
                    backing_queue_state = undefined,
                    q                   = Q} = State,
                 gen_server2:reply(From, {owner_died, Q}),
                 BQ = backing_queue_module(Q),
                 BQS = bq_init(BQ, Q, Recover),
                 %% Rely on terminate to delete the queue.
                 {stop, {shutdown, missing_owner},
                  State#q{backing_queue = BQ, backing_queue_state = BQS}}
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

handle_call({notify_down, ChPid}, _From, State) ->
    %% we want to do this synchronously, so that auto_deleted queues
    %% are no longer visible by the time we send a response to the
    %% client.  The queue is ultimately deleted in terminate/2; if we
    %% return stop with a reply, terminate/2 will be called by
    %% gen_server2 *before* the reply is sent.
    case handle_ch_down(ChPid, State) of
        {ok, State1}   -> reply(ok, State1);
        {stop, State1} -> stop(ok, State1)
    end;

handle_call({basic_get, ChPid, NoAck, LimiterPid}, _From,
            State = #q{q = #amqqueue{name = QName}}) ->
    AckRequired = not NoAck,
    State1 = ensure_expiry_timer(State),
    case fetch(AckRequired, State1) of
        {empty, State2} ->
            reply(empty, State2);
        {{Message, IsDelivered, AckTag}, State2} ->
            State3 = #q{backing_queue = BQ, backing_queue_state = BQS} =
                case AckRequired of
                    true  -> C = #cr{acktags = ChAckTags} =
                                 ch_record(ChPid, LimiterPid),
                             ChAckTags1 = queue:in(AckTag, ChAckTags),
                             update_ch_record(C#cr{acktags = ChAckTags1}),
                             State2;
                    false -> State2
                end,
            Msg = {QName, self(), AckTag, IsDelivered, Message},
            reply({ok, BQ:len(BQS), Msg}, State3)
    end;

handle_call({basic_consume, NoAck, ChPid, LimiterPid, LimiterActive,
             ConsumerTag, ExclusiveConsume, CreditArgs, OtherArgs, OkMsg},
            _From, State = #q{exclusive_consumer = Holder}) ->
    case check_exclusive_access(Holder, ExclusiveConsume, State) of
        in_use ->
            reply({error, exclusive_consume_unavailable}, State);
        ok ->
            C = #cr{consumer_count = Count,
                    limiter        = Limiter} = ch_record(ChPid, LimiterPid),
            Limiter1 = case LimiterActive of
                           true  -> rabbit_limiter:activate(Limiter);
                           false -> Limiter
                       end,
            Limiter2 = case CreditArgs of
                           none         -> Limiter1;
                           {Crd, Drain} -> rabbit_limiter:credit(
                                             Limiter1, ConsumerTag, Crd, Drain)
                       end,
            C1 = update_ch_record(C#cr{consumer_count = Count + 1,
                                       limiter        = Limiter2}),
            case is_empty(State) of
                true  -> send_drained(C1);
                false -> ok
            end,
            Consumer = #consumer{tag          = ConsumerTag,
                                 ack_required = not NoAck,
                                 args         = OtherArgs},
            ExclusiveConsumer = if ExclusiveConsume -> {ChPid, ConsumerTag};
                                   true             -> Holder
                                end,
            State1 = State#q{has_had_consumers = true,
                             exclusive_consumer = ExclusiveConsumer},
            ok = maybe_send_reply(ChPid, OkMsg),
            emit_consumer_created(ChPid, ConsumerTag, ExclusiveConsume,
                                  not NoAck, qname(State1), OtherArgs),
            AC1 = add_consumer({ChPid, Consumer}, State1#q.active_consumers),
            State2 = State1#q{active_consumers = AC1},
            notify_decorators(
              basic_consume, [{consumer_tag, ConsumerTag}], State2),
            reply(ok, run_message_queue(State2))
    end;

handle_call({basic_cancel, ChPid, ConsumerTag, OkMsg}, _From,
            State = #q{exclusive_consumer = Holder}) ->
    ok = maybe_send_reply(ChPid, OkMsg),
    case lookup_ch(ChPid) of
        not_found ->
            reply(ok, State);
        C = #cr{consumer_count    = Count,
                limiter           = Limiter,
                blocked_consumers = Blocked} ->
            emit_consumer_deleted(ChPid, ConsumerTag, qname(State)),
            Blocked1 = remove_consumer(ChPid, ConsumerTag, Blocked),
            Limiter1 = case Count of
                           1 -> rabbit_limiter:deactivate(Limiter);
                           _ -> Limiter
                       end,
            Limiter2 = rabbit_limiter:forget_consumer(Limiter1, ConsumerTag),
            update_ch_record(C#cr{consumer_count    = Count - 1,
                                  limiter           = Limiter2,
                                  blocked_consumers = Blocked1}),
            State1 = State#q{
                       exclusive_consumer = case Holder of
                                                {ChPid, ConsumerTag} -> none;
                                                _                    -> Holder
                                            end,
                       active_consumers   = remove_consumer(
                                              ChPid, ConsumerTag,
                                              State#q.active_consumers)},
            notify_decorators(
              basic_cancel, [{consumer_tag, ConsumerTag}], State1),
            case should_auto_delete(State1) of
                false -> reply(ok, ensure_expiry_timer(State1));
                true  -> stop(ok, State1)
            end
    end;

handle_call(stat, _From, State) ->
    State1 = #q{backing_queue = BQ, backing_queue_state = BQS} =
        ensure_expiry_timer(State),
    reply({ok, BQ:len(BQS), consumer_count()}, State1);

handle_call({delete, IfUnused, IfEmpty}, _From,
            State = #q{backing_queue_state = BQS, backing_queue = BQ}) ->
    IsEmpty  = BQ:is_empty(BQS),
    IsUnused = is_unused(State),
    if
        IfEmpty  and not(IsEmpty)  -> reply({error, not_empty}, State);
        IfUnused and not(IsUnused) -> reply({error,    in_use}, State);
        true                       -> stop({ok, BQ:len(BQS)}, State)
    end;

handle_call(purge, _From, State = #q{backing_queue       = BQ,
                                     backing_queue_state = BQS}) ->
    {Count, BQS1} = BQ:purge(BQS),
    State1 = State#q{backing_queue_state = BQS1},
    reply({ok, Count}, maybe_send_drained(Count =:= 0, State1));

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
                         Ch, CTag, false, AckRequired, QName, Args) ||
                          {Ch, CTag, AckRequired, Args} <- consumers(State)];
        {Ch, CTag} -> [{Ch, CTag, AckRequired, Args}] = consumers(State),
                      emit_consumer_created(
                        Ch, CTag, true, AckRequired, QName, Args)
    end,
    reply(ok, State).

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

handle_cast({resume, ChPid}, State) ->
    noreply(
      possibly_unblock(State, ChPid,
                       fun (C = #cr{limiter = Limiter}) ->
                               C#cr{limiter = rabbit_limiter:resume(Limiter)}
                       end));

handle_cast({notify_sent, ChPid, Credit}, State) ->
    noreply(
      possibly_unblock(State, ChPid,
                       fun (C = #cr{unsent_message_count = Count}) ->
                               C#cr{unsent_message_count = Count - Credit}
                       end));

handle_cast({activate_limit, ChPid}, State) ->
    noreply(
      possibly_unblock(State, ChPid,
                       fun (C = #cr{limiter = Limiter}) ->
                               C#cr{limiter = rabbit_limiter:activate(Limiter)}
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

handle_cast({credit, ChPid, CTag, Credit, Drain},
            State = #q{backing_queue       = BQ,
                       backing_queue_state = BQS}) ->
    Len = BQ:len(BQS),
    rabbit_channel:send_credit_reply(ChPid, Len),
    C = #cr{limiter = Limiter} = lookup_ch(ChPid),
    C1 = C#cr{limiter = rabbit_limiter:credit(Limiter, CTag, Credit, Drain)},
    noreply(case Drain andalso Len == 0 of
                true  -> update_ch_record(C1),
                         send_drained(C1),
                         State;
                false -> case is_ch_blocked(C1) of
                             true  -> update_ch_record(C1),
                                      State;
                             false -> unblock(State, C1)
                         end
            end);

handle_cast(notify_decorators, State) ->
    notify_decorators(refresh, [], State),
    noreply(State);

handle_cast(policy_changed, State = #q{q = #amqqueue{name = Name}}) ->
    %% We depend on the #q.q field being up to date at least WRT
    %% policy (but not slave pids) in various places, so when it
    %% changes we go and read it from Mnesia again.
    %%
    %% This also has the side effect of waking us up so we emit a
    %% stats event - so event consumers see the changed policy.
    {ok, Q} = rabbit_amqqueue:lookup(Name),
    noreply(process_args_policy(State#q{q = Q})).

handle_info({maybe_expire, Vsn}, State = #q{args_policy_version = Vsn}) ->
    case is_unused(State) of
        true  -> stop(State);
        false -> noreply(State#q{expiry_timer_ref = undefined})
    end;

handle_info({maybe_expire, _Vsn}, State) ->
    noreply(State);

handle_info({drop_expired, Vsn}, State = #q{args_policy_version = Vsn}) ->
    WasEmpty = is_empty(State),
    State1 = drop_expired_msgs(State#q{ttl_timer_ref = undefined}),
    noreply(maybe_send_drained(WasEmpty, State1));

handle_info({drop_expired, _Vsn}, State) ->
    noreply(State);

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

handle_info({'DOWN', _MonitorRef, process, DownPid, _Reason}, State) ->
    case handle_ch_down(DownPid, State) of
        {ok, State1}   -> noreply(State1);
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
