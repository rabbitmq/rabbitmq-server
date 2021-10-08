%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_amqqueue_process).
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include("amqqueue.hrl").

-behaviour(gen_server2).

-define(SYNC_INTERVAL,                 200). %% milliseconds
-define(RAM_DURATION_UPDATE_INTERVAL, 5000).
-define(CONSUMER_BIAS_RATIO,           2.0). %% i.e. consume 100% faster

-export([info_keys/0]).

-export([init_with_backing_queue_state/7]).

-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2, handle_pre_hibernate/1, prioritise_call/4,
         prioritise_cast/3, prioritise_info/3, format_message_queue/2]).
-export([format/1]).
-export([is_policy_applicable/2]).

%% Queue's state
-record(q, {
            %% an #amqqueue record
            q :: amqqueue:amqqueue(),
            %% none | {exclusive consumer channel PID, consumer tag} | {single active consumer channel PID, consumer}
            active_consumer,
            %% Set to true if a queue has ever had a consumer.
            %% This is used to determine when to delete auto-delete queues.
            has_had_consumers,
            %% backing queue module.
            %% for mirrored queues, this will be rabbit_mirror_queue_master.
            %% for non-priority and non-mirrored queues, rabbit_variable_queue.
            %% see rabbit_backing_queue.
            backing_queue,
            %% backing queue state.
            %% see rabbit_backing_queue, rabbit_variable_queue.
            backing_queue_state,
            %% consumers state, see rabbit_queue_consumers
            consumers,
            %% queue expiration value
            expires,
            %% timer used to periodically sync (flush) queue index
            sync_timer_ref,
            %% timer used to update ingress/egress rates and queue RAM duration target
            rate_timer_ref,
            %% timer used to clean up this queue due to TTL (on when unused)
            expiry_timer_ref,
            %% stats emission timer
            stats_timer,
            %% maps message IDs to {channel pid, MsgSeqNo}
            %% pairs
            msg_id_to_channel,
            %% message TTL value
            ttl,
            %% timer used to delete expired messages
            ttl_timer_ref,
            ttl_timer_expiry,
            %% Keeps track of channels that publish to this queue.
            %% When channel process goes down, queues have to perform
            %% certain cleanup.
            senders,
            %% dead letter exchange as a #resource record, if any
            dlx,
            dlx_routing_key,
            %% max length in messages, if configured
            max_length,
            %% max length in bytes, if configured
            max_bytes,
            %% an action to perform if queue is to be over a limit,
            %% can be either drop-head (default), reject-publish or reject-publish-dlx
            overflow,
            %% when policies change, this version helps queue
            %% determine what previously scheduled/set up state to ignore,
            %% e.g. message expiration messages from previously set up timers
            %% that may or may not be still valid
            args_policy_version,
            %% used to discard outdated/superseded policy updates,
            %% e.g. when policies are applied concurrently. See
            %% https://github.com/rabbitmq/rabbitmq-server/issues/803 for one
            %% example.
            mirroring_policy_version = 0,
            %% running | flow | idle
            status,
            %% true | false
            single_active_consumer_on
           }).

%%----------------------------------------------------------------------------

-define(STATISTICS_KEYS,
        [messages_ready,
         messages_unacknowledged,
         messages,
         reductions,
         name,
         policy,
         operator_policy,
         effective_policy_definition,
         exclusive_consumer_pid,
         exclusive_consumer_tag,
         single_active_consumer_pid,
         single_active_consumer_tag,
         consumers,
         consumer_utilisation,
         consumer_capacity,
         memory,
         slave_pids,
         synchronised_slave_pids,
         recoverable_slaves,
         state,
         garbage_collection
        ]).

-define(CREATION_EVENT_KEYS,
        [name,
         durable,
         auto_delete,
         arguments,
         owner_pid,
         exclusive,
         user_who_performed_action
        ]).

-define(INFO_KEYS, [pid | ?CREATION_EVENT_KEYS ++ ?STATISTICS_KEYS -- [name]]).

%%----------------------------------------------------------------------------

-spec info_keys() -> rabbit_types:info_keys().

info_keys()       -> ?INFO_KEYS       ++ rabbit_backing_queue:info_keys().
statistics_keys() -> ?STATISTICS_KEYS ++ rabbit_backing_queue:info_keys().

%%----------------------------------------------------------------------------

init(Q) ->
    process_flag(trap_exit, true),
    ?store_proc_name(amqqueue:get_name(Q)),
    {ok, init_state(amqqueue:set_pid(Q, self())), hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE},
    ?MODULE}.

init_state(Q) ->
    SingleActiveConsumerOn = case rabbit_misc:table_lookup(amqqueue:get_arguments(Q), <<"x-single-active-consumer">>) of
        {bool, true} -> true;
        _            -> false
    end,
    State = #q{q                         = Q,
               active_consumer           = none,
               has_had_consumers         = false,
               consumers                 = rabbit_queue_consumers:new(),
               senders                   = pmon:new(delegate),
               msg_id_to_channel         = #{},
               status                    = running,
               args_policy_version       = 0,
               overflow                  = 'drop-head',
               single_active_consumer_on = SingleActiveConsumerOn},
    rabbit_event:init_stats_timer(State, #q.stats_timer).

init_it(Recover, From, State = #q{q = Q})
  when ?amqqueue_exclusive_owner_is(Q, none) ->
    init_it2(Recover, From, State);

%% You used to be able to declare an exclusive durable queue. Sadly we
%% need to still tidy up after that case, there could be the remnants
%% of one left over from an upgrade. So that's why we don't enforce
%% Recover = new here.
init_it(Recover, From, State = #q{q = Q0}) ->
    Owner = amqqueue:get_exclusive_owner(Q0),
    case rabbit_misc:is_process_alive(Owner) of
        true  -> erlang:monitor(process, Owner),
                 init_it2(Recover, From, State);
        false -> #q{backing_queue       = undefined,
                    backing_queue_state = undefined,
                    q                   = Q} = State,
                 send_reply(From, {owner_died, Q}),
                 BQ = backing_queue_module(Q),
                 {_, Terms} = recovery_status(Recover),
                 BQS = bq_init(BQ, Q, Terms),
                 %% Rely on terminate to delete the queue.
                 log_delete_exclusive(Owner, State),
                 {stop, {shutdown, missing_owner},
                  State#q{backing_queue = BQ, backing_queue_state = BQS}}
    end.

init_it2(Recover, From, State = #q{q                   = Q,
                                   backing_queue       = undefined,
                                   backing_queue_state = undefined}) ->
    {Barrier, TermsOrNew} = recovery_status(Recover),
    case rabbit_amqqueue:internal_declare(Q, Recover /= new) of
        {Res, Q1}
          when ?is_amqqueue(Q1) andalso
               (Res == created orelse Res == existing) ->
            case matches(Recover, Q, Q1) of
                true ->
                    ok = file_handle_cache:register_callback(
                           rabbit_amqqueue, set_maximum_since_use, [self()]),
                    ok = rabbit_memory_monitor:register(
                           self(), {rabbit_amqqueue,
                                    set_ram_duration_target, [self()]}),
                    BQ = backing_queue_module(Q1),
                    BQS = bq_init(BQ, Q, TermsOrNew),
                    send_reply(From, {new, Q}),
                    recovery_barrier(Barrier),
                    State1 = process_args_policy(
                               State#q{backing_queue       = BQ,
                                       backing_queue_state = BQS}),
                    notify_decorators(startup, State),
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

recovery_status(new)              -> {no_barrier, new};
recovery_status({Recover, Terms}) -> {Recover,    Terms}.

send_reply(none, _Q) -> ok;
send_reply(From, Q)  -> gen_server2:reply(From, Q).

matches(new, Q1, Q2) ->
    %% i.e. not policy
    amqqueue:get_name(Q1)            =:= amqqueue:get_name(Q2)            andalso
    amqqueue:is_durable(Q1)          =:= amqqueue:is_durable(Q2)          andalso
    amqqueue:is_auto_delete(Q1)      =:= amqqueue:is_auto_delete(Q2)      andalso
    amqqueue:get_exclusive_owner(Q1) =:= amqqueue:get_exclusive_owner(Q2) andalso
    amqqueue:get_arguments(Q1)       =:= amqqueue:get_arguments(Q2)       andalso
    amqqueue:get_pid(Q1)             =:= amqqueue:get_pid(Q2)             andalso
    amqqueue:get_slave_pids(Q1)      =:= amqqueue:get_slave_pids(Q2);
%% FIXME: Should v1 vs. v2 of the same record match?
matches(_,  Q,   Q) -> true;
matches(_, _Q, _Q1) -> false.

recovery_barrier(no_barrier) ->
    ok;
recovery_barrier(BarrierPid) ->
    MRef = erlang:monitor(process, BarrierPid),
    receive
        {BarrierPid, go}              -> erlang:demonitor(MRef, [flush]);
        {'DOWN', MRef, process, _, _} -> ok
    end.

-spec init_with_backing_queue_state
        (amqqueue:amqqueue(), atom(), tuple(), any(),
         [rabbit_types:delivery()], pmon:pmon(), map()) ->
            #q{}.

init_with_backing_queue_state(Q, BQ, BQS,
                              RateTRef, Deliveries, Senders, MTC) ->
    Owner = amqqueue:get_exclusive_owner(Q),
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
                                 maybe_deliver_or_enqueue(Delivery, true, StateN)
                         end, State2, Deliveries),
    notify_decorators(startup, State3),
    State3.

terminate(shutdown = R,      State = #q{backing_queue = BQ, q = Q0}) ->
    QName = amqqueue:get_name(Q0),
    rabbit_core_metrics:queue_deleted(qname(State)),
    terminate_shutdown(
    fun (BQS) ->
        rabbit_misc:execute_mnesia_transaction(
             fun() ->
                [Q] = mnesia:read({rabbit_queue, QName}),
                Q2 = amqqueue:set_state(Q, stopped),
                %% amqqueue migration:
                %% The amqqueue was read from this transaction, no need
                %% to handle migration.
                rabbit_amqqueue:store_queue(Q2)
             end),
        BQ:terminate(R, BQS)
    end, State);
terminate({shutdown, missing_owner} = Reason, State) ->
    %% if the owner was missing then there will be no queue, so don't emit stats
    terminate_shutdown(terminate_delete(false, Reason, State), State);
terminate({shutdown, _} = R, State = #q{backing_queue = BQ}) ->
    rabbit_core_metrics:queue_deleted(qname(State)),
    terminate_shutdown(fun (BQS) -> BQ:terminate(R, BQS) end, State);
terminate(normal, State = #q{status = {terminated_by, auto_delete}}) ->
    %% auto_delete case
    %% To increase performance we want to avoid a mnesia_sync:sync call
    %% after every transaction, as we could be deleting simultaneously
    %% thousands of queues. A optimisation introduced by server#1513
    %% needs to be reverted by this case, avoiding to guard the delete
    %% operation on `rabbit_durable_queue`
    terminate_shutdown(terminate_delete(true, auto_delete, State), State);
terminate(normal,            State) -> %% delete case
    terminate_shutdown(terminate_delete(true, normal, State), State);
%% If we crashed don't try to clean up the BQS, probably best to leave it.
terminate(_Reason,           State = #q{q = Q}) ->
    terminate_shutdown(fun (BQS) ->
                               Q2 = amqqueue:set_state(Q, crashed),
                               rabbit_misc:execute_mnesia_transaction(
                                 fun() ->
                                     ?try_mnesia_tx_or_upgrade_amqqueue_and_retry(
                                        rabbit_amqqueue:store_queue(Q2),
                                        begin
                                            Q3 = amqqueue:upgrade(Q2),
                                            rabbit_amqqueue:store_queue(Q3)
                                        end)
                                 end),
                               BQS
                       end, State).

terminate_delete(EmitStats, Reason0,
                 State = #q{q = Q,
                            backing_queue = BQ,
                            status = Status}) ->
    QName = amqqueue:get_name(Q),
    ActingUser = terminated_by(Status),
    fun (BQS) ->
        Reason = case Reason0 of
                     auto_delete -> normal;
                     Any -> Any
                 end,
        BQS1 = BQ:delete_and_terminate(Reason, BQS),
        if EmitStats -> rabbit_event:if_enabled(State, #q.stats_timer,
                                                fun() -> emit_stats(State) end);
           true      -> ok
        end,
        %% This try-catch block transforms throws to errors since throws are not
        %% logged.
        try
            %% don't care if the internal delete doesn't return 'ok'.
            rabbit_amqqueue:internal_delete(QName, ActingUser, Reason0)
        catch
            {error, ReasonE} -> error(ReasonE)
        end,
        BQS1
    end.

terminated_by({terminated_by, auto_delete}) ->
    ?INTERNAL_USER;
terminated_by({terminated_by, ActingUser}) ->
    ActingUser;
terminated_by(_) ->
    ?INTERNAL_USER.

terminate_shutdown(Fun, #q{status = Status} = State) ->
    ActingUser = terminated_by(Status),
    State1 = #q{backing_queue_state = BQS, consumers = Consumers} =
        lists:foldl(fun (F, S) -> F(S) end, State,
                    [fun stop_sync_timer/1,
                     fun stop_rate_timer/1,
                     fun stop_expiry_timer/1,
                     fun stop_ttl_timer/1]),
    case BQS of
        undefined -> State1;
        _         -> ok = rabbit_memory_monitor:deregister(self()),
                     QName = qname(State),
                     notify_decorators(shutdown, State),
                     [emit_consumer_deleted(Ch, CTag, QName, ActingUser) ||
                         {Ch, CTag, _, _, _, _, _, _} <-
                             rabbit_queue_consumers:all(Consumers)],
                     State1#q{backing_queue_state = Fun(BQS)}
    end.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

maybe_notify_decorators(false, State) -> State;
maybe_notify_decorators(true,  State) -> notify_decorators(State), State.

notify_decorators(Event, State) -> decorator_callback(qname(State), Event, []).

notify_decorators(State = #q{consumers           = Consumers,
                             backing_queue       = BQ,
                             backing_queue_state = BQS}) ->
    P = rabbit_queue_consumers:max_active_priority(Consumers),
    decorator_callback(qname(State), consumer_state_changed,
                       [P, BQ:is_empty(BQS)]).

decorator_callback(QName, F, A) ->
    %% Look up again in case policy and hence decorators have changed
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            Ds = amqqueue:get_decorators(Q),
            [ok = apply(M, F, [Q|A]) || M <- rabbit_queue_decorator:select(Ds)];
        {error, not_found} ->
            ok
    end.

bq_init(BQ, Q, Recover) ->
    Self = self(),
    BQ:init(Q, Recover,
            fun (Mod, Fun) ->
                    rabbit_amqqueue:run_backing_queue(Self, Mod, Fun)
            end).

process_args_policy(State = #q{q                   = Q,
                               args_policy_version = N}) ->
      ArgsTable =
        [{<<"expires">>,                 fun res_min/2, fun init_exp/2},
         {<<"dead-letter-exchange">>,    fun res_arg/2, fun init_dlx/2},
         {<<"dead-letter-routing-key">>, fun res_arg/2, fun init_dlx_rkey/2},
         {<<"message-ttl">>,             fun res_min/2, fun init_ttl/2},
         {<<"max-length">>,              fun res_min/2, fun init_max_length/2},
         {<<"max-length-bytes">>,        fun res_min/2, fun init_max_bytes/2},
         {<<"overflow">>,                fun res_arg/2, fun init_overflow/2},
         {<<"queue-mode">>,              fun res_arg/2, fun init_queue_mode/2}],
         %% @todo queue-version   res_arg   init_queue_version
      drop_expired_msgs(
         lists:foldl(fun({Name, Resolve, Fun}, StateN) ->
                             Fun(rabbit_queue_type_util:args_policy_lookup(Name, Resolve, Q), StateN)
                     end, State#q{args_policy_version = N + 1}, ArgsTable)).

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
init_dlx(DLX, State = #q{q = Q}) ->
    QName = amqqueue:get_name(Q),
    State#q{dlx = rabbit_misc:r(QName, exchange, DLX)}.

init_dlx_rkey(RoutingKey, State) -> State#q{dlx_routing_key = RoutingKey}.

init_max_length(MaxLen, State) ->
    {_Dropped, State1} = maybe_drop_head(State#q{max_length = MaxLen}),
    State1.

init_max_bytes(MaxBytes, State) ->
    {_Dropped, State1} = maybe_drop_head(State#q{max_bytes = MaxBytes}),
    State1.

%% Reset overflow to default 'drop-head' value if it's undefined.
init_overflow(undefined, #q{overflow = 'drop-head'} = State) ->
    State;
init_overflow(undefined, State) ->
    {_Dropped, State1} = maybe_drop_head(State#q{overflow = 'drop-head'}),
    State1;
init_overflow(Overflow, State) ->
    OverflowVal = binary_to_existing_atom(Overflow, utf8),
    case OverflowVal of
        'drop-head' ->
            {_Dropped, State1} = maybe_drop_head(State#q{overflow = OverflowVal}),
            State1;
        _ ->
            State#q{overflow = OverflowVal}
    end.

init_queue_mode(undefined, State) ->
    State;
init_queue_mode(Mode, State = #q {backing_queue = BQ,
                                  backing_queue_state = BQS}) ->
    BQS1 = BQ:set_queue_mode(binary_to_existing_atom(Mode, utf8), BQS),
    State#q{backing_queue_state = BQS1}.

reply(Reply, NewState) ->
    {NewState1, Timeout} = next_state(NewState),
    {reply, Reply, ensure_stats_timer(ensure_rate_timer(NewState1)), Timeout}.

noreply(NewState) ->
    {NewState1, Timeout} = next_state(NewState),
    {noreply, ensure_stats_timer(ensure_rate_timer(NewState1)), Timeout}.

next_state(State = #q{q = Q,
                      backing_queue       = BQ,
                      backing_queue_state = BQS,
                      msg_id_to_channel   = MTC}) ->
    assert_invariant(State),
    {MsgIds, BQS1} = BQ:drain_confirmed(BQS),
    MTC1 = confirm_messages(MsgIds, MTC, amqqueue:get_name(Q)),
    State1 = State#q{backing_queue_state = BQS1, msg_id_to_channel = MTC1},
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
    After = (case Expiry - os:system_time(micro_seconds) of
                 V when V > 0 -> V + 999; %% always fire later
                 _            -> 0
             end) div 1000,
    TRef = rabbit_misc:send_after(After, self(), {drop_expired, Version}),
    State#q{ttl_timer_ref = TRef, ttl_timer_expiry = Expiry};
ensure_ttl_timer(Expiry, State = #q{ttl_timer_ref    = TRef,
                                    ttl_timer_expiry = TExpiry})
  when Expiry + 1000 < TExpiry ->
    rabbit_misc:cancel_timer(TRef),
    ensure_ttl_timer(Expiry, State#q{ttl_timer_ref = undefined});
ensure_ttl_timer(_Expiry, State) ->
    State.

stop_ttl_timer(State) -> rabbit_misc:stop_timer(State, #q.ttl_timer_ref).

ensure_stats_timer(State) ->
    rabbit_event:ensure_stats_timer(State, #q.stats_timer, emit_stats).

assert_invariant(#q{single_active_consumer_on = true}) ->
    %% queue may contain messages and have available consumers with exclusive consumer
    ok;
assert_invariant(State = #q{consumers = Consumers, single_active_consumer_on = false}) ->
    true = (rabbit_queue_consumers:inactive(Consumers) orelse is_empty(State)).

is_empty(#q{backing_queue = BQ, backing_queue_state = BQS}) -> BQ:is_empty(BQS).

maybe_send_drained(WasEmpty, State) ->
    case (not WasEmpty) andalso is_empty(State) of
        true  -> notify_decorators(State),
                 rabbit_queue_consumers:send_drained();
        false -> ok
    end,
    State.

confirm_messages([], MTC, _QName) ->
    MTC;
confirm_messages(MsgIds, MTC, QName) ->
    {CMs, MTC1} =
        lists:foldl(
          fun(MsgId, {CMs, MTC0}) ->
                  case maps:get(MsgId, MTC0, none) of
                      none ->
                          {CMs, MTC0};
                      {SenderPid, MsgSeqNo} ->
                          {maps:update_with(SenderPid,
                                            fun(MsgSeqNos) ->
                                                [MsgSeqNo | MsgSeqNos]
                                            end,
                                            [MsgSeqNo],
                                            CMs),
                           maps:remove(MsgId, MTC0)}

                  end
          end, {#{}, MTC}, MsgIds),
    maps:fold(
        fun(Pid, MsgSeqNos, _) ->
            confirm_to_sender(Pid, QName, MsgSeqNos)
        end,
        ok,
        CMs),
    MTC1.

send_or_record_confirm(#delivery{confirm    = false}, State) ->
    {never, State};
send_or_record_confirm(#delivery{confirm    = true,
                                 sender     = SenderPid,
                                 msg_seq_no = MsgSeqNo,
                                 message    = #basic_message {
                                   is_persistent = true,
                                   id            = MsgId}},
                       State = #q{q                 = Q,
                                  msg_id_to_channel = MTC})
  when ?amqqueue_is_durable(Q) ->
    MTC1 = maps:put(MsgId, {SenderPid, MsgSeqNo}, MTC),
    {eventually, State#q{msg_id_to_channel = MTC1}};
send_or_record_confirm(#delivery{confirm    = true,
                                 sender     = SenderPid,
                                 msg_seq_no = MsgSeqNo},
                       #q{q = Q} = State) ->
    confirm_to_sender(SenderPid, amqqueue:get_name(Q), [MsgSeqNo]),
    {immediately, State}.

%% This feature was used by `rabbit_amqqueue_process` and
%% `rabbit_mirror_queue_slave` up-to and including RabbitMQ 3.7.x. It is
%% unused in 3.8.x and thus deprecated. We keep it to support in-place
%% upgrades to 3.8.x (i.e. mixed-version clusters), but it is a no-op
%% starting with that version.
send_mandatory(#delivery{mandatory  = false}) ->
    ok;
send_mandatory(#delivery{mandatory  = true,
                         sender     = SenderPid,
                         msg_seq_no = MsgSeqNo}) ->
    gen_server2:cast(SenderPid, {mandatory_received, MsgSeqNo}).

discard(#delivery{confirm = Confirm,
                  sender  = SenderPid,
                  flow    = Flow,
                  message = #basic_message{id = MsgId}}, BQ, BQS, MTC, QName) ->
    MTC1 = case Confirm of
               true  -> confirm_messages([MsgId], MTC, QName);
               false -> MTC
           end,
    BQS1 = BQ:discard(MsgId, SenderPid, Flow, BQS),
    {BQS1, MTC1}.

run_message_queue(State) -> run_message_queue(false, State).

run_message_queue(ActiveConsumersChanged, State) ->
    case is_empty(State) of
        true  -> maybe_notify_decorators(ActiveConsumersChanged, State);
        false -> case rabbit_queue_consumers:deliver(
                        fun(AckRequired) -> fetch(AckRequired, State) end,
                        qname(State), State#q.consumers,
                        State#q.single_active_consumer_on, State#q.active_consumer) of
                     {delivered, ActiveConsumersChanged1, State1, Consumers} ->
                         run_message_queue(
                           ActiveConsumersChanged or ActiveConsumersChanged1,
                           State1#q{consumers = Consumers});
                     {undelivered, ActiveConsumersChanged1, Consumers} ->
                         maybe_notify_decorators(
                           ActiveConsumersChanged or ActiveConsumersChanged1,
                           State#q{consumers = Consumers})
                 end
    end.

attempt_delivery(Delivery = #delivery{sender  = SenderPid,
                                      flow    = Flow,
                                      message = Message},
                 Props, Delivered, State = #q{q                   = Q,
                                              backing_queue       = BQ,
                                              backing_queue_state = BQS,
                                              msg_id_to_channel   = MTC}) ->
    case rabbit_queue_consumers:deliver(
           fun (true)  -> true = BQ:is_empty(BQS),
                          {AckTag, BQS1} =
                              BQ:publish_delivered(
                                Message, Props, SenderPid, Flow, BQS),
                          {{Message, Delivered, AckTag}, {BQS1, MTC}};
               (false) -> {{Message, Delivered, undefined},
                           discard(Delivery, BQ, BQS, MTC, amqqueue:get_name(Q))}
           end, qname(State), State#q.consumers, State#q.single_active_consumer_on, State#q.active_consumer) of
        {delivered, ActiveConsumersChanged, {BQS1, MTC1}, Consumers} ->
            {delivered,   maybe_notify_decorators(
                            ActiveConsumersChanged,
                            State#q{backing_queue_state = BQS1,
                                    msg_id_to_channel   = MTC1,
                                    consumers           = Consumers})};
        {undelivered, ActiveConsumersChanged, Consumers} ->
            {undelivered, maybe_notify_decorators(
                            ActiveConsumersChanged,
                            State#q{consumers = Consumers})}
    end.

maybe_deliver_or_enqueue(Delivery = #delivery{message = Message},
                         Delivered,
                         State = #q{overflow            = Overflow,
                                    backing_queue       = BQ,
                                    backing_queue_state = BQS,
                                    dlx                 = DLX,
                                    dlx_routing_key     = RK}) ->
    send_mandatory(Delivery), %% must do this before confirms
    case {will_overflow(Delivery, State), Overflow} of
        {true, 'reject-publish'} ->
            %% Drop publish and nack to publisher
            send_reject_publish(Delivery, Delivered, State);
        {true, 'reject-publish-dlx'} ->
            %% Publish to DLX
            with_dlx(
              DLX,
              fun (X) ->
                      QName = qname(State),
                      rabbit_dead_letter:publish(Message, maxlen, X, RK, QName)
              end,
              fun () -> ok end),
            %% Drop publish and nack to publisher
            send_reject_publish(Delivery, Delivered, State);
        _ ->
            {IsDuplicate, BQS1} = BQ:is_duplicate(Message, BQS),
            State1 = State#q{backing_queue_state = BQS1},
            case IsDuplicate of
                true -> State1;
                {true, drop} -> State1;
                %% Drop publish and nack to publisher
                {true, reject} ->
                    send_reject_publish(Delivery, Delivered, State1);
                %% Enqueue and maybe drop head later
                false ->
                    deliver_or_enqueue(Delivery, Delivered, State1)
            end
    end.

deliver_or_enqueue(Delivery = #delivery{message = Message,
                                        sender  = SenderPid,
                                        flow    = Flow},
                   Delivered,
                   State = #q{q = Q, backing_queue = BQ}) ->
    {Confirm, State1} = send_or_record_confirm(Delivery, State),
    Props = message_properties(Message, Confirm, State1),
    case attempt_delivery(Delivery, Props, Delivered, State1) of
        {delivered, State2} ->
            State2;
        %% The next one is an optimisation
        {undelivered, State2 = #q{ttl = 0, dlx = undefined,
                                  backing_queue_state = BQS,
                                  msg_id_to_channel   = MTC}} ->
            {BQS1, MTC1} = discard(Delivery, BQ, BQS, MTC, amqqueue:get_name(Q)),
            State2#q{backing_queue_state = BQS1, msg_id_to_channel = MTC1};
        {undelivered, State2 = #q{backing_queue_state = BQS}} ->

            BQS1 = BQ:publish(Message, Props, Delivered, SenderPid, Flow, BQS),
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
            case {Dropped, QLen =:= 1, Props#message_properties.expiry} of
                {false, false,         _} -> State3;
                {true,  true,  undefined} -> State3;
                {_,     _,             _} -> drop_expired_msgs(State3)
            end
    end.

maybe_drop_head(State = #q{max_length = undefined,
                           max_bytes  = undefined}) ->
    {false, State};
maybe_drop_head(State = #q{overflow = 'reject-publish'}) ->
    {false, State};
maybe_drop_head(State = #q{overflow = 'reject-publish-dlx'}) ->
    {false, State};
maybe_drop_head(State = #q{overflow = 'drop-head'}) ->
    maybe_drop_head(false, State).

maybe_drop_head(AlreadyDropped, State = #q{backing_queue       = BQ,
                                           backing_queue_state = BQS}) ->
    case over_max_length(State) of
        true ->
            maybe_drop_head(true,
                            with_dlx(
                              State#q.dlx,
                              fun (X) -> dead_letter_maxlen_msg(X, State) end,
                              fun () ->
                                      {_, BQS1} = BQ:drop(false, BQS),
                                      State#q{backing_queue_state = BQS1}
                              end));
        false ->
            {AlreadyDropped, State}
    end.

send_reject_publish(#delivery{confirm = true,
                              sender = SenderPid,
                              flow = Flow,
                              msg_seq_no = MsgSeqNo,
                              message = #basic_message{id = MsgId}},
                      _Delivered,
                      State = #q{ q = Q,
                                  backing_queue = BQ,
                                  backing_queue_state = BQS,
                                  msg_id_to_channel   = MTC}) ->
    ok = rabbit_classic_queue:send_rejection(SenderPid,
                                             amqqueue:get_name(Q), MsgSeqNo),

    MTC1 = maps:remove(MsgId, MTC),
    BQS1 = BQ:discard(MsgId, SenderPid, Flow, BQS),
    State#q{ backing_queue_state = BQS1, msg_id_to_channel = MTC1 };
send_reject_publish(#delivery{confirm = false},
                      _Delivered, State) ->
    State.

will_overflow(_, #q{max_length = undefined,
                    max_bytes  = undefined}) -> false;
will_overflow(#delivery{message = Message},
              #q{max_length          = MaxLen,
                 max_bytes           = MaxBytes,
                 backing_queue       = BQ,
                 backing_queue_state = BQS}) ->
    ExpectedQueueLength = BQ:len(BQS) + 1,

    #basic_message{content = #content{payload_fragments_rev = PFR}} = Message,
    MessageSize = iolist_size(PFR),
    ExpectedQueueSizeBytes = BQ:info(message_bytes_ready, BQS) + MessageSize,

    ExpectedQueueLength > MaxLen orelse ExpectedQueueSizeBytes > MaxBytes.

over_max_length(#q{max_length          = MaxLen,
                   max_bytes           = MaxBytes,
                   backing_queue       = BQ,
                   backing_queue_state = BQS}) ->
    BQ:len(BQS) > MaxLen orelse BQ:info(message_bytes_ready, BQS) > MaxBytes.

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

possibly_unblock(Update, ChPid, State = #q{consumers = Consumers}) ->
    case rabbit_queue_consumers:possibly_unblock(Update, ChPid, Consumers) of
        unchanged               -> State;
        {unblocked, Consumers1} -> State1 = State#q{consumers = Consumers1},
                                   run_message_queue(true, State1)
    end.

should_auto_delete(#q{q = Q})
  when not ?amqqueue_is_auto_delete(Q) -> false;
should_auto_delete(#q{has_had_consumers = false}) -> false;
should_auto_delete(State) -> is_unused(State).

handle_ch_down(DownPid, State = #q{consumers                 = Consumers,
                                   active_consumer           = Holder,
                                   single_active_consumer_on = SingleActiveConsumerOn,
                                   senders                   = Senders}) ->
    State1 = State#q{senders = case pmon:is_monitored(DownPid, Senders) of
                                   false ->
                                       Senders;
                                   true  ->
    %% A rabbit_channel process died. Here credit_flow will take care
    %% of cleaning up the rabbit_amqqueue_process process dictionary
    %% with regards to the credit we were tracking for the channel
    %% process. See handle_cast({deliver, Deliver, ...}, State) in this
    %% module. In that cast function we process deliveries from the
    %% channel, which means we credit_flow:ack/1 said
    %% messages. credit_flow:ack'ing messages means we are increasing
    %% a counter to know when we need to send MoreCreditAfter. Since
    %% the process died, the credit_flow flow module will clean up
    %% that for us.
                                       credit_flow:peer_down(DownPid),
                                       pmon:demonitor(DownPid, Senders)
                               end},
    case rabbit_queue_consumers:erase_ch(DownPid, Consumers) of
        not_found ->
            {ok, State1};
        {ChAckTags, ChCTags, Consumers1} ->
            QName = qname(State1),
            [emit_consumer_deleted(DownPid, CTag, QName, ?INTERNAL_USER) || CTag <- ChCTags],
            Holder1 = new_single_active_consumer_after_channel_down(DownPid, Holder, SingleActiveConsumerOn, Consumers1),
            State2 = State1#q{consumers          = Consumers1,
                              active_consumer    = Holder1},
            maybe_notify_consumer_updated(State2, Holder, Holder1),
            notify_decorators(State2),
            case should_auto_delete(State2) of
                true  ->
                    log_auto_delete(
                        io_lib:format(
                            "because all of its consumers (~p) were on a channel that was closed",
                            [length(ChCTags)]),
                        State),
                    {stop, State2};
                false -> {ok, requeue_and_run(ChAckTags,
                                              ensure_expiry_timer(State2))}
            end
    end.

new_single_active_consumer_after_channel_down(DownChPid, CurrentSingleActiveConsumer, _SingleActiveConsumerIsOn = true, Consumers) ->
    case CurrentSingleActiveConsumer of
        {DownChPid, _} ->
            % the single active consumer is on the down channel, we have to replace it
            case rabbit_queue_consumers:get_consumer(Consumers) of
                undefined -> none;
                Consumer  -> Consumer
            end;
        _ ->
            CurrentSingleActiveConsumer
    end;
new_single_active_consumer_after_channel_down(DownChPid, CurrentSingleActiveConsumer, _SingleActiveConsumerIsOn = false, _Consumers) ->
    case CurrentSingleActiveConsumer of
        {DownChPid, _} -> none;
        Other          -> Other
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

is_unused(_State) -> rabbit_queue_consumers:count() == 0.

maybe_send_reply(_ChPid, undefined) -> ok;
maybe_send_reply(ChPid, Msg) -> ok = rabbit_channel:send_command(ChPid, Msg).

qname(#q{q = Q}) -> amqqueue:get_name(Q).

backing_queue_timeout(State = #q{backing_queue       = BQ,
                                 backing_queue_state = BQS}) ->
    State#q{backing_queue_state = BQ:timeout(BQS)}.

subtract_acks(ChPid, AckTags, State = #q{consumers = Consumers}, Fun) ->
    case rabbit_queue_consumers:subtract_acks(ChPid, AckTags, Consumers) of
        not_found               -> State;
        unchanged               -> Fun(State);
        {unblocked, Consumers1} -> State1 = State#q{consumers = Consumers1},
                                   run_message_queue(true, Fun(State1))
    end.

message_properties(Message = #basic_message{content = Content},
                   Confirm, #q{ttl = TTL}) ->
    #content{payload_fragments_rev = PFR} = Content,
    #message_properties{expiry           = calculate_msg_expiry(Message, TTL),
                        needs_confirming = Confirm == eventually,
                        size             = iolist_size(PFR)}.

calculate_msg_expiry(#basic_message{content = Content}, TTL) ->
    #content{properties = Props} =
        rabbit_binary_parser:ensure_content_decoded(Content),
    %% We assert that the expiration must be valid - we check in the channel.
    {ok, MsgTTL} = rabbit_basic:parse_expiration(Props),
    case lists:min([TTL, MsgTTL]) of
        undefined -> undefined;
        T         -> os:system_time(micro_seconds) + T * 1000
    end.

%% Logically this function should invoke maybe_send_drained/2.
%% However, that is expensive. Since some frequent callers of
%% drop_expired_msgs/1, in particular deliver_or_enqueue/3, cannot
%% possibly cause the queue to become empty, we push the
%% responsibility to the callers. So be cautious when adding new ones.
drop_expired_msgs(State) ->
    case is_empty(State) of
        true  -> State;
        false -> drop_expired_msgs(os:system_time(micro_seconds),
                                   State)
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

dead_letter_maxlen_msg(X, State = #q{backing_queue = BQ}) ->
    {ok, State1} =
        dead_letter_msgs(
          fun (DLFun, Acc, BQS) ->
                  {{Msg, _, AckTag}, BQS1} = BQ:fetch(true, BQS),
                  {ok, DLFun(Msg, AckTag, Acc), BQS1}
          end, maxlen, X, State),
    State1.

dead_letter_msgs(Fun, Reason, X, State = #q{dlx_routing_key     = RK,
                                            backing_queue_state = BQS,
                                            backing_queue       = BQ}) ->
    QName = qname(State),
    {Res, Acks1, BQS1} =
        Fun(fun (Msg, AckTag, Acks) ->
                    rabbit_dead_letter:publish(Msg, Reason, X, RK, QName),
                    [AckTag | Acks]
            end, [], BQS),
    {_Guids, BQS2} = BQ:ack(Acks1, BQS1),
    {Res, State#q{backing_queue_state = BQS2}}.

stop(State) -> stop(noreply, State).

stop(noreply, State) -> {stop, normal, State};
stop(Reply,   State) -> {stop, normal, Reply, State}.

infos(Items, #q{q = Q} = State) ->
    lists:foldr(fun(totals, Acc) ->
                        [{messages_ready, i(messages_ready, State)},
                         {messages, i(messages, State)},
                         {messages_unacknowledged, i(messages_unacknowledged, State)}] ++ Acc;
                   (type_specific, Acc) ->
                        format(Q) ++ Acc;
                   (Item, Acc) ->
                        [{Item, i(Item, State)} | Acc]
                end, [], Items).

i(name,        #q{q = Q}) -> amqqueue:get_name(Q);
i(durable,     #q{q = Q}) -> amqqueue:is_durable(Q);
i(auto_delete, #q{q = Q}) -> amqqueue:is_auto_delete(Q);
i(arguments,   #q{q = Q}) -> amqqueue:get_arguments(Q);
i(pid, _) ->
    self();
i(owner_pid, #q{q = Q}) when ?amqqueue_exclusive_owner_is(Q, none) ->
    '';
i(owner_pid, #q{q = Q}) ->
    amqqueue:get_exclusive_owner(Q);
i(exclusive, #q{q = Q}) ->
    ExclusiveOwner = amqqueue:get_exclusive_owner(Q),
    is_pid(ExclusiveOwner);
i(policy,    #q{q = Q}) ->
    case rabbit_policy:name(Q) of
        none   -> '';
        Policy -> Policy
    end;
i(operator_policy,    #q{q = Q}) ->
    case rabbit_policy:name_op(Q) of
        none   -> '';
        Policy -> Policy
    end;
i(effective_policy_definition,  #q{q = Q}) ->
    case rabbit_policy:effective_definition(Q) of
        undefined -> [];
        Def       -> Def
    end;
i(exclusive_consumer_pid, #q{active_consumer = {ChPid, _ConsumerTag}, single_active_consumer_on = false}) ->
    ChPid;
i(exclusive_consumer_pid, _) ->
    '';
i(exclusive_consumer_tag, #q{active_consumer = {_ChPid, ConsumerTag}, single_active_consumer_on = false}) ->
    ConsumerTag;
i(exclusive_consumer_tag, _) ->
    '';
i(single_active_consumer_pid, #q{active_consumer = {ChPid, _Consumer}, single_active_consumer_on = true}) ->
    ChPid;
i(single_active_consumer_pid, _) ->
    '';
i(single_active_consumer_tag, #q{active_consumer = {_ChPid, Consumer}, single_active_consumer_on = true}) ->
    rabbit_queue_consumers:consumer_tag(Consumer);
i(single_active_consumer_tag, _) ->
    '';
i(messages_ready, #q{backing_queue_state = BQS, backing_queue = BQ}) ->
    BQ:len(BQS);
i(messages_unacknowledged, _) ->
    rabbit_queue_consumers:unacknowledged_message_count();
i(messages, State) ->
    lists:sum([i(Item, State) || Item <- [messages_ready,
                                          messages_unacknowledged]]);
i(consumers, _) ->
    rabbit_queue_consumers:count();
i(consumer_utilisation, State) ->
    i(consumer_capacity, State);
i(consumer_capacity, #q{consumers = Consumers}) ->
    case rabbit_queue_consumers:count() of
        0 -> 0;
        _ -> rabbit_queue_consumers:capacity(Consumers)
    end;
i(memory, _) ->
    {memory, M} = process_info(self(), memory),
    M;
i(slave_pids, #q{q = Q0}) ->
    Name = amqqueue:get_name(Q0),
    {ok, Q} = rabbit_amqqueue:lookup(Name),
    case rabbit_mirror_queue_misc:is_mirrored(Q) of
        false -> '';
        true  -> amqqueue:get_slave_pids(Q)
    end;
i(synchronised_slave_pids, #q{q = Q0}) ->
    Name = amqqueue:get_name(Q0),
    {ok, Q} = rabbit_amqqueue:lookup(Name),
    case rabbit_mirror_queue_misc:is_mirrored(Q) of
        false -> '';
        true  -> amqqueue:get_sync_slave_pids(Q)
    end;
i(recoverable_slaves, #q{q = Q0}) ->
    Name = amqqueue:get_name(Q0),
    Durable = amqqueue:is_durable(Q0),
    {ok, Q} = rabbit_amqqueue:lookup(Name),
    case Durable andalso rabbit_mirror_queue_misc:is_mirrored(Q) of
        false -> '';
        true  -> amqqueue:get_recoverable_slaves(Q)
    end;
i(state, #q{status = running}) -> credit_flow:state();
i(state, #q{status = State})   -> State;
i(garbage_collection, _State) ->
    rabbit_misc:get_gc_info(self());
i(reductions, _State) ->
    {reductions, Reductions} = erlang:process_info(self(), reductions),
    Reductions;
i(user_who_performed_action, #q{q = Q}) ->
    Opts = amqqueue:get_options(Q),
    maps:get(user, Opts, ?UNKNOWN_USER);
i(type, _) -> classic;
i(Item, #q{backing_queue_state = BQS, backing_queue = BQ}) ->
    BQ:info(Item, BQS).

emit_stats(State) ->
    emit_stats(State, []).

emit_stats(State, Extra) ->
    ExtraKs = [K || {K, _} <- Extra],
    [{messages_ready, MR}, {messages_unacknowledged, MU}, {messages, M},
     {reductions, R}, {name, Name} | Infos] = All
	= [{K, V} || {K, V} <- infos(statistics_keys(), State),
		     not lists:member(K, ExtraKs)],
    rabbit_core_metrics:queue_stats(Name, Extra ++ Infos),
    rabbit_core_metrics:queue_stats(Name, MR, MU, M, R),
    rabbit_event:notify(queue_stats, Extra ++ All).

emit_consumer_created(ChPid, CTag, Exclusive, AckRequired, QName,
                      PrefetchCount, Args, Ref, ActingUser) ->
    rabbit_event:notify(consumer_created,
                        [{consumer_tag,   CTag},
                         {exclusive,      Exclusive},
                         {ack_required,   AckRequired},
                         {channel,        ChPid},
                         {queue,          QName},
                         {prefetch_count, PrefetchCount},
                         {arguments,      Args},
                         {user_who_performed_action, ActingUser}],
                        Ref).

emit_consumer_deleted(ChPid, ConsumerTag, QName, ActingUser) ->
    rabbit_core_metrics:consumer_deleted(ChPid, ConsumerTag, QName),
    rabbit_event:notify(consumer_deleted,
                        [{consumer_tag, ConsumerTag},
                         {channel,      ChPid},
                         {queue,        QName},
                         {user_who_performed_action, ActingUser}]).

%%----------------------------------------------------------------------------

prioritise_call(Msg, _From, _Len, State) ->
    case Msg of
        info                                       -> 9;
        {info, _Items}                             -> 9;
        consumers                                  -> 9;
        stat                                       -> 7;
        {basic_consume, _, _, _, _, _, _, _, _, _} -> consumer_bias(State, 0, 2);
        {basic_cancel, _, _, _}                    -> consumer_bias(State, 0, 2);
        _                                          -> 0
    end.

prioritise_cast(Msg, _Len, State) ->
    case Msg of
        delete_immediately                   -> 8;
        {delete_exclusive, _Pid}             -> 8;
        {set_ram_duration_target, _Duration} -> 8;
        {set_maximum_since_use, _Age}        -> 8;
        {run_backing_queue, _Mod, _Fun}      -> 6;
        {ack, _AckTags, _ChPid}              -> 4; %% [1]
        {resume, _ChPid}                     -> 3;
        {notify_sent, _ChPid, _Credit}       -> consumer_bias(State, 0, 2);
        _                                    -> 0
    end.

%% [1] It should be safe to always prioritise ack / resume since they
%% will be rate limited by how fast consumers receive messages -
%% i.e. by notify_sent. We prioritise ack and resume to discourage
%% starvation caused by prioritising notify_sent. We don't vary their
%% priority since acks should stay in order (some parts of the queue
%% stack are optimised for that) and to make things easier to reason
%% about. Finally, we prioritise ack over resume since it should
%% always reduce memory use.
%% bump_reduce_memory_use is prioritised over publishes, because sending
%% credit to self is hard to reason about. Consumers can continue while
%% reduce_memory_use is in progress.

consumer_bias(#q{backing_queue = BQ, backing_queue_state = BQS}, Low, High) ->
    case BQ:msg_rates(BQS) of
        {0.0,          _} -> Low;
        {Ingress, Egress} when Egress / Ingress < ?CONSUMER_BIAS_RATIO -> High;
        {_,            _} -> Low
    end.

prioritise_info(Msg, _Len, #q{q = Q}) ->
    DownPid = amqqueue:get_exclusive_owner(Q),
    case Msg of
        {'DOWN', _, process, DownPid, _}     -> 8;
        update_ram_duration                  -> 8;
        {maybe_expire, _Version}             -> 8;
        {drop_expired, _Version}             -> 8;
        emit_stats                           -> 7;
        sync_timeout                         -> 6;
        bump_reduce_memory_use               -> 1;
        _                                    -> 0
    end.

handle_call({init, Recover}, From, State) ->
    try
	init_it(Recover, From, State)
    catch
	{coordinator_not_started, Reason} ->
	    %% The GM can shutdown before the coordinator has started up
	    %% (lost membership or missing group), thus the start_link of
	    %% the coordinator returns {error, shutdown} as rabbit_amqqueue_process
	    %% is trapping exists. The master captures this return value and
	    %% throws the current exception.
	    {stop, Reason, State}
    end;

handle_call(info, _From, State) ->
    reply({ok, infos(info_keys(), State)}, State);

handle_call({info, Items}, _From, State) ->
    try
        reply({ok, infos(Items, State)}, State)
    catch Error -> reply({error, Error}, State)
    end;

handle_call(consumers, _From, State = #q{consumers = Consumers, single_active_consumer_on = false}) ->
    reply(rabbit_queue_consumers:all(Consumers), State);
handle_call(consumers, _From, State = #q{consumers = Consumers, active_consumer = ActiveConsumer}) ->
    reply(rabbit_queue_consumers:all(Consumers, ActiveConsumer, true), State);

handle_call({notify_down, ChPid}, _From, State) ->
    %% we want to do this synchronously, so that auto_deleted queues
    %% are no longer visible by the time we send a response to the
    %% client.  The queue is ultimately deleted in terminate/2; if we
    %% return stop with a reply, terminate/2 will be called by
    %% gen_server2 *before* the reply is sent.
    case handle_ch_down(ChPid, State) of
        {ok, State1}   -> reply(ok, State1);
        {stop, State1} -> stop(ok, State1#q{status = {terminated_by, auto_delete}})
    end;

handle_call({basic_get, ChPid, NoAck, LimiterPid}, _From,
            State = #q{q = Q}) ->
    QName = amqqueue:get_name(Q),
    AckRequired = not NoAck,
    State1 = ensure_expiry_timer(State),
    case fetch(AckRequired, State1) of
        {empty, State2} ->
            reply(empty, State2);
        {{Message, IsDelivered, AckTag},
         #q{backing_queue = BQ, backing_queue_state = BQS} = State2} ->
            case AckRequired of
                true  -> ok = rabbit_queue_consumers:record_ack(
                                ChPid, LimiterPid, AckTag);
                false -> ok
            end,
            Msg = {QName, self(), AckTag, IsDelivered, Message},
            reply({ok, BQ:len(BQS), Msg}, State2)
    end;

handle_call({basic_consume, NoAck, ChPid, LimiterPid, LimiterActive,
             PrefetchCount, ConsumerTag, ExclusiveConsume, Args, OkMsg, ActingUser},
            _From, State = #q{consumers             = Consumers,
                              active_consumer = Holder,
                              single_active_consumer_on = SingleActiveConsumerOn}) ->
    ConsumerRegistration = case SingleActiveConsumerOn of
        true ->
            case ExclusiveConsume of
                true  ->
                    {error, reply({error, exclusive_consume_unavailable}, State)};
                false ->
                  Consumers1 = rabbit_queue_consumers:add(
                    ChPid, ConsumerTag, NoAck,
                    LimiterPid, LimiterActive,
                    PrefetchCount, Args, is_empty(State),
                    ActingUser, Consumers),

                  case Holder of
                      none ->
                          NewConsumer = rabbit_queue_consumers:get(ChPid, ConsumerTag, Consumers1),
                          {state, State#q{consumers          = Consumers1,
                                          has_had_consumers  = true,
                                          active_consumer    = NewConsumer}};
                      _    ->
                          {state, State#q{consumers          = Consumers1,
                                          has_had_consumers  = true}}
                  end
            end;
        false ->
            case check_exclusive_access(Holder, ExclusiveConsume, State) of
              in_use -> {error, reply({error, exclusive_consume_unavailable}, State)};
              ok     ->
                    Consumers1 = rabbit_queue_consumers:add(
                                   ChPid, ConsumerTag, NoAck,
                                   LimiterPid, LimiterActive,
                                   PrefetchCount, Args, is_empty(State),
                                   ActingUser, Consumers),
                    ExclusiveConsumer =
                        if ExclusiveConsume -> {ChPid, ConsumerTag};
                           true             -> Holder
                        end,
                    {state, State#q{consumers          = Consumers1,
                                    has_had_consumers  = true,
                                    active_consumer    = ExclusiveConsumer}}
            end
    end,
    case ConsumerRegistration of
        {error, Reply} ->
            Reply;
        {state, State1} ->
            ok = maybe_send_reply(ChPid, OkMsg),
            QName = qname(State1),
            AckRequired = not NoAck,
            TheConsumer = rabbit_queue_consumers:get(ChPid, ConsumerTag, State1#q.consumers),
            {ConsumerIsActive, ActivityStatus} =
                case {SingleActiveConsumerOn, State1#q.active_consumer} of
                    {true, TheConsumer} ->
                       {true, single_active};
                    {true, _} ->
                       {false, waiting};
                    {false, _} ->
                       {true, up}
                end,
            rabbit_core_metrics:consumer_created(
                ChPid, ConsumerTag, ExclusiveConsume, AckRequired, QName,
                PrefetchCount, ConsumerIsActive, ActivityStatus, Args),
            emit_consumer_created(ChPid, ConsumerTag, ExclusiveConsume,
                AckRequired, QName, PrefetchCount,
                Args, none, ActingUser),
            notify_decorators(State1),
            reply(ok, run_message_queue(State1))
    end;

handle_call({basic_cancel, ChPid, ConsumerTag, OkMsg, ActingUser}, _From,
            State = #q{consumers                 = Consumers,
                       active_consumer           = Holder,
                       single_active_consumer_on = SingleActiveConsumerOn }) ->
    ok = maybe_send_reply(ChPid, OkMsg),
    case rabbit_queue_consumers:remove(ChPid, ConsumerTag, Consumers) of
        not_found ->
            reply(ok, State);
        Consumers1 ->
            Holder1 = new_single_active_consumer_after_basic_cancel(ChPid, ConsumerTag,
                Holder, SingleActiveConsumerOn, Consumers1
            ),
            State1 = State#q{consumers          = Consumers1,
                             active_consumer    = Holder1},
            maybe_notify_consumer_updated(State1, Holder, Holder1),
            emit_consumer_deleted(ChPid, ConsumerTag, qname(State1), ActingUser),
            notify_decorators(State1),
            case should_auto_delete(State1) of
                false -> reply(ok, ensure_expiry_timer(State1));
                true  ->
                    log_auto_delete(
                        io_lib:format(
                            "because its last consumer with tag '~s' was cancelled",
                            [ConsumerTag]),
                        State),
                    stop(ok, State1)
            end
    end;

handle_call(stat, _From, State) ->
    State1 = #q{backing_queue = BQ, backing_queue_state = BQS} =
        ensure_expiry_timer(State),
    reply({ok, BQ:len(BQS), rabbit_queue_consumers:count()}, State1);

handle_call({delete, IfUnused, IfEmpty, ActingUser}, _From,
            State = #q{backing_queue_state = BQS, backing_queue = BQ}) ->
    IsEmpty  = BQ:is_empty(BQS),
    IsUnused = is_unused(State),
    if
        IfEmpty  and not(IsEmpty)  -> reply({error, not_empty}, State);
        IfUnused and not(IsUnused) -> reply({error,    in_use}, State);
        true                       -> stop({ok, BQ:len(BQS)},
                                           State#q{status = {terminated_by, ActingUser}})
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
    reply({ok, not_syncing}, State).

new_single_active_consumer_after_basic_cancel(ChPid, ConsumerTag, CurrentSingleActiveConsumer,
            _SingleActiveConsumerIsOn = true, Consumers) ->
    case rabbit_queue_consumers:is_same(ChPid, ConsumerTag, CurrentSingleActiveConsumer) of
        true ->
            case rabbit_queue_consumers:get_consumer(Consumers) of
                undefined -> none;
                Consumer  -> Consumer
            end;
        false ->
            CurrentSingleActiveConsumer
    end;
new_single_active_consumer_after_basic_cancel(ChPid, ConsumerTag, CurrentSingleActiveConsumer,
            _SingleActiveConsumerIsOn = false, _Consumers) ->
    case CurrentSingleActiveConsumer of
        {ChPid, ConsumerTag} -> none;
        _                    -> CurrentSingleActiveConsumer
    end.

maybe_notify_consumer_updated(#q{single_active_consumer_on = false}, _, _) ->
    ok;
maybe_notify_consumer_updated(#q{single_active_consumer_on = true}, SingleActiveConsumer, SingleActiveConsumer) ->
    % the single active consumer didn't change, nothing to do
    ok;
maybe_notify_consumer_updated(#q{single_active_consumer_on = true} = State, _PreviousConsumer, NewConsumer) ->
    case NewConsumer of
        {ChPid, Consumer} ->
            {Tag, Ack, Prefetch, Args} = rabbit_queue_consumers:get_infos(Consumer),
            rabbit_core_metrics:consumer_updated(
                ChPid, Tag, false, Ack, qname(State),
                Prefetch, true, single_active, Args
            ),
            ok;
        _ ->
            ok
    end.

handle_cast(init, State) ->
    try
	init_it({no_barrier, non_clean_shutdown}, none, State)
    catch
	{coordinator_not_started, Reason} ->
	    %% The GM can shutdown before the coordinator has started up
	    %% (lost membership or missing group), thus the start_link of
	    %% the coordinator returns {error, shutdown} as rabbit_amqqueue_process
	    %% is trapping exists. The master captures this return value and
	    %% throws the current exception.
	    {stop, Reason, State}
    end;

handle_cast({run_backing_queue, Mod, Fun},
            State = #q{backing_queue = BQ, backing_queue_state = BQS}) ->
    noreply(State#q{backing_queue_state = BQ:invoke(Mod, Fun, BQS)});

handle_cast({deliver,
             Delivery = #delivery{sender = Sender,
                                  flow   = Flow},
             SlaveWhenPublished},
            State = #q{senders = Senders}) ->
    Senders1 = case Flow of
    %% In both credit_flow:ack/1 we are acking messages to the channel
    %% process that sent us the message delivery. See handle_ch_down
    %% for more info.
                   flow   -> credit_flow:ack(Sender),
                             case SlaveWhenPublished of
                                 true  -> credit_flow:ack(Sender); %% [0]
                                 false -> ok
                             end,
                             pmon:monitor(Sender, Senders);
                   noflow -> Senders
               end,
    State1 = State#q{senders = Senders1},
    noreply(maybe_deliver_or_enqueue(Delivery, SlaveWhenPublished, State1));
%% [0] The second ack is since the channel thought we were a mirror at
%% the time it published this message, so it used two credits (see
%% rabbit_queue_type:deliver/2).

handle_cast({ack, AckTags, ChPid}, State) ->
    noreply(ack(AckTags, ChPid, State));

handle_cast({reject, true,  AckTags, ChPid}, State) ->
    noreply(requeue(AckTags, ChPid, State));

handle_cast({reject, false, AckTags, ChPid}, State) ->
    noreply(with_dlx(
              State#q.dlx,
              fun (X) -> subtract_acks(ChPid, AckTags, State,
                                       fun (State1) ->
                                               dead_letter_rejected_msgs(
                                                 AckTags, X, State1)
                                       end) end,
              fun () -> ack(AckTags, ChPid, State) end));

handle_cast({delete_exclusive, ConnPid}, State) ->
    log_delete_exclusive(ConnPid, State),
    stop(State);

handle_cast(delete_immediately, State) ->
    stop(State);

handle_cast({resume, ChPid}, State) ->
    noreply(possibly_unblock(rabbit_queue_consumers:resume_fun(),
                             ChPid, State));

handle_cast({notify_sent, ChPid, Credit}, State) ->
    noreply(possibly_unblock(rabbit_queue_consumers:notify_sent_fun(Credit),
                             ChPid, State));

handle_cast({activate_limit, ChPid}, State) ->
    noreply(possibly_unblock(rabbit_queue_consumers:activate_limit_fun(),
                             ChPid, State));

handle_cast({deactivate_limit, ChPid}, State) ->
    noreply(possibly_unblock(rabbit_queue_consumers:deactivate_limit_fun(),
                             ChPid, State));

handle_cast({set_ram_duration_target, Duration},
            State = #q{backing_queue = BQ, backing_queue_state = BQS}) ->
    BQS1 = BQ:set_ram_duration_target(Duration, BQS),
    noreply(State#q{backing_queue_state = BQS1});

handle_cast({set_maximum_since_use, Age}, State) ->
    ok = file_handle_cache:set_maximum_since_use(Age),
    noreply(State);

handle_cast(update_mirroring, State = #q{q = Q,
                                         mirroring_policy_version = Version}) ->
    case needs_update_mirroring(Q, Version) of
        false ->
            noreply(State);
        {Policy, NewVersion} ->
            State1 = State#q{mirroring_policy_version = NewVersion},
            noreply(update_mirroring(Policy, State1))
    end;

handle_cast({credit, ChPid, CTag, Credit, Drain},
            State = #q{consumers           = Consumers,
                       backing_queue       = BQ,
                       backing_queue_state = BQS,
                       q = Q}) ->
    Len = BQ:len(BQS),
    rabbit_classic_queue:send_queue_event(ChPid, amqqueue:get_name(Q), {send_credit_reply, Len}),
    noreply(
      case rabbit_queue_consumers:credit(Len == 0, Credit, Drain, ChPid, CTag,
                                         Consumers) of
          unchanged               -> State;
          {unblocked, Consumers1} -> State1 = State#q{consumers = Consumers1},
                                     run_message_queue(true, State1)
      end);

% Note: https://www.pivotaltracker.com/story/show/166962656
% This event is necessary for the stats timer to be initialized with
% the correct values once the management agent has started
handle_cast({force_event_refresh, Ref},
            State = #q{consumers = Consumers}) ->
    rabbit_event:notify(queue_created, infos(?CREATION_EVENT_KEYS, State), Ref),
    QName = qname(State),
    AllConsumers = rabbit_queue_consumers:all(Consumers),
    rabbit_log:debug("Queue ~s forced to re-emit events, consumers: ~p", [rabbit_misc:rs(QName), AllConsumers]),
    [emit_consumer_created(
       Ch, CTag, ActiveOrExclusive, AckRequired, QName, Prefetch,
       Args, Ref, ActingUser) ||
        {Ch, CTag, AckRequired, Prefetch, ActiveOrExclusive, _, Args, ActingUser}
            <- AllConsumers],
    noreply(rabbit_event:init_stats_timer(State, #q.stats_timer));

handle_cast(notify_decorators, State) ->
    notify_decorators(State),
    noreply(State);

handle_cast(policy_changed, State = #q{q = Q0}) ->
    Name = amqqueue:get_name(Q0),
    %% We depend on the #q.q field being up to date at least WRT
    %% policy (but not mirror pids) in various places, so when it
    %% changes we go and read it from Mnesia again.
    %%
    %% This also has the side effect of waking us up so we emit a
    %% stats event - so event consumers see the changed policy.
    {ok, Q} = rabbit_amqqueue:lookup(Name),
    noreply(process_args_policy(State#q{q = Q}));

handle_cast({sync_start, _, _}, State = #q{q = Q}) ->
    Name = amqqueue:get_name(Q),
    %% Only a mirror should receive this, it means we are a duplicated master
    rabbit_mirror_queue_misc:log_warning(
      Name, "Stopping after receiving sync_start from another master", []),
    stop(State).

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
            State = #q{q = Q}) when ?amqqueue_exclusive_owner_is(Q, DownPid) ->
    %% Exclusively owned queues must disappear with their owner.  In
    %% the case of clean shutdown we delete the queue synchronously in
    %% the reader - although not required by the spec this seems to
    %% match what people expect (see bug 21824). However we need this
    %% monitor-and-async- delete in case the connection goes away
    %% unexpectedly.
    log_delete_exclusive(DownPid, State),
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

handle_info({bump_credit, Msg}, State = #q{backing_queue       = BQ,
                                           backing_queue_state = BQS}) ->
    %% The message_store is granting us more credit. This means the
    %% backing queue (for the rabbit_variable_queue case) might
    %% continue paging messages to disk if it still needs to. We
    %% consume credits from the message_store whenever we need to
    %% persist a message to disk. See:
    %% rabbit_variable_queue:msg_store_write/4.
    credit_flow:handle_bump_msg(Msg),
    noreply(State#q{backing_queue_state = BQ:resume(BQS)});
handle_info(bump_reduce_memory_use, State = #q{backing_queue       = BQ,
                                               backing_queue_state = BQS0}) ->
    BQS1 = BQ:handle_info(bump_reduce_memory_use, BQS0),
    noreply(State#q{backing_queue_state = BQ:resume(BQS1)});

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
      fun () -> emit_stats(State,
                           [{idle_since,
                             os:system_time(milli_seconds)}])
                end),
    State1 = rabbit_event:stop_stats_timer(State#q{backing_queue_state = BQS3},
                                           #q.stats_timer),
    {hibernate, stop_rate_timer(State1)}.

format_message_queue(Opt, MQ) -> rabbit_misc:format_message_queue(Opt, MQ).

format(Q) when ?is_amqqueue(Q) ->
    case rabbit_mirror_queue_misc:is_mirrored(Q) of
        false ->
            [{node, node(amqqueue:get_pid(Q))}];
        true ->
            Slaves = amqqueue:get_slave_pids(Q),
            SSlaves = amqqueue:get_sync_slave_pids(Q),
            [{slave_nodes, [node(S) || S <- Slaves]},
             {synchronised_slave_nodes, [node(S) || S <- SSlaves]},
             {node, node(amqqueue:get_pid(Q))}]
    end.

-spec is_policy_applicable(amqqueue:amqqueue(), any()) -> boolean().
is_policy_applicable(_Q, _Policy) ->
    true.

log_delete_exclusive({ConPid, _ConRef}, State) ->
    log_delete_exclusive(ConPid, State);
log_delete_exclusive(ConPid, #q{ q = Q }) ->
    Resource = amqqueue:get_name(Q),
    #resource{ name = QName, virtual_host = VHost } = Resource,
    rabbit_log_queue:debug("Deleting exclusive queue '~s' in vhost '~s' " ++
                           "because its declaring connection ~p was closed",
                           [QName, VHost, ConPid]).

log_auto_delete(Reason, #q{ q = Q }) ->
    Resource = amqqueue:get_name(Q),
    #resource{ name = QName, virtual_host = VHost } = Resource,
    rabbit_log_queue:debug("Deleting auto-delete queue '~s' in vhost '~s' " ++
                           Reason,
                           [QName, VHost]).

needs_update_mirroring(Q, Version) ->
    {ok, UpQ} = rabbit_amqqueue:lookup(amqqueue:get_name(Q)),
    DBVersion = amqqueue:get_policy_version(UpQ),
    case DBVersion > Version of
        true -> {rabbit_policy:get(<<"ha-mode">>, UpQ), DBVersion};
        false -> false
    end.


update_mirroring(Policy, State = #q{backing_queue = BQ}) ->
    case update_to(Policy, BQ) of
        start_mirroring ->
            start_mirroring(State);
        stop_mirroring ->
            stop_mirroring(State);
        ignore ->
            State;
        update_ha_mode ->
            update_ha_mode(State)
    end.

update_to(undefined, rabbit_mirror_queue_master) ->
    stop_mirroring;
update_to(_, rabbit_mirror_queue_master) ->
    update_ha_mode;
update_to(undefined, BQ) when BQ =/= rabbit_mirror_queue_master ->
    ignore;
update_to(_, BQ) when BQ =/= rabbit_mirror_queue_master ->
    start_mirroring.

start_mirroring(State = #q{backing_queue       = BQ,
                           backing_queue_state = BQS}) ->
    %% lookup again to get policy for init_with_existing_bq
    {ok, Q} = rabbit_amqqueue:lookup(qname(State)),
    true = BQ =/= rabbit_mirror_queue_master, %% assertion
    BQ1 = rabbit_mirror_queue_master,
    BQS1 = BQ1:init_with_existing_bq(Q, BQ, BQS),
    State#q{backing_queue       = BQ1,
            backing_queue_state = BQS1}.

stop_mirroring(State = #q{backing_queue       = BQ,
                          backing_queue_state = BQS}) ->
    BQ = rabbit_mirror_queue_master, %% assertion
    {BQ1, BQS1} = BQ:stop_mirroring(BQS),
    State#q{backing_queue       = BQ1,
            backing_queue_state = BQS1}.

update_ha_mode(State) ->
    {ok, Q} = rabbit_amqqueue:lookup(qname(State)),
    ok = rabbit_mirror_queue_misc:update_mirrors(Q),
    State.

confirm_to_sender(Pid, QName, MsgSeqNos) ->
    rabbit_classic_queue:confirm_to_sender(Pid, QName, MsgSeqNos).


