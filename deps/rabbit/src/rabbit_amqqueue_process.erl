%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_amqqueue_process).
-include_lib("rabbit_common/include/rabbit.hrl").
-include("amqqueue.hrl").

-behaviour(gen_server2).

-define(SYNC_INTERVAL,         200). %% milliseconds
-define(UPDATE_RATES_INTERVAL, 5000).
-define(CONSUMER_BIAS_RATIO,   2.0). %% i.e. consume 100% faster

-export([info_keys/0]).

-export([init_with_backing_queue_state/7]).

-export([start_link/2]).
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
            %% for non-priority queues, this will be rabbit_variable_queue.
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
            %% timer used to update ingress/egress rates
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
            mirroring_policy_version = 0, %% reserved
            %% running | flow | idle
            status,
            %% boolean()
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
         state,
         garbage_collection
        ]).

-define(CREATION_EVENT_KEYS,
        [name,
         type,
         durable,
         auto_delete,
         arguments,
         owner_pid,
         exclusive,
         user_who_performed_action,
         leader,
         members
        ]).

-define(INFO_KEYS, [pid | ?CREATION_EVENT_KEYS ++ ?STATISTICS_KEYS -- [name, type]]).

%%----------------------------------------------------------------------------

-spec info_keys() -> rabbit_types:info_keys().

info_keys()       -> ?INFO_KEYS       ++ rabbit_backing_queue:info_keys().
statistics_keys() -> ?STATISTICS_KEYS ++ rabbit_backing_queue:info_keys().

%%----------------------------------------------------------------------------

-spec start_link(amqqueue:amqqueue(), pid())
                      -> rabbit_types:ok_pid_or_error().

start_link(Q, Marker) ->
    gen_server2:start_link(?MODULE, {Q, Marker}, []).

init({Q, Marker}) ->
    case is_process_alive(Marker) of
        true  ->
            %% start
            init(Q);
        false ->
            %% restart
            QueueName = amqqueue:get_name(Q),
            {ok, Q1} = rabbit_amqqueue:lookup(QueueName),
            rabbit_log:error("Restarting crashed ~ts.", [rabbit_misc:rs(QueueName)]),
            gen_server2:cast(self(), init),
            init(Q1)
    end;

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

init_it(Recover, From, State = #q{q = Q0}) ->
    Owner = amqqueue:get_exclusive_owner(Q0),
    case rabbit_misc:is_process_alive(Owner) of
        true  -> erlang:monitor(process, Owner),
                 init_it2(Recover, From, State);
        false -> %% Tidy up exclusive durable queue.
                 #q{backing_queue       = undefined,
                    backing_queue_state = undefined,
                    q                   = Q} = State,
                 BQ = backing_queue_module(),
                 {_, Terms} = recovery_status(Recover),
                 BQS = bq_init(BQ, Q, Terms),
                 %% Rely on terminate to delete the queue.
                 log_delete_exclusive(Owner, State),
                 {stop, {shutdown, missing_owner},
                  {{reply_to, From},
                   State#q{backing_queue = BQ, backing_queue_state = BQS}}}
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
                    BQ = backing_queue_module(),
                    BQS = bq_init(BQ, Q, TermsOrNew),
                    send_reply(From, {new, Q}),
                    recovery_barrier(Barrier),
                    State1 = process_args_policy(
                               State#q{backing_queue       = BQ,
                                       backing_queue_state = BQS}),
                    notify_decorators(startup, State),
                    rabbit_event:notify(queue_created,
                                        queue_created_infos(State1)),
                    rabbit_event:if_enabled(State1, #q.stats_timer,
                                            fun() -> emit_stats(State1) end),
                    noreply(State1);
                false ->
                    {stop, normal, {existing, Q1}, State}
            end;
        {error, timeout} ->
            Reason = {protocol_error, internal_error,
                      "Could not declare ~ts on node '~ts' because the "
                      "metadata store operation timed out",
                      [rabbit_misc:rs(amqqueue:get_name(Q)), node()]},
            {stop, normal, Reason, State};
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
    amqqueue:get_pid(Q1)             =:= amqqueue:get_pid(Q2);
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
    _ = case Owner of
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
    rabbit_core_metrics:queue_deleted(qname(State)),
    terminate_shutdown(
    fun (BQS) ->
        _ = update_state(stopped, Q0),
        BQ:terminate(R, BQS)
    end, State);
terminate({shutdown, missing_owner = Reason}, {{reply_to, From}, #q{q = Q} = State}) ->
    %% if the owner was missing then there will be no queue, so don't emit stats
    State1 = terminate_shutdown(terminate_delete(false, Reason, none, State), State),
    send_reply(From, {owner_died, Q}),
    State1;
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
    terminate_shutdown(terminate_delete(true, auto_delete, none, State), State);
terminate(normal, {{reply_to, ReplyTo}, State}) -> %% delete case
    terminate_shutdown(terminate_delete(true, normal, ReplyTo, State), State);
terminate(normal, State) ->
    terminate_shutdown(terminate_delete(true, normal, none, State), State);
%% If we crashed don't try to clean up the BQS, probably best to leave it.
terminate(_Reason,           State = #q{q = Q}) ->
    terminate_shutdown(fun (BQS) ->
                               Q2 = amqqueue:set_state(Q, crashed),
                               %% When mnesia is removed this update can become
                               %% an async Khepri command.
                               _ = rabbit_amqqueue:store_queue(Q2),
                               BQS
                       end, State).

terminate_delete(EmitStats, Reason0, ReplyTo,
                 State = #q{q = Q,
                            backing_queue = BQ,
                            status = Status}) ->
    ActingUser = terminated_by(Status),
    fun (BQS) ->
        Reason = case Reason0 of
                     auto_delete -> normal;
                     missing_owner -> normal;
                     Any -> Any
                 end,
        Len = BQ:len(BQS),
        BQS1 = BQ:delete_and_terminate(Reason, BQS),
        if EmitStats -> rabbit_event:if_enabled(State, #q.stats_timer,
                                                fun() -> emit_stats(State) end);
           true      -> ok
        end,
        %% This try-catch block transforms throws to errors since throws are not
        %% logged. When mnesia is removed this `try` can be removed: Khepri
        %% returns errors as error tuples instead.
        Reply = try rabbit_amqqueue:internal_delete(Q, ActingUser, Reason0) of
                    ok ->
                        {ok, Len};
                    {error, _} = Err ->
                        Err
                catch
                    {error, ReasonE} -> error(ReasonE)
                end,
        send_reply(ReplyTo, Reply),
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
        _         -> QName = qname(State),
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

notify_decorators_if_became_empty(WasEmpty, State) ->
    case (not WasEmpty) andalso is_empty(State) of
        true -> notify_decorators(State);
        false -> ok
    end,
    State.

notify_decorators(Event, State) ->
    _ = decorator_callback(qname(State), Event, []),
    ok.

notify_decorators(State = #q{consumers           = Consumers,
                             backing_queue       = BQ,
                             backing_queue_state = BQS}) ->
    P = rabbit_queue_consumers:max_active_priority(Consumers),
    _ = decorator_callback(qname(State), consumer_state_changed,
                       [P, BQ:is_empty(BQS)]),
    ok.

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
         {<<"queue-mode">>,              fun res_arg/2, fun init_queue_mode/2},
         {<<"queue-version">>,           fun res_arg/2, fun init_queue_version/2}],
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

init_queue_version(Version0, State = #q {backing_queue = BQ,
                                         backing_queue_state = BQS}) ->
    Version = case Version0 of
        undefined -> 2;
        _ -> Version0
    end,
    BQS1 = BQ:set_queue_version(Version, BQS),
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

backing_queue_module() ->
    {ok, BQM} = application:get_env(backing_queue_module),
    BQM.

ensure_sync_timer(State) ->
    rabbit_misc:ensure_timer(State, #q.sync_timer_ref,
                             ?SYNC_INTERVAL, sync_timeout).

stop_sync_timer(State) -> rabbit_misc:stop_timer(State, #q.sync_timer_ref).

ensure_rate_timer(State) ->
    rabbit_misc:ensure_timer(State, #q.rate_timer_ref,
                             ?UPDATE_RATES_INTERVAL,
                             update_rates).

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
    After = (case Expiry - os:system_time(microsecond) of
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
                                 message    = Message
                                },
                       State = #q{q                 = Q,
                                  msg_id_to_channel = MTC}) ->
    Persistent = mc:is_persistent(Message),
    MsgId = mc:get_annotation(id, Message),
    case Persistent  of
        true when ?amqqueue_is_durable(Q) ->
            MTC1 = maps:put(MsgId, {SenderPid, MsgSeqNo}, MTC),
            {eventually, State#q{msg_id_to_channel = MTC1}};
        _ ->
            confirm_to_sender(SenderPid, amqqueue:get_name(Q), [MsgSeqNo]),
            {immediately, State}
    end.

discard(#delivery{confirm = Confirm,
                  sender  = SenderPid,
                  message = Msg}, BQ, BQS, MTC, QName) ->
    MsgId = mc:get_annotation(id, Msg),
    MTC1 = case Confirm of
               true  -> confirm_messages([MsgId], MTC, QName);
               false -> MTC
           end,
    BQS1 = BQ:discard(Msg, SenderPid, BQS),
    {BQS1, MTC1}.

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
                                      message = Message},
                 Props, Delivered, State = #q{q                   = Q,
                                              backing_queue       = BQ,
                                              backing_queue_state = BQS,
                                              msg_id_to_channel   = MTC}) ->
    case rabbit_queue_consumers:deliver(
           fun (true)  -> {AckTag, BQS1} =
                              BQ:publish_delivered(
                                Message, Props, SenderPid, BQS),
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
    case {will_overflow(Delivery, State), Overflow} of
        {true, 'reject-publish'} ->
            %% Drop publish and nack to publisher
            send_reject_publish(Delivery, State);
        {true, 'reject-publish-dlx'} ->
            %% Publish to DLX
            _ = with_dlx(
              DLX,
              fun (X) ->
                      rabbit_global_counters:messages_dead_lettered(maxlen, rabbit_classic_queue,
                                                                    at_most_once, 1),
                      QName = qname(State),
                      rabbit_dead_letter:publish(Message, maxlen, X, RK, QName)
              end,
              fun () -> rabbit_global_counters:messages_dead_lettered(maxlen, rabbit_classic_queue,
                                                                      disabled, 1)
              end),
            %% Drop publish and nack to publisher
            send_reject_publish(Delivery, State);
        _ ->
            {IsDuplicate, BQS1} = BQ:is_duplicate(Message, BQS),
            State1 = State#q{backing_queue_state = BQS1},
            case IsDuplicate of
                true ->
                    %% Publish to DLX
                    _ = with_dlx(
                          DLX,
                          fun (X) ->
                                  rabbit_global_counters:messages_dead_lettered(maxlen,
                                                                                rabbit_classic_queue,
                                                                                at_most_once, 1),
                                  QName = qname(State1),
                                  rabbit_dead_letter:publish(Message, maxlen, X, RK, QName)
                          end,
                          fun () ->
                                  rabbit_global_counters:messages_dead_lettered(maxlen,
                                                                                rabbit_classic_queue,
                                                                                disabled, 1)
                          end),
                    %% Drop publish and nack to publisher
                    send_reject_publish(Delivery, State1);
                false ->
                    %% Enqueue and maybe drop head later
                    deliver_or_enqueue(Delivery, Delivered, State1)
            end
    end.

deliver_or_enqueue(Delivery = #delivery{message = Message,
                                        sender  = SenderPid},
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
            rabbit_global_counters:messages_dead_lettered(expired, rabbit_classic_queue,
                                                          disabled, 1),
            {BQS1, MTC1} = discard(Delivery, BQ, BQS, MTC, amqqueue:get_name(Q)),
            State2#q{backing_queue_state = BQS1, msg_id_to_channel = MTC1};
        {undelivered, State2 = #q{backing_queue_state = BQS}} ->

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
                                      rabbit_global_counters:messages_dead_lettered(maxlen,
                                                                                    rabbit_classic_queue,
                                                                                    disabled, 1),
                                      {_, BQS1} = BQ:drop(false, BQS),
                                      State#q{backing_queue_state = BQS1}
                              end));
        false ->
            {AlreadyDropped, State}
    end.

send_reject_publish(#delivery{confirm = true,
                              sender = SenderPid,
                              msg_seq_no = MsgSeqNo,
                              message = Msg},
                      State = #q{ q = Q,
                                  backing_queue = BQ,
                                  backing_queue_state = BQS,
                                  msg_id_to_channel   = MTC}) ->
    MsgId = mc:get_annotation(id, Msg),
    ok = rabbit_classic_queue:send_rejection(SenderPid,
                                             amqqueue:get_name(Q), MsgSeqNo),

    MTC1 = maps:remove(MsgId, MTC),
    BQS1 = BQ:discard(Msg, SenderPid, BQS),
    State#q{ backing_queue_state = BQS1, msg_id_to_channel = MTC1 };
send_reject_publish(#delivery{confirm = false}, State) ->
    State.

will_overflow(_, #q{max_length = undefined,
                    max_bytes  = undefined}) -> false;
will_overflow(#delivery{message = Message},
              #q{max_length          = MaxLen,
                 max_bytes           = MaxBytes,
                 backing_queue       = BQ,
                 backing_queue_state = BQS}) ->
    ExpectedQueueLength = BQ:len(BQS) + 1,

    {_, PayloadSize} = mc:size(Message),
    ExpectedQueueSizeBytes = BQ:info(message_bytes_ready, BQS) + PayloadSize,

    ExpectedQueueLength > MaxLen orelse ExpectedQueueSizeBytes > MaxBytes.

over_max_length(#q{max_length          = MaxLen,
                   max_bytes           = MaxBytes,
                   backing_queue       = BQ,
                   backing_queue_state = BQS}) ->
    BQ:len(BQS) > MaxLen orelse BQ:info(message_bytes_ready, BQS) > MaxBytes.

fetch(AckRequired, State = #q{backing_queue       = BQ,
                              backing_queue_state = BQS}) ->
    %% @todo We should first drop expired messages then fetch
    %%       the message, not the other way around. Otherwise
    %%       we will send expired messages at times.
    {Result, BQS1} = BQ:fetch(AckRequired, BQS),
    State1 = drop_expired_msgs(State#q{backing_queue_state = BQS1}),
    {Result, notify_decorators_if_became_empty(Result =:= empty, State1)}.

ack(AckTags, ChPid, State) ->
    subtract_acks(ChPid, AckTags, State,
                  fun (State1 = #q{backing_queue       = BQ,
                                   backing_queue_state = BQS}) ->
                          {_Guids, BQS1} = BQ:ack(AckTags, BQS),
                          State1#q{backing_queue_state = BQS1}
                  end).

requeue(AckTags, ChPid, State) ->
    subtract_acks(ChPid, AckTags, State,
                  fun (State1) -> requeue_and_run(AckTags, false, State1) end).

requeue_and_run(AckTags,
                ActiveConsumersChanged,
                #q{backing_queue = BQ,
                   backing_queue_state = BQS0} = State0) ->
    WasEmpty = BQ:is_empty(BQS0),
    {_MsgIds, BQS} = BQ:requeue(AckTags, BQS0),
    State1 = State0#q{backing_queue_state = BQS},
    {_Dropped, State2} = maybe_drop_head(State1),
    State3 = drop_expired_msgs(State2),
    State = notify_decorators_if_became_empty(WasEmpty, State3),
    run_message_queue(ActiveConsumersChanged, State).

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
            [rabbit_core_metrics:consumer_deleted(DownPid, CTag, QName) || CTag <- ChCTags],
            Holder1 = new_single_active_consumer_after_channel_down(DownPid, Holder, SingleActiveConsumerOn, Consumers1),
            State2 = State1#q{consumers          = Consumers1,
                              active_consumer    = Holder1},
            maybe_notify_consumer_updated(State2, Holder, Holder1),
            notify_decorators(State2),
            case should_auto_delete(State2) of
                true ->
                    log_auto_delete(
                        io_lib:format(
                            "because all of its consumers (~tp) were on a channel that was closed",
                            [length(ChCTags)]),
                        State),
                    {stop, State2};
                false ->
                    State3 = ensure_expiry_timer(State2),
                    State4 = requeue_and_run(ChAckTags, false, State3),
                    {ok, State4}
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

message_properties(Message, Confirm, #q{ttl = TTL}) ->
    {_, Size} = mc:size(Message),
    #message_properties{expiry           = calculate_msg_expiry(Message, TTL),
                        needs_confirming = Confirm == eventually,
                        size             = Size}.

calculate_msg_expiry(Msg, TTL) ->
    MsgTTL = mc:ttl(Msg),
    case min(TTL, MsgTTL) of
        undefined -> undefined;
        T ->
            os:system_time(microsecond) + T * 1000
    end.

drop_expired_msgs(State) ->
    case is_empty(State) of
        true  -> State;
        false -> drop_expired_msgs(os:system_time(microsecond),
                                   State)
    end.

drop_expired_msgs(Now, State = #q{backing_queue_state = BQS,
                                  backing_queue       = BQ }) ->
    ExpirePred = fun (#message_properties{expiry = Exp}) -> Now >= Exp end,
    ExpirePredIncrement = fun(Properties) ->
                                  ExpirePred(Properties) andalso
                                  rabbit_global_counters:messages_dead_lettered(expired,
                                                                                rabbit_classic_queue,
                                                                                disabled,
                                                                                1) =:= ok
                          end,
    {Props, State1} =
        with_dlx(
          State#q.dlx,
          fun (X) -> dead_letter_expired_msgs(ExpirePred, X, State) end,
          fun () -> {Next, BQS1} = BQ:dropwhile(ExpirePredIncrement, BQS),
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
                    rabbit_global_counters:messages_dead_lettered(Reason, rabbit_classic_queue,
                                                                  at_most_once, 1),
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
i(leader, State) -> node(i(pid, State));
i(members, State) -> [i(leader, State)];
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
    [{messages_ready, MR},
     {messages_unacknowledged, MU},
     {messages, M},
     {reductions, R},
     {name, Name} | Infos]
    = [{K, V} || {K, V} <- infos(statistics_keys(), State),
                 not lists:member(K, ExtraKs)],
    rabbit_core_metrics:queue_stats(Name, Extra ++ Infos),
    rabbit_core_metrics:queue_stats(Name, MR, MU, M, R).

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

prioritise_call(Msg, _From, _Len, _State) ->
    case Msg of
        info                                       -> 9;
        {info, _Items}                             -> 9;
        consumers                                  -> 9;
        stat                                       -> 7;
        _                                          -> 0
    end.

prioritise_cast(Msg, _Len, State) ->
    case Msg of
        delete_immediately                   -> 8;
        {delete_exclusive, _Pid}             -> 8;
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

consumer_bias(#q{backing_queue = BQ, backing_queue_state = BQS}, Low, High) ->
    case BQ:msg_rates(BQS) of
        {Ingress,      _} when Ingress =:= +0.0 orelse Ingress =:= -0.0 -> Low;
        {Ingress, Egress} when Egress / Ingress < ?CONSUMER_BIAS_RATIO -> High;
        {_,            _} -> Low
    end.

prioritise_info(Msg, _Len, #q{q = Q}) ->
    DownPid = amqqueue:get_exclusive_owner(Q),
    case Msg of
        {'DOWN', _, process, DownPid, _} -> 8;
        {maybe_expire, _Version}         -> 8;
        {drop_expired, _Version}         -> 8;
        emit_stats                       -> 7;
        sync_timeout                     -> 6;
        _                                -> 0
    end.

handle_call({init, Recover}, From, State) ->
    init_it(Recover, From, State);

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
             ModeOrPrefetch, ConsumerTag, ExclusiveConsume, Args, OkMsg, ActingUser},
            _From, State = #q{consumers = Consumers,
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
                                   LimiterPid, LimiterActive, ModeOrPrefetch,
                                   Args, ActingUser, Consumers),
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
                                   LimiterPid, LimiterActive, ModeOrPrefetch,
                                   Args, ActingUser, Consumers),
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
                PrefetchCount = rabbit_queue_consumers:parse_prefetch_count(ModeOrPrefetch),
                rabbit_core_metrics:consumer_created(
                ChPid, ConsumerTag, ExclusiveConsume, AckRequired, QName,
                PrefetchCount, ConsumerIsActive, ActivityStatus, Args),
            emit_consumer_created(ChPid, ConsumerTag, ExclusiveConsume,
                AckRequired, QName, PrefetchCount,
                Args, none, ActingUser),
            notify_decorators(State1),
            reply(ok, run_message_queue(false, State1))
    end;

handle_call({basic_cancel, ChPid, ConsumerTag, OkMsg, ActingUser}, From, State) ->
    handle_call({stop_consumer, #{pid => ChPid,
                                  consumer_tag => ConsumerTag,
                                  ok_msg => OkMsg,
                                  user => ActingUser}},
                From, State);

handle_call({stop_consumer, #{pid := ChPid,
                              consumer_tag := ConsumerTag,
                              user := ActingUser} = Spec},
            _From,
            State = #q{consumers = Consumers,
                       active_consumer = Holder,
                       single_active_consumer_on = SingleActiveConsumerOn}) ->
    Reason = maps:get(reason, Spec, cancel),
    OkMsg = maps:get(ok_msg, Spec, undefined),
    ok = maybe_send_reply(ChPid, OkMsg),
    case rabbit_queue_consumers:remove(ChPid, ConsumerTag, Reason, Consumers) of
        not_found ->
            reply(ok, State);
        {AckTags, Consumers1} ->
            Holder1 = new_single_active_consumer_after_basic_cancel(
                        ChPid, ConsumerTag, Holder, SingleActiveConsumerOn, Consumers1),
            State1 = State#q{consumers = Consumers1,
                             active_consumer = Holder1},
            maybe_notify_consumer_updated(State1, Holder, Holder1),
            emit_consumer_deleted(ChPid, ConsumerTag, qname(State1), ActingUser),
            notify_decorators(State1),
            case should_auto_delete(State1) of
                false ->
                    State2 = requeue_and_run(AckTags, Holder =/= Holder1, State1),
                    State3 = ensure_expiry_timer(State2),
                    reply(ok, State3);
                true  ->
                    log_auto_delete(
                      io_lib:format(
                        "because its last consumer with tag '~ts' was cancelled",
                        [ConsumerTag]),
                      State),
                    stop(ok, State1)
            end
    end;

handle_call(stat, _From, State) ->
    State1 = #q{backing_queue = BQ, backing_queue_state = BQS} =
        ensure_expiry_timer(State),
    reply({ok, BQ:len(BQS), rabbit_queue_consumers:count()}, State1);

handle_call({delete, IfUnused, IfEmpty, ActingUser}, From,
            State = #q{backing_queue_state = BQS, backing_queue = BQ}) ->
    IsEmpty  = BQ:is_empty(BQS),
    IsUnused = is_unused(State),
    if
        IfEmpty  and not(IsEmpty)  -> reply({error, not_empty}, State);
        IfUnused and not(IsUnused) -> reply({error,    in_use}, State);
        true ->
            State1 = State#q{status = {terminated_by, ActingUser}},
            stop({{reply_to, From}, State1})
    end;

handle_call(purge, _From, State = #q{backing_queue       = BQ,
                                     backing_queue_state = BQS}) ->
    {Count, BQS1} = BQ:purge(BQS),
    State1 = State#q{backing_queue_state = BQS1},
    reply({ok, Count}, notify_decorators_if_became_empty(Count =:= 0, State1));

handle_call({requeue, AckTags, ChPid}, From, State) ->
    gen_server2:reply(From, ok),
    noreply(requeue(AckTags, ChPid, State)).

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
    init_it({no_barrier, non_clean_shutdown}, none, State);

handle_cast({run_backing_queue, Mod, Fun},
            State = #q{backing_queue = BQ, backing_queue_state = BQS}) ->
    noreply(State#q{backing_queue_state = BQ:invoke(Mod, Fun, BQS)});

handle_cast({deliver,
             Delivery = #delivery{sender = Sender,
                                  flow   = Flow},
             Delivered},
            State = #q{senders = Senders}) ->
    Senders1 = case Flow of
    %% In both credit_flow:ack/1 we are acking messages to the channel
    %% process that sent us the message delivery. See handle_ch_down
    %% for more info.
                   flow   -> credit_flow:ack(Sender),
                             pmon:monitor(Sender, Senders);
                   noflow -> Senders
               end,
    State1 = State#q{senders = Senders1},
    noreply(maybe_deliver_or_enqueue(Delivery, Delivered, State1));

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
              fun () -> rabbit_global_counters:messages_dead_lettered(rejected, rabbit_classic_queue,
                                                                      disabled, length(AckTags)),
                        ack(AckTags, ChPid, State) end));

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

handle_cast({credit, SessionPid, CTag, Credit, Drain},
            #q{q = Q,
               backing_queue = BQ,
               backing_queue_state = BQS0} = State) ->
    %% Credit API v1.
    %% Delete this function clause when feature flag rabbitmq_4.0.0 becomes required.
    %% Behave like non-native AMQP 1.0: Send send_credit_reply before deliveries.
    rabbit_classic_queue:send_credit_reply_credit_api_v1(
      SessionPid, amqqueue:get_name(Q), BQ:len(BQS0)),
    handle_cast({credit, SessionPid, CTag, credit_api_v1, Credit, Drain}, State);
handle_cast({credit, SessionPid, CTag, DeliveryCountRcv, Credit, Drain},
            #q{consumers = Consumers0,
               q = Q} = State0) ->
    QName = amqqueue:get_name(Q),
    State = #q{backing_queue_state = PostBQS,
               backing_queue = BQ} =
        case rabbit_queue_consumers:process_credit(
               DeliveryCountRcv, Credit, SessionPid, CTag, Consumers0) of
            unchanged ->
                State0;
            {unblocked, Consumers1} ->
                State1 = State0#q{consumers = Consumers1},
                run_message_queue(true, State1)
        end,
    case rabbit_queue_consumers:get_link_state(SessionPid, CTag) of
        {credit_api_v1, PostCred}
          when Drain andalso
               is_integer(PostCred) andalso PostCred > 0 ->
            %% credit API v1
            rabbit_queue_consumers:drained(credit_api_v1, SessionPid, CTag),
            rabbit_classic_queue:send_drained_credit_api_v1(SessionPid, QName, CTag, PostCred);
        {PostDeliveryCountSnd, PostCred}
          when is_integer(PostDeliveryCountSnd) andalso
               Drain andalso
               is_integer(PostCred) andalso PostCred > 0 ->
            %% credit API v2
            AdvancedDeliveryCount = serial_number:add(PostDeliveryCountSnd, PostCred),
            rabbit_queue_consumers:drained(AdvancedDeliveryCount, SessionPid, CTag),
            Avail = BQ:len(PostBQS),
            rabbit_classic_queue:send_credit_reply(
              SessionPid, QName, CTag, AdvancedDeliveryCount, 0, Avail, Drain);
        {PostDeliveryCountSnd, PostCred}
          when is_integer(PostDeliveryCountSnd) ->
            %% credit API v2
            Avail = BQ:len(PostBQS),
            rabbit_classic_queue:send_credit_reply(
              SessionPid, QName, CTag, PostDeliveryCountSnd, PostCred, Avail, Drain);
        _ ->
            ok
    end,
    noreply(State);

% Note: https://www.pivotaltracker.com/story/show/166962656
% This event is necessary for the stats timer to be initialized with
% the correct values once the management agent has started
handle_cast({force_event_refresh, Ref},
            State = #q{consumers = Consumers}) ->
    rabbit_event:notify(queue_created, queue_created_infos(State), Ref),
    QName = qname(State),
    AllConsumers = rabbit_queue_consumers:all(Consumers),
    rabbit_log:debug("Queue ~ts forced to re-emit events, consumers: ~tp", [rabbit_misc:rs(QName), AllConsumers]),
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
    %% policy in various places, so when it
    %% changes we go and read it from the database again.
    %%
    %% This also has the side effect of waking us up so we emit a
    %% stats event - so event consumers see the changed policy.
    {ok, Q} = rabbit_amqqueue:lookup(Name),
    noreply(process_args_policy(State#q{q = Q}));

handle_cast({policy_changed, Q0}, State) ->
    Name = amqqueue:get_name(Q0),
    PolicyVersion0 = amqqueue:get_policy_version(Q0),
    %% We depend on the #q.q field being up to date at least WRT
    %% policy in various places, so when it
    %% changes we go and read it from the database again.
    %%
    %% This also has the side effect of waking us up so we emit a
    %% stats event - so event consumers see the changed policy.
    {ok, Q} = rabbit_amqqueue:lookup(Name),
    PolicyVersion = amqqueue:get_policy_version(Q),
    case PolicyVersion >= PolicyVersion0 of
        true ->
            noreply(process_args_policy(State#q{q = Q}));
        false ->
            noreply(process_args_policy(State#q{q = Q0}))
    end.

handle_info({maybe_expire, Vsn}, State = #q{q = Q, expires = Expiry, args_policy_version = Vsn}) ->
    case is_unused(State) of
        true  ->
            QResource = rabbit_misc:rs(amqqueue:get_name(Q)),
            rabbit_log_queue:debug("Deleting 'classic ~ts' on expiry after ~tp milliseconds", [QResource, Expiry]),
            stop(State);
        false -> noreply(State#q{expiry_timer_ref = undefined})
    end;

handle_info({maybe_expire, _Vsn}, State) ->
    noreply(State);

handle_info({drop_expired, Vsn}, State = #q{args_policy_version = Vsn}) ->
    WasEmpty = is_empty(State),
    State1 = drop_expired_msgs(State#q{ttl_timer_ref = undefined}),
    noreply(notify_decorators_if_became_empty(WasEmpty, State1));

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

handle_info(update_rates, State = #q{backing_queue = BQ,
                                     backing_queue_state = BQS}) ->
    BQS1 = BQ:update_rates(BQS),
    %% Don't call noreply/1, we don't want to set timers
    {State1, Timeout} = next_state(State#q{rate_timer_ref      = undefined,
                                           backing_queue_state = BQS1}),
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

handle_info(Info, State) ->
    {stop, {unhandled_info, Info}, State}.

handle_pre_hibernate(State = #q{backing_queue_state = undefined}) ->
    {hibernate, State};
handle_pre_hibernate(State = #q{backing_queue = BQ,
                                backing_queue_state = BQS}) ->
    BQS1 = BQ:update_rates(BQS),
    BQS3 = BQ:handle_pre_hibernate(BQS1),
    rabbit_event:if_enabled(
      State, #q.stats_timer,
      fun () -> emit_stats(State,
                           [{idle_since,
                             os:system_time(millisecond)}])
                end),
    State1 = rabbit_event:stop_stats_timer(State#q{backing_queue_state = BQS3},
                                           #q.stats_timer),
    {hibernate, stop_rate_timer(State1)}.

format_message_queue(Opt, MQ) -> rabbit_misc:format_message_queue(Opt, MQ).

%% TODO: this can be removed after 3.13
format(Q) when ?is_amqqueue(Q) ->
    [{node, node(amqqueue:get_pid(Q))}].

-spec is_policy_applicable(amqqueue:amqqueue(), any()) -> boolean().
is_policy_applicable(_Q, _Policy) ->
    true.

log_delete_exclusive({ConPid, _ConRef}, State) ->
    log_delete_exclusive(ConPid, State);
log_delete_exclusive(ConPid, #q{ q = Q }) ->
    Resource = amqqueue:get_name(Q),
    #resource{ name = QName, virtual_host = VHost } = Resource,
    rabbit_log_queue:debug("Deleting exclusive queue '~ts' in vhost '~ts' " ++
                           "because its declaring connection ~tp was closed",
                           [QName, VHost, ConPid]).

log_auto_delete(Reason, #q{ q = Q }) ->
    Resource = amqqueue:get_name(Q),
    #resource{ name = QName, virtual_host = VHost } = Resource,
    rabbit_log_queue:debug("Deleting auto-delete queue '~ts' in vhost '~ts' " ++
                           Reason,
                           [QName, VHost]).

confirm_to_sender(Pid, QName, MsgSeqNos) ->
    rabbit_classic_queue:confirm_to_sender(Pid, QName, MsgSeqNos).

update_state(State, Q) ->
    Decorators = rabbit_queue_decorator:active(Q),
    rabbit_db_queue:update(amqqueue:get_name(Q),
                           fun(Q0) ->
                                   Q1 = amqqueue:set_state(Q0, State),
                                   amqqueue:set_decorators(Q1, Decorators)
                           end).

queue_created_infos(State) ->
    %% On the events API, we use long names for queue types
    Keys = ?CREATION_EVENT_KEYS -- [type],
    infos(Keys, State) ++ [{type, rabbit_classic_queue}].
