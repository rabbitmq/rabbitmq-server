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
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_amqqueue_process).
-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-behaviour(gen_server2).

-define(UNSENT_MESSAGE_LIMIT,          200).
-define(SYNC_INTERVAL,                 25). %% milliseconds
-define(RAM_DURATION_UPDATE_INTERVAL,  5000).

-export([start_link/1, info_keys/0]).

-export([init_with_backing_queue_state/8]).

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
            senders,
            publish_seqno,
            unconfirmed,
            delayed_stop,
            queue_monitors,
            dlx,
            dlx_routing_key
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
-spec(init_with_backing_queue_state/8 ::
        (rabbit_types:amqqueue(), atom(), tuple(), any(), [any()],
         [rabbit_types:delivery()], pmon:pmon(), dict()) -> #q{}).

-endif.

%%----------------------------------------------------------------------------

-define(STATISTICS_KEYS,
        [pid,
         exclusive_consumer_pid,
         exclusive_consumer_tag,
         messages_ready,
         messages_unacknowledged,
         messages,
         consumers,
         memory,
         slave_pids,
         backing_queue_status
        ]).

-define(CREATION_EVENT_KEYS,
        [pid,
         name,
         durable,
         auto_delete,
         arguments,
         owner_pid,
         slave_pids,
         synchronised_slave_pids
        ]).

-define(INFO_KEYS,
        ?CREATION_EVENT_KEYS ++ ?STATISTICS_KEYS -- [pid, slave_pids]).

%%----------------------------------------------------------------------------

start_link(Q) -> gen_server2:start_link(?MODULE, Q, []).

info_keys() -> ?INFO_KEYS.

%%----------------------------------------------------------------------------

init(Q) ->
    process_flag(trap_exit, true),

    State = #q{q                   = Q#amqqueue{pid = self()},
               exclusive_consumer  = none,
               has_had_consumers   = false,
               backing_queue       = backing_queue_module(Q),
               backing_queue_state = undefined,
               active_consumers    = queue:new(),
               expires             = undefined,
               sync_timer_ref      = undefined,
               rate_timer_ref      = undefined,
               expiry_timer_ref    = undefined,
               ttl                 = undefined,
               senders             = pmon:new(),
               dlx                 = undefined,
               dlx_routing_key     = undefined,
               publish_seqno       = 1,
               unconfirmed         = dtree:empty(),
               delayed_stop        = undefined,
               queue_monitors      = pmon:new(),
               msg_id_to_channel   = gb_trees:empty()},
    {ok, rabbit_event:init_stats_timer(State, #q.stats_timer), hibernate,
     {backoff, ?HIBERNATE_AFTER_MIN, ?HIBERNATE_AFTER_MIN, ?DESIRED_HIBERNATE}}.

init_with_backing_queue_state(Q = #amqqueue{exclusive_owner = Owner}, BQ, BQS,
                              RateTRef, AckTags, Deliveries, Senders, MTC) ->
    case Owner of
        none -> ok;
        _    -> erlang:monitor(process, Owner)
    end,
    State = #q{q                   = Q,
               exclusive_consumer  = none,
               has_had_consumers   = false,
               backing_queue       = BQ,
               backing_queue_state = BQS,
               active_consumers    = queue:new(),
               expires             = undefined,
               sync_timer_ref      = undefined,
               rate_timer_ref      = RateTRef,
               expiry_timer_ref    = undefined,
               ttl                 = undefined,
               senders             = Senders,
               publish_seqno       = 1,
               unconfirmed         = dtree:empty(),
               delayed_stop        = undefined,
               queue_monitors      = pmon:new(),
               msg_id_to_channel   = MTC},
    State1 = requeue_and_run(AckTags, process_args(
                                        rabbit_event:init_stats_timer(
                                          State, #q.stats_timer))),
    lists:foldl(
      fun (Delivery, StateN) -> deliver_or_enqueue(Delivery, StateN) end,
      State1, Deliveries).

terminate(shutdown = R,      State = #q{backing_queue = BQ}) ->
    terminate_shutdown(fun (BQS) -> BQ:terminate(R, BQS) end, State);
terminate({shutdown, _} = R, State = #q{backing_queue = BQ}) ->
    terminate_shutdown(fun (BQS) -> BQ:terminate(R, BQS) end, State);
terminate(Reason,            State = #q{q             = #amqqueue{name = QName},
                                        backing_queue = BQ}) ->
    %% FIXME: How do we cancel active subscriptions?
    terminate_shutdown(
      fun (BQS) ->
              BQS1 = BQ:delete_and_terminate(Reason, BQS),
              %% don't care if the internal delete doesn't return 'ok'.
              rabbit_amqqueue:internal_delete(QName, self()),
              BQS1
      end, State).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

declare(Recover, From, State = #q{q                   = Q,
                                  backing_queue       = BQ,
                                  backing_queue_state = undefined}) ->
    case rabbit_amqqueue:internal_declare(Q, Recover) of
        not_found -> {stop, normal, not_found, State};
        Q         -> gen_server2:reply(From, {new, Q}),
                     ok = file_handle_cache:register_callback(
                            rabbit_amqqueue, set_maximum_since_use,
                            [self()]),
                     ok = rabbit_memory_monitor:register(
                            self(), {rabbit_amqqueue,
                                     set_ram_duration_target, [self()]}),
                     BQS = bq_init(BQ, Q, Recover),
                     State1 = process_args(State#q{backing_queue_state = BQS}),
                     rabbit_event:notify(queue_created,
                                         infos(?CREATION_EVENT_KEYS, State1)),
                     rabbit_event:if_enabled(State1, #q.stats_timer,
                                             fun() -> emit_stats(State1) end),
                     noreply(State1);
        Q1        -> {stop, normal, {existing, Q1}, State}
    end.

bq_init(BQ, Q, Recover) ->
    Self = self(),
    BQ:init(Q, Recover,
            fun (Mod, Fun) ->
                    rabbit_amqqueue:run_backing_queue(Self, Mod, Fun)
            end).

process_args(State = #q{q = #amqqueue{arguments = Arguments}}) ->
    lists:foldl(
      fun({Arg, Fun}, State1) ->
              case rabbit_misc:table_lookup(Arguments, Arg) of
                  {_Type, Val} -> Fun(Val, State1);
                  undefined    -> State1
              end
      end, State,
      [{<<"x-expires">>,                 fun init_expires/2},
       {<<"x-message-ttl">>,             fun init_ttl/2},
       {<<"x-dead-letter-exchange">>,    fun init_dlx/2},
       {<<"x-dead-letter-routing-key">>, fun init_dlx_routing_key/2}]).

init_expires(Expires, State) -> ensure_expiry_timer(State#q{expires = Expires}).

init_ttl(TTL, State) -> drop_expired_messages(State#q{ttl = TTL}).

init_dlx(DLX, State = #q{q = #amqqueue{name = QName}}) ->
    State#q{dlx = rabbit_misc:r(QName, exchange, DLX)}.

init_dlx_routing_key(RoutingKey, State) ->
    State#q{dlx_routing_key = RoutingKey}.

terminate_shutdown(Fun, State) ->
    State1 = #q{backing_queue_state = BQS} =
        stop_sync_timer(stop_rate_timer(State)),
    case BQS of
        undefined -> State1;
        _         -> ok = rabbit_memory_monitor:deregister(self()),
                     [emit_consumer_deleted(Ch, CTag)
                      || {Ch, CTag, _} <- consumers(State1)],
                     State1#q{backing_queue_state = Fun(BQS)}
    end.

reply(Reply, NewState) ->
    assert_invariant(NewState),
    {NewState1, Timeout} = next_state(NewState),
    {reply, Reply, NewState1, Timeout}.

noreply(NewState) ->
    assert_invariant(NewState),
    {NewState1, Timeout} = next_state(NewState),
    {noreply, NewState1, Timeout}.

next_state(State = #q{backing_queue = BQ, backing_queue_state = BQS}) ->
    {MsgIds, BQS1} = BQ:drain_confirmed(BQS),
    State1 = ensure_stats_timer(
               ensure_rate_timer(
                 confirm_messages(MsgIds, State#q{
                                            backing_queue_state = BQS1}))),
    case BQ:needs_timeout(BQS1) of
        false -> {stop_sync_timer(State1),   hibernate     };
        idle  -> {stop_sync_timer(State1),   ?SYNC_INTERVAL};
        timed -> {ensure_sync_timer(State1), 0             }
    end.

backing_queue_module(#amqqueue{arguments = Args}) ->
    case rabbit_misc:table_lookup(Args, <<"x-ha-policy">>) of
        undefined -> {ok, BQM} = application:get_env(backing_queue_module),
                     BQM;
        _Policy   -> rabbit_mirror_queue_master
    end.

ensure_sync_timer(State = #q{sync_timer_ref = undefined}) ->
    TRef = erlang:send_after(?SYNC_INTERVAL, self(), sync_timeout),
    State#q{sync_timer_ref = TRef};
ensure_sync_timer(State) ->
    State.

stop_sync_timer(State = #q{sync_timer_ref = undefined}) ->
    State;
stop_sync_timer(State = #q{sync_timer_ref = TRef}) ->
    erlang:cancel_timer(TRef),
    State#q{sync_timer_ref = undefined}.

ensure_rate_timer(State = #q{rate_timer_ref = undefined}) ->
    TRef = erlang:send_after(
             ?RAM_DURATION_UPDATE_INTERVAL, self(), update_ram_duration),
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
    erlang:cancel_timer(TRef),
    State#q{rate_timer_ref = undefined}.

stop_expiry_timer(State = #q{expiry_timer_ref = undefined}) ->
    State;
stop_expiry_timer(State = #q{expiry_timer_ref = TRef}) ->
    erlang:cancel_timer(TRef),
    State#q{expiry_timer_ref = undefined}.

%% We wish to expire only when there are no consumers *and* the expiry
%% hasn't been refreshed (by queue.declare or basic.get) for the
%% configured period.
ensure_expiry_timer(State = #q{expires = undefined}) ->
    State;
ensure_expiry_timer(State = #q{expires = Expires}) ->
    case is_unused(State) of
        true  -> NewState = stop_expiry_timer(State),
                 TRef = erlang:send_after(Expires, self(), maybe_expire),
                 NewState#q{expiry_timer_ref = TRef};
        false -> State
    end.

ensure_stats_timer(State) ->
    rabbit_event:ensure_stats_timer(State, #q.stats_timer, emit_stats).

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
        undefined -> MonitorRef = erlang:monitor(process, ChPid),
                     C = #cr{ch_pid               = ChPid,
                             monitor_ref          = MonitorRef,
                             acktags              = sets:new(),
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
    case {sets:size(ChAckTags), ConsumerCount, UnsentMessageCount} of
        {0, 0, 0} -> ok = erase_ch_record(C);
        _         -> ok = store_ch_record(C)
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
                     true  -> sets:add_element(AckTag, ChAckTags);
                     false -> ChAckTags
                 end,
    update_ch_record(C#cr{acktags              = ChAckTags1,
                          unsent_message_count = Count + 1}),
    {Stop, State1}.

deliver_from_queue_deliver(AckRequired, State) ->
    {{Message, IsDelivered, AckTag, Remaining}, State1} =
        fetch(AckRequired, State),
    {{Message, IsDelivered, AckTag}, 0 == Remaining, State1}.

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

should_confirm_message(#delivery{msg_seq_no = undefined}, _State) ->
    never;
should_confirm_message(#delivery{sender     = SenderPid,
                                 msg_seq_no = MsgSeqNo,
                                 message    = #basic_message {
                                   is_persistent = true,
                                   id            = MsgId}},
                       #q{q = #amqqueue{durable = true}}) ->
    {eventually, SenderPid, MsgSeqNo, MsgId};
should_confirm_message(#delivery{sender     = SenderPid,
                                 msg_seq_no = MsgSeqNo},
                       _State) ->
    {immediately, SenderPid, MsgSeqNo}.

needs_confirming({eventually, _, _, _}) -> true;
needs_confirming(_)                     -> false.

maybe_record_confirm_message({eventually, SenderPid, MsgSeqNo, MsgId},
                             State = #q{msg_id_to_channel = MTC}) ->
    State#q{msg_id_to_channel =
                gb_trees:insert(MsgId, {SenderPid, MsgSeqNo}, MTC)};
maybe_record_confirm_message({immediately, SenderPid, MsgSeqNo}, State) ->
    rabbit_misc:confirm_to_sender(SenderPid, [MsgSeqNo]),
    State;
maybe_record_confirm_message(_Confirm, State) ->
    State.

run_message_queue(State) ->
    State1 = #q{backing_queue = BQ, backing_queue_state = BQS} =
        drop_expired_messages(State),
    {_IsEmpty1, State2} = deliver_msgs_to_consumers(
                            fun deliver_from_queue_deliver/2,
                            BQ:is_empty(BQS), State1),
    State2.

attempt_delivery(#delivery{sender = SenderPid, message = Message}, Confirm,
                 State = #q{backing_queue = BQ, backing_queue_state = BQS}) ->
    case BQ:is_duplicate(Message, BQS) of
        {false, BQS1} ->
            deliver_msgs_to_consumers(
              fun (AckRequired, State1 = #q{backing_queue_state = BQS2}) ->
                      Props = message_properties(Confirm, State1),
                      {AckTag, BQS3} = BQ:publish_delivered(
                                         AckRequired, Message, Props,
                                         SenderPid, BQS2),
                      {{Message, false, AckTag}, true,
                       State1#q{backing_queue_state = BQS3}}
              end, false, State#q{backing_queue_state = BQS1});
        {Duplicate, BQS1} ->
            %% if the message has previously been seen by the BQ then
            %% it must have been seen under the same circumstances as
            %% now: i.e. if it is now a deliver_immediately then it
            %% must have been before.
            {case Duplicate of
                 published -> true;
                 discarded -> false
             end,
             State#q{backing_queue_state = BQS1}}
    end.

deliver_or_enqueue(Delivery = #delivery{message    = Message,
                                        msg_seq_no = MsgSeqNo,
                                        sender     = SenderPid}, State) ->
    Confirm = should_confirm_message(Delivery, State),
    case attempt_delivery(Delivery, Confirm, State) of
        {true, State1} ->
            maybe_record_confirm_message(Confirm, State1);
        %% the next two are optimisations
        {false, State1 = #q{ttl = 0, dlx = undefined}} when Confirm == never ->
            discard_delivery(Delivery, State1);
        {false, State1 = #q{ttl = 0, dlx = undefined}} ->
            rabbit_misc:confirm_to_sender(SenderPid, [MsgSeqNo]),
            discard_delivery(Delivery, State1);
        {false, State1} ->
            State2 = #q{backing_queue = BQ, backing_queue_state = BQS} =
                maybe_record_confirm_message(Confirm, State1),
            Props = message_properties(Confirm, State2),
            BQS1 = BQ:publish(Message, Props, SenderPid, BQS),
            ensure_ttl_timer(State2#q{backing_queue_state = BQS1})
    end.

requeue_and_run(AckTags, State = #q{backing_queue = BQ}) ->
    run_backing_queue(BQ, fun (M, BQS) ->
                                  {_MsgIds, BQS1} = M:requeue(AckTags, BQS),
                                  BQS1
                          end, State).

fetch(AckRequired, State = #q{backing_queue_state = BQS,
                              backing_queue       = BQ}) ->
    {Result, BQS1} = BQ:fetch(AckRequired, BQS),
    {Result, State#q{backing_queue_state = BQS1}}.

remove_consumer(ChPid, ConsumerTag, Queue) ->
    queue:filter(fun ({CP, #consumer{tag = CTag}}) ->
                         (CP /= ChPid) or (CTag /= ConsumerTag)
                 end, Queue).

remove_consumers(ChPid, Queue) ->
    queue:filter(fun ({CP, #consumer{tag = CTag}}) when CP =:= ChPid ->
                         emit_consumer_deleted(ChPid, CTag),
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
            _ = remove_consumers(ChPid, Blocked), %% for stats emission
            ok = erase_ch_record(C),
            State1 = State#q{
                       exclusive_consumer = case Holder of
                                                {ChPid, _} -> none;
                                                Other      -> Other
                                            end,
                       active_consumers = remove_consumers(
                                            ChPid, State#q.active_consumers),
                       senders          = Senders1},
            case should_auto_delete(State1) of
                true  -> {stop, State1};
                false -> {ok, requeue_and_run(sets:to_list(ChAckTags),
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

consumer_count() -> consumer_count(fun (_) -> false end).

active_consumer_count() -> consumer_count(fun is_ch_blocked/1).

consumer_count(Exclude) ->
    lists:sum([Count || C = #cr{consumer_count = Count} <- all_ch_record(),
                        not Exclude(C)]).

is_unused(_State) -> consumer_count() == 0.

maybe_send_reply(_ChPid, undefined) -> ok;
maybe_send_reply(ChPid, Msg) -> ok = rabbit_channel:send_command(ChPid, Msg).

qname(#q{q = #amqqueue{name = QName}}) -> QName.

backing_queue_timeout(State = #q{backing_queue = BQ}) ->
    run_backing_queue(BQ, fun (M, BQS) -> M:timeout(BQS) end, State).

run_backing_queue(Mod, Fun, State = #q{backing_queue = BQ,
                                       backing_queue_state = BQS}) ->
    run_message_queue(State#q{backing_queue_state = BQ:invoke(Mod, Fun, BQS)}).

subtract_acks(ChPid, AckTags, State, Fun) ->
    case lookup_ch(ChPid) of
        not_found ->
            State;
        C = #cr{acktags = ChAckTags} ->
            update_ch_record(C#cr{acktags = lists:foldl(fun sets:del_element/2,
                                                        ChAckTags, AckTags)}),
            Fun(State)
    end.

discard_delivery(#delivery{sender = SenderPid,
                           message = Message},
                 State = #q{backing_queue = BQ,
                            backing_queue_state = BQS}) ->
    State#q{backing_queue_state = BQ:discard(Message, SenderPid, BQS)}.

message_properties(Confirm, #q{ttl = TTL}) ->
    #message_properties{expiry           = calculate_msg_expiry(TTL),
                        needs_confirming = needs_confirming(Confirm)}.

calculate_msg_expiry(undefined) -> undefined;
calculate_msg_expiry(TTL)       -> now_micros() + (TTL * 1000).

drop_expired_messages(State = #q{ttl = undefined}) ->
    State;
drop_expired_messages(State = #q{backing_queue_state = BQS,
                                 backing_queue       = BQ }) ->
    Now = now_micros(),
    DLXFun = dead_letter_fun(expired, State),
    ExpirePred = fun (#message_properties{expiry = Expiry}) -> Now > Expiry end,
    case DLXFun of
        undefined -> {undefined, BQS1} = BQ:dropwhile(ExpirePred, false, BQS),
                     BQS1;
        _         -> {Msgs, BQS1} = BQ:dropwhile(ExpirePred, true, BQS),
                     lists:foreach(
                       fun({Msg, AckTag}) -> DLXFun(Msg, AckTag) end, Msgs),
                     BQS1
    end,
    ensure_ttl_timer(State#q{backing_queue_state = BQS1}).

ensure_ttl_timer(State = #q{backing_queue       = BQ,
                            backing_queue_state = BQS,
                            ttl                 = TTL,
                            ttl_timer_ref       = undefined})
  when TTL =/= undefined ->
    case BQ:is_empty(BQS) of
        true  -> State;
        false -> TRef = erlang:send_after(TTL, self(), drop_expired),
                 State#q{ttl_timer_ref = TRef}
    end;
ensure_ttl_timer(State) ->
    State.

ack_if_no_dlx(AckTags, State = #q{dlx                 = undefined,
                                  backing_queue       = BQ,
                                  backing_queue_state = BQS }) ->
    {_Guids, BQS1} = BQ:ack(AckTags, BQS),
    State#q{backing_queue_state = BQS1};
ack_if_no_dlx(_AckTags, State) ->
    State.

dead_letter_fun(_Reason, #q{dlx = undefined}) ->
    undefined;
dead_letter_fun(Reason, _State) ->
    fun(Msg, AckTag) ->
            gen_server2:cast(self(), {dead_letter, {Msg, AckTag}, Reason})
    end.

dead_letter_publish(Msg, Reason, State = #q{publish_seqno = MsgSeqNo}) ->
    DLMsg = #basic_message{exchange_name = XName} =
        make_dead_letter_msg(Reason, Msg, State),
    case rabbit_exchange:lookup(XName) of
        {ok, X} ->
            Delivery = rabbit_basic:delivery(false, false, DLMsg, MsgSeqNo),
            {Queues, Cycles} = detect_dead_letter_cycles(
                                 DLMsg, rabbit_exchange:route(X, Delivery)),
            lists:foreach(fun log_cycle_once/1, Cycles),
            QPids = rabbit_amqqueue:lookup(Queues),
            {_, DeliveredQPids} = rabbit_amqqueue:deliver(QPids, Delivery),
            DeliveredQPids;
        {error, not_found} ->
            []
    end.

dead_letter_msg(Msg, AckTag, Reason, State = #q{publish_seqno = MsgSeqNo,
                                                unconfirmed   = UC}) ->
    QPids = dead_letter_publish(Msg, Reason, State),
    State1 = State#q{queue_monitors = pmon:monitor_all(
                                        QPids, State#q.queue_monitors),
                     publish_seqno  = MsgSeqNo + 1},
    case QPids of
        [] -> cleanup_after_confirm([AckTag], State1);
        _  -> UC1 = dtree:insert(MsgSeqNo, QPids, AckTag, UC),
              noreply(State1#q{unconfirmed = UC1})
    end.

handle_queue_down(QPid, Reason, State = #q{queue_monitors = QMons,
                                           unconfirmed    = UC}) ->
    case pmon:is_monitored(QPid, QMons) of
        false -> noreply(State);
        true  -> case rabbit_misc:is_abnormal_termination(Reason) of
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

stop_later(Reason, State) ->
    stop_later(Reason, undefined, noreply, State).

stop_later(Reason, From, Reply, State = #q{unconfirmed = UC}) ->
    case {dtree:is_empty(UC), Reply} of
        {true, noreply} ->
            {stop, Reason, State};
        {true, _} ->
            {stop, Reason, Reply, State};
        {false, _} ->
            noreply(State#q{delayed_stop = {Reason, {From, Reply}}})
    end.

cleanup_after_confirm(AckTags, State = #q{delayed_stop        = DS,
                                          unconfirmed         = UC,
                                          backing_queue       = BQ,
                                          backing_queue_state = BQS}) ->
    {_Guids, BQS1} = BQ:ack(AckTags, BQS),
    State1 = State#q{backing_queue_state = BQS1},
    case dtree:is_empty(UC) andalso DS =/= undefined of
        true  -> case DS of
                     {_, {_, noreply}}  -> ok;
                     {_, {From, Reply}} -> gen_server2:reply(From, Reply)
                 end,
                 {Reason, _} = DS,
                 {stop, Reason, State1};
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

make_dead_letter_msg(Reason,
                     Msg = #basic_message{content       = Content,
                                          exchange_name = Exchange,
                                          routing_keys  = RoutingKeys},
                     State = #q{dlx = DLX, dlx_routing_key = DlxRoutingKey}) ->
    {DeathRoutingKeys, HeadersFun1} =
        case DlxRoutingKey of
            undefined -> {RoutingKeys, fun (H) -> H end};
            _         -> {[DlxRoutingKey],
                          fun (H) -> lists:keydelete(<<"CC">>, 1, H) end}
        end,
    ReasonBin = list_to_binary(atom_to_list(Reason)),
    #resource{name = QName} = qname(State),
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
                HeadersFun1(rabbit_basic:append_table_header(<<"x-death">>,
                                                             Info, Headers))
        end,
    Content1 = rabbit_basic:map_headers(HeadersFun2, Content),
    Msg#basic_message{exchange_name = DLX, id = rabbit_guid:gen(),
                      routing_keys = DeathRoutingKeys, content = Content1}.

now_micros() -> timer:now_diff(now(), {0,0,0}).

infos(Items, State) ->
    {Prefix, Items1} =
        case lists:member(synchronised_slave_pids, Items) of
            true  -> Prefix1 = slaves_status(State),
                     case lists:member(slave_pids, Items) of
                         true  -> {Prefix1, Items -- [slave_pids]};
                         false -> {proplists:delete(slave_pids, Prefix1), Items}
                     end;
            false -> {[], Items}
        end,
    Prefix ++ [{Item, i(Item, State)}
               || Item <- (Items1 -- [synchronised_slave_pids])].

slaves_status(#q{q = #amqqueue{name = Name}}) ->
    case rabbit_amqqueue:lookup(Name) of
        {ok, #amqqueue{mirror_nodes = undefined}} ->
            [{slave_pids, ''}, {synchronised_slave_pids, ''}];
        {ok, #amqqueue{slave_pids = SPids}} ->
            {Results, _Bad} =
                delegate:invoke(SPids, fun rabbit_mirror_queue_slave:info/1),
            {SPids1, SSPids} =
                lists:foldl(
                  fun ({Pid, Infos}, {SPidsN, SSPidsN}) ->
                          {[Pid | SPidsN],
                           case proplists:get_bool(is_synchronised, Infos) of
                               true  -> [Pid | SSPidsN];
                               false -> SSPidsN
                           end}
                  end, {[], []}, Results),
            [{slave_pids, SPids1}, {synchronised_slave_pids, SSPids}]
    end.

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
i(consumers, _) ->
    consumer_count();
i(memory, _) ->
    {memory, M} = process_info(self(), memory),
    M;
i(slave_pids, #q{q = #amqqueue{name = Name}}) ->
    case rabbit_amqqueue:lookup(Name) of
        {ok, #amqqueue{mirror_nodes = undefined}} -> [];
        {ok, #amqqueue{slave_pids = SPids}}       -> SPids
    end;
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

emit_consumer_created(ChPid, ConsumerTag, Exclusive, AckRequired) ->
    rabbit_event:notify(consumer_created,
                        [{consumer_tag, ConsumerTag},
                         {exclusive,    Exclusive},
                         {ack_required, AckRequired},
                         {channel,      ChPid},
                         {queue,        self()}]).

emit_consumer_deleted(ChPid, ConsumerTag) ->
    rabbit_event:notify(consumer_deleted,
                        [{consumer_tag, ConsumerTag},
                         {channel,      ChPid},
                         {queue,        self()}]).

%%----------------------------------------------------------------------------

prioritise_call(Msg, _From, _State) ->
    case Msg of
        info                                 -> 9;
        {info, _Items}                       -> 9;
        consumers                            -> 9;
        {basic_consume, _, _, _, _, _, _}    -> 7;
        {basic_cancel, _, _, _}              -> 7;
        stat                                 -> 7;
        _                                    -> 0
    end.

prioritise_cast(Msg, _State) ->
    case Msg of
        delete_immediately                   -> 8;
        {set_ram_duration_target, _Duration} -> 8;
        {set_maximum_since_use, _Age}        -> 8;
        {ack, _AckTags, _ChPid}              -> 7;
        {reject, _AckTags, _Requeue, _ChPid} -> 7;
        {notify_sent, _ChPid, _Credit}       -> 7;
        {unblock, _ChPid}                    -> 7;
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
                     true -> ok;
                     _    -> rabbit_log:warning(
                               "Queue ~p exclusive owner went away~n", [QName])
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

handle_call({deliver, Delivery = #delivery{immediate = true}}, _From, State) ->
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
    Confirm = should_confirm_message(Delivery, State),
    {Delivered, State1} = attempt_delivery(Delivery, Confirm, State),
    reply(Delivered, case Delivered of
                         true  -> maybe_record_confirm_message(Confirm, State1);
                         false -> discard_delivery(Delivery, State1)
                     end);

handle_call({deliver, Delivery = #delivery{mandatory = true}}, From, State) ->
    gen_server2:reply(From, true),
    noreply(deliver_or_enqueue(Delivery, State));

handle_call({notify_down, ChPid}, From, State) ->
    %% we want to do this synchronously, so that auto_deleted queues
    %% are no longer visible by the time we send a response to the
    %% client.  The queue is ultimately deleted in terminate/2; if we
    %% return stop with a reply, terminate/2 will be called by
    %% gen_server2 *before* the reply is sent.
    case handle_ch_down(ChPid, State) of
        {ok, State1}   -> reply(ok, State1);
        {stop, State1} -> stop_later(normal, From, ok, State1)
    end;

handle_call({basic_get, ChPid, NoAck}, _From,
            State = #q{q = #amqqueue{name = QName}}) ->
    AckRequired = not NoAck,
    State1 = ensure_expiry_timer(State),
    case fetch(AckRequired, drop_expired_messages(State1)) of
        {empty, State2} ->
            reply(empty, State2);
        {{Message, IsDelivered, AckTag, Remaining}, State2} ->
            State3 =
                case AckRequired of
                    true  -> C = #cr{acktags = ChAckTags} = ch_record(ChPid),
                             ChAckTags1 = sets:add_element(AckTag, ChAckTags),
                             update_ch_record(C#cr{acktags = ChAckTags1}),
                             State2;
                    false -> State2
                end,
            Msg = {QName, self(), AckTag, IsDelivered, Message},
            reply({ok, Remaining, Msg}, State3)
    end;

handle_call({basic_consume, NoAck, ChPid, Limiter,
             ConsumerTag, ExclusiveConsume, OkMsg},
            _From, State = #q{exclusive_consumer = ExistingHolder}) ->
    case check_exclusive_access(ExistingHolder, ExclusiveConsume,
                                State) of
        in_use ->
            reply({error, exclusive_consume_unavailable}, State);
        ok ->
            C = ch_record(ChPid),
            C1 = update_consumer_count(C#cr{limiter = Limiter}, +1),
            Consumer = #consumer{tag = ConsumerTag,
                                 ack_required = not NoAck},
            ExclusiveConsumer = if ExclusiveConsume -> {ChPid, ConsumerTag};
                                   true             -> ExistingHolder
                                end,
            State1 = State#q{has_had_consumers = true,
                             exclusive_consumer = ExclusiveConsumer},
            ok = maybe_send_reply(ChPid, OkMsg),
            E = {ChPid, Consumer},
            State2 =
                case is_ch_blocked(C1) of
                    true  -> block_consumer(C1, E),
                             State1;
                    false -> update_ch_record(C1),
                             AC1 = queue:in(E, State1#q.active_consumers),
                             run_message_queue(State1#q{active_consumers = AC1})
                end,
            emit_consumer_created(ChPid, ConsumerTag, ExclusiveConsume,
                                  not NoAck),
            reply(ok, State2)
    end;

handle_call({basic_cancel, ChPid, ConsumerTag, OkMsg}, From,
            State = #q{exclusive_consumer = Holder}) ->
    ok = maybe_send_reply(ChPid, OkMsg),
    case lookup_ch(ChPid) of
        not_found ->
            reply(ok, State);
        C = #cr{blocked_consumers = Blocked} ->
            emit_consumer_deleted(ChPid, ConsumerTag),
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
                true  -> stop_later(normal, From, ok, State1)
            end
    end;

handle_call(stat, _From, State) ->
    State1 = #q{backing_queue = BQ, backing_queue_state = BQS} =
        drop_expired_messages(ensure_expiry_timer(State)),
    reply({ok, BQ:len(BQS), active_consumer_count()}, State1);

handle_call({delete, IfUnused, IfEmpty}, From,
            State = #q{backing_queue_state = BQS, backing_queue = BQ}) ->
    IsEmpty = BQ:is_empty(BQS),
    IsUnused = is_unused(State),
    if
        IfEmpty and not(IsEmpty)   -> reply({error, not_empty}, State);
        IfUnused and not(IsUnused) -> reply({error, in_use}, State);
        true                       -> stop_later(normal, From,
                                                 {ok, BQ:len(BQS)}, State)
    end;

handle_call(purge, _From, State = #q{backing_queue       = BQ,
                                     backing_queue_state = BQS}) ->
    {Count, BQS1} = BQ:purge(BQS),
    reply({ok, Count}, State#q{backing_queue_state = BQS1});

handle_call({requeue, AckTags, ChPid}, From, State) ->
    gen_server2:reply(From, ok),
    noreply(subtract_acks(
              ChPid, AckTags, State,
              fun (State1) -> requeue_and_run(AckTags, State1) end));

handle_call(force_event_refresh, _From,
            State = #q{exclusive_consumer = Exclusive}) ->
    rabbit_event:notify(queue_created, infos(?CREATION_EVENT_KEYS, State)),
    case Exclusive of
        none       -> [emit_consumer_created(Ch, CTag, false, AckRequired) ||
                          {Ch, CTag, AckRequired} <- consumers(State)];
        {Ch, CTag} -> [{Ch, CTag, AckRequired}] = consumers(State),
                      emit_consumer_created(Ch, CTag, true, AckRequired)
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

handle_cast({run_backing_queue, Mod, Fun}, State) ->
    noreply(run_backing_queue(Mod, Fun, State));

handle_cast({deliver, Delivery = #delivery{sender = Sender}, Flow},
            State = #q{senders = Senders}) ->
    %% Asynchronous, non-"mandatory", non-"immediate" deliver mode.
    Senders1 = case Flow of
                   flow   -> credit_flow:ack(Sender),
                             pmon:monitor(Sender, Senders);
                   noflow -> Senders
               end,
    State1 = State#q{senders = Senders1},
    noreply(deliver_or_enqueue(Delivery, State1));

handle_cast({ack, AckTags, ChPid}, State) ->
    noreply(subtract_acks(
              ChPid, AckTags, State,
              fun (State1 = #q{backing_queue       = BQ,
                               backing_queue_state = BQS}) ->
                      {_Guids, BQS1} = BQ:ack(AckTags, BQS),
                      State1#q{backing_queue_state = BQS1}
              end));

handle_cast({reject, AckTags, Requeue, ChPid}, State) ->
    noreply(subtract_acks(
              ChPid, AckTags, State,
              case Requeue of
                  true  -> fun (State1) -> requeue_and_run(AckTags, State1) end;
                  false -> fun (State1 = #q{backing_queue       = BQ,
                                            backing_queue_state = BQS}) ->
                                   Fun = dead_letter_fun(rejected, State1),
                                   BQS1 = BQ:fold(Fun, BQS, AckTags),
                                   ack_if_no_dlx(
                                     AckTags,
                                     State1#q{backing_queue_state = BQS1})
                           end
              end));

handle_cast(delete_immediately, State) ->
    stop_later(normal, State);

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

handle_cast({dead_letter, {Msg, AckTag}, Reason}, State) ->
    dead_letter_msg(Msg, AckTag, Reason, State).

%% We need to not ignore this as we need to remove outstanding
%% confirms due to queue death.
handle_info({'DOWN', _MonitorRef, process, DownPid, Reason},
            State = #q{delayed_stop = DS}) when DS =/= undefined ->
    handle_queue_down(DownPid, Reason, State);

handle_info(_, State = #q{delayed_stop = DS}) when DS =/= undefined ->
    noreply(State);

handle_info(maybe_expire, State) ->
    case is_unused(State) of
        true  -> stop_later(normal, State);
        false -> noreply(ensure_expiry_timer(State))
    end;

handle_info(drop_expired, State) ->
    noreply(drop_expired_messages(State#q{ttl_timer_ref = undefined}));

handle_info(emit_stats, State) ->
    %% Do not invoke noreply as it would see no timer and create a new one.
    emit_stats(State),
    State1 = rabbit_event:reset_stats_timer(State, #q.stats_timer),
    assert_invariant(State1),
    {noreply, State1, hibernate};

handle_info({'DOWN', _MonitorRef, process, DownPid, _Reason},
            State = #q{q = #amqqueue{exclusive_owner = DownPid}}) ->
    %% Exclusively owned queues must disappear with their owner.  In
    %% the case of clean shutdown we delete the queue synchronously in
    %% the reader - although not required by the spec this seems to
    %% match what people expect (see bug 21824). However we need this
    %% monitor-and-async- delete in case the connection goes away
    %% unexpectedly.
    stop_later(normal, State);

handle_info({'DOWN', _MonitorRef, process, DownPid, Reason}, State) ->
    case handle_ch_down(DownPid, State) of
        {ok, State1}   -> handle_queue_down(DownPid, Reason, State1);
        {stop, State1} -> stop_later(normal, State1)
    end;

handle_info(update_ram_duration, State = #q{backing_queue = BQ,
                                            backing_queue_state = BQS}) ->
    {RamDuration, BQS1} = BQ:ram_duration(BQS),
    DesiredDuration =
        rabbit_memory_monitor:report_ram_duration(self(), RamDuration),
    BQS2 = BQ:set_ram_duration_target(DesiredDuration, BQS1),
    noreply(State#q{rate_timer_ref = just_measured,
                    backing_queue_state = BQS2});

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
