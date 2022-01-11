%% This Source Code Form is subject tconsumer_ido the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
%%
%% before post gc 1M msg: 203MB, after recovery + gc: 203MB

-module(rabbit_fifo).

-behaviour(ra_machine).

-compile(inline_list_funcs).
-compile(inline).
-compile({no_auto_import, [apply/3]}).
-dialyzer(no_improper_lists).

-include("rabbit_fifo.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-export([
         %% ra_machine callbacks
         init/1,
         apply/3,
         state_enter/2,
         tick/2,
         overview/1,

         get_checked_out/4,
         %% versioning
         version/0,
         which_module/1,
         %% aux
         init_aux/1,
         handle_aux/6,
         % queries
         query_messages_ready/1,
         query_messages_checked_out/1,
         query_messages_total/1,
         query_processes/1,
         query_ra_indexes/1,
         query_consumer_count/1,
         query_consumers/1,
         query_stat/1,
         query_stat_dlx/1,
         query_single_active_consumer/1,
         query_in_memory_usage/1,
         query_peek/2,
         query_notify_decorators_info/1,
         usage/1,

         zero/1,

         %% misc
         dehydrate_state/1,
         dehydrate_message/1,
         normalize/1,
         get_msg_header/1,
         get_header/2,

         %% protocol helpers
         make_enqueue/3,
         make_register_enqueuer/1,
         make_checkout/3,
         make_settle/2,
         make_return/2,
         make_discard/2,
         make_credit/4,
         make_purge/0,
         make_purge_nodes/1,
         make_update_config/1,
         make_garbage_collection/0
        ]).

%% command records representing all the protocol actions that are supported
-record(enqueue, {pid :: option(pid()),
                  seq :: option(msg_seqno()),
                  msg :: raw_msg()}).
-record(requeue, {consumer_id :: consumer_id(),
                  msg_id :: msg_id(),
                  index :: ra:index(),
                  header :: msg_header(),
                  msg :: indexed_msg()}).
-record(register_enqueuer, {pid :: pid()}).
-record(checkout, {consumer_id :: consumer_id(),
                   spec :: checkout_spec(),
                   meta :: consumer_meta()}).
-record(settle, {consumer_id :: consumer_id(),
                 msg_ids :: [msg_id()]}).
-record(return, {consumer_id :: consumer_id(),
                 msg_ids :: [msg_id()]}).
-record(discard, {consumer_id :: consumer_id(),
                  msg_ids :: [msg_id()]}).
-record(credit, {consumer_id :: consumer_id(),
                 credit :: non_neg_integer(),
                 delivery_count :: non_neg_integer(),
                 drain :: boolean()}).
-record(purge, {}).
-record(purge_nodes, {nodes :: [node()]}).
-record(update_config, {config :: config()}).
-record(garbage_collection, {}).

-opaque protocol() ::
    #enqueue{} |
    #requeue{} |
    #register_enqueuer{} |
    #checkout{} |
    #settle{} |
    #return{} |
    #discard{} |
    #credit{} |
    #purge{} |
    #purge_nodes{} |
    #update_config{} |
    #garbage_collection{}.

-type command() :: protocol() |
                   rabbit_fifo_dlx:protocol() |
                   ra_machine:builtin_command().
%% all the command types supported by ra fifo

-type client_msg() :: delivery().
%% the messages `rabbit_fifo' can send to consumers.

-opaque state() :: #?MODULE{}.

-export_type([protocol/0,
              delivery/0,
              command/0,
              credit_mode/0,
              consumer_tag/0,
              consumer_meta/0,
              consumer_id/0,
              client_msg/0,
              indexed_msg/0,
              msg/0,
              msg_id/0,
              msg_seqno/0,
              delivery_msg/0,
              state/0,
              config/0]).

%% This function is never called since only rabbit_fifo_v0:init/1 is called.
%% See https://github.com/rabbitmq/ra/blob/e0d1e6315a45f5d3c19875d66f9d7bfaf83a46e3/src/ra_machine.erl#L258-L265
-spec init(config()) -> state().
init(#{name := Name,
       queue_resource := Resource} = Conf) ->
    update_config(Conf, #?MODULE{cfg = #cfg{name = Name,
                                            resource = Resource}}).

update_config(Conf, State) ->
    DLH = maps:get(dead_letter_handler, Conf, undefined),
    BLH = maps:get(become_leader_handler, Conf, undefined),
    RCI = maps:get(release_cursor_interval, Conf, ?RELEASE_CURSOR_EVERY),
    Overflow = maps:get(overflow_strategy, Conf, drop_head),
    MaxLength = maps:get(max_length, Conf, undefined),
    MaxBytes = maps:get(max_bytes, Conf, undefined),
    MaxMemoryLength = maps:get(max_in_memory_length, Conf, undefined),
    MaxMemoryBytes = maps:get(max_in_memory_bytes, Conf, undefined),
    DeliveryLimit = maps:get(delivery_limit, Conf, undefined),
    Expires = maps:get(expires, Conf, undefined),
    MsgTTL = maps:get(msg_ttl, Conf, undefined),
    ConsumerStrategy = case maps:get(single_active_consumer_on, Conf, false) of
                           true ->
                               single_active;
                           false ->
                               competing
                       end,
    Cfg = State#?MODULE.cfg,
    RCISpec = {RCI, RCI},

    LastActive = maps:get(created, Conf, undefined),
    MaxMemoryBytes = maps:get(max_in_memory_bytes, Conf, undefined),
    State#?MODULE{cfg = Cfg#cfg{release_cursor_interval = RCISpec,
                                dead_letter_handler = DLH,
                                become_leader_handler = BLH,
                                overflow_strategy = Overflow,
                                max_length = MaxLength,
                                max_bytes = MaxBytes,
                                max_in_memory_length = MaxMemoryLength,
                                max_in_memory_bytes = MaxMemoryBytes,
                                consumer_strategy = ConsumerStrategy,
                                delivery_limit = DeliveryLimit,
                                expires = Expires,
                                msg_ttl = MsgTTL},
                  last_active = LastActive}.

zero(_) ->
    0.

% msg_ids are scoped per consumer
% ra_indexes holds all raft indexes for enqueues currently on queue
-spec apply(ra_machine:command_meta_data(), command(), state()) ->
    {state(), Reply :: term(), ra_machine:effects()} |
    {state(), Reply :: term()}.
apply(Meta, #enqueue{pid = From, seq = Seq,
                     msg = RawMsg}, State00) ->
    apply_enqueue(Meta, From, Seq, RawMsg, State00);
apply(_Meta, #register_enqueuer{pid = Pid},
      #?MODULE{enqueuers = Enqueuers0,
               cfg = #cfg{overflow_strategy = Overflow}} = State0) ->

    State = case maps:is_key(Pid, Enqueuers0) of
                true ->
                    %% if the enqueuer exits just echo the overflow state
                    State0;
                false ->
                    State0#?MODULE{enqueuers = Enqueuers0#{Pid => #enqueuer{}}}
            end,
    Res = case is_over_limit(State) of
              true when Overflow == reject_publish ->
                  reject_publish;
              _ ->
                  ok
          end,
    {State, Res, [{monitor, process, Pid}]};
apply(Meta,
      #settle{msg_ids = MsgIds, consumer_id = ConsumerId},
      #?MODULE{consumers = Cons0} = State) ->
    case Cons0 of
        #{ConsumerId := Con0} ->
            complete_and_checkout(Meta, MsgIds, ConsumerId,
                                  Con0, [], State);
        _ ->
            {State, ok}

    end;
apply(Meta, #discard{msg_ids = MsgIds, consumer_id = ConsumerId},
      #?MODULE{consumers = Cons,
               dlx = DlxState0,
               cfg = #cfg{dead_letter_handler = DLH}} = State0) ->
    case Cons of
        #{ConsumerId := #consumer{checked_out = Checked} = Con} ->
            % Publishing to dead-letter exchange must maintain same order as messages got rejected.
            DiscardMsgs = lists:filtermap(fun(Id) ->
                                                  case maps:find(Id, Checked) of
                                                      {ok, Msg} ->
                                                          {true, Msg};
                                                      error ->
                                                          false
                                                  end
                                          end, MsgIds),
            {DlxState, Effects} = rabbit_fifo_dlx:discard(DiscardMsgs, rejected, DLH, DlxState0),
            State = State0#?MODULE{dlx = DlxState},
            complete_and_checkout(Meta, MsgIds, ConsumerId, Con, Effects, State);
        _ ->
            {State0, ok}
    end;
apply(Meta, #return{msg_ids = MsgIds, consumer_id = ConsumerId},
      #?MODULE{consumers = Cons0} = State) ->
    case Cons0 of
        #{ConsumerId := #consumer{checked_out = Checked0}} ->
            Returned = maps:with(MsgIds, Checked0),
            return(Meta, ConsumerId, Returned, [], State);
        _ ->
            {State, ok}
    end;
apply(#{index := Idx} = Meta,
      #requeue{consumer_id = ConsumerId,
               msg_id = MsgId,
               %% as we read the message from disk it is already
               %% an inmemory message
               index = OldIdx,
               header = Header0,
               msg = _Msg},
      #?MODULE{consumers = Cons0,
               messages = Messages,
               ra_indexes = Indexes0} = State00) ->
    case Cons0 of
        #{ConsumerId := #consumer{checked_out = Checked0} = Con0}
          when is_map_key(MsgId, Checked0) ->
            %% construct an index message with the current raft index
            %% and update delivery count before adding it to the message queue
            % ?INDEX_MSG(_, ?MSG(Header, _)) =
            %     update_msg_header(delivery_count, fun incr/1, 1, ?INDEX_MSG(Idx, Msg)),
            Header = update_header(delivery_count, fun incr/1, 1, Header0),

            State0 = add_bytes_return(Header, State00),

            IdxMsg = ?INDEX_MSG(Idx, ?DISK_MSG(Header)),
            % {State1, IdxMsg} = {State0, ?INDEX_MSG(Idx, ?DISK_MSG(Header))},
                % case evaluate_memory_limit(Header, State0) of
                %     true ->
                %         % indexed message with header map
                %         {State0, ?INDEX_MSG(Idx, ?DISK_MSG(Header))};
                %     false ->
                %         {add_in_memory_counts(Header, State0), IdxMsg0}
                % end,
            Con = Con0#consumer{checked_out = maps:remove(MsgId, Checked0),
                                credit = increase_credit(Con0, 1)},
            State2 = update_or_remove_sub(
                       Meta,
                       ConsumerId,
                       Con,
                       State0#?MODULE{ra_indexes = rabbit_fifo_index:delete(OldIdx, Indexes0),
                                      messages = lqueue:in(IdxMsg, Messages)}),

            %% We have to increment the enqueue counter to ensure release cursors
            %% are generated
            State3 = incr_enqueue_count(State2),

            {State, Ret, Effs} = checkout(Meta, State0, State3, []),
            update_smallest_raft_index(Idx, Ret,
                                       maybe_store_dehydrated_state(Idx,  State),
                                       Effs);
        _ ->
            {State00, ok}
    end;
apply(Meta, #credit{credit = NewCredit, delivery_count = RemoteDelCnt,
                    drain = Drain, consumer_id = ConsumerId},
      #?MODULE{consumers = Cons0,
               service_queue = ServiceQueue0,
               waiting_consumers = Waiting0} = State0) ->
    case Cons0 of
        #{ConsumerId := #consumer{delivery_count = DelCnt} = Con0} ->
            %% this can go below 0 when credit is reduced
            C = max(0, RemoteDelCnt + NewCredit - DelCnt),
            %% grant the credit
            Con1 = Con0#consumer{credit = C},
            ServiceQueue = maybe_queue_consumer(ConsumerId, Con1,
                                                ServiceQueue0),
            Cons = maps:put(ConsumerId, Con1, Cons0),
            {State1, ok, Effects} =
            checkout(Meta, State0,
                     State0#?MODULE{service_queue = ServiceQueue,
                                    consumers = Cons}, [], false),
            Response = {send_credit_reply, messages_ready(State1)},
            %% by this point all checkouts for the updated credit value
            %% should be processed so we can evaluate the drain
            case Drain of
                false ->
                    %% just return the result of the checkout
                    {State1, Response, Effects};
                true ->
                    Con = #consumer{credit = PostCred} =
                        maps:get(ConsumerId, State1#?MODULE.consumers),
                    %% add the outstanding credit to the delivery count
                    DeliveryCount = Con#consumer.delivery_count + PostCred,
                    Consumers = maps:put(ConsumerId,
                                         Con#consumer{delivery_count = DeliveryCount,
                                                      credit = 0},
                                         State1#?MODULE.consumers),
                    Drained = Con#consumer.credit,
                    {CTag, _} = ConsumerId,
                    {State1#?MODULE{consumers = Consumers},
                     %% returning a multi response with two client actions
                     %% for the channel to execute
                     {multi, [Response, {send_drained, {CTag, Drained}}]},
                     Effects}
            end;
        _ when Waiting0 /= [] ->
            %% there are waiting consuemrs
            case lists:keytake(ConsumerId, 1, Waiting0) of
                {value, {_, Con0 = #consumer{delivery_count = DelCnt}}, Waiting} ->
                    %% the consumer is a waiting one
                    %% grant the credit
                    C = max(0, RemoteDelCnt + NewCredit - DelCnt),
                    Con = Con0#consumer{credit = C},
                    State = State0#?MODULE{waiting_consumers =
                                           [{ConsumerId, Con} | Waiting]},
                    {State, {send_credit_reply, messages_ready(State)}};
                false ->
                    {State0, ok}
            end;
        _ ->
            %% credit for unknown consumer - just ignore
            {State0, ok}
    end;
apply(_, #checkout{spec = {dequeue, _}},
      #?MODULE{cfg = #cfg{consumer_strategy = single_active}} = State0) ->
    {State0, {error, {unsupported, single_active_consumer}}};
apply(#{index := Index,
        system_time := Ts,
        from := From} = Meta, #checkout{spec = {dequeue, Settlement},
                                        meta = ConsumerMeta,
                                        consumer_id = ConsumerId},
      #?MODULE{consumers = Consumers} = State00) ->
    %% dequeue always updates last_active
    State0 = State00#?MODULE{last_active = Ts},
    %% all dequeue operations result in keeping the queue from expiring
    Exists = maps:is_key(ConsumerId, Consumers),
    case messages_ready(State0) of
        0 ->
            update_smallest_raft_index(Index, {dequeue, empty}, State0,
                                       [notify_decorators_effect(State0)]);
        _ when Exists ->
            %% a dequeue using the same consumer_id isn't possible at this point
            {State0, {dequeue, empty}};
        Ready ->
            State1 = update_consumer(ConsumerId, ConsumerMeta,
                                     {once, 1, simple_prefetch}, 0,
                                     State0),
            {success, _, MsgId, Msg, State2, Effects0} = checkout_one(Meta, State1, []),
            {State4, Effects1} = case Settlement of
                                     unsettled ->
                                         {_, Pid} = ConsumerId,
                                         {State2, [{monitor, process, Pid} | Effects0]};
                                     settled ->
                                         %% immediately settle the checkout
                                         {State3, _, SettleEffects} =
                                         apply(Meta, make_settle(ConsumerId, [MsgId]),
                                               State2),
                                         {State3, SettleEffects ++ Effects0}
                                 end,
            {Reply, Effects2} =
            case Msg of
                ?INDEX_MSG(RaftIdx, ?DISK_MSG(Header)) ->
                    {'$ra_no_reply',
                     [reply_log_effect(RaftIdx, MsgId, Header, Ready - 1, From) |
                      Effects1]}
                % ?INDEX_MSG(_, ?MSG(Header, Body)) ->
                %     {{dequeue, {MsgId, {Header, Body}}, Ready-1}, Effects1}

            end,
            NotifyEffect = notify_decorators_effect(State4),
            case evaluate_limit(Index, false, State0, State4, [NotifyEffect | Effects2]) of
                {State, true, Effects} ->
                    update_smallest_raft_index(Index, Reply, State, Effects);
                {State, false, Effects} ->
                    {State, Reply, Effects}
            end
    end;
apply(#{index := Idx} = Meta,
      #checkout{spec = cancel,
                consumer_id = ConsumerId}, State0) ->
    {State1, Effects1} = cancel_consumer(Meta, ConsumerId, State0, [],
                                         consumer_cancel),
    {State, Reply, Effects} = checkout(Meta, State0, State1, Effects1),
    update_smallest_raft_index(Idx, Reply, State, Effects);
apply(Meta, #checkout{spec = Spec, meta = ConsumerMeta,
                      consumer_id = {_, Pid} = ConsumerId},
      State0) ->
    Priority = get_priority_from_args(ConsumerMeta),
    State1 = update_consumer(ConsumerId, ConsumerMeta, Spec, Priority, State0),
    checkout(Meta, State0, State1, [{monitor, process, Pid}]);
apply(#{index := Index}, #purge{},
      #?MODULE{messages_total = Tot,
               returns = Returns,
               messages = Messages,
               ra_indexes = Indexes0,
               dlx = DlxState} = State0) ->
    NumReady = messages_ready(State0),
    Indexes1 = lists:foldl(fun (?INDEX_MSG(I, _), Acc0) when is_integer(I) ->
                                   rabbit_fifo_index:delete(I, Acc0);
                               (_, Acc) ->
                                   Acc
                           end, Indexes0, lqueue:to_list(Returns)),
    Indexes = lists:foldl(fun (?INDEX_MSG(I, _), Acc0) when is_integer(I) ->
                                  rabbit_fifo_index:delete(I, Acc0);
                              (_, Acc) ->
                                  Acc
                          end, Indexes1, lqueue:to_list(Messages)),
    {NumDlx, _} = rabbit_fifo_dlx:stat(DlxState),
    State1 = State0#?MODULE{ra_indexes = Indexes,
                            dlx = rabbit_fifo_dlx:purge(DlxState),
                            messages = lqueue:new(),
                            messages_total = Tot - NumReady,
                            returns = lqueue:new(),
                            msg_bytes_enqueue = 0,
                            prefix_msgs = {0, [], 0, []},
                            msg_bytes_in_memory = 0,
                            msgs_ready_in_memory = 0},
    Effects0 = [garbage_collection],
    Reply = {purge, NumReady + NumDlx},
    {State, _, Effects} = evaluate_limit(Index, false, State0,
                                         State1, Effects0),
    update_smallest_raft_index(Index, Reply, State, Effects);
apply(#{index := Idx}, #garbage_collection{}, State) ->
    update_smallest_raft_index(Idx, ok, State, [{aux, garbage_collection}]);
apply(#{system_time := Ts} = Meta, {timeout, expire_msgs}, State0) ->
    {State, Effects} = expire_msgs(Ts, State0, []),
    checkout(Meta, State, State, Effects, false);
apply(#{system_time := Ts} = Meta, {down, Pid, noconnection},
      #?MODULE{consumers = Cons0,
               cfg = #cfg{consumer_strategy = single_active},
               waiting_consumers = Waiting0,
               enqueuers = Enqs0} = State0) ->
    Node = node(Pid),
    %% if the pid refers to an active or cancelled consumer,
    %% mark it as suspected and return it to the waiting queue
    {State1, Effects0} =
        maps:fold(fun({_, P} = Cid, C0, {S0, E0})
                        when node(P) =:= Node ->
                          %% the consumer should be returned to waiting
                          %% and checked out messages should be returned
                          Effs = consumer_update_active_effects(
                                   S0, Cid, C0, false, suspected_down, E0),
                          Checked = C0#consumer.checked_out,
                          Credit = increase_credit(C0, maps:size(Checked)),
                          {St, Effs1} = return_all(Meta, S0, Effs,
                                                   Cid, C0#consumer{credit = Credit}),
                          %% if the consumer was cancelled there is a chance it got
                          %% removed when returning hence we need to be defensive here
                          Waiting = case St#?MODULE.consumers of
                                        #{Cid := C} ->
                                            Waiting0 ++ [{Cid, C}];
                                        _ ->
                                            Waiting0
                                    end,
                          {St#?MODULE{consumers = maps:remove(Cid, St#?MODULE.consumers),
                                      waiting_consumers = Waiting,
                                      last_active = Ts},
                           Effs1};
                     (_, _, S) ->
                          S
                  end, {State0, []}, Cons0),
    WaitingConsumers = update_waiting_consumer_status(Node, State1,
                                                      suspected_down),

    %% select a new consumer from the waiting queue and run a checkout
    State2 = State1#?MODULE{waiting_consumers = WaitingConsumers},
    {State, Effects1} = activate_next_consumer(State2, Effects0),

    %% mark any enquers as suspected
    Enqs = maps:map(fun(P, E) when node(P) =:= Node ->
                            E#enqueuer{status = suspected_down};
                       (_, E) -> E
                    end, Enqs0),
    Effects = [{monitor, node, Node} | Effects1],
    checkout(Meta, State0, State#?MODULE{enqueuers = Enqs}, Effects);
apply(#{system_time := Ts} = Meta, {down, Pid, noconnection},
      #?MODULE{consumers = Cons0,
               enqueuers = Enqs0} = State0) ->
    %% A node has been disconnected. This doesn't necessarily mean that
    %% any processes on this node are down, they _may_ come back so here
    %% we just mark them as suspected (effectively deactivated)
    %% and return all checked out messages to the main queue for delivery to any
    %% live consumers
    %%
    %% all pids for the disconnected node will be marked as suspected not just
    %% the one we got the `down' command for
    Node = node(Pid),

    {State, Effects1} =
        maps:fold(
          fun({_, P} = Cid, #consumer{checked_out = Checked0,
                                      status = up} = C0,
              {St0, Eff}) when node(P) =:= Node ->
                  Credit = increase_credit(C0, map_size(Checked0)),
                  C = C0#consumer{status = suspected_down,
                                  credit = Credit},
                  {St, Eff0} = return_all(Meta, St0, Eff, Cid, C),
                  Eff1 = consumer_update_active_effects(St, Cid, C, false,
                                                        suspected_down, Eff0),
                  {St, Eff1};
             (_, _, {St, Eff}) ->
                  {St, Eff}
          end, {State0, []}, Cons0),
    Enqs = maps:map(fun(P, E) when node(P) =:= Node ->
                            E#enqueuer{status = suspected_down};
                       (_, E) -> E
                    end, Enqs0),

    % Monitor the node so that we can "unsuspect" these processes when the node
    % comes back, then re-issue all monitors and discover the final fate of
    % these processes

    Effects = case maps:size(State#?MODULE.consumers) of
                  0 ->
                      [{aux, inactive}, {monitor, node, Node}];
                  _ ->
                      [{monitor, node, Node}]
              end ++ Effects1,
    checkout(Meta, State0, State#?MODULE{enqueuers = Enqs,
                                         last_active = Ts}, Effects);
apply(Meta, {down, Pid, _Info}, State0) ->
    {State, Effects} = handle_down(Meta, Pid, State0),
    checkout(Meta, State0, State, Effects);
apply(Meta, {nodeup, Node}, #?MODULE{consumers = Cons0,
                                     enqueuers = Enqs0,
                                     service_queue = _SQ0} = State0) ->
    %% A node we are monitoring has come back.
    %% If we have suspected any processes of being
    %% down we should now re-issue the monitors for them to detect if they're
    %% actually down or not
    Monitors = [{monitor, process, P}
                || P <- suspected_pids_for(Node, State0)],

    Enqs1 = maps:map(fun(P, E) when node(P) =:= Node ->
                             E#enqueuer{status = up};
                        (_, E) -> E
                     end, Enqs0),
    ConsumerUpdateActiveFun = consumer_active_flag_update_function(State0),
    %% mark all consumers as up
    {State1, Effects1} =
        maps:fold(fun({_, P} = ConsumerId, C, {SAcc, EAcc})
                        when (node(P) =:= Node) and
                             (C#consumer.status =/= cancelled) ->
                          EAcc1 = ConsumerUpdateActiveFun(SAcc, ConsumerId,
                                                          C, true, up, EAcc),
                          {update_or_remove_sub(Meta, ConsumerId,
                                                C#consumer{status = up},
                                                SAcc), EAcc1};
                     (_, _, Acc) ->
                          Acc
                  end, {State0, Monitors}, Cons0),
    Waiting = update_waiting_consumer_status(Node, State1, up),
    State2 = State1#?MODULE{
                            enqueuers = Enqs1,
                            waiting_consumers = Waiting},
    {State, Effects} = activate_next_consumer(State2, Effects1),
    checkout(Meta, State0, State, Effects);
apply(_, {nodedown, _Node}, State) ->
    {State, ok};
apply(#{index := Idx} = Meta, #purge_nodes{nodes = Nodes}, State0) ->
    {State, Effects} = lists:foldl(fun(Node, {S, E}) ->
                                           purge_node(Meta, Node, S, E)
                                   end, {State0, []}, Nodes),
    update_smallest_raft_index(Idx, ok, State, Effects);
apply(#{index := Idx} = Meta,
      #update_config{config = #{dead_letter_handler := NewDLH} = Conf},
      #?MODULE{cfg = #cfg{dead_letter_handler = OldDLH,
                          resource = QRes},
               dlx = DlxState0} = State0) ->
    {DlxState, Effects0} = rabbit_fifo_dlx:update_config(OldDLH, NewDLH, QRes, DlxState0),
    State1 = update_config(Conf, State0#?MODULE{dlx = DlxState}),
    {State, Reply, Effects} = checkout(Meta, State0, State1, Effects0),
    update_smallest_raft_index(Idx, Reply, State, Effects);
apply(_Meta, {machine_version, FromVersion, ToVersion}, V0State) ->
    State = convert(FromVersion, ToVersion, V0State),
    {State, ok, [{aux, {dlx, setup}}]};
apply(#{index := IncomingRaftIdx} = Meta, {dlx, _} = Cmd,
      #?MODULE{cfg = #cfg{dead_letter_handler = DLH},
               dlx = DlxState0} = State0) ->
    {DlxState, Effects0} = rabbit_fifo_dlx:apply(Meta, Cmd, DLH, DlxState0),
    State1 = State0#?MODULE{dlx = DlxState},
    {State, ok, Effects} = checkout(Meta, State0, State1, Effects0, false),
    update_smallest_raft_index(IncomingRaftIdx, State, Effects);
apply(_Meta, Cmd, State) ->
    %% handle unhandled commands gracefully
    rabbit_log:debug("rabbit_fifo: unhandled command ~W", [Cmd, 10]),
    {State, ok, []}.

convert_msg({RaftIdx, {Header, empty}}) ->
    ?INDEX_MSG(RaftIdx, ?DISK_MSG(Header));
convert_msg({RaftIdx, {Header, _Msg}}) when is_integer(RaftIdx) ->
    ?INDEX_MSG(RaftIdx, ?DISK_MSG(Header));
convert_msg({'$empty_msg', Header}) ->
    %% dummy index
    ?INDEX_MSG(0, ?DISK_MSG(Header));
convert_msg({'$prefix_msg', Header}) ->
    %% dummy index
    ?INDEX_MSG(0, ?DISK_MSG(Header)).

convert_v1_to_v2(V1State) ->
    IndexesV1 = rabbit_fifo_v1:get_field(ra_indexes, V1State),
    ReturnsV1 = rabbit_fifo_v1:get_field(returns, V1State),
    MessagesV1 = rabbit_fifo_v1:get_field(messages, V1State),
    % EnqueuersV1 = rabbit_fifo_v1:get_field(enqueuers, V1State),
    ConsumersV1 = rabbit_fifo_v1:get_field(consumers, V1State),
    %% remove all raft idx in messages from index
    MessagesV2 = lqueue:foldl(fun ({_, IdxMsg}, Acc) ->
                                      lqueue:in(convert_msg(IdxMsg), Acc)
                              end, lqueue:new(), MessagesV1),
    ReturnsV2 = lqueue:foldl(fun ({_SeqId, Msg}, Acc) ->
                                     lqueue:in(convert_msg(Msg), Acc)
                             end, lqueue:new(), ReturnsV1),

    ConsumersV2 = maps:map(
                    fun (_, #consumer{checked_out = Ch} = C) ->
                            C#consumer{
                              checked_out = maps:map(
                                              fun (_, {_SeqId, IdxMsg}) ->
                                                      convert_msg(IdxMsg)
                                              end, Ch)}
                    end, ConsumersV1),

    %% The (old) format of dead_letter_handler in RMQ < v3.10 is:
    %%   {Module, Function, Args}
    %% The (new) format of dead_letter_handler in RMQ >= v3.10 is:
    %%   undefined | {at_most_once, {Module, Function, Args}} | at_least_once
    %%
    %% Note that the conversion must convert both from old format to new format
    %% as well as from new format to new format. The latter is because quorum queues
    %% created in RMQ >= v3.10 are still initialised with rabbit_fifo_v0 as described in
    %% https://github.com/rabbitmq/ra/blob/e0d1e6315a45f5d3c19875d66f9d7bfaf83a46e3/src/ra_machine.erl#L258-L265
    DLH = case rabbit_fifo_v1:get_cfg_field(dead_letter_handler, V1State) of
              {_M, _F, _A = [_DLX = undefined|_]} ->
                  %% queue was declared in RMQ < v3.10 and no DLX configured
                  undefined;
              {_M, _F, _A} = MFA ->
                  %% queue was declared in RMQ < v3.10 and DLX configured
                  {at_most_once, MFA};
              Other ->
                  Other
          end,

    %% Then add all pending messages back into the index
    Cfg = #cfg{name = rabbit_fifo_v1:get_cfg_field(name, V1State),
               resource = rabbit_fifo_v1:get_cfg_field(resource, V1State),
               release_cursor_interval = rabbit_fifo_v1:get_cfg_field(release_cursor_interval, V1State),
               dead_letter_handler = DLH,
               become_leader_handler = rabbit_fifo_v1:get_cfg_field(become_leader_handler, V1State),
               %% TODO: what if policy enabling reject_publish was applied before conversion?
               overflow_strategy = rabbit_fifo_v1:get_cfg_field(overflow_strategy, V1State),
               max_length = rabbit_fifo_v1:get_cfg_field(max_length, V1State),
               max_bytes = rabbit_fifo_v1:get_cfg_field(max_bytes, V1State),
               consumer_strategy = rabbit_fifo_v1:get_cfg_field(consumer_strategy, V1State),
               delivery_limit = rabbit_fifo_v1:get_cfg_field(delivery_limit, V1State),
               max_in_memory_length = rabbit_fifo_v1:get_cfg_field(max_in_memory_length, V1State),
               max_in_memory_bytes = rabbit_fifo_v1:get_cfg_field(max_in_memory_bytes, V1State),
               expires = rabbit_fifo_v1:get_cfg_field(expires, V1State)
              },

    #?MODULE{
        cfg = Cfg,
        messages = MessagesV2,
        messages_total = rabbit_fifo_v1:query_messages_total(V1State),
        returns = ReturnsV2,
        enqueue_count = rabbit_fifo_v1:get_field(enqueue_count, V1State),
        enqueuers = rabbit_fifo_v1:get_field(enqueuers, V1State),
        ra_indexes = IndexesV1,
        release_cursors = rabbit_fifo_v1:get_field(release_cursors, V1State),
        consumers = ConsumersV2,
        service_queue = rabbit_fifo_v1:get_field(service_queue, V1State),
        prefix_msgs = rabbit_fifo_v1:get_field(prefix_msgs, V1State),
        msg_bytes_enqueue = rabbit_fifo_v1:get_field(msg_bytes_enqueue, V1State),
        msg_bytes_checkout = rabbit_fifo_v1:get_field(msg_bytes_checkout, V1State),
        waiting_consumers = rabbit_fifo_v1:get_field(waiting_consumers, V1State),
        msg_bytes_in_memory = 0,
        msgs_ready_in_memory = 0,
        last_active = rabbit_fifo_v1:get_field(last_active, V1State)
       }.

purge_node(Meta, Node, State, Effects) ->
    lists:foldl(fun(Pid, {S0, E0}) ->
                        {S, E} = handle_down(Meta, Pid, S0),
                        {S, E0 ++ E}
                end, {State, Effects}, all_pids_for(Node, State)).

%% any downs that re not noconnection
handle_down(Meta, Pid, #?MODULE{consumers = Cons0,
                                enqueuers = Enqs0} = State0) ->
    % Remove any enqueuer for the down pid
    State1 = State0#?MODULE{enqueuers = maps:remove(Pid, Enqs0)},
    {Effects1, State2} = handle_waiting_consumer_down(Pid, State1),
    % return checked out messages to main queue
    % Find the consumers for the down pid
    DownConsumers = maps:keys(
                      maps:filter(fun({_, P}, _) -> P =:= Pid end, Cons0)),
    lists:foldl(fun(ConsumerId, {S, E}) ->
                        cancel_consumer(Meta, ConsumerId, S, E, down)
                end, {State2, Effects1}, DownConsumers).

consumer_active_flag_update_function(#?MODULE{cfg = #cfg{consumer_strategy = competing}}) ->
    fun(State, ConsumerId, Consumer, Active, ActivityStatus, Effects) ->
        consumer_update_active_effects(State, ConsumerId, Consumer, Active,
                                       ActivityStatus, Effects)
    end;
consumer_active_flag_update_function(#?MODULE{cfg = #cfg{consumer_strategy = single_active}}) ->
    fun(_, _, _, _, _, Effects) ->
        Effects
    end.

handle_waiting_consumer_down(_Pid,
                             #?MODULE{cfg = #cfg{consumer_strategy = competing}} = State) ->
    {[], State};
handle_waiting_consumer_down(_Pid,
                             #?MODULE{cfg = #cfg{consumer_strategy = single_active},
                                      waiting_consumers = []} = State) ->
    {[], State};
handle_waiting_consumer_down(Pid,
                             #?MODULE{cfg = #cfg{consumer_strategy = single_active},
                                      waiting_consumers = WaitingConsumers0} = State0) ->
    % get cancel effects for down waiting consumers
    Down = lists:filter(fun({{_, P}, _}) -> P =:= Pid end,
                        WaitingConsumers0),
    Effects = lists:foldl(fun ({ConsumerId, _}, Effects) ->
                                  cancel_consumer_effects(ConsumerId, State0,
                                                          Effects)
                          end, [], Down),
    % update state to have only up waiting consumers
    StillUp = lists:filter(fun({{_, P}, _}) -> P =/= Pid end,
                           WaitingConsumers0),
    State = State0#?MODULE{waiting_consumers = StillUp},
    {Effects, State}.

update_waiting_consumer_status(Node,
                               #?MODULE{waiting_consumers = WaitingConsumers},
                               Status) ->
    [begin
         case node(Pid) of
             Node ->
                 {ConsumerId, Consumer#consumer{status = Status}};
             _ ->
                 {ConsumerId, Consumer}
         end
     end || {{_, Pid} = ConsumerId, Consumer} <- WaitingConsumers,
     Consumer#consumer.status =/= cancelled].

-spec state_enter(ra_server:ra_state(), state()) -> ra_machine:effects().
state_enter(RaState, #?MODULE{cfg = #cfg{dead_letter_handler = DLH,
                                         resource = QRes},
                              dlx = DlxState} = State) ->
    Effects = rabbit_fifo_dlx:state_enter(RaState, QRes, DLH, DlxState),
    state_enter0(RaState, State, Effects).

state_enter0(leader, #?MODULE{consumers = Cons,
                              enqueuers = Enqs,
                              waiting_consumers = WaitingConsumers,
                              cfg = #cfg{name = Name,
                                         resource = Resource,
                                         become_leader_handler = BLH},
                              prefix_msgs = {0, [], 0, []}
                             } = State,
             Effects0) ->
    TimerEffs = timer_effect(erlang:system_time(millisecond), State, Effects0),
    % return effects to monitor all current consumers and enqueuers
    Pids = lists:usort(maps:keys(Enqs)
        ++ [P || {_, P} <- maps:keys(Cons)]
        ++ [P || {{_, P}, _} <- WaitingConsumers]),
    Mons = [{monitor, process, P} || P <- Pids],
    Nots = [{send_msg, P, leader_change, ra_event} || P <- Pids],
    NodeMons = lists:usort([{monitor, node, node(P)} || P <- Pids]),
    FHReservation = [{mod_call, rabbit_quorum_queue, file_handle_leader_reservation, [Resource]}],
    Effects = TimerEffs ++ Mons ++ Nots ++ NodeMons ++ FHReservation,
    case BLH of
        undefined ->
            Effects;
        {Mod, Fun, Args} ->
            [{mod_call, Mod, Fun, Args ++ [Name]} | Effects]
    end;
state_enter0(eol, #?MODULE{enqueuers = Enqs,
                           consumers = Custs0,
                           waiting_consumers = WaitingConsumers0},
             Effects) ->
    Custs = maps:fold(fun({_, P}, V, S) -> S#{P => V} end, #{}, Custs0),
    WaitingConsumers1 = lists:foldl(fun({{_, P}, V}, Acc) -> Acc#{P => V} end,
                                    #{}, WaitingConsumers0),
    AllConsumers = maps:merge(Custs, WaitingConsumers1),
    [{send_msg, P, eol, ra_event}
     || P <- maps:keys(maps:merge(Enqs, AllConsumers))] ++
    [{aux, eol},
     {mod_call, rabbit_quorum_queue, file_handle_release_reservation, []} | Effects];
state_enter0(State, #?MODULE{cfg = #cfg{resource = _Resource}}, Effects)
  when State =/= leader ->
    FHReservation = {mod_call, rabbit_quorum_queue, file_handle_other_reservation, []},
    [FHReservation | Effects];
state_enter0(_, _, Effects) ->
    %% catch all as not handling all states
    Effects.

-spec tick(non_neg_integer(), state()) -> ra_machine:effects().
tick(Ts, #?MODULE{cfg = #cfg{name = Name,
                             resource = QName},
                  msg_bytes_enqueue = EnqueueBytes,
                  msg_bytes_checkout = CheckoutBytes,
                  dlx = DlxState} = State) ->
    case is_expired(Ts, State) of
        true ->
            [{mod_call, rabbit_quorum_queue, spawn_deleter, [QName]}];
        false ->
            {_, MsgBytesDiscard} = rabbit_fifo_dlx:stat(DlxState),
            Metrics = {Name,
                       messages_ready(State),
                       num_checked_out(State), % checked out
                       messages_total(State),
                       query_consumer_count(State), % Consumers
                       EnqueueBytes,
                       CheckoutBytes,
                       MsgBytesDiscard},
            [{mod_call, rabbit_quorum_queue,
              handle_tick, [QName, Metrics, all_nodes(State)]}]
    end.

-spec overview(state()) -> map().
overview(#?MODULE{consumers = Cons,
                  enqueuers = Enqs,
                  release_cursors = Cursors,
                  enqueue_count = EnqCount,
                  msgs_ready_in_memory = InMemReady,
                  msg_bytes_in_memory = InMemBytes,
                  msg_bytes_enqueue = EnqueueBytes,
                  msg_bytes_checkout = CheckoutBytes,
                  cfg = Cfg,
                  dlx = DlxState} = State) ->
    Conf = #{name => Cfg#cfg.name,
             resource => Cfg#cfg.resource,
             release_cursor_interval => Cfg#cfg.release_cursor_interval,
             dead_lettering_enabled => undefined =/= Cfg#cfg.dead_letter_handler,
             max_length => Cfg#cfg.max_length,
             max_bytes => Cfg#cfg.max_bytes,
             consumer_strategy => Cfg#cfg.consumer_strategy,
             max_in_memory_length => Cfg#cfg.max_in_memory_length,
             max_in_memory_bytes => Cfg#cfg.max_in_memory_bytes,
             expires => Cfg#cfg.expires,
             msg_ttl => Cfg#cfg.msg_ttl,
             delivery_limit => Cfg#cfg.delivery_limit
            },
    Overview = #{type => ?MODULE,
                 config => Conf,
                 num_consumers => maps:size(Cons),
                 num_checked_out => num_checked_out(State),
                 num_enqueuers => maps:size(Enqs),
                 num_ready_messages => messages_ready(State),
                 num_in_memory_ready_messages => InMemReady,
                 num_messages => messages_total(State),
                 num_release_cursors => lqueue:len(Cursors),
                 release_cursors => [{I, messages_total(S)} || {_, I, S} <- lqueue:to_list(Cursors)],
                 release_cursor_enqueue_counter => EnqCount,
                 enqueue_message_bytes => EnqueueBytes,
                 checkout_message_bytes => CheckoutBytes,
                 in_memory_message_bytes => InMemBytes,
                 smallest_raft_index => smallest_raft_index(State)},
    DlxOverview = rabbit_fifo_dlx:overview(DlxState),
    maps:merge(Overview, DlxOverview).

-spec get_checked_out(consumer_id(), msg_id(), msg_id(), state()) ->
    [delivery_msg()].
get_checked_out(Cid, From, To, #?MODULE{consumers = Consumers}) ->
    case Consumers of
        #{Cid := #consumer{checked_out = Checked}} ->
            [begin
                 ?INDEX_MSG(I, ?DISK_MSG(H)) = maps:get(K, Checked),
                 {K, {I, H}}
             end || K <- lists:seq(From, To), maps:is_key(K, Checked)];
        _ ->
            []
    end.

-spec version() -> pos_integer().
version() -> 2.

which_module(0) -> rabbit_fifo_v0;
which_module(1) -> rabbit_fifo_v1;
which_module(2) -> ?MODULE.

-define(AUX, aux_v2).

-record(aux_gc, {last_raft_idx = 0 :: ra:index()}).
-record(aux, {name :: atom(),
              capacity :: term(),
              gc = #aux_gc{} :: #aux_gc{}}).
-record(?AUX, {name :: atom(),
               capacity :: term(),
               gc = #aux_gc{} :: #aux_gc{},
               unused,
               unused2}).

init_aux(Name) when is_atom(Name) ->
    %% TODO: catch specific exception throw if table already exists
    ok = ra_machine_ets:create_table(rabbit_fifo_usage,
                                     [named_table, set, public,
                                      {write_concurrency, true}]),
    Now = erlang:monotonic_time(micro_seconds),
    #?AUX{name = Name,
          capacity = {inactive, Now, 1, 1.0}}.

handle_aux(RaftState, Tag, Cmd, #aux{name = Name,
                                     capacity = Cap,
                                     gc = Gc}, Log, MacState) ->
    %% convert aux state to new version
    Aux = #?AUX{name = Name,
                capacity = Cap,
                gc = Gc},
    handle_aux(RaftState, Tag, Cmd, Aux, Log, MacState);
handle_aux(leader, _, garbage_collection, Aux, Log, MacState) ->
    {no_reply, force_eval_gc(Log, MacState, Aux), Log};
handle_aux(follower, _, garbage_collection, Aux, Log, MacState) ->
    {no_reply, force_eval_gc(Log, MacState, Aux), Log};
handle_aux(leader, cast, {#return{msg_ids = MsgIds,
                                  consumer_id = ConsumerId}, Corr, Pid},
           Aux0, Log0, #?MODULE{cfg = #cfg{delivery_limit = undefined},
                                consumers = Consumers,
                                ra_indexes = _Indexes}) ->
    case Consumers of
        #{ConsumerId := #consumer{checked_out = Checked}} ->
            {Log, ToReturn} =
                maps:fold(
                  fun (MsgId, ?INDEX_MSG(Idx, ?DISK_MSG(Header)), {L0, Acc}) ->
                          %% it is possible this is not found if the consumer
                          %% crashed and the message got removed
                          %% TODO: handle when log entry is not found
                          case ra_log:fetch(Idx, L0) of
                              {{_, _, {_, _, Cmd, _}}, L} ->
                                  Msg = case Cmd of
                                            #enqueue{msg = M} -> M;
                                            #requeue{msg = M} -> M
                                        end,
                                  IdxMsg = ?INDEX_MSG(Idx, ?TUPLE(Header, Msg)),
                                  {L, [{MsgId, IdxMsg} | Acc]};
                              {undefined, L} ->
                                  {L, Acc}
                          end
                          %% TODO: handle old formats?

                      % (MsgId, IdxMsg, {L0, Acc}) ->
                      %     {L0, [{MsgId, IdxMsg} | Acc]}
                  end, {Log0, []}, maps:with(MsgIds, Checked)),

            Appends = make_requeue(ConsumerId, {notify, Corr, Pid},
                                   lists:sort(ToReturn), []),
            {no_reply, Aux0, Log, Appends};
        _ ->
            {no_reply, Aux0, Log0}
    end;
handle_aux(leader, cast, {#return{} = Ret, Corr, Pid},
           Aux0, Log, #?MODULE{}) ->
    %% for returns with a delivery limit set we can just return as before
    {no_reply, Aux0, Log, [{append, Ret, {notify, Corr, Pid}}]};
handle_aux(leader, cast, eval, Aux0, Log, MacState) ->
    %% this is called after each batch of commands have been applied
    %% set timer for message expire
    %% should really be the last applied index ts but this will have to do
    Ts = erlang:system_time(millisecond),
    Effects = timer_effect(Ts, MacState, []),
    {no_reply, Aux0, Log, Effects};
handle_aux(_RaftState, cast, eval, Aux0, Log, _MacState) ->
    {no_reply, Aux0, Log};
handle_aux(_RaState, cast, Cmd, #?AUX{capacity = Use0} = Aux0,
           Log, _MacState)
  when Cmd == active orelse Cmd == inactive ->
    {no_reply, Aux0#?AUX{capacity = update_use(Use0, Cmd)}, Log};
handle_aux(_RaState, cast, tick, #?AUX{name = Name,
                                       capacity = Use0} = State0,
           Log, MacState) ->
    true = ets:insert(rabbit_fifo_usage,
                      {Name, capacity(Use0)}),
    Aux = eval_gc(Log, MacState, State0),
    {no_reply, Aux, Log};
handle_aux(_RaState, cast, eol, #?AUX{name = Name} = Aux, Log, _) ->
    ets:delete(rabbit_fifo_usage, Name),
    {no_reply, Aux, Log};
handle_aux(_RaState, {call, _From}, oldest_entry_timestamp, Aux,
           Log, #?MODULE{} = State) ->
    Ts = case smallest_raft_index(State) of
             %% if there are no entries, we return current timestamp
             %% so that any previously obtained entries are considered older than this
             undefined ->
                 erlang:system_time(millisecond);
             Idx when is_integer(Idx) ->
                 %% TODO: make more defensive to avoid potential crash
                 {{_, _, {_, Meta, _, _}}, _Log1} = ra_log:fetch(Idx, Log),
                 #{ts := Timestamp} = Meta,
                 Timestamp
         end,
    {reply, {ok, Ts}, Aux, Log};
handle_aux(_RaState, {call, _From}, {peek, Pos}, Aux0,
           Log0, MacState) ->
    case rabbit_fifo:query_peek(Pos, MacState) of
        {ok, ?INDEX_MSG(Idx, ?DISK_MSG(Header))} ->
            %% need to re-hydrate from the log
           {{_, _, {_, _, Cmd, _}}, Log} = ra_log:fetch(Idx, Log0),
           %% TODO: handle requeue?
           #enqueue{msg = Msg} = Cmd,
           {reply, {ok, {Header, Msg}}, Aux0, Log};
        % {ok, ?INDEX_MSG(_Idx, ?MSG(Header, Msg))} ->
        %    {reply, {ok, {Header, Msg}}, Aux0, Log0};
        Err ->
            {reply, Err, Aux0, Log0}
    end;
handle_aux(RaState, _, {dlx, _} = Cmd, Aux0, Log,
           #?MODULE{dlx = DlxState,
                    cfg = #cfg{dead_letter_handler = DLH,
                               resource = QRes}}) ->
    Aux = rabbit_fifo_dlx:handle_aux(RaState, Cmd, Aux0, QRes, DLH, DlxState),
    {no_reply, Aux, Log}.

eval_gc(Log, #?MODULE{cfg = #cfg{resource = QR}} = MacState,
        #?AUX{gc = #aux_gc{last_raft_idx = LastGcIdx} = Gc} = AuxState) ->
    {Idx, _} = ra_log:last_index_term(Log),
    {memory, Mem} = erlang:process_info(self(), memory),
    case messages_total(MacState) of
        0 when Idx > LastGcIdx andalso
               Mem > ?GC_MEM_LIMIT_B ->
            garbage_collect(),
            {memory, MemAfter} = erlang:process_info(self(), memory),
            rabbit_log:debug("~s: full GC sweep complete. "
                            "Process memory changed from ~.2fMB to ~.2fMB.",
                            [rabbit_misc:rs(QR), Mem/?MB, MemAfter/?MB]),
            AuxState#?AUX{gc = Gc#aux_gc{last_raft_idx = Idx}};
        _ ->
            AuxState
    end.

force_eval_gc(Log, #?MODULE{cfg = #cfg{resource = QR}},
              #?AUX{gc = #aux_gc{last_raft_idx = LastGcIdx} = Gc} = AuxState) ->
    {Idx, _} = ra_log:last_index_term(Log),
    {memory, Mem} = erlang:process_info(self(), memory),
    case Idx > LastGcIdx of
        true ->
            garbage_collect(),
            {memory, MemAfter} = erlang:process_info(self(), memory),
            rabbit_log:debug("~s: full GC sweep complete. "
                            "Process memory changed from ~.2fMB to ~.2fMB.",
                             [rabbit_misc:rs(QR), Mem/?MB, MemAfter/?MB]),
            AuxState#?AUX{gc = Gc#aux_gc{last_raft_idx = Idx}};
        false ->
            AuxState
    end.

%%% Queries

query_messages_ready(State) ->
    messages_ready(State).

query_messages_checked_out(#?MODULE{consumers = Consumers}) ->
    maps:fold(fun (_, #consumer{checked_out = C}, S) ->
                      maps:size(C) + S
              end, 0, Consumers).

query_messages_total(State) ->
    messages_total(State).

query_processes(#?MODULE{enqueuers = Enqs, consumers = Cons0}) ->
    Cons = maps:fold(fun({_, P}, V, S) -> S#{P => V} end, #{}, Cons0),
    maps:keys(maps:merge(Enqs, Cons)).


query_ra_indexes(#?MODULE{ra_indexes = RaIndexes}) ->
    RaIndexes.

query_consumer_count(#?MODULE{consumers = Consumers,
                              waiting_consumers = WaitingConsumers}) ->
    Up = maps:filter(fun(_ConsumerId, #consumer{status = Status}) ->
                             Status =/= suspected_down
                     end, Consumers),
    maps:size(Up) + length(WaitingConsumers).

query_consumers(#?MODULE{consumers = Consumers,
                         waiting_consumers = WaitingConsumers,
                         cfg = #cfg{consumer_strategy = ConsumerStrategy}} = State) ->
    ActiveActivityStatusFun =
        case ConsumerStrategy of
            competing ->
                fun(_ConsumerId,
                    #consumer{status = Status}) ->
                        case Status of
                            suspected_down  ->
                                {false, Status};
                            _ ->
                                {true, Status}
                        end
                end;
            single_active ->
                SingleActiveConsumer = query_single_active_consumer(State),
                fun({Tag, Pid} = _Consumer, _) ->
                        case SingleActiveConsumer of
                            {value, {Tag, Pid}} ->
                                {true, single_active};
                            _ ->
                                {false, waiting}
                        end
                end
        end,
    FromConsumers =
        maps:fold(fun (_, #consumer{status = cancelled}, Acc) ->
                          Acc;
                      ({Tag, Pid}, #consumer{meta = Meta} = Consumer, Acc) ->
                          {Active, ActivityStatus} =
                              ActiveActivityStatusFun({Tag, Pid}, Consumer),
                          maps:put({Tag, Pid},
                                   {Pid, Tag,
                                    maps:get(ack, Meta, undefined),
                                    maps:get(prefetch, Meta, undefined),
                                    Active,
                                    ActivityStatus,
                                    maps:get(args, Meta, []),
                                    maps:get(username, Meta, undefined)},
                                   Acc)
                  end, #{}, Consumers),
    FromWaitingConsumers =
        lists:foldl(fun ({_, #consumer{status = cancelled}}, Acc) ->
                                      Acc;
                        ({{Tag, Pid}, #consumer{meta = Meta} = Consumer}, Acc) ->
                            {Active, ActivityStatus} =
                                ActiveActivityStatusFun({Tag, Pid}, Consumer),
                            maps:put({Tag, Pid},
                                     {Pid, Tag,
                                      maps:get(ack, Meta, undefined),
                                      maps:get(prefetch, Meta, undefined),
                                      Active,
                                      ActivityStatus,
                                      maps:get(args, Meta, []),
                                      maps:get(username, Meta, undefined)},
                                     Acc)
                    end, #{}, WaitingConsumers),
    maps:merge(FromConsumers, FromWaitingConsumers).


query_single_active_consumer(#?MODULE{cfg = #cfg{consumer_strategy = single_active},
                                      consumers = Consumers}) ->
    case maps:size(Consumers) of
        0 ->
            {error, no_value};
        1 ->
            {value, lists:nth(1, maps:keys(Consumers))};
        _
          ->
            {error, illegal_size}
    end ;
query_single_active_consumer(_) ->
    disabled.

query_stat(#?MODULE{consumers = Consumers} = State) ->
    {messages_ready(State), maps:size(Consumers)}.

query_in_memory_usage(#?MODULE{msg_bytes_in_memory = Bytes,
                               msgs_ready_in_memory = Length}) ->
    {Length, Bytes}.

query_stat_dlx(#?MODULE{dlx = DlxState}) ->
    rabbit_fifo_dlx:stat(DlxState).

query_peek(Pos, State0) when Pos > 0 ->
    case take_next_msg(State0) of
        empty ->
            {error, no_message_at_pos};
        {IdxMsg, _State}
          when Pos == 1 ->
            {ok, IdxMsg};
        {_Msg, State} ->
            query_peek(Pos-1, State)
    end.

query_notify_decorators_info(#?MODULE{consumers = Consumers} = State) ->
    MaxActivePriority = maps:fold(fun(_, #consumer{credit = C,
                                                   status = up,
                                                   priority = P0}, MaxP)
                                        when C > 0 ->
                                          P = -P0,
                                          case MaxP of
                                              empty -> P;
                                              MaxP when MaxP > P -> MaxP;
                                              _ -> P
                                          end;
                                     (_, _, MaxP) ->
                                          MaxP
                                  end, empty, Consumers),
    IsEmpty = (messages_ready(State) == 0),
    {MaxActivePriority, IsEmpty}.

-spec usage(atom()) -> float().
usage(Name) when is_atom(Name) ->
    case ets:lookup(rabbit_fifo_usage, Name) of
        [] -> 0.0;
        [{_, Use}] -> Use
    end.

%%% Internal

messages_ready(#?MODULE{messages = M,
                        prefix_msgs = {RCnt, _R, PCnt, _P},
                        returns = R}) ->
    lqueue:len(M) + lqueue:len(R) + RCnt + PCnt.

messages_total(#?MODULE{messages = _M,
                        messages_total = Total,
                        ra_indexes = _Indexes,
                        prefix_msgs = _,
                        dlx = DlxState}) ->
    % lqueue:len(M) + rabbit_fifo_index:size(Indexes) + RCnt + PCnt.
    {DlxTotal, _} = rabbit_fifo_dlx:stat(DlxState),
    Total + DlxTotal;
%% release cursors might be old state (e.g. after recent upgrade)
messages_total(State) ->
    try
        rabbit_fifo_v1:query_messages_total(State)
    catch _:_ ->
              rabbit_fifo_v0:query_messages_total(State)
    end.

update_use({inactive, _, _, _} = CUInfo, inactive) ->
    CUInfo;
update_use({active, _, _} = CUInfo, active) ->
    CUInfo;
update_use({active, Since, Avg}, inactive) ->
    Now = erlang:monotonic_time(micro_seconds),
    {inactive, Now, Now - Since, Avg};
update_use({inactive, Since, Active, Avg},   active) ->
    Now = erlang:monotonic_time(micro_seconds),
    {active, Now, use_avg(Active, Now - Since, Avg)}.

capacity({active, Since, Avg}) ->
    use_avg(erlang:monotonic_time(micro_seconds) - Since, 0, Avg);
capacity({inactive, _, 1, 1.0}) ->
    1.0;
capacity({inactive, Since, Active, Avg}) ->
    use_avg(Active, erlang:monotonic_time(micro_seconds) - Since, Avg).

use_avg(0, 0, Avg) ->
    Avg;
use_avg(Active, Inactive, Avg) ->
    Time = Inactive + Active,
    moving_average(Time, ?USE_AVG_HALF_LIFE, Active / Time, Avg).

moving_average(_Time, _, Next, undefined) ->
    Next;
moving_average(Time, HalfLife, Next, Current) ->
    Weight = math:exp(Time * math:log(0.5) / HalfLife),
    Next * (1 - Weight) + Current * Weight.

num_checked_out(#?MODULE{consumers = Cons}) ->
    maps:fold(fun (_, #consumer{checked_out = C}, Acc) ->
                      maps:size(C) + Acc
              end, 0, Cons).

cancel_consumer(Meta, ConsumerId,
                #?MODULE{cfg = #cfg{consumer_strategy = competing}} = State,
                Effects, Reason) ->
    cancel_consumer0(Meta, ConsumerId, State, Effects, Reason);
cancel_consumer(Meta, ConsumerId,
                #?MODULE{cfg = #cfg{consumer_strategy = single_active},
                         waiting_consumers = []} = State,
                Effects, Reason) ->
    %% single active consumer on, no consumers are waiting
    cancel_consumer0(Meta, ConsumerId, State, Effects, Reason);
cancel_consumer(Meta, ConsumerId,
                #?MODULE{consumers = Cons0,
                         cfg = #cfg{consumer_strategy = single_active},
                         waiting_consumers = Waiting0} = State0,
               Effects0, Reason) ->
    %% single active consumer on, consumers are waiting
    case maps:is_key(ConsumerId, Cons0) of
        true ->
            % The active consumer is to be removed
            {State1, Effects1} = cancel_consumer0(Meta, ConsumerId, State0,
                                                  Effects0, Reason),
            activate_next_consumer(State1, Effects1);
        false ->
            % The cancelled consumer is not active or cancelled
            % Just remove it from idle_consumers
            Waiting = lists:keydelete(ConsumerId, 1, Waiting0),
            Effects = cancel_consumer_effects(ConsumerId, State0, Effects0),
            % A waiting consumer isn't supposed to have any checked out messages,
            % so nothing special to do here
            {State0#?MODULE{waiting_consumers = Waiting}, Effects}
    end.

consumer_update_active_effects(#?MODULE{cfg = #cfg{resource = QName}},
                               ConsumerId, #consumer{meta = Meta},
                               Active, ActivityStatus,
                               Effects) ->
    Ack = maps:get(ack, Meta, undefined),
    Prefetch = maps:get(prefetch, Meta, undefined),
    Args = maps:get(args, Meta, []),
    [{mod_call, rabbit_quorum_queue, update_consumer_handler,
      [QName, ConsumerId, false, Ack, Prefetch, Active, ActivityStatus, Args]}
      | Effects].

cancel_consumer0(Meta, ConsumerId,
                 #?MODULE{consumers = C0} = S0, Effects0, Reason) ->
    case C0 of
        #{ConsumerId := Consumer} ->
            {S, Effects2} = maybe_return_all(Meta, ConsumerId, Consumer,
                                             S0, Effects0, Reason),

            %% The effects are emitted before the consumer is actually removed
            %% if the consumer has unacked messages. This is a bit weird but
            %% in line with what classic queues do (from an external point of
            %% view)
            Effects = cancel_consumer_effects(ConsumerId, S, Effects2),

            case maps:size(S#?MODULE.consumers) of
                0 ->
                    {S, [{aux, inactive} | Effects]};
                _ ->
                    {S, Effects}
            end;
        _ ->
            %% already removed: do nothing
            {S0, Effects0}
    end.

activate_next_consumer(#?MODULE{consumers = Cons,
                                waiting_consumers = Waiting0} = State0,
                       Effects0) ->
    case maps:filter(fun (_, #consumer{status = S}) -> S == up end, Cons) of
        Up when map_size(Up) == 0 ->
            %% there are no active consumer in the consumer map
            case lists:filter(fun ({_, #consumer{status = Status}}) ->
                                      Status == up
                              end, Waiting0) of
                [{NextConsumerId, NextConsumer} | _] ->
                    %% there is a potential next active consumer
                    Remaining = lists:keydelete(NextConsumerId, 1, Waiting0),
                    #?MODULE{service_queue = ServiceQueue} = State0,
                    ServiceQueue1 = maybe_queue_consumer(NextConsumerId,
                                                         NextConsumer,
                                                         ServiceQueue),
                    State = State0#?MODULE{consumers = Cons#{NextConsumerId => NextConsumer},
                                           service_queue = ServiceQueue1,
                                           waiting_consumers = Remaining},
                    Effects = consumer_update_active_effects(State, NextConsumerId,
                                                             NextConsumer, true,
                                                             single_active, Effects0),
                    {State, Effects};
                [] ->
                    {State0, [{aux, inactive} | Effects0]}
            end;
        _ ->
                    {State0, Effects0}
    end.



maybe_return_all(#{system_time := Ts} = Meta, ConsumerId, Consumer, S0, Effects0, Reason) ->
    case Reason of
        consumer_cancel ->
            {update_or_remove_sub(Meta, ConsumerId,
                                  Consumer#consumer{lifetime = once,
                                                    credit = 0,
                                                    status = cancelled},
                                  S0), Effects0};
        down ->
            {S1, Effects1} = return_all(Meta, S0, Effects0, ConsumerId, Consumer),
            {S1#?MODULE{consumers = maps:remove(ConsumerId, S1#?MODULE.consumers),
                        last_active = Ts},
             Effects1}
    end.

apply_enqueue(#{index := RaftIdx,
                system_time := Ts} = Meta, From, Seq, RawMsg, State0) ->
    case maybe_enqueue(RaftIdx, Ts, From, Seq, RawMsg, [], State0) of
        {ok, State1, Effects1} ->
            State2 = incr_enqueue_count(incr_total(State1)),
            {State, ok, Effects} = checkout(Meta, State0, State2, Effects1, false),
            {maybe_store_dehydrated_state(RaftIdx, State), ok, Effects};
        {out_of_sequence, State, Effects} ->
            {State, not_enqueued, Effects};
        {duplicate, State, Effects} ->
            {State, ok, Effects}
    end.

decr_total(#?MODULE{messages_total = Tot} = State) ->
    State#?MODULE{messages_total = Tot - 1}.

incr_total(#?MODULE{messages_total = Tot} = State) ->
    State#?MODULE{messages_total = Tot + 1}.

drop_head(#?MODULE{ra_indexes = Indexes0} = State0, Effects) ->
    case take_next_msg(State0) of
        % {?PREFIX_MEM_MSG(Header), State1} ->
        %     State2 = subtract_in_memory_counts(Header,
        %                                        add_bytes_drop(Header, State1)),
        %     {decr_total(State2), Effects};
        % {?DISK_MSG(Header), State1} ->
        %     State2 = add_bytes_drop(Header, State1),
        %     {decr_total(State2), Effects};
        {?INDEX_MSG(Idx, ?DISK_MSG(Header)) = IdxMsg, State1} ->
            Indexes = rabbit_fifo_index:delete(Idx, Indexes0),
            State2 = State1#?MODULE{ra_indexes = Indexes},
            State3 = decr_total(add_bytes_drop(Header, State2)),
            #?MODULE{cfg = #cfg{dead_letter_handler = DLH},
                     dlx = DlxState} = State = State3,
            {_, DlxEffects} = rabbit_fifo_dlx:discard([IdxMsg], maxlen, DLH, DlxState),
            {State, DlxEffects ++ Effects};
        empty ->
            {State0, Effects}
    end.

enqueue(RaftIdx, Ts, RawMsg, #?MODULE{messages = Messages} = State0) ->
    %% the initial header is an integer only - it will get expanded to a map
    %% when the next required key is added
    Header0 = message_size(RawMsg),
    Header = maybe_set_msg_ttl(RawMsg, Ts, Header0, State0),
    %% TODO: enqueue as in memory message if there are no ready messages
    %% and there are consumers with credit available.
    %% I.e. the message will be immedately delivered so no benefit
    %% in reading it back from the log
    {State1, Msg} = {State0, ?INDEX_MSG(RaftIdx, ?DISK_MSG(Header))},
        % case evaluate_memory_limit(Header, State0) of
        %     true ->
        %         % indexed message with header map
        %         {State0,
        %          ?INDEX_MSG(RaftIdx, ?DISK_MSG(Header))};
        %     false ->
        %         {add_in_memory_counts(Header, State0),
        %          ?INDEX_MSG(RaftIdx, ?MSG(Header, RawMsg))}
        % end,
    State = add_bytes_enqueue(Header, State1),
    State#?MODULE{messages = lqueue:in(Msg, Messages)}.

maybe_set_msg_ttl(#basic_message{content = #content{properties = none}},
                  _, Header,
                  #?MODULE{cfg = #cfg{msg_ttl = undefined}}) ->
    Header;
maybe_set_msg_ttl(#basic_message{content = #content{properties = none}},
                  RaCmdTs, Header,
                  #?MODULE{cfg = #cfg{msg_ttl = PerQueueMsgTTL}}) ->
    update_expiry_header(RaCmdTs, PerQueueMsgTTL, Header);
maybe_set_msg_ttl(#basic_message{content = #content{properties = Props}},
                  RaCmdTs, Header,
                  #?MODULE{cfg = #cfg{msg_ttl = PerQueueMsgTTL}}) ->
    %% rabbit_quorum_queue will leave the properties decoded if and only if
    %% per message message TTL is set.
    %% We already check in the channel that expiration must be valid.
    {ok, PerMsgMsgTTL} = rabbit_basic:parse_expiration(Props),
    TTL = min(PerMsgMsgTTL, PerQueueMsgTTL),
    update_expiry_header(RaCmdTs, TTL, Header);
maybe_set_msg_ttl(_, _, Header,
                  #?MODULE{cfg = #cfg{}}) ->
    Header.

update_expiry_header(_, undefined, Header) ->
    Header;
update_expiry_header(RaCmdTs, 0, Header) ->
    %% We do not comply exactly with the "TTL=0 models AMQP immediate flag" semantics
    %% as done for classic queues where the message is discarded if it cannot be
    %% consumed immediately.
    %% Instead, we discard the message if it cannot be consumed within the same millisecond
    %% when it got enqueued. This behaviour should be good enough.
    update_expiry_header(RaCmdTs + 1, Header);
update_expiry_header(RaCmdTs, TTL, Header) ->
    update_expiry_header(RaCmdTs + TTL, Header).

update_expiry_header(ExpiryTs, Header) ->
    update_header(expiry, fun(Ts) -> Ts end, ExpiryTs, Header).

incr_enqueue_count(#?MODULE{enqueue_count = EC,
                            cfg = #cfg{release_cursor_interval = {_Base, C}}
                            } = State0) when EC >= C ->
    %% this will trigger a dehydrated version of the state to be stored
    %% at this raft index for potential future snapshot generation
    %% Q: Why don't we just stash the release cursor here?
    %% A: Because it needs to be the very last thing we do and we
    %% first needs to run the checkout logic.
    State0#?MODULE{enqueue_count = 0};
incr_enqueue_count(#?MODULE{enqueue_count = C} = State) ->
    State#?MODULE{enqueue_count = C + 1}.

maybe_store_dehydrated_state(RaftIdx,
                             #?MODULE{cfg =
                                      #cfg{release_cursor_interval = {Base, _}}
                                      = Cfg,
                                      ra_indexes = _Indexes,
                                      enqueue_count = 0,
                                      release_cursors = Cursors0} = State0) ->
    case messages_total(State0) of
        0 ->
            %% message must have been immediately dropped
            State0;
        Total ->
            Interval = case Base of
                           0 -> 0;
                           _ ->
                               min(max(Total, Base), ?RELEASE_CURSOR_EVERY_MAX)
                       end,
            State = State0#?MODULE{cfg = Cfg#cfg{release_cursor_interval =
                                                 {Base, Interval}}},
            Dehydrated = dehydrate_state(State),
            Cursor = {release_cursor, RaftIdx, Dehydrated},
            Cursors = lqueue:in(Cursor, Cursors0),
            State#?MODULE{release_cursors = Cursors}
    end;
maybe_store_dehydrated_state(_RaftIdx, State) ->
    State.

maybe_enqueue(RaftIdx, Ts, undefined, undefined, RawMsg, Effects, State0) ->
    % direct enqueue without tracking
    State = enqueue(RaftIdx, Ts, RawMsg, State0),
    {ok, State, Effects};
maybe_enqueue(RaftIdx, Ts, From, MsgSeqNo, RawMsg, Effects0,
              #?MODULE{enqueuers = Enqueuers0} = State0) ->

    case maps:get(From, Enqueuers0, undefined) of
        undefined ->
            State1 = State0#?MODULE{enqueuers = Enqueuers0#{From => #enqueuer{}}},
            {Res, State, Effects} = maybe_enqueue(RaftIdx, Ts, From, MsgSeqNo,
                                                 RawMsg, Effects0, State1),
            {Res, State, [{monitor, process, From} | Effects]};
        #enqueuer{next_seqno = MsgSeqNo} = Enq0 ->
            % it is the next expected seqno
            State1 = enqueue(RaftIdx, Ts, RawMsg, State0),
            Enq = Enq0#enqueuer{next_seqno = MsgSeqNo + 1},
            State = State1#?MODULE{enqueuers = Enqueuers0#{From => Enq}},
            {ok, State, Effects0};
        #enqueuer{next_seqno = Next}
          when MsgSeqNo > Next ->
            %% TODO: when can this happen?
            {out_of_sequence, State0, Effects0};
        #enqueuer{next_seqno = Next} when MsgSeqNo =< Next ->
            % duplicate delivery
            {duplicate, State0, Effects0}
    end.

return(#{index := IncomingRaftIdx} = Meta, ConsumerId, Returned,
       Effects0, State0) ->
    {State1, Effects1} = maps:fold(
                           fun(MsgId, Msg, {S0, E0}) ->
                                   return_one(Meta, MsgId, Msg, S0, E0, ConsumerId)
                           end, {State0, Effects0}, Returned),
    State2 =
        case State1#?MODULE.consumers of
            #{ConsumerId := Con0} ->
                Con = Con0#consumer{credit = increase_credit(Con0,
                                                             map_size(Returned))},
                update_or_remove_sub(Meta, ConsumerId, Con, State1);
            _ ->
                State1
        end,
    {State, ok, Effects} = checkout(Meta, State0, State2, Effects1, false),
    update_smallest_raft_index(IncomingRaftIdx, State, Effects).

% used to process messages that are finished
complete(Meta, ConsumerId, DiscardedMsgIds,
         #consumer{checked_out = Checked} = Con0,
         #?MODULE{messages_total = Tot} = State0) ->
    %% credit_mode = simple_prefetch should automatically top-up credit
    %% as messages are simple_prefetch or otherwise returned
    Discarded = maps:with(DiscardedMsgIds, Checked),
    DiscardedMsgs = maps:values(Discarded),
    Len = length(DiscardedMsgs),
    Con = Con0#consumer{checked_out = maps:without(DiscardedMsgIds, Checked),
                        credit = increase_credit(Con0, Len)},
    State1 = update_or_remove_sub(Meta, ConsumerId, Con, State0),
    State2 = lists:foldl(fun(Msg, Acc) ->
                                 add_bytes_settle(
                                   get_msg_header(Msg), Acc)
                         end, State1, DiscardedMsgs),
    State = State2#?MODULE{messages_total = Tot - Len},
    delete_indexes(DiscardedMsgs, State).

delete_indexes(Msgs, #?MODULE{ra_indexes = Indexes0} = State) ->
    %% TODO: optimise by passing a list to rabbit_fifo_index
    Indexes = lists:foldl(fun (?INDEX_MSG(I, _), Acc) when is_integer(I) ->
                                  rabbit_fifo_index:delete(I, Acc);
                              (_, Acc) ->
                                  Acc
                          end, Indexes0, Msgs),
    State#?MODULE{ra_indexes = Indexes}.

increase_credit(#consumer{lifetime = once,
                          credit = Credit}, _) ->
    %% once consumers cannot increment credit
    Credit;
increase_credit(#consumer{lifetime = auto,
                          credit_mode = credited,
                          credit = Credit}, _) ->
    %% credit_mode: credit also doesn't automatically increment credit
    Credit;
increase_credit(#consumer{credit = Current}, Credit) ->
    Current + Credit.

complete_and_checkout(#{index := IncomingRaftIdx} = Meta, MsgIds, ConsumerId,
                      #consumer{} = Con0,
                      Effects0, State0) ->
    State1 = complete(Meta, ConsumerId, MsgIds, Con0, State0),
    {State, ok, Effects} = checkout(Meta, State0, State1, Effects0, false),
    update_smallest_raft_index(IncomingRaftIdx, State, Effects).

cancel_consumer_effects(ConsumerId,
                        #?MODULE{cfg = #cfg{resource = QName}} = State, Effects) ->
    [{mod_call, rabbit_quorum_queue,
      cancel_consumer_handler, [QName, ConsumerId]},
     notify_decorators_effect(State) | Effects].

update_smallest_raft_index(Idx, State, Effects) ->
    update_smallest_raft_index(Idx, ok, State, Effects).

update_smallest_raft_index(IncomingRaftIdx, Reply,
                           #?MODULE{cfg = Cfg,
                                    release_cursors = Cursors0} = State0,
                           Effects) ->
    Total = messages_total(State0),
    %% TODO: optimise
    case smallest_raft_index(State0) of
        undefined when Total == 0 ->
            % there are no messages on queue anymore and no pending enqueues
            % we can forward release_cursor all the way until
            % the last received command, hooray
            %% reset the release cursor interval
            #cfg{release_cursor_interval = {Base, _}} = Cfg,
            RCI = {Base, Base},
            State = State0#?MODULE{cfg = Cfg#cfg{release_cursor_interval = RCI},
                                   release_cursors = lqueue:new(),
                                   enqueue_count = 0},
            {State, Reply, Effects ++ [{release_cursor, IncomingRaftIdx, State}]};
        undefined ->
            {State0, Reply, Effects};
        Smallest when is_integer(Smallest) ->
            case find_next_cursor(Smallest, Cursors0) of
                empty ->
                    {State0, Reply, Effects};
                {Cursor, Cursors} ->
                    %% we can emit a release cursor when we've passed the smallest
                    %% release cursor available.
                    {State0#?MODULE{release_cursors = Cursors}, Reply,
                     Effects ++ [Cursor]}
            end
    end.

find_next_cursor(Idx, Cursors) ->
    find_next_cursor(Idx, Cursors, empty).

find_next_cursor(Smallest, Cursors0, Potential) ->
    case lqueue:out(Cursors0) of
        {{value, {_, Idx, _} = Cursor}, Cursors} when Idx < Smallest ->
            %% we found one but it may not be the largest one
            find_next_cursor(Smallest, Cursors, Cursor);
        _ when Potential == empty ->
            empty;
        _ ->
            {Potential, Cursors0}
    end.

update_msg_header(Key, Fun, Def, ?INDEX_MSG(Idx, ?DISK_MSG(Header))) ->
    ?INDEX_MSG(Idx, ?DISK_MSG(update_header(Key, Fun, Def, Header))).
% update_msg_header(Key, Fun, Def, ?DISK_MSG(Header)) ->
%     ?DISK_MSG(update_header(Key, Fun, Def, Header)).
% update_msg_header(Key, Fun, Def, ?PREFIX_MEM_MSG(Header)) ->
%     ?PREFIX_MEM_MSG(update_header(Key, Fun, Def, Header)).

update_header(Key, UpdateFun, Default, Header)
  when is_integer(Header) ->
    update_header(Key, UpdateFun, Default, #{size => Header});
update_header(Key, UpdateFun, Default, Header) ->
    maps:update_with(Key, UpdateFun, Default, Header).

% get_msg_header(Key, ?INDEX_MSG(_Idx, ?MSG(Header, _Body))) ->
%     get_header(Key, Header);
% get_msg_header(Key, ?DISK_MSG(Header)) ->
%     get_header(Key, Header);
% get_msg_header(Key, ?PREFIX_MEM_MSG(Header)) ->
%     get_header(Key, Header).

get_msg_header(?INDEX_MSG(_Idx, ?DISK_MSG(Header))) ->
    Header.
% get_msg_header(?DISK_MSG(Header)) ->
%     Header;
% get_msg_header(?PREFIX_MEM_MSG(Header)) ->
%     Header.

get_header(size, Header)
  when is_integer(Header) ->
    Header;
get_header(_Key, Header) when is_integer(Header) ->
    undefined;
get_header(Key, Header) when is_map(Header) ->
    maps:get(Key, Header, undefined).

return_one(Meta, MsgId, Msg0,
           #?MODULE{returns = Returns,
                    consumers = Consumers,
                    dlx = DlxState0,
                    cfg = #cfg{delivery_limit = DeliveryLimit,
                               dead_letter_handler = DLH}} = State0,
           Effects0, ConsumerId) ->
    #consumer{checked_out = Checked} = Con0 = maps:get(ConsumerId, Consumers),
    Msg = update_msg_header(delivery_count, fun incr/1, 1, Msg0),
    Header = get_msg_header(Msg),
    case get_header(delivery_count, Header) of
        DeliveryCount when DeliveryCount > DeliveryLimit ->
            %% TODO: don't do for prefix msgs
            {DlxState, DlxEffects} = rabbit_fifo_dlx:discard([Msg], delivery_limit, DLH, DlxState0),
            State1 = State0#?MODULE{dlx = DlxState},
            State = complete(Meta, ConsumerId, [MsgId], Con0, State1),
            {State, DlxEffects ++ Effects0};
        _ ->
            Con = Con0#consumer{checked_out = maps:remove(MsgId, Checked)},

            % {RtnMsg, State1} = case is_disk_msg(Msg) of
            %                        true ->
            %                            {Msg, State0};
            %                        false ->
            %                            case evaluate_memory_limit(Header, State0) of
            %                                true ->
            %                                    {to_disk_msg(Msg), State0};
            %                                false ->
            %                                    {Msg, add_in_memory_counts(Header, State0)}
            %                            end
            %                    end,
            {add_bytes_return(
               Header,
               State0#?MODULE{consumers = Consumers#{ConsumerId => Con},
                              returns = lqueue:in(Msg, Returns)}),
             Effects0}
    end.

% is_disk_msg(?INDEX_MSG(RaftIdx, ?DISK_MSG(_))) when is_integer(RaftIdx) ->
%     true;
% is_disk_msg(?DISK_MSG(_)) ->
%     true;
% is_disk_msg(_) ->
%     false.

% to_disk_msg(?INDEX_MSG(RaftIdx, ?DISK_MSG(_)) = Msg) when is_integer(RaftIdx) ->
%     Msg;
% to_disk_msg(?INDEX_MSG(RaftIdx, ?MSG(Header, _))) when is_integer(RaftIdx) ->
%     ?INDEX_MSG(RaftIdx, ?DISK_MSG(Header));
% to_disk_msg(?PREFIX_MEM_MSG(Header)) ->
%     ?DISK_MSG(Header).

return_all(Meta, #?MODULE{consumers = Cons} = State0, Effects0, ConsumerId,
           #consumer{checked_out = Checked} = Con) ->
    State = State0#?MODULE{consumers = Cons#{ConsumerId => Con}},
    lists:foldl(fun ({MsgId, Msg}, {S, E}) ->
                        return_one(Meta, MsgId, Msg, S, E, ConsumerId)
                end, {State, Effects0}, lists:sort(maps:to_list(Checked))).

%% checkout new messages to consumers
checkout(Meta, OldState, State, Effects) ->
    checkout(Meta, OldState, State, Effects, true).

checkout(#{index := Index} = Meta,
         #?MODULE{cfg = #cfg{resource = QName}} = OldState,
         State0, Effects0, HandleConsumerChanges) ->
    {#?MODULE{cfg = #cfg{dead_letter_handler = DLH},
              dlx = DlxState0} = State1, _Result, Effects1} =
    checkout0(Meta, checkout_one(Meta, State0, Effects0), #{}),
    {DlxState, DlxDeliveryEffects} = rabbit_fifo_dlx:checkout(DLH, DlxState0),
    State2 = State1#?MODULE{dlx = DlxState},
    Effects2 = DlxDeliveryEffects ++ Effects1,
    case evaluate_limit(Index, false, OldState, State2, Effects2) of
        {State, true, Effects} ->
            case maybe_notify_decorators(State, HandleConsumerChanges) of
                {true, {MaxActivePriority, IsEmpty}} ->
                    NotifyEffect = notify_decorators_effect(QName, MaxActivePriority,
                                                            IsEmpty),
                    update_smallest_raft_index(Index, State, [NotifyEffect | Effects]);
                false ->
                    update_smallest_raft_index(Index, State, Effects)
            end;
        {State, false, Effects} ->
            case maybe_notify_decorators(State, HandleConsumerChanges) of
                {true, {MaxActivePriority, IsEmpty}} ->
                    NotifyEffect = notify_decorators_effect(QName, MaxActivePriority,
                                                            IsEmpty),
                    {State, ok, [NotifyEffect | Effects]};
                false ->
                    {State, ok, Effects}
            end
    end.

checkout0(Meta, {success, ConsumerId, MsgId,
                 ?INDEX_MSG(RaftIdx, ?DISK_MSG(Header)), State, Effects},
          SendAcc0) when is_integer(RaftIdx) ->
    DelMsg = {RaftIdx, {MsgId, Header}},
    SendAcc = maps:update_with(ConsumerId,
                               fun ({InMem, LogMsgs}) ->
                                       {InMem, [DelMsg | LogMsgs]}
                               end, {[], [DelMsg]}, SendAcc0),
    checkout0(Meta, checkout_one(Meta, State, Effects), SendAcc);
% checkout0(Meta, {success, ConsumerId, MsgId,
%                  ?INDEX_MSG(Idx, ?MSG(Header, Msg)), State, Effects},
%           SendAcc0) when is_integer(Idx) ->
%     DelMsg = {MsgId, {Header, Msg}},
%     SendAcc = maps:update_with(ConsumerId,
%                                fun ({InMem, LogMsgs}) ->
%                                        {[DelMsg | InMem], LogMsgs}
%                                end, {[DelMsg], []}, SendAcc0),
%     checkout0(Meta, checkout_one(Meta, State, Effects), SendAcc);
% checkout0(Meta, {success, _ConsumerId, _MsgId, ?TUPLE(_, _), State, Effects},
%           SendAcc) ->
%     %% Do not append delivery effect for prefix messages.
%     %% Prefix messages do not exist anymore, but they still go through the
%     %% normal checkout flow to derive correct consumer states
%     %% after recovery and will still be settled or discarded later on.
%     checkout0(Meta, checkout_one(Meta, State, Effects), SendAcc);
checkout0(_Meta, {Activity, State0, Effects0}, SendAcc) ->
    Effects1 = case Activity of
                   nochange ->
                       append_delivery_effects(Effects0, SendAcc);
                   inactive ->
                       [{aux, inactive}
                        | append_delivery_effects(Effects0, SendAcc)]
               end,
    {State0, ok, lists:reverse(Effects1)}.

evaluate_limit(_Index, Result, _BeforeState,
               #?MODULE{cfg = #cfg{max_length = undefined,
                                   max_bytes = undefined}} = State,
               Effects) ->
    {State, Result, Effects};
evaluate_limit(Index, Result, BeforeState,
               #?MODULE{cfg = #cfg{overflow_strategy = Strategy},
                        enqueuers = Enqs0} = State0,
               Effects0) ->
    case is_over_limit(State0) of
        true when Strategy == drop_head ->
            {State, Effects} = drop_head(State0, Effects0),
            evaluate_limit(Index, true, BeforeState, State, Effects);
        true when Strategy == reject_publish ->
            %% generate send_msg effect for each enqueuer to let them know
            %% they need to block
            {Enqs, Effects} =
                maps:fold(
                  fun (P, #enqueuer{blocked = undefined} = E0, {Enqs, Acc})  ->
                          E = E0#enqueuer{blocked = Index},
                          {Enqs#{P => E},
                           [{send_msg, P, {queue_status, reject_publish},
                             [ra_event]} | Acc]};
                      (_P, _E, Acc) ->
                          Acc
                  end, {Enqs0, Effects0}, Enqs0),
            {State0#?MODULE{enqueuers = Enqs}, Result, Effects};
        false when Strategy == reject_publish ->
            %% TODO: optimise as this case gets called for every command
            %% pretty much
            Before = is_below_soft_limit(BeforeState),
            case {Before, is_below_soft_limit(State0)} of
                {false, true} ->
                    %% we have moved below the lower limit which
                    {Enqs, Effects} =
                    maps:fold(
                      fun (P, #enqueuer{} = E0, {Enqs, Acc})  ->
                              E = E0#enqueuer{blocked = undefined},
                              {Enqs#{P => E},
                               [{send_msg, P, {queue_status, go}, [ra_event]}
                                | Acc]};
                          (_P, _E, Acc) ->
                              Acc
                      end, {Enqs0, Effects0}, Enqs0),
                    {State0#?MODULE{enqueuers = Enqs}, Result, Effects};
                _ ->
                    {State0, Result, Effects0}
            end;
        false ->
            {State0, Result, Effects0}
    end.

% evaluate_memory_limit(_Header,
%                       #?MODULE{cfg = #cfg{max_in_memory_length = undefined,
%                                           max_in_memory_bytes = undefined}}) ->
%     false;
% evaluate_memory_limit(#{size := Size}, State) ->
%     evaluate_memory_limit(Size, State);
% evaluate_memory_limit(Header,
%                       #?MODULE{cfg = #cfg{max_in_memory_length = MaxLength,
%                                           max_in_memory_bytes = MaxBytes},
%                                msg_bytes_in_memory = Bytes,
%                                msgs_ready_in_memory = Length}) ->
%     Size = get_header(size, Header),
%     (Length >= MaxLength) orelse ((Bytes + Size) > MaxBytes).

append_delivery_effects(Effects0, AccMap) when map_size(AccMap) == 0 ->
    %% does this ever happen?
    Effects0;
append_delivery_effects(Effects0, AccMap) ->
    [{aux, active} |
     maps:fold(fun (C, {InMemMsgs, DiskMsgs}, Ef) ->
                       [delivery_effect(C, lists:reverse(DiskMsgs), InMemMsgs) | Ef]
               end, Effects0, AccMap)
    ].

%% next message is determined as follows:
%% First we check if there are are prefex returns
%% Then we check if there are current returns
%% then we check prefix msgs
%% then we check current messages
%%
%% When we return it is always done to the current return queue
%% for both prefix messages and current messages
% take_next_msg(#?MODULE{prefix_msgs = {NumR, [Msg | Rem],
%                                       NumP, P}} = State) ->
%     %% there are prefix returns, these should be served first
%     {Msg, State#?MODULE{prefix_msgs = {NumR-1, Rem, NumP, P}}};
take_next_msg(#?MODULE{returns = Returns0,
                       messages = Messages0,
                       ra_indexes = Indexes0
                       % prefix_msgs = {NumR, R, NumP, P}
                      } = State) ->
    case lqueue:out(Returns0) of
        {{value, NextMsg}, Returns} ->
            {NextMsg, State#?MODULE{returns = Returns}};
        % {empty, _} when P == [] ->
        {empty, _} ->
            case lqueue:out(Messages0) of
                {empty, _} ->
                    empty;
                {{value, ?INDEX_MSG(RaftIdx, _) = IndexMsg}, Messages} ->
                    %% add index here
                    Indexes = rabbit_fifo_index:append(RaftIdx, Indexes0),
                    {IndexMsg, State#?MODULE{messages = Messages,
                                             ra_indexes = Indexes}}
            end
        % {empty, _} ->
        %     case P of
        %         [?PREFIX_MEM_MSG(_Header) = Msg | Rem] ->
        %             {Msg, State#?MODULE{prefix_msgs = {NumR, R, NumP-1, Rem}}};
        %         [?DISK_MSG(_Header) = Msg | Rem] ->
        %             {Msg, State#?MODULE{prefix_msgs = {NumR, R, NumP-1, Rem}}}
        %     end
    end.

% peek_next_msg(#?MODULE{prefix_msgs = {_NumR, [Msg | _],
%                                       _NumP, _P}}) ->
%     %% there are prefix returns, these should be served first
%     {value, Msg};
peek_next_msg(#?MODULE{returns = Returns0,
                       messages = Messages0
                       % prefix_msgs = {_NumR, _R, _NumP, P}
                      }) ->
    case lqueue:peek(Returns0) of
        {value, _} = Msg ->
            Msg;
        empty ->
            lqueue:peek(Messages0)
        % empty ->
        %     case P of
        %         [?PREFIX_MEM_MSG(_Header) = Msg | _] ->
        %             {value, Msg};
        %         [?DISK_MSG(_Header) = Msg | _] ->
        %             {value, Msg}
        %     end
    end.

% delivery_effect({CTag, CPid}, [], InMemMsgs) ->
%     {send_msg, CPid, {delivery, CTag, lists:reverse(InMemMsgs)},
%      [local, ra_event]};
delivery_effect({CTag, CPid}, IdxMsgs, []) -> %% InMemMsgs
    {RaftIdxs, Data} = lists:unzip(IdxMsgs),
    {log, RaftIdxs,
     fun(Log) ->
             Msgs = lists:zipwith(
                      fun (#enqueue{msg = Msg},
                           {MsgId, Header}) ->
                              {MsgId, {Header, Msg}};
                          (#requeue{msg = Msg},
                           {MsgId, Header}) ->
                              {MsgId, {Header, Msg}}
                      end, Log, Data),
             [{send_msg, CPid, {delivery, CTag, Msgs}, [local, ra_event]}]
     end,
     {local, node(CPid)}}.

reply_log_effect(RaftIdx, MsgId, Header, Ready, From) ->
    {log, [RaftIdx],
     fun
         ([#enqueue{msg = Msg}]) ->
             [{reply, From, {wrap_reply,
                             {dequeue, {MsgId, {Header, Msg}}, Ready}}}];
         ([#requeue{msg = Msg}]) ->
             [{reply, From, {wrap_reply,
                             {dequeue, {MsgId, {Header, Msg}}, Ready}}}]
     end}.

checkout_one(#{system_time := Ts} = Meta, InitState0, Effects0) ->
    %% Before checking out any messsage to any consumer,
    %% first remove all expired messages from the head of the queue.
    {#?MODULE{service_queue = SQ0,
             messages = Messages0,
             consumers = Cons0} = InitState, Effects1} =
        expire_msgs(Ts, InitState0, Effects0),

    case priority_queue:out(SQ0) of
        {{value, ConsumerId}, SQ1}
          when is_map_key(ConsumerId, Cons0) ->
            case take_next_msg(InitState) of
                {ConsumerMsg, State0} ->
                    %% there are consumers waiting to be serviced
                    %% process consumer checkout
                    case maps:get(ConsumerId, Cons0) of
                        #consumer{credit = 0} ->
                            %% no credit but was still on queue
                            %% can happen when draining
                            %% recurse without consumer on queue
                            %% NB: these retry cases introduce the "queue list reversal"
                            %% inefficiency but this is a rare thing to happen
                            %% so should not need optimising
                            checkout_one(Meta, InitState#?MODULE{service_queue = SQ1}, Effects1);
                        #consumer{status = cancelled} ->
                            checkout_one(Meta, InitState#?MODULE{service_queue = SQ1}, Effects1);
                        #consumer{status = suspected_down} ->
                            checkout_one(Meta, InitState#?MODULE{service_queue = SQ1}, Effects1);
                        #consumer{checked_out = Checked0,
                                  next_msg_id = Next,
                                  credit = Credit,
                                  delivery_count = DelCnt} = Con0 ->
                            Checked = maps:put(Next, ConsumerMsg, Checked0),
                            Con = Con0#consumer{checked_out = Checked,
                                                next_msg_id = Next + 1,
                                                credit = Credit - 1,
                                                delivery_count = DelCnt + 1},
                            State1 = update_or_remove_sub(
                                       Meta, ConsumerId, Con,
                                       State0#?MODULE{service_queue = SQ1}),
                            Header = get_msg_header(ConsumerMsg),
                            State = add_bytes_checkout(Header, State1),
                            % State = case is_disk_msg(ConsumerMsg) of
                            %             true ->
                            %                 add_bytes_checkout(Header, State1);
                            %             false ->
                            %                 %% TODO do not subtract from memory here since
                            %                 %% messages are still in memory when checked out
                            %                 subtract_in_memory_counts(
                            %                   Header, add_bytes_checkout(Header, State1))
                            %         end,
                            {success, ConsumerId, Next, ConsumerMsg, State, Effects1}
                    end;
                empty ->
                    {nochange, InitState, Effects1}
            end;
        {{value, _ConsumerId}, SQ1} ->
            %% consumer did not exist but was queued, recurse
            checkout_one(Meta, InitState#?MODULE{service_queue = SQ1}, Effects1);
        {empty, _} ->
            % Effects = timer_effect(Ts, InitState, Effects1),
            case lqueue:len(Messages0) of
                0 ->
                    {nochange, InitState, Effects1};
                _ ->
                    {inactive, InitState, Effects1}
            end
    end.

%% dequeue all expired messages
expire_msgs(RaCmdTs, State, Effects) ->
    %% In the normal case, there are no expired messages.
    %% Therefore, first queue:peek/1 to check whether we need to queue:out/1
    %% because the latter can be much slower than the former.
    case peek_next_msg(State) of
        % {value, ?DISK_MSG(#{expiry := Expiry} = Header)}
        %   when RaCmdTs >= Expiry ->
        %     expire(RaCmdTs, Header, State, Effects);
        {value, ?INDEX_MSG(_Idx, ?DISK_MSG(#{expiry := Expiry} = Header))}
          when RaCmdTs >= Expiry ->
            expire(RaCmdTs, Header, State, Effects);
        % {value, ?PREFIX_MEM_MSG(#{expiry := Expiry} = Header)}
        %   when RaCmdTs >= Expiry ->
        %     expire(RaCmdTs, Header, State, Effects);
        _ ->
            {State, Effects}
    end.

expire(RaCmdTs, Header, State0, Effects) ->
    {Msg, State1} = take_next_msg(State0),
    #?MODULE{cfg = #cfg{dead_letter_handler = DLH},
             dlx = DlxState0,
             ra_indexes = Indexes0} = State2 = add_bytes_drop(Header, State1),
    {DlxState, DlxEffects} = rabbit_fifo_dlx:discard([Msg], expired, DLH, DlxState0),
    State3 = State2#?MODULE{dlx = DlxState},
    State5 = case Msg of
                 ?INDEX_MSG(Idx, ?DISK_MSG(_Header))
                   when is_integer(Idx) ->
                     Indexes = rabbit_fifo_index:delete(Idx, Indexes0),
                     State3#?MODULE{ra_indexes = Indexes};
                 % ?INDEX_MSG(Idx, ?MSG(_Header, _))
                 %   when is_integer(Idx) ->
                 %     Indexes = rabbit_fifo_index:delete(Idx, Indexes0),
                 %     State4 = State3#?MODULE{ra_indexes = Indexes},
                 %     subtract_in_memory_counts(Header, State4);
                 % ?PREFIX_MEM_MSG(_) ->
                 %     subtract_in_memory_counts(Header, State3);
                 ?DISK_MSG(_) ->
                     State3
             end,
    State = decr_total(State5),
    expire_msgs(RaCmdTs, State, DlxEffects ++ Effects).

timer_effect(RaCmdTs, State, Effects) ->
    T = case peek_next_msg(State) of
            {value, ?INDEX_MSG(_, ?DISK_MSG(#{expiry := Expiry}))}
              when is_number(Expiry) ->
                %% Next message contains 'expiry' header.
                %% (Re)set timer so that mesage will be dropped or dead-lettered on time.
                max(0, Expiry - RaCmdTs);
            _ ->
                %% Next message does not contain 'expiry' header.
                %% Therefore, do not set timer or cancel timer if it was set.
                infinity
        end,
    [{timer, expire_msgs, T} | Effects].

update_or_remove_sub(_Meta, ConsumerId, #consumer{lifetime = auto,
                                                  credit = 0} = Con,
                     #?MODULE{consumers = Cons} = State) ->
    State#?MODULE{consumers = maps:put(ConsumerId, Con, Cons)};
update_or_remove_sub(_Meta, ConsumerId, #consumer{lifetime = auto} = Con,
                     #?MODULE{consumers = Cons,
                              service_queue = ServiceQueue} = State) ->
    State#?MODULE{consumers = maps:put(ConsumerId, Con, Cons),
                  service_queue = uniq_queue_in(ConsumerId, Con, ServiceQueue)};
update_or_remove_sub(#{system_time := Ts},
                     ConsumerId, #consumer{lifetime = once,
                                           checked_out = Checked,
                                           credit = 0} = Con,
                     #?MODULE{consumers = Cons} = State) ->
    case maps:size(Checked) of
        0 ->
            % we're done with this consumer
            State#?MODULE{consumers = maps:remove(ConsumerId, Cons),
                          last_active = Ts};
        _ ->
            % there are unsettled items so need to keep around
            State#?MODULE{consumers = maps:put(ConsumerId, Con, Cons)}
    end;
update_or_remove_sub(_Meta, ConsumerId, #consumer{lifetime = once} = Con,
                     #?MODULE{consumers = Cons,
                              service_queue = ServiceQueue} = State) ->
    State#?MODULE{consumers = maps:put(ConsumerId, Con, Cons),
                  service_queue = uniq_queue_in(ConsumerId, Con, ServiceQueue)}.

uniq_queue_in(Key, #consumer{priority = P}, Queue) ->
    % TODO: queue:member could surely be quite expensive, however the practical
    % number of unique consumers may not be large enough for it to matter
    case priority_queue:member(Key, Queue) of
        true ->
            Queue;
        false ->
            priority_queue:in(Key, P, Queue)
    end.

update_consumer(ConsumerId, Meta, Spec, Priority,
                #?MODULE{cfg = #cfg{consumer_strategy = competing}} = State0) ->
    %% general case, single active consumer off
    update_consumer0(ConsumerId, Meta, Spec, Priority, State0);
update_consumer(ConsumerId, Meta, Spec, Priority,
                #?MODULE{consumers = Cons0,
                         cfg = #cfg{consumer_strategy = single_active}} = State0)
  when map_size(Cons0) == 0 orelse
       is_map_key(ConsumerId, Cons0) ->
    %% single active consumer on, no one is consuming yet or
    %% the currently active consumer is the same
    update_consumer0(ConsumerId, Meta, Spec, Priority, State0);
update_consumer(ConsumerId, Meta, {Life, Credit, Mode}, Priority,
                #?MODULE{cfg = #cfg{consumer_strategy = single_active},
                         waiting_consumers = WaitingConsumers0} = State0) ->
    %% single active consumer on and one active consumer already
    %% adding the new consumer to the waiting list
    Consumer = #consumer{lifetime = Life, meta = Meta,
                         priority = Priority,
                         credit = Credit, credit_mode = Mode},
    WaitingConsumers1 = WaitingConsumers0 ++ [{ConsumerId, Consumer}],
    State0#?MODULE{waiting_consumers = WaitingConsumers1}.

update_consumer0(ConsumerId, Meta, {Life, Credit, Mode}, Priority,
                 #?MODULE{consumers = Cons0,
                          service_queue = ServiceQueue0} = State0) ->
    %% TODO: this logic may not be correct for updating a pre-existing consumer
    Init = #consumer{lifetime = Life, meta = Meta,
                     priority = Priority,
                     credit = Credit, credit_mode = Mode},
    Cons = maps:update_with(ConsumerId,
                            fun(S) ->
                                %% remove any in-flight messages from
                                %% the credit update
                                N = maps:size(S#consumer.checked_out),
                                C = max(0, Credit - N),
                                S#consumer{lifetime = Life, credit = C}
                            end, Init, Cons0),
    ServiceQueue = maybe_queue_consumer(ConsumerId, maps:get(ConsumerId, Cons),
                                        ServiceQueue0),
    State0#?MODULE{consumers = Cons, service_queue = ServiceQueue}.

maybe_queue_consumer(ConsumerId, #consumer{credit = Credit} = Con,
                     ServiceQueue0) ->
    case Credit > 0 of
        true ->
            % consumerect needs service - check if already on service queue
            uniq_queue_in(ConsumerId, Con, ServiceQueue0);
        false ->
            ServiceQueue0
    end.

%% creates a dehydrated version of the current state to be cached and
%% potentially used to for a snaphot at a later point
dehydrate_state(#?MODULE{cfg = #cfg{},
                         dlx = DlxState} = State) ->
    % no messages are kept in memory, no need to
    % overly mutate the current state apart from removing indexes and cursors
    State#?MODULE{
             ra_indexes = rabbit_fifo_index:empty(),
             release_cursors = lqueue:new(),
             dlx = rabbit_fifo_dlx:dehydrate(DlxState)}.
% dehydrate_state(#?MODULE{messages = Messages,
%                          consumers = Consumers,
%                          returns = Returns,
%                          prefix_msgs = {PRCnt, PrefRet0, PPCnt, PrefMsg0},
%                          waiting_consumers = Waiting0,
%                          dlx = DlxState} = State) ->
%     RCnt = lqueue:len(Returns),
%     %% TODO: optimise this function as far as possible
%     PrefRet1 = lists:foldr(fun (M, Acc) ->
%                                    [dehydrate_message(M) | Acc]
%                            end, [], lqueue:to_list(Returns)),
%     PrefRet = PrefRet0 ++ PrefRet1,
%     PrefMsgsSuff = dehydrate_messages(Messages),
%     %% prefix messages are not populated in normal operation only after
%     %% recovering from a snapshot
%     PrefMsgs = PrefMsg0 ++ PrefMsgsSuff,
%     Waiting = [{Cid, dehydrate_consumer(C)} || {Cid, C} <- Waiting0],
%     State#?MODULE{messages = lqueue:new(),
%                   ra_indexes = rabbit_fifo_index:empty(),
%                   release_cursors = lqueue:new(),
%                   consumers = maps:map(fun (_, C) ->
%                                                dehydrate_consumer(C)
%                                        end, Consumers),
%                   returns = lqueue:new(),
%                   prefix_msgs = {PRCnt + RCnt, PrefRet,
%                                  PPCnt + lqueue:len(Messages), PrefMsgs},
%                   waiting_consumers = Waiting,
%                   dlx = rabbit_fifo_dlx:dehydrate(DlxState)}.

% dehydrate_messages(Msgs0)  ->
%     {OutRes, Msgs} = lqueue:out(Msgs0),
%     case OutRes of
%         {value, Msg} ->
%             [dehydrate_message(Msg) | dehydrate_messages(Msgs)];
%         empty ->
%             []
%     end.

% dehydrate_consumer(#consumer{checked_out = Checked0} = Con) ->
%     Checked = maps:map(fun (_, M) ->
%                                dehydrate_message(M)
%                        end, Checked0),
%     Con#consumer{checked_out = Checked}.

% dehydrate_message(?PREFIX_MEM_MSG(_) = M) ->
%     M;
% dehydrate_message(?DISK_MSG(_) = M) ->
%     M;
dehydrate_message(?INDEX_MSG(_Idx, ?DISK_MSG(_Header) = Msg)) ->
    %% Use disk msgs directly as prefix messages.
    %% This avoids memory allocation since we do not convert.
    Msg.
% dehydrate_message(?INDEX_MSG(Idx, ?MSG(Header, _))) when is_integer(Idx) ->
%     ?PREFIX_MEM_MSG(Header).

%% make the state suitable for equality comparison
normalize(#?MODULE{ra_indexes = _Indexes,
                   returns = Returns,
                   messages = Messages,
                   release_cursors = Cursors,
                   dlx = DlxState} = State) ->
    State#?MODULE{returns = lqueue:from_list(lqueue:to_list(Returns)),
                  messages = lqueue:from_list(lqueue:to_list(Messages)),
                  release_cursors = lqueue:from_list(lqueue:to_list(Cursors)),
                  dlx = rabbit_fifo_dlx:normalize(DlxState)}.

is_over_limit(#?MODULE{cfg = #cfg{max_length = undefined,
                                  max_bytes = undefined}}) ->
    false;
is_over_limit(#?MODULE{cfg = #cfg{max_length = MaxLength,
                                  max_bytes = MaxBytes},
                       msg_bytes_enqueue = BytesEnq} = State) ->
    messages_ready(State) > MaxLength orelse (BytesEnq > MaxBytes).

is_below_soft_limit(#?MODULE{cfg = #cfg{max_length = undefined,
                                        max_bytes = undefined}}) ->
    false;
is_below_soft_limit(#?MODULE{cfg = #cfg{max_length = MaxLength,
                                        max_bytes = MaxBytes},
                            msg_bytes_enqueue = BytesEnq} = State) ->
    is_below(MaxLength, messages_ready(State)) andalso
    is_below(MaxBytes, BytesEnq).

is_below(undefined, _Num) ->
    true;
is_below(Val, Num) when is_integer(Val) andalso is_integer(Num) ->
    Num =< trunc(Val * ?LOW_LIMIT).

-spec make_enqueue(option(pid()), option(msg_seqno()), raw_msg()) -> protocol().
make_enqueue(Pid, Seq, Msg) ->
    #enqueue{pid = Pid, seq = Seq, msg = Msg}.

-spec make_register_enqueuer(pid()) -> protocol().
make_register_enqueuer(Pid) ->
    #register_enqueuer{pid = Pid}.

-spec make_checkout(consumer_id(),
                    checkout_spec(), consumer_meta()) -> protocol().
make_checkout(ConsumerId, Spec, Meta) ->
    #checkout{consumer_id = ConsumerId,
              spec = Spec, meta = Meta}.

-spec make_settle(consumer_id(), [msg_id()]) -> protocol().
make_settle(ConsumerId, MsgIds) when is_list(MsgIds) ->
    #settle{consumer_id = ConsumerId, msg_ids = MsgIds}.

-spec make_return(consumer_id(), [msg_id()]) -> protocol().
make_return(ConsumerId, MsgIds) ->
    #return{consumer_id = ConsumerId, msg_ids = MsgIds}.

-spec make_discard(consumer_id(), [msg_id()]) -> protocol().
make_discard(ConsumerId, MsgIds) ->
    #discard{consumer_id = ConsumerId, msg_ids = MsgIds}.

-spec make_credit(consumer_id(), non_neg_integer(), non_neg_integer(),
                  boolean()) -> protocol().
make_credit(ConsumerId, Credit, DeliveryCount, Drain) ->
    #credit{consumer_id = ConsumerId,
            credit = Credit,
            delivery_count = DeliveryCount,
            drain = Drain}.

-spec make_purge() -> protocol().
make_purge() -> #purge{}.

-spec make_garbage_collection() -> protocol().
make_garbage_collection() -> #garbage_collection{}.

-spec make_purge_nodes([node()]) -> protocol().
make_purge_nodes(Nodes) ->
    #purge_nodes{nodes = Nodes}.

-spec make_update_config(config()) -> protocol().
make_update_config(Config) ->
    #update_config{config = Config}.

add_bytes_enqueue(Header,
                  #?MODULE{msg_bytes_enqueue = Enqueue} = State) ->
    Size = get_header(size, Header),
    State#?MODULE{msg_bytes_enqueue = Enqueue + Size}.

add_bytes_drop(Header,
               #?MODULE{msg_bytes_enqueue = Enqueue} = State) ->
    Size = get_header(size, Header),
    State#?MODULE{msg_bytes_enqueue = Enqueue - Size}.


add_bytes_checkout(Header,
                   #?MODULE{msg_bytes_checkout = Checkout,
                            msg_bytes_enqueue = Enqueue } = State) ->
    Size = get_header(size, Header),
    State#?MODULE{msg_bytes_checkout = Checkout + Size,
                  msg_bytes_enqueue = Enqueue - Size}.

add_bytes_settle(Header,
                 #?MODULE{msg_bytes_checkout = Checkout} = State) ->
    Size = get_header(size, Header),
    State#?MODULE{msg_bytes_checkout = Checkout - Size}.

add_bytes_return(Header,
                 #?MODULE{msg_bytes_checkout = Checkout,
                          msg_bytes_enqueue = Enqueue} = State) ->
    Size = get_header(size, Header),
    State#?MODULE{msg_bytes_checkout = Checkout - Size,
                  msg_bytes_enqueue = Enqueue + Size}.

% add_in_memory_counts(Header,
%                      #?MODULE{msg_bytes_in_memory = InMemoryBytes,
%                               msgs_ready_in_memory = InMemoryCount} = State) ->
%     Size = get_header(size, Header),
%     State#?MODULE{msg_bytes_in_memory = InMemoryBytes + Size,
%                   msgs_ready_in_memory = InMemoryCount + 1}.

% subtract_in_memory_counts(Header,
%                           #?MODULE{msg_bytes_in_memory = InMemoryBytes,
%                                    msgs_ready_in_memory = InMemoryCount} = State) ->
%     Size = get_header(size, Header),
%     State#?MODULE{msg_bytes_in_memory = InMemoryBytes - Size,
%                   msgs_ready_in_memory = InMemoryCount - 1}.

message_size(#basic_message{content = Content}) ->
    #content{payload_fragments_rev = PFR} = Content,
    iolist_size(PFR);
% message_size(?PREFIX_MEM_MSG(Header)) ->
%     get_header(size, Header);
% message_size(Header) ?IS_HEADER(Header) ->
%     get_header(size, Header);
message_size(B) when is_binary(B) ->
    byte_size(B);
message_size(Msg) ->
    %% probably only hit this for testing so ok to use erts_debug
    erts_debug:size(Msg).


all_nodes(#?MODULE{consumers = Cons0,
                   enqueuers = Enqs0,
                   waiting_consumers = WaitingConsumers0}) ->
    Nodes0 = maps:fold(fun({_, P}, _, Acc) ->
                               Acc#{node(P) => ok}
                       end, #{}, Cons0),
    Nodes1 = maps:fold(fun(P, _, Acc) ->
                               Acc#{node(P) => ok}
                       end, Nodes0, Enqs0),
    maps:keys(
      lists:foldl(fun({{_, P}, _}, Acc) ->
                          Acc#{node(P) => ok}
                  end, Nodes1, WaitingConsumers0)).

all_pids_for(Node, #?MODULE{consumers = Cons0,
                            enqueuers = Enqs0,
                            waiting_consumers = WaitingConsumers0}) ->
    Cons = maps:fold(fun({_, P}, _, Acc)
                           when node(P) =:= Node ->
                             [P | Acc];
                        (_, _, Acc) -> Acc
                     end, [], Cons0),
    Enqs = maps:fold(fun(P, _, Acc)
                           when node(P) =:= Node ->
                             [P | Acc];
                        (_, _, Acc) -> Acc
                     end, Cons, Enqs0),
    lists:foldl(fun({{_, P}, _}, Acc)
                      when node(P) =:= Node ->
                        [P | Acc];
                   (_, Acc) -> Acc
                end, Enqs, WaitingConsumers0).

suspected_pids_for(Node, #?MODULE{consumers = Cons0,
                                  enqueuers = Enqs0,
                                  waiting_consumers = WaitingConsumers0}) ->
    Cons = maps:fold(fun({_, P}, #consumer{status = suspected_down}, Acc)
                           when node(P) =:= Node ->
                             [P | Acc];
                        (_, _, Acc) -> Acc
                     end, [], Cons0),
    Enqs = maps:fold(fun(P, #enqueuer{status = suspected_down}, Acc)
                           when node(P) =:= Node ->
                             [P | Acc];
                        (_, _, Acc) -> Acc
                     end, Cons, Enqs0),
    lists:foldl(fun({{_, P},
                     #consumer{status = suspected_down}}, Acc)
                      when node(P) =:= Node ->
                        [P | Acc];
                   (_, Acc) -> Acc
                end, Enqs, WaitingConsumers0).

is_expired(Ts, #?MODULE{cfg = #cfg{expires = Expires},
                        last_active = LastActive,
                        consumers = Consumers})
  when is_number(LastActive) andalso is_number(Expires) ->
    %% TODO: should it be active consumers?
    Active = maps:filter(fun (_, #consumer{status = suspected_down}) ->
                                 false;
                             (_, _) ->
                                 true
                         end, Consumers),

    Ts > (LastActive + Expires) andalso maps:size(Active) == 0;
is_expired(_Ts, _State) ->
    false.

get_priority_from_args(#{args := Args}) ->
    case rabbit_misc:table_lookup(Args, <<"x-priority">>) of
        {_Key, Value} ->
            Value;
        _ -> 0
    end;
get_priority_from_args(_) ->
    0.

maybe_notify_decorators(_, false) ->
    false;
maybe_notify_decorators(State, _) ->
    {true, query_notify_decorators_info(State)}.

notify_decorators_effect(#?MODULE{cfg = #cfg{resource = QName}} = State) ->
    {MaxActivePriority, IsEmpty} = query_notify_decorators_info(State),
    notify_decorators_effect(QName, MaxActivePriority, IsEmpty).

notify_decorators_effect(QName, MaxActivePriority, IsEmpty) ->
    {mod_call, rabbit_quorum_queue, spawn_notify_decorators,
     [QName, consumer_state_changed, [MaxActivePriority, IsEmpty]]}.

convert(To, To, State0) ->
    State0;
convert(0, To, State0) ->
    convert(1, To, rabbit_fifo_v1:convert_v0_to_v1(State0));
convert(1, To, State0) ->
    convert(2, To, convert_v1_to_v2(State0)).

smallest_raft_index(#?MODULE{messages = Messages,
                             ra_indexes = Indexes,
                             dlx = DlxState}) ->
    SmallestDlxRaIdx = rabbit_fifo_dlx:smallest_raft_index(DlxState),
    SmallestMsgsRaIdx = case lqueue:peek(Messages) of
                            {value, ?INDEX_MSG(I, _)} ->
                                I;
                            _ ->
                                undefined
                        end,
    SmallestRaIdx = rabbit_fifo_index:smallest(Indexes),
    lists:min([SmallestDlxRaIdx, SmallestMsgsRaIdx, SmallestRaIdx]).

make_requeue(ConsumerId, Notify, [{MsgId, ?INDEX_MSG(Idx, ?TUPLE(Header, Msg))}], Acc) ->
    lists:reverse([{append,
                    #requeue{consumer_id = ConsumerId,
                             index = Idx,
                             header = Header,
                             msg_id = MsgId,
                             msg = Msg},
                    Notify}
                   | Acc]);
make_requeue(ConsumerId, Notify, [{MsgId, ?INDEX_MSG(Idx, ?TUPLE(Header, Msg))} | Rem], Acc) ->
    make_requeue(ConsumerId, Notify, Rem,
                 [{append,
                   #requeue{consumer_id = ConsumerId,
                            index = Idx,
                            header = Header,
                            msg_id = MsgId,
                            msg = Msg},
                   noreply}
                  | Acc]);
make_requeue(_ConsumerId, _Notify, [], []) ->
    [].

incr(I) ->
    I + 1.
