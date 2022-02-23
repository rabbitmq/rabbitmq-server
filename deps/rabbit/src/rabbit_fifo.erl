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
         query_waiting_consumers/1,
         query_consumer_count/1,
         query_consumers/1,
         query_stat/1,
         query_stat_dlx/1,
         query_single_active_consumer/1,
         query_in_memory_usage/1,
         query_peek/2,
         query_notify_decorators_info/1,
         usage/1,

         %% misc
         dehydrate_state/1,
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

-ifdef(TEST).
-export([update_header/4]).
-endif.

-define(SETTLE_V2, '$s').
-define(RETURN_V2, '$r').
-define(DISCARD_V2, '$d').
-define(CREDIT_V2, '$c').

%% command records representing all the protocol actions that are supported
-record(enqueue, {pid :: option(pid()),
                  seq :: option(msg_seqno()),
                  msg :: raw_msg()}).
-record(requeue, {consumer_id :: consumer_id(),
                  msg_id :: msg_id(),
                  index :: ra:index(),
                  header :: msg_header(),
                  msg :: msg()}).
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
%% v2 alternative commands
%% each consumer is assigned an integer index which can be used
%% instead of the consumer id to identify the consumer
-type consumer_idx() :: non_neg_integer().

-record(?SETTLE_V2, {consumer_idx :: consumer_idx(),
                     msg_ids :: [msg_id()]}).
-record(?RETURN_V2, {consumer_idx :: consumer_idx(),
                     msg_ids :: [msg_id()]}).
-record(?DISCARD_V2, {consumer_idx :: consumer_idx(),
                      msg_ids :: [msg_id()]}).
-record(?CREDIT_V2, {consumer_idx :: consumer_idx(),
                     credit :: non_neg_integer(),
                     delivery_count :: non_neg_integer(),
                     drain :: boolean()}).

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
    #garbage_collection{} |
    % v2
    #?SETTLE_V2{} |
    #?RETURN_V2{} |
    #?DISCARD_V2{} |
    #?CREDIT_V2{}.

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
    State#?MODULE{cfg = Cfg#cfg{release_cursor_interval = RCISpec,
                                dead_letter_handler = DLH,
                                become_leader_handler = BLH,
                                overflow_strategy = Overflow,
                                max_length = MaxLength,
                                max_bytes = MaxBytes,
                                consumer_strategy = ConsumerStrategy,
                                delivery_limit = DeliveryLimit,
                                expires = Expires,
                                msg_ttl = MsgTTL},
                  last_active = LastActive}.

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
                                                  case maps:get(Id, Checked, undefined) of
                                                      undefined ->
                                                          false;
                                                      Msg ->
                                                          {true, Msg}
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
               index = OldIdx,
               header = Header0,
               msg = _Msg},
      #?MODULE{consumers = Cons0,
               messages = Messages,
               ra_indexes = Indexes0,
               enqueue_count = EnqCount} = State00) ->
    case Cons0 of
        #{ConsumerId := #consumer{checked_out = Checked0} = Con0}
          when is_map_key(MsgId, Checked0) ->
            %% construct a message with the current raft index
            %% and update delivery count before adding it to the message queue
            Header = update_header(delivery_count, fun incr/1, 1, Header0),
            State0 = add_bytes_return(Header, State00),
            Con = Con0#consumer{checked_out = maps:remove(MsgId, Checked0),
                                credit = increase_credit(Con0, 1)},
            State1 = State0#?MODULE{ra_indexes = rabbit_fifo_index:delete(OldIdx, Indexes0),
                                    messages = lqueue:in(?MSG(Idx, Header), Messages),
                                    enqueue_count = EnqCount + 1},
            State2 = update_or_remove_sub(Meta, ConsumerId, Con, State1),
            {State, Ret, Effs} = checkout(Meta, State0, State2, []),
            update_smallest_raft_index(Idx, Ret,
                                       maybe_store_release_cursor(Idx,  State),
                                       Effs);
        _ ->
            {State00, ok, []}
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
                                    consumers = Cons}, []),
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
            update_smallest_raft_index(Index, {dequeue, empty}, State0, []);
        _ when Exists ->
            %% a dequeue using the same consumer_id isn't possible at this point
            {State0, {dequeue, empty}};
        Ready ->
            State1 = update_consumer(Meta, ConsumerId, ConsumerMeta,
                                     {once, 1, simple_prefetch}, 0,
                                     State0),
            {success, _, MsgId, ?MSG(RaftIdx, Header), ExpiredMsg, State2, Effects0} =
                checkout_one(Meta, false, State1, []),
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
            Effects2 = [reply_log_effect(RaftIdx, MsgId, Header, Ready - 1, From) | Effects1],
            {State, DroppedMsg, Effects} = evaluate_limit(Index, false, State0, State4,
                                                          Effects2),
            Reply = '$ra_no_reply',
            case {DroppedMsg, ExpiredMsg} of
                {false, false} ->
                    {State, Reply, Effects};
                _ ->
                    update_smallest_raft_index(Index, Reply, State, Effects)
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
                      consumer_id = {_, Pid} = ConsumerId}, State0) ->
    Priority = get_priority_from_args(ConsumerMeta),
    State1 = update_consumer(Meta, ConsumerId, ConsumerMeta, Spec, Priority, State0),
    {State2, Effs} = activate_next_consumer(State1, []),
    checkout(Meta, State0, State2, [{monitor, process, Pid} | Effs]);
apply(#{index := Index}, #purge{},
      #?MODULE{messages_total = Tot,
               returns = Returns,
               messages = Messages,
               ra_indexes = Indexes0,
               dlx = DlxState} = State0) ->
    NumReady = messages_ready(State0),
    Indexes1 = lists:foldl(fun(?MSG(I, _), Acc0) when is_integer(I) ->
                                   rabbit_fifo_index:delete(I, Acc0)
                           end, Indexes0, lqueue:to_list(Returns)),
    Indexes = lists:foldl(fun(?MSG(I, _), Acc0) when is_integer(I) ->
                                  rabbit_fifo_index:delete(I, Acc0)
                          end, Indexes1, lqueue:to_list(Messages)),
    {NumDlx, _} = rabbit_fifo_dlx:stat(DlxState),
    State1 = State0#?MODULE{ra_indexes = Indexes,
                            dlx = rabbit_fifo_dlx:purge(DlxState),
                            messages = lqueue:new(),
                            messages_total = Tot - NumReady,
                            returns = lqueue:new(),
                            msg_bytes_enqueue = 0
                           },
    Effects0 = [garbage_collection],
    Reply = {purge, NumReady + NumDlx},
    {State, _, Effects} = evaluate_limit(Index, false, State0,
                                         State1, Effects0),
    update_smallest_raft_index(Index, Reply, State, Effects);
apply(#{index := Idx}, #garbage_collection{}, State) ->
    update_smallest_raft_index(Idx, ok, State, [{aux, garbage_collection}]);
apply(Meta, {timeout, expire_msgs}, State) ->
    checkout(Meta, State, State, []);
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

    Effects = [{monitor, node, Node} | Effects1],
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
    State2 = State1#?MODULE{enqueuers = Enqs1,
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
    {State, ok, Effects} = checkout(Meta, State0, State1, Effects0),
    update_smallest_raft_index(IncomingRaftIdx, State, Effects);
apply(_Meta, Cmd, State) ->
    %% handle unhandled commands gracefully
    rabbit_log:debug("rabbit_fifo: unhandled command ~W", [Cmd, 10]),
    {State, ok, []}.

convert_msg({RaftIdx, {Header, empty}}) when is_integer(RaftIdx) ->
    ?MSG(RaftIdx, Header);
convert_msg({RaftIdx, {Header, _Msg}}) when is_integer(RaftIdx) ->
    ?MSG(RaftIdx, Header);
convert_msg({'$empty_msg', Header}) ->
    %% dummy index
    ?MSG(undefined, Header);
convert_msg({'$prefix_msg', Header}) ->
    %% dummy index
    ?MSG(undefined, Header);
convert_msg({Header, empty}) ->
    convert_msg(Header);
convert_msg(Header) when ?IS_HEADER(Header) ->
    ?MSG(undefined, Header).

convert_consumer({ConsumerTag, Pid}, CV1) ->
    Meta = element(2, CV1),
    CheckedOut = element(3, CV1),
    NextMsgId = element(4, CV1),
    Credit = element(5, CV1),
    DeliveryCount = element(6, CV1),
    CreditMode = element(7, CV1),
    LifeTime = element(8, CV1),
    Status = element(9, CV1),
    Priority = element(10, CV1),
    #consumer{cfg = #consumer_cfg{tag = ConsumerTag,
                                  pid = Pid,
                                  meta = Meta,
                                  credit_mode = CreditMode,
                                  lifetime = LifeTime,
                                  priority = Priority},
              credit = Credit,
              status = Status,
              delivery_count = DeliveryCount,
              next_msg_id = NextMsgId,
              checked_out = maps:map(
                              fun (_, {Tag, _} = Msg) when is_atom(Tag) ->
                                      convert_msg(Msg);
                                  (_, {_Seq, Msg}) ->
                                      convert_msg(Msg)
                              end, CheckedOut)
             }.

convert_v1_to_v2(V1State0) ->
    V1State = rabbit_fifo_v1:enqueue_all_pending(V1State0),
    IndexesV1 = rabbit_fifo_v1:get_field(ra_indexes, V1State),
    ReturnsV1 = rabbit_fifo_v1:get_field(returns, V1State),
    MessagesV1 = rabbit_fifo_v1:get_field(messages, V1State),
    ConsumersV1 = rabbit_fifo_v1:get_field(consumers, V1State),
    WaitingConsumersV1 = rabbit_fifo_v1:get_field(waiting_consumers, V1State),
    %% remove all raft idx in messages from index
    {_, PrefMsgs, _, PrefReturns} = rabbit_fifo_v1:get_field(prefix_msgs, V1State),
    V2PrefMsgs = lists:foldl(fun(Hdr, Acc) ->
                                     lqueue:in(convert_msg(Hdr), Acc)
                             end, lqueue:new(), PrefMsgs),
    V2PrefReturns = lists:foldl(fun(Hdr, Acc) ->
                                        lqueue:in(convert_msg(Hdr), Acc)
                                end, lqueue:new(), PrefReturns),
    MessagesV2 = lqueue:fold(fun ({_, Msg}, Acc) ->
                                     lqueue:in(convert_msg(Msg), Acc)
                             end, V2PrefMsgs, MessagesV1),
    ReturnsV2 = lqueue:fold(fun ({_SeqId, Msg}, Acc) ->
                                    lqueue:in(convert_msg(Msg), Acc)
                            end, V2PrefReturns, ReturnsV1),
    ConsumersV2 = maps:map(
                    fun (ConsumerId, CV1) ->
                            convert_consumer(ConsumerId, CV1)
                    end, ConsumersV1),
    WaitingConsumersV2 = lists:map(
                           fun ({ConsumerId, CV1}) ->
                                   {ConsumerId, convert_consumer(ConsumerId, CV1)}
                           end, WaitingConsumersV1),
    EnqueuersV1 = rabbit_fifo_v1:get_field(enqueuers, V1State),
    EnqueuersV2 = maps:map(fun (_EnqPid, Enq) ->
                                   Enq#enqueuer{unused = undefined}
                           end, EnqueuersV1),

    %% do after state conversion
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
               expires = rabbit_fifo_v1:get_cfg_field(expires, V1State)
              },

    #?MODULE{cfg = Cfg,
             messages = MessagesV2,
             messages_total = rabbit_fifo_v1:query_messages_total(V1State),
             returns = ReturnsV2,
             enqueue_count = rabbit_fifo_v1:get_field(enqueue_count, V1State),
             enqueuers = EnqueuersV2,
             ra_indexes = IndexesV1,
             release_cursors = rabbit_fifo_v1:get_field(release_cursors, V1State),
             consumers = ConsumersV2,
             service_queue = rabbit_fifo_v1:get_field(service_queue, V1State),
             msg_bytes_enqueue = rabbit_fifo_v1:get_field(msg_bytes_enqueue, V1State),
             msg_bytes_checkout = rabbit_fifo_v1:get_field(msg_bytes_checkout, V1State),
             waiting_consumers = WaitingConsumersV2,
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

consumer_active_flag_update_function(
  #?MODULE{cfg = #cfg{consumer_strategy = competing}}) ->
    fun(State, ConsumerId, Consumer, Active, ActivityStatus, Effects) ->
            consumer_update_active_effects(State, ConsumerId, Consumer, Active,
                                           ActivityStatus, Effects)
    end;
consumer_active_flag_update_function(
  #?MODULE{cfg = #cfg{consumer_strategy = single_active}}) ->
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

-spec state_enter(ra_server:ra_state() | eol, state()) ->
    ra_machine:effects().
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
                                         become_leader_handler = BLH}
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
                 num_in_memory_ready_messages => 0, %% backwards compat
                 num_messages => messages_total(State),
                 num_release_cursors => lqueue:len(Cursors),
                 release_cursors => [I || {_, I, _} <- lqueue:to_list(Cursors)],
                 release_cursor_enqueue_counter => EnqCount,
                 enqueue_message_bytes => EnqueueBytes,
                 checkout_message_bytes => CheckoutBytes,
                 in_memory_message_bytes => 0, %% backwards compat
                 smallest_raft_index => smallest_raft_index(State)},
    DlxOverview = rabbit_fifo_dlx:overview(DlxState),
    maps:merge(Overview, DlxOverview).

-spec get_checked_out(consumer_id(), msg_id(), msg_id(), state()) ->
    [delivery_msg()].
get_checked_out(Cid, From, To, #?MODULE{consumers = Consumers}) ->
    case Consumers of
        #{Cid := #consumer{checked_out = Checked}} ->
            [begin
                 ?MSG(I, H) = maps:get(K, Checked),
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
               last_decorators_state :: term(),
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
                                consumers = Consumers}) ->
    case Consumers of
        #{ConsumerId := #consumer{checked_out = Checked}} ->
            {Log, ToReturn} =
                maps:fold(
                  fun (MsgId, ?MSG(Idx, Header), {L0, Acc}) ->
                          %% it is possible this is not found if the consumer
                          %% crashed and the message got removed
                          case ra_log:fetch(Idx, L0) of
                              {{_, _, {_, _, Cmd, _}}, L} ->
                                  Msg = case Cmd of
                                            #enqueue{msg = M} -> M;
                                            #requeue{msg = M} -> M
                                        end,
                                  {L, [{MsgId, Idx, Header, Msg} | Acc]};
                              {undefined, L} ->
                                  {L, Acc}
                          end
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
handle_aux(leader, cast, eval, #?AUX{last_decorators_state = LastDec} = Aux0,
           Log, #?MODULE{cfg = #cfg{resource = QName}} = MacState) ->
    %% this is called after each batch of commands have been applied
    %% set timer for message expire
    %% should really be the last applied index ts but this will have to do
    Ts = erlang:system_time(millisecond),
    Effects0 = timer_effect(Ts, MacState, []),
    case query_notify_decorators_info(MacState) of
        LastDec ->
            {no_reply, Aux0, Log, Effects0};
        {MaxActivePriority, IsEmpty} = NewLast ->
            Effects = [notify_decorators_effect(QName, MaxActivePriority, IsEmpty)
                       | Effects0],
            {no_reply, Aux0#?AUX{last_decorators_state = NewLast}, Log, Effects}
    end;
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
        {ok, ?MSG(Idx, Header)} ->
            %% need to re-hydrate from the log
            {{_, _, {_, _, Cmd, _}}, Log} = ra_log:fetch(Idx, Log0),
            Msg = case Cmd of
                      #enqueue{msg = M} -> M;
                      #requeue{msg = M} -> M
                  end,
            {reply, {ok, {Header, Msg}}, Aux0, Log};
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

query_waiting_consumers(#?MODULE{waiting_consumers = WaitingConsumers}) ->
    WaitingConsumers.

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
                      ({Tag, Pid},
                       #consumer{cfg = #consumer_cfg{meta = Meta}} = Consumer,
                       Acc) ->
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
                        ({{Tag, Pid},
                          #consumer{cfg = #consumer_cfg{meta = Meta}} = Consumer},
                         Acc) ->
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


query_single_active_consumer(
  #?MODULE{cfg = #cfg{consumer_strategy = single_active},
           consumers = Consumers}) ->
    case active_consumer(Consumers) of
        undefined ->
            {error, no_value};
        {ActiveCid, _} ->
            {value, ActiveCid}
    end;
query_single_active_consumer(_) ->
    disabled.

query_stat(#?MODULE{consumers = Consumers} = State) ->
    {messages_ready(State), maps:size(Consumers)}.

query_in_memory_usage(#?MODULE{ }) ->
    {0, 0}.

query_stat_dlx(#?MODULE{dlx = DlxState}) ->
    rabbit_fifo_dlx:stat(DlxState).

query_peek(Pos, State0) when Pos > 0 ->
    case take_next_msg(State0) of
        empty ->
            {error, no_message_at_pos};
        {Msg, _State}
          when Pos == 1 ->
            {ok, Msg};
        {_Msg, State} ->
            query_peek(Pos-1, State)
    end.

query_notify_decorators_info(#?MODULE{consumers = Consumers} = State) ->
    MaxActivePriority = maps:fold(
                          fun(_, #consumer{credit = C,
                                           status = up,
                                           cfg = #consumer_cfg{priority = P0}},
                              MaxP) when C > 0 ->
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
                        returns = R}) ->
    lqueue:len(M) + lqueue:len(R).

messages_total(#?MODULE{messages_total = Total,
                        dlx = DlxState}) ->
    {DlxTotal, _} = rabbit_fifo_dlx:stat(DlxState),
    Total + DlxTotal.

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
    case Cons0 of
        #{ConsumerId := #consumer{status = _}} ->
            % The active consumer is to be removed
            {State1, Effects1} = cancel_consumer0(Meta, ConsumerId, State0,
                                                  Effects0, Reason),
            activate_next_consumer(State1, Effects1);
        _ ->
            % The cancelled consumer is not active or cancelled
            % Just remove it from idle_consumers
            Waiting = lists:keydelete(ConsumerId, 1, Waiting0),
            Effects = cancel_consumer_effects(ConsumerId, State0, Effects0),
            % A waiting consumer isn't supposed to have any checked out messages,
            % so nothing special to do here
            {State0#?MODULE{waiting_consumers = Waiting}, Effects}
    end.

consumer_update_active_effects(#?MODULE{cfg = #cfg{resource = QName}},
                               ConsumerId,
                               #consumer{cfg = #consumer_cfg{meta = Meta}},
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
            {S, Effects};
        _ ->
            %% already removed: do nothing
            {S0, Effects0}
    end.

activate_next_consumer(#?MODULE{cfg = #cfg{consumer_strategy = competing}} = State0,
                       Effects0) ->
    {State0, Effects0};
activate_next_consumer(#?MODULE{consumers = Cons,
                                waiting_consumers = Waiting0} = State0,
                       Effects0) ->
    case has_active_consumer(Cons) of
        false ->
            case lists:filter(fun ({_, #consumer{status = Status}}) ->
                                      Status == up
                              end, Waiting0) of
                [{NextConsumerId, #consumer{cfg = NextCCfg} = NextConsumer} | _] ->
                    Remaining = lists:keydelete(NextConsumerId, 1, Waiting0),
                    Consumer = case maps:get(NextConsumerId, Cons, undefined) of
                                   undefined ->
                                       NextConsumer;
                                   Existing ->
                                       %% there was an exisiting non-active consumer
                                       %% just update the existing cancelled consumer
                                       %% with the new config
                                       Existing#consumer{cfg =  NextCCfg}
                               end,
                    #?MODULE{service_queue = ServiceQueue} = State0,
                    ServiceQueue1 = maybe_queue_consumer(NextConsumerId,
                                                         Consumer,
                                                         ServiceQueue),
                    State = State0#?MODULE{consumers = Cons#{NextConsumerId => Consumer},
                                           service_queue = ServiceQueue1,
                                           waiting_consumers = Remaining},
                    Effects = consumer_update_active_effects(State, NextConsumerId,
                                                             Consumer, true,
                                                             single_active, Effects0),
                    {State, Effects};
                [] ->
                    {State0, Effects0}
            end;
        true ->
            {State0, Effects0}
    end.

has_active_consumer(Consumers) ->
    active_consumer(Consumers) /= undefined.

active_consumer({Cid, #consumer{status = up} = Consumer, _I}) ->
    {Cid, Consumer};
active_consumer({_Cid, #consumer{status = _}, I}) ->
    active_consumer(maps:next(I));
active_consumer(none) ->
    undefined;
active_consumer(M) when is_map(M) ->
    I = maps:iterator(M),
    active_consumer(maps:next(I)).

maybe_return_all(#{system_time := Ts} = Meta, ConsumerId,
                 #consumer{cfg = CCfg} = Consumer, S0,
                 Effects0, Reason) ->
    case Reason of
        consumer_cancel ->
            {update_or_remove_sub(
               Meta, ConsumerId,
               Consumer#consumer{cfg = CCfg#consumer_cfg{lifetime = once},
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
            {State, ok, Effects} = checkout(Meta, State0, State1, Effects1),
            {maybe_store_release_cursor(RaftIdx, State), ok, Effects};
        {out_of_sequence, State, Effects} ->
            {State, not_enqueued, Effects};
        {duplicate, State, Effects} ->
            {State, ok, Effects}
    end.

decr_total(#?MODULE{messages_total = Tot} = State) ->
    State#?MODULE{messages_total = Tot - 1}.

drop_head(#?MODULE{ra_indexes = Indexes0} = State0, Effects) ->
    case take_next_msg(State0) of
        {?MSG(Idx, Header) = Msg, State1} ->
            Indexes = rabbit_fifo_index:delete(Idx, Indexes0),
            State2 = State1#?MODULE{ra_indexes = Indexes},
            State3 = decr_total(add_bytes_drop(Header, State2)),
            #?MODULE{cfg = #cfg{dead_letter_handler = DLH},
                     dlx = DlxState} = State = State3,
            {_, DlxEffects} = rabbit_fifo_dlx:discard([Msg], maxlen, DLH, DlxState),
            {State, DlxEffects ++ Effects};
        empty ->
            {State0, Effects}
    end.

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

maybe_store_release_cursor(RaftIdx,
                           #?MODULE{cfg = #cfg{release_cursor_interval = {Base, C}} = Cfg,
                                    enqueue_count = EC,
                                    release_cursors = Cursors0} = State0)
  when EC >= C ->
    case messages_total(State0) of
        0 ->
            %% message must have been immediately dropped
            State0#?MODULE{enqueue_count = 0};
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
            State#?MODULE{enqueue_count = 0,
                          release_cursors = Cursors}
    end;
maybe_store_release_cursor(_RaftIdx, State) ->
    State.

maybe_enqueue(RaftIdx, Ts, undefined, undefined, RawMsg, Effects,
              #?MODULE{msg_bytes_enqueue = Enqueue,
                       enqueue_count = EnqCount,
                       messages = Messages,
                       messages_total = Total} = State0) ->
    % direct enqueue without tracking
    Size = message_size(RawMsg),
    Header = maybe_set_msg_ttl(RawMsg, Ts, Size, State0),
    Msg = ?MSG(RaftIdx, Header),
    State = State0#?MODULE{msg_bytes_enqueue = Enqueue + Size,
                           enqueue_count = EnqCount + 1,
                           messages_total = Total + 1,
                           messages = lqueue:in(Msg, Messages)
                          },
    {ok, State, Effects};
maybe_enqueue(RaftIdx, Ts, From, MsgSeqNo, RawMsg, Effects0,
              #?MODULE{msg_bytes_enqueue = Enqueue,
                       enqueue_count = EnqCount,
                       enqueuers = Enqueuers0,
                       messages = Messages,
                       messages_total = Total} = State0) ->

    case maps:get(From, Enqueuers0, undefined) of
        undefined ->
            State1 = State0#?MODULE{enqueuers = Enqueuers0#{From => #enqueuer{}}},
            {Res, State, Effects} = maybe_enqueue(RaftIdx, Ts, From, MsgSeqNo,
                                                  RawMsg, Effects0, State1),
            {Res, State, [{monitor, process, From} | Effects]};
        #enqueuer{next_seqno = MsgSeqNo} = Enq0 ->
            % it is the next expected seqno
            Size = message_size(RawMsg),
            Header = maybe_set_msg_ttl(RawMsg, Ts, Size, State0),
            Msg = ?MSG(RaftIdx, Header),
            Enq = Enq0#enqueuer{next_seqno = MsgSeqNo + 1},
            MsgCache = case can_immediately_deliver(State0) of
                           true ->
                               {RaftIdx, RawMsg};
                           false ->
                               undefined
                       end,
            State = State0#?MODULE{msg_bytes_enqueue = Enqueue + Size,
                                   enqueue_count = EnqCount + 1,
                                   messages_total = Total + 1,
                                   messages = lqueue:in(Msg, Messages),
                                   enqueuers = Enqueuers0#{From => Enq},
                                   msg_cache = MsgCache
                                  },
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
    {State, ok, Effects} = checkout(Meta, State0, State2, Effects1),
    update_smallest_raft_index(IncomingRaftIdx, State, Effects).

% used to process messages that are finished
complete(Meta, ConsumerId, DiscardedMsgIds,
         #consumer{checked_out = Checked} = Con0,
         #?MODULE{ra_indexes = Indexes0,
                  msg_bytes_checkout = BytesCheckout,
                  messages_total = Tot} = State0) ->
    %% credit_mode = simple_prefetch should automatically top-up credit
    %% as messages are simple_prefetch or otherwise returned
    Discarded = maps:with(DiscardedMsgIds, Checked),
    DiscardedMsgs = maps:values(Discarded),
    Len = map_size(Discarded),
    Con = Con0#consumer{checked_out = maps:without(DiscardedMsgIds, Checked),
                        credit = increase_credit(Con0, Len)},
    State1 = update_or_remove_sub(Meta, ConsumerId, Con, State0),
    SettledSize = lists:foldl(fun(Msg, Acc) ->
                                      get_header(size, get_msg_header(Msg)) + Acc
                              end, 0, DiscardedMsgs),
    Indexes = lists:foldl(fun(?MSG(I, _), Acc) when is_integer(I) ->
                                  rabbit_fifo_index:delete(I, Acc)
                          end, Indexes0, DiscardedMsgs),
    State1#?MODULE{ra_indexes = Indexes,
                   msg_bytes_checkout = BytesCheckout - SettledSize,
                   messages_total = Tot - Len}.

increase_credit(#consumer{cfg = #consumer_cfg{lifetime = once},
                          credit = Credit}, _) ->
    %% once consumers cannot increment credit
    Credit;
increase_credit(#consumer{cfg = #consumer_cfg{lifetime = auto,
                                              credit_mode = credited},
                          credit = Credit}, _) ->
    %% credit_mode: `credited' also doesn't automatically increment credit
    Credit;
increase_credit(#consumer{credit = Current}, Credit) ->
    Current + Credit.

complete_and_checkout(#{index := IncomingRaftIdx} = Meta, MsgIds, ConsumerId,
                      #consumer{} = Con0,
                      Effects0, State0) ->
    State1 = complete(Meta, ConsumerId, MsgIds, Con0, State0),
    {State, ok, Effects} = checkout(Meta, State0, State1, Effects0),
    update_smallest_raft_index(IncomingRaftIdx, State, Effects).

cancel_consumer_effects(ConsumerId,
                        #?MODULE{cfg = #cfg{resource = QName}} = _State,
                        Effects) ->
    [{mod_call, rabbit_quorum_queue,
      cancel_consumer_handler, [QName, ConsumerId]} | Effects].

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

update_msg_header(Key, Fun, Def, ?MSG(Idx, Header)) ->
    ?MSG(Idx, update_header(Key, Fun, Def, Header)).

update_header(expiry, _, Expiry, Size)
  when is_integer(Size) ->
    ?TUPLE(Size, Expiry);
update_header(Key, UpdateFun, Default, Size)
  when is_integer(Size) ->
    update_header(Key, UpdateFun, Default, #{size => Size});
update_header(Key, UpdateFun, Default, ?TUPLE(Size, Expiry))
  when is_integer(Size), is_integer(Expiry) ->
    update_header(Key, UpdateFun, Default, #{size => Size,
                                             expiry => Expiry});
update_header(Key, UpdateFun, Default, Header)
  when is_map(Header), is_map_key(size, Header) ->
    maps:update_with(Key, UpdateFun, Default, Header).

get_msg_header(?MSG(_Idx, Header)) ->
    Header.

get_header(size, Size)
  when is_integer(Size) ->
    Size;
get_header(_Key, Size)
  when is_integer(Size) ->
    undefined;
get_header(size, ?TUPLE(Size, Expiry))
  when is_integer(Size), is_integer(Expiry) ->
    Size;
get_header(expiry, ?TUPLE(Size, Expiry))
  when is_integer(Size), is_integer(Expiry) ->
    Expiry;
get_header(_Key, ?TUPLE(Size, Expiry))
  when is_integer(Size), is_integer(Expiry) ->
    undefined;
get_header(Key, Header)
  when is_map(Header) andalso is_map_key(size, Header) ->
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
            {DlxState, DlxEffects} = rabbit_fifo_dlx:discard([Msg], delivery_limit, DLH, DlxState0),
            State1 = State0#?MODULE{dlx = DlxState},
            State = complete(Meta, ConsumerId, [MsgId], Con0, State1),
            {State, DlxEffects ++ Effects0};
        _ ->
            Con = Con0#consumer{checked_out = maps:remove(MsgId, Checked)},
            {add_bytes_return(
               Header,
               State0#?MODULE{consumers = Consumers#{ConsumerId => Con},
                              returns = lqueue:in(Msg, Returns)}),
             Effects0}
    end.

return_all(Meta, #?MODULE{consumers = Cons} = State0, Effects0, ConsumerId,
           #consumer{checked_out = Checked} = Con) ->
    State = State0#?MODULE{consumers = Cons#{ConsumerId => Con}},
    lists:foldl(fun ({MsgId, Msg}, {S, E}) ->
                        return_one(Meta, MsgId, Msg, S, E, ConsumerId)
                end, {State, Effects0}, lists:sort(maps:to_list(Checked))).

checkout(#{index := Index} = Meta,
         #?MODULE{cfg = #cfg{resource = _QName}} = OldState,
         State0, Effects0) ->
    {#?MODULE{cfg = #cfg{dead_letter_handler = DLH},
              dlx = DlxState0} = State1, ExpiredMsg, Effects1} =
        checkout0(Meta, checkout_one(Meta, false, State0, Effects0), #{}),
    {DlxState, DlxDeliveryEffects} = rabbit_fifo_dlx:checkout(DLH, DlxState0),
    %% TODO: only update dlx state if it has changed?
    State2 = State1#?MODULE{msg_cache = undefined, %% by this time the cache should be used
                            dlx = DlxState},
    Effects2 = DlxDeliveryEffects ++ Effects1,
    case evaluate_limit(Index, false, OldState, State2, Effects2) of
        {State, false, Effects} when ExpiredMsg == false ->
            {State, ok, Effects};
        {State, _, Effects} ->
            update_smallest_raft_index(Index, State, Effects)
    end.

checkout0(Meta, {success, ConsumerId, MsgId,
                 ?MSG(RaftIdx, Header), ExpiredMsg, State, Effects},
          SendAcc0) when is_integer(RaftIdx) ->
    DelMsg = {RaftIdx, {MsgId, Header}},
    SendAcc = case maps:get(ConsumerId, SendAcc0, undefined) of
                  undefined ->
                      SendAcc0#{ConsumerId => [DelMsg]};
                  LogMsgs ->
                      SendAcc0#{ConsumerId => [DelMsg | LogMsgs]}
              end,
    checkout0(Meta, checkout_one(Meta, ExpiredMsg, State, Effects), SendAcc);
checkout0(_Meta, {_Activity, ExpiredMsg, State0, Effects0}, SendAcc) ->
    Effects = append_delivery_effects(Effects0, SendAcc, State0),
    {State0, ExpiredMsg, lists:reverse(Effects)}.

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
                    %% we have moved below the lower limit
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

append_delivery_effects(Effects0, AccMap, _State) when map_size(AccMap) == 0 ->
    %% does this ever happen?
    Effects0;
append_delivery_effects(Effects0, AccMap, State) ->
     maps:fold(fun (C, DiskMsgs, Ef) when is_list(DiskMsgs) ->
                       [delivery_effect(C, lists:reverse(DiskMsgs), State) | Ef]
               end, Effects0, AccMap).

take_next_msg(#?MODULE{returns = Returns0,
                       messages = Messages0,
                       ra_indexes = Indexes0
                      } = State) ->
    case lqueue:out(Returns0) of
        {{value, NextMsg}, Returns} ->
            {NextMsg, State#?MODULE{returns = Returns}};
        {empty, _} ->
            case lqueue:out(Messages0) of
                {empty, _} ->
                    empty;
                {{value, ?MSG(RaftIdx, _) = Msg}, Messages} ->
                    %% add index here
                    Indexes = rabbit_fifo_index:append(RaftIdx, Indexes0),
                    {Msg, State#?MODULE{messages = Messages,
                                        ra_indexes = Indexes}}
            end
    end.

get_next_msg(#?MODULE{returns = Returns0,
                       messages = Messages0}) ->
    case lqueue:get(Returns0, empty) of
        empty ->
            lqueue:get(Messages0, empty);
        Msg ->
            Msg
    end.

delivery_effect({CTag, CPid}, [{Idx, {MsgId, Header}}],
                #?MODULE{msg_cache = {Idx, RawMsg}}) ->
    {send_msg, CPid, {delivery, CTag, [{MsgId, {Header, RawMsg}}]},
     [local, ra_event]};
delivery_effect({CTag, CPid}, Msgs, _State) ->
    {RaftIdxs, Data} = lists:unzip(Msgs),
    {log, RaftIdxs,
     fun(Log) ->
             DelMsgs = lists:zipwith(
                      fun (#enqueue{msg = Msg},
                           {MsgId, Header}) ->
                              {MsgId, {Header, Msg}};
                          (#requeue{msg = Msg},
                           {MsgId, Header}) ->
                              {MsgId, {Header, Msg}}
                      end, Log, Data),
             [{send_msg, CPid, {delivery, CTag, DelMsgs}, [local, ra_event]}]
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

checkout_one(#{system_time := Ts} = Meta, ExpiredMsg0, InitState0, Effects0) ->
    %% Before checking out any messsage to any consumer,
    %% first remove all expired messages from the head of the queue.
    {ExpiredMsg, #?MODULE{service_queue = SQ0,
                          messages = Messages0,
                          msg_bytes_checkout = BytesCheckout,
                          msg_bytes_enqueue = BytesEnqueue,
                          consumers = Cons0} = InitState, Effects1} =
        expire_msgs(Ts, ExpiredMsg0, InitState0, Effects0),

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
                            checkout_one(Meta, ExpiredMsg,
                                         InitState#?MODULE{service_queue = SQ1}, Effects1);
                        #consumer{status = cancelled} ->
                            checkout_one(Meta, ExpiredMsg,
                                         InitState#?MODULE{service_queue = SQ1}, Effects1);
                        #consumer{status = suspected_down} ->
                            checkout_one(Meta, ExpiredMsg,
                                         InitState#?MODULE{service_queue = SQ1}, Effects1);
                        #consumer{checked_out = Checked0,
                                  next_msg_id = Next,
                                  credit = Credit,
                                  delivery_count = DelCnt} = Con0 ->
                            Checked = maps:put(Next, ConsumerMsg, Checked0),
                            Con = Con0#consumer{checked_out = Checked,
                                                next_msg_id = Next + 1,
                                                credit = Credit - 1,
                                                delivery_count = DelCnt + 1},
                            Size = get_header(size, get_msg_header(ConsumerMsg)),
                            State = update_or_remove_sub(
                                       Meta, ConsumerId, Con,
                                       State0#?MODULE{service_queue = SQ1,
                                                      msg_bytes_checkout = BytesCheckout + Size,
                                                      msg_bytes_enqueue = BytesEnqueue - Size}),
                            {success, ConsumerId, Next, ConsumerMsg, ExpiredMsg,
                             State, Effects1}
                    end;
                empty ->
                    {nochange, ExpiredMsg, InitState, Effects1}
            end;
        {{value, _ConsumerId}, SQ1} ->
            %% consumer did not exist but was queued, recurse
            checkout_one(Meta, ExpiredMsg,
                         InitState#?MODULE{service_queue = SQ1}, Effects1);
        {empty, _} ->
            case lqueue:len(Messages0) of
                0 ->
                    {nochange, ExpiredMsg, InitState, Effects1};
                _ ->
                    {inactive, ExpiredMsg, InitState, Effects1}
            end
    end.

%% dequeue all expired messages
expire_msgs(RaCmdTs, Result, State, Effects) ->
    %% In the normal case, there are no expired messages.
    %% Therefore, first lqueue:get/2 to check whether we need to lqueue:out/1
    %% because the latter can be much slower than the former.
    case get_next_msg(State) of
        ?MSG(_, ?TUPLE(Size, Expiry))
          when is_integer(Size), is_integer(Expiry), RaCmdTs >= Expiry ->
            expire(RaCmdTs, State, Effects);
        ?MSG(_, #{expiry := Expiry})
          when is_integer(Expiry), RaCmdTs >= Expiry ->
            expire(RaCmdTs, State, Effects);
        _ ->
            {Result, State, Effects}
    end.

expire(RaCmdTs, State0, Effects) ->
    {?MSG(Idx, Header) = Msg,
     #?MODULE{cfg = #cfg{dead_letter_handler = DLH},
              dlx = DlxState0,
              ra_indexes = Indexes0,
              messages_total = Tot,
              msg_bytes_enqueue = MsgBytesEnqueue} = State1} = take_next_msg(State0),
    {DlxState, DlxEffects} = rabbit_fifo_dlx:discard([Msg], expired, DLH, DlxState0),
    Indexes = rabbit_fifo_index:delete(Idx, Indexes0),
    State = State1#?MODULE{dlx = DlxState,
                           ra_indexes = Indexes,
                           messages_total = Tot - 1,
                           msg_bytes_enqueue = MsgBytesEnqueue - get_header(size, Header)},
    expire_msgs(RaCmdTs, true, State, DlxEffects ++ Effects).

timer_effect(RaCmdTs, State, Effects) ->
    T = case get_next_msg(State) of
            ?MSG(_, ?TUPLE(Size, Expiry))
              when is_integer(Size), is_integer(Expiry) ->
                %% Next message contains 'expiry' header.
                %% (Re)set timer so that mesage will be dropped or dead-lettered on time.
                max(0, Expiry - RaCmdTs);
            ?MSG(_, #{expiry := Expiry})
              when is_integer(Expiry) ->
                max(0, Expiry - RaCmdTs);
            _ ->
                %% Next message does not contain 'expiry' header.
                %% Therefore, do not set timer or cancel timer if it was set.
                infinity
        end,
    [{timer, expire_msgs, T} | Effects].

update_or_remove_sub(Meta, ConsumerId,
                     #consumer{cfg = #consumer_cfg{lifetime = once},
                               checked_out = Checked,
                               credit = 0} = Con,
                     #?MODULE{consumers = Cons} = State) ->
    case map_size(Checked) of
        0 ->
            #{system_time := Ts} = Meta,
            % we're done with this consumer
            State#?MODULE{consumers = maps:remove(ConsumerId, Cons),
                          last_active = Ts};
        _ ->
            % there are unsettled items so need to keep around
            State#?MODULE{consumers = maps:put(ConsumerId, Con, Cons)}
    end;
update_or_remove_sub(_Meta, ConsumerId,
                     #consumer{cfg = #consumer_cfg{}} = Con,
                     #?MODULE{consumers = Cons,
                              service_queue = ServiceQueue} = State) ->
    State#?MODULE{consumers = maps:put(ConsumerId, Con, Cons),
                  service_queue = uniq_queue_in(ConsumerId, Con, ServiceQueue)}.

uniq_queue_in(Key, #consumer{credit = Credit,
                             status = up,
                             cfg = #consumer_cfg{priority = P}}, ServiceQueue)
  when Credit > 0 ->
    % TODO: queue:member could surely be quite expensive, however the practical
    % number of unique consumers may not be large enough for it to matter
    case priority_queue:member(Key, ServiceQueue) of
        true ->
            ServiceQueue;
        false ->
            priority_queue:in(Key, P, ServiceQueue)
    end;
uniq_queue_in(_Key, _Consumer, ServiceQueue) ->
    ServiceQueue.

update_consumer(Meta, {Tag, Pid} = ConsumerId, ConsumerMeta,
                {Life, Credit, Mode} = Spec, Priority,
                #?MODULE{cfg = #cfg{consumer_strategy = competing},
                         consumers = Cons0} = State0) ->
    Consumer = case Cons0 of
                   #{ConsumerId := #consumer{} = Consumer0} ->
                       merge_consumer(Consumer0, ConsumerMeta, Spec, Priority);
                   _ ->
                       #consumer{cfg = #consumer_cfg{tag = Tag,
                                                     pid = Pid,
                                                     lifetime = Life,
                                                     meta = ConsumerMeta,
                                                     priority = Priority,
                                                     credit_mode = Mode},
                                 credit = Credit}
               end,
    update_or_remove_sub(Meta, ConsumerId, Consumer, State0);
update_consumer(Meta, {Tag, Pid} = ConsumerId, ConsumerMeta,
                {Life, Credit, Mode} = Spec, Priority,
                #?MODULE{cfg = #cfg{consumer_strategy = single_active},
                         consumers = Cons0,
                         waiting_consumers = Waiting,
                         service_queue = _ServiceQueue0} = State0) ->
    %% if it is the current active consumer, just update
    %% if it is a cancelled active consumer, add to waiting unless it is the only
    %% one, then merge
    case active_consumer(Cons0) of
        {ConsumerId, #consumer{status = up} = Consumer0} ->
            Consumer = merge_consumer(Consumer0, ConsumerMeta, Spec, Priority),
            update_or_remove_sub(Meta, ConsumerId, Consumer, State0);
        undefined when is_map_key(ConsumerId, Cons0) ->
            %% there is no active consumer and the current consumer is in the
            %% consumers map and thus must be cancelled, in this case we can just
            %% merge and effectively make this the current active one
            Consumer0 = maps:get(ConsumerId, Cons0),
            Consumer = merge_consumer(Consumer0, ConsumerMeta, Spec, Priority),
            update_or_remove_sub(Meta, ConsumerId, Consumer, State0);
        _ ->
            %% add as a new waiting consumer
            Consumer = #consumer{cfg = #consumer_cfg{tag = Tag,
                                                     pid = Pid,
                                                     lifetime = Life,
                                                     meta = ConsumerMeta,
                                                     priority = Priority,
                                                     credit_mode = Mode},
                                 credit = Credit},

            State0#?MODULE{waiting_consumers = Waiting ++ [{ConsumerId, Consumer}]}
    end.

merge_consumer(#consumer{cfg = CCfg, checked_out = Checked} = Consumer,
               ConsumerMeta, {Life, Credit, Mode}, Priority) ->
    NumChecked = map_size(Checked),
    NewCredit = max(0, Credit - NumChecked),
    Consumer#consumer{cfg = CCfg#consumer_cfg{priority = Priority,
                                              meta = ConsumerMeta,
                                              credit_mode = Mode,
                                              lifetime = Life},
                      status = up,
                      credit = NewCredit}.

maybe_queue_consumer(ConsumerId, #consumer{credit = Credit} = Con,
                     ServiceQueue0) ->
    case Credit > 0 of
        true ->
            % consumer needs service - check if already on service queue
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
    State#?MODULE{ra_indexes = rabbit_fifo_index:empty(),
                  release_cursors = lqueue:new(),
                  enqueue_count = 0,
                  msg_cache = undefined,
                  dlx = rabbit_fifo_dlx:dehydrate(DlxState)}.

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
                       msg_bytes_enqueue = BytesEnq,
                       dlx = DlxState} = State) ->
    {NumDlx, BytesDlx} = rabbit_fifo_dlx:stat(DlxState),
    (messages_ready(State) + NumDlx > MaxLength) orelse
    (BytesEnq + BytesDlx > MaxBytes).

is_below_soft_limit(#?MODULE{cfg = #cfg{max_length = undefined,
                                        max_bytes = undefined}}) ->
    false;
is_below_soft_limit(#?MODULE{cfg = #cfg{max_length = MaxLength,
                                        max_bytes = MaxBytes},
                             msg_bytes_enqueue = BytesEnq,
                             dlx = DlxState} = State) ->
    {NumDlx, BytesDlx} = rabbit_fifo_dlx:stat(DlxState),
    is_below(MaxLength, messages_ready(State) + NumDlx) andalso
    is_below(MaxBytes, BytesEnq + BytesDlx).

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
make_checkout({_, _} = ConsumerId, Spec, Meta) ->
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

add_bytes_drop(Header,
               #?MODULE{msg_bytes_enqueue = Enqueue} = State) ->
    Size = get_header(size, Header),
    State#?MODULE{msg_bytes_enqueue = Enqueue - Size}.


add_bytes_return(Header,
                 #?MODULE{msg_bytes_checkout = Checkout,
                          msg_bytes_enqueue = Enqueue} = State) ->
    Size = get_header(size, Header),
    State#?MODULE{msg_bytes_checkout = Checkout - Size,
                  msg_bytes_enqueue = Enqueue + Size}.

message_size(#basic_message{content = Content}) ->
    #content{payload_fragments_rev = PFR} = Content,
    iolist_size(PFR);
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
    Cons = maps:fold(fun({_, P},
                         #consumer{status = suspected_down},
                         Acc)
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
    SmallestMsgsRaIdx = case lqueue:get(Messages, empty) of
                            ?MSG(I, _) ->
                                I;
                            _ ->
                                undefined
                        end,
    SmallestRaIdx = rabbit_fifo_index:smallest(Indexes),
    lists:min([SmallestDlxRaIdx, SmallestMsgsRaIdx, SmallestRaIdx]).

make_requeue(ConsumerId, Notify, [{MsgId, Idx, Header, Msg}], Acc) ->
    lists:reverse([{append,
                    #requeue{consumer_id = ConsumerId,
                             index = Idx,
                             header = Header,
                             msg_id = MsgId,
                             msg = Msg},
                    Notify}
                   | Acc]);
make_requeue(ConsumerId, Notify, [{MsgId, Idx, Header, Msg} | Rem], Acc) ->
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

can_immediately_deliver(#?MODULE{service_queue = SQ,
                                 consumers = Consumers} = State) ->
    case messages_ready(State) of
        0 when map_size(Consumers) > 0 ->
            %% TODO: is is probably good enough but to be 100% we'd need to
            %% scan all consumers and ensure at least one has credit
            priority_queue:is_empty(SQ) == false;
        _ ->
            false
    end.

incr(I) ->
   I + 1.
