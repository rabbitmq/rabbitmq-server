%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved.
%% The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.
%% All rights reserved.
-module(rabbit_fifo).

-behaviour(ra_machine).

-compile(inline_list_funcs).
-compile(inline).
-compile({no_auto_import, [apply/3]}).
-dialyzer({nowarn_function, convert_v7_to_v8/2}).
-dialyzer(no_improper_lists).

-include("rabbit_fifo.hrl").
-include_lib("kernel/include/logger.hrl").

-define(STATE, ?MODULE).
-define(DLX, rabbit_fifo_dlx).

-define(CONSUMER_PID(Pid), #consumer{cfg = #consumer_cfg{pid = Pid}}).
-define(CONSUMER_PRIORITY(P), #consumer{cfg = #consumer_cfg{priority = P}}).
-define(CONSUMER_TAG_PID(Tag, Pid),
        #consumer{cfg = #consumer_cfg{tag = Tag,
                                      pid = Pid}}).

-define(ENQ_OVERHEAD, 256).
-ifdef(TEST).
-define(SIZE(Msg),
        case mc:is(Msg) of
            true ->
                mc:size(Msg);
            false when is_binary(Msg) ->
                {0, byte_size(Msg)};
            false ->
                {0, erts_debug:size(Msg)}
        end).
-else.
-define(SIZE(Msg), mc:size(Msg)).
-endif.

-export([
         %% ra_machine callbacks
         init/1,
         apply/3,
         live_indexes/1,
         snapshot_installed/4,
         state_enter/2,
         tick/2,
         overview/1,

         get_checked_out/4,
         %% versioning
         version/0,
         which_module/1,
         %% aux
         init_aux/1,
         handle_aux/5,
         % queries
         query_messages_ready/1,
         query_messages_checked_out/1,
         query_messages_total/1,
         query_processes/1,
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
         get_msg_header/1,
         get_header/2,
         annotate_msg/2,
         get_msg_from_cmd/1,

         %% protocol helpers
         make_enqueue/3,
         make_register_enqueuer/1,
         make_checkout/3,
         make_settle/2,
         make_return/2,
         is_return/1,
         make_discard/2,
         make_credit/4,
         make_modify/5,
         make_purge/0,
         make_purge_nodes/1,
         make_update_config/1,
         make_garbage_collection/0,

         exec_read/3

        ]).

-ifdef(TEST).
-export([update_header/4,
         chunk_disk_msgs/3,
         smallest_raft_index/1,
         make_requeue/4]).
-endif.

-import(serial_number, [add/2, diff/2]).
-define(ENQ_V2, e).

%% command records representing all the protocol actions that are supported
-record(enqueue, {pid :: option(pid()),
                  seq :: option(msg_seqno()),
                  msg :: raw_msg()}).
-record(?ENQ_V2, {seq :: option(msg_seqno()),
                  msg :: raw_msg(),
                  size :: {MetadataSize :: non_neg_integer(),
                           PayloadSize :: non_neg_integer()}}).
-record(requeue, {consumer_key :: consumer_key(),
                  msg_id :: msg_id(),
                  index :: ra:index(),
                  header :: msg_header(),
                  msg :: raw_msg()}).
-record(register_enqueuer, {pid :: pid()}).
-record(checkout, {consumer_id :: consumer_id(),
                   spec :: checkout_spec(),
                   meta :: consumer_meta()}).
-record(settle, {consumer_key :: consumer_key(),
                 msg_ids :: [msg_id()]}).
-record(return, {consumer_key :: consumer_key(),
                 msg_ids :: [msg_id()]}).
-record(discard, {consumer_key :: consumer_key(),
                  msg_ids :: [msg_id()]}).
-record(credit, {consumer_key :: consumer_key(),
                 credit :: non_neg_integer(),
                 delivery_count :: rabbit_queue_type:delivery_count(),
                 drain :: boolean()}).
-record(modify, {consumer_key :: consumer_key(),
                 msg_ids :: [msg_id()],
                 delivery_failed :: boolean(),
                 undeliverable_here :: boolean(),
                 annotations :: mc:annotations()}).
-record(purge, {}).
-record(purge_nodes, {nodes :: [node()]}).
-record(update_config, {config :: config()}).
-record(garbage_collection, {}).

-opaque protocol() ::
    #enqueue{} |
    #?ENQ_V2{} |
    #requeue{} |
    #register_enqueuer{} |
    #checkout{} |
    #settle{} |
    #return{} |
    #discard{} |
    #credit{} |
    #modify{} |
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

-opaque state() :: #?STATE{}.

-export_type([protocol/0,
              delivery/0,
              command/0,
              credit_mode/0,
              consumer_meta/0,
              consumer_id/0,
              consumer_key/0,
              client_msg/0,
              msg/0,
              msg_id/0,
              msg_seqno/0,
              delivery_msg/0,
              state/0,
              config/0]).

-spec init(config()) -> state().
init(#{name := Name,
       queue_resource := Resource} = Conf) ->
    update_config(Conf, #?STATE{cfg = #cfg{name = Name,
                                           resource = Resource}}).

update_config(Conf, State) ->
    DLH = maps:get(dead_letter_handler, Conf, undefined),
    % BLH = maps:get(become_leader_handler, Conf, undefined),
    Overflow = maps:get(overflow_strategy, Conf, drop_head),
    MaxLength = maps:get(max_length, Conf, undefined),
    MaxBytes = maps:get(max_bytes, Conf, undefined),
    DeliveryLimit = case maps:get(delivery_limit, Conf, undefined) of
                        DL when is_number(DL) andalso
                                DL < 0 ->
                            undefined;
                        DL ->
                            DL
                    end,

    Expires = maps:get(expires, Conf, undefined),
    MsgTTL = maps:get(msg_ttl, Conf, undefined),
    ConsumerStrategy = case maps:get(single_active_consumer_on, Conf, false) of
                           true ->
                               single_active;
                           false ->
                               competing
                       end,
    Cfg = State#?STATE.cfg,

    LastActive = maps:get(created, Conf, undefined),
    State#?STATE{cfg = Cfg#cfg{dead_letter_handler = DLH,
                               overflow_strategy = Overflow,
                               max_length = MaxLength,
                               max_bytes = MaxBytes,
                               consumer_strategy = ConsumerStrategy,
                               delivery_limit = DeliveryLimit,
                               expires = Expires,
                               msg_ttl = MsgTTL},
                 last_active = LastActive}.

% msg_ids are scoped per consumer
-spec apply(ra_machine:command_meta_data(), command(), state()) ->
    {state(), ra_machine:reply(), ra_machine:effects() | ra_machine:effect()} |
    {state(), ra_machine:reply()}.
apply(Meta, {machine_version, FromVersion, ToVersion}, VXState) ->
    %% machine version upgrades cant be done in apply_
    State = convert(Meta, FromVersion, ToVersion, VXState),
    %% TODO: force snapshot now?
    {State, ok, [{aux, {dlx, setup}}]};
apply(Meta, Cmd, #?STATE{discarded_bytes = DiscBytes} = State) ->
    %% add estimated discared_bytes
    %% TODO: optimise!
    %% this is the simplest way to record the discarded bytes for most
    %% commands but it is a bit mory garby as almost always creates a new
    %% state copy before even processing the command
    Bytes = estimate_discarded_size(Cmd),
    apply_(Meta, Cmd, State#?STATE{discarded_bytes = DiscBytes + Bytes}).

apply_(Meta, #enqueue{pid = From, seq = Seq,
                      msg = RawMsg}, State00) ->
    apply_enqueue(Meta, From, Seq, RawMsg, message_size(RawMsg), State00);
apply_(#{reply_mode := {notify, _Corr, EnqPid}} = Meta,
      #?ENQ_V2{seq = Seq, msg = RawMsg, size = Size}, State00) ->
    apply_enqueue(Meta, EnqPid, Seq, RawMsg, Size, State00);
apply_(Meta, #?ENQ_V2{seq = Seq, msg = RawMsg, size = Size}, State00) ->
    %% untracked
    apply_enqueue(Meta, undefined, Seq, RawMsg, Size, State00);
apply_(_Meta, #register_enqueuer{pid = Pid},
      #?STATE{enqueuers = Enqueuers0,
              cfg = #cfg{overflow_strategy = Overflow}} = State0) ->
    State = case maps:is_key(Pid, Enqueuers0) of
                true ->
                    %% if the enqueuer exits just echo the overflow state
                    State0;
                false ->
                    State0#?STATE{enqueuers = Enqueuers0#{Pid => #enqueuer{}}}
            end,
    Res = case is_over_limit(State) of
              true when Overflow == reject_publish ->
                  reject_publish;
              _ ->
                  ok
          end,
    {State, Res, [{monitor, process, Pid}]};
apply_(Meta, #settle{msg_ids = MsgIds,
                     consumer_key = Key},
      #?STATE{consumers = Consumers} = State) ->
    case find_consumer(Key, Consumers) of
        {ConsumerKey, Con0} ->
            %% find_consumer/2 returns the actual consumer key even if
            %% if id was passed instead for example
            complete_and_checkout(Meta, MsgIds, ConsumerKey,
                                  Con0, [], State);
        _ ->
            {State, ok}
    end;
apply_(Meta, #discard{consumer_key = ConsumerKey,
                      msg_ids = MsgIds},
      #?STATE{consumers = Consumers } = State0) ->
    case find_consumer(ConsumerKey, Consumers) of
        {ActualConsumerKey, #consumer{} = Con} ->
            discard(Meta, MsgIds, ActualConsumerKey, Con, true, #{}, State0);
        _ ->
            {State0, ok}
    end;
apply_(Meta, #return{consumer_key = ConsumerKey,
                     msg_ids = MsgIds},
       #?STATE{consumers = Cons} = State) ->
    case find_consumer(ConsumerKey, Cons) of
        {ActualConsumerKey, #consumer{checked_out = Checked}} ->
            return(Meta, ActualConsumerKey, MsgIds, false,
                   #{}, Checked, [], State);
        _ ->
            {State, ok}
    end;
apply_(Meta, #modify{consumer_key = ConsumerKey,
                     delivery_failed = DeliveryFailed,
                     undeliverable_here = UndelHere,
                     annotations = Anns,
                     msg_ids = MsgIds},
       #?STATE{consumers = Cons} = State) ->
    case find_consumer(ConsumerKey, Cons) of
        {ActualConsumerKey, #consumer{checked_out = Checked}}
          when UndelHere == false ->
            return(Meta, ActualConsumerKey, MsgIds, DeliveryFailed,
                   Anns, Checked, [], State);
        {ActualConsumerKey, #consumer{} = Con}
          when UndelHere == true ->
            discard(Meta, MsgIds, ActualConsumerKey,
                    Con, DeliveryFailed, Anns, State);
        _ ->
            {State, ok}
    end;
apply_(#{index := Idx} = Meta,
      #requeue{consumer_key = ConsumerKey,
               msg_id = MsgId,
               index = _OldIdx,
               header = Header0},
      #?STATE{consumers = Cons,
              messages = Messages} = State00) ->
    %% the actual consumer key was looked up in the aux handler so we
    %% dont need to use find_consumer/2 here
    case Cons of
        #{ConsumerKey := #consumer{checked_out = Checked0} = Con0}
          when is_map_key(MsgId, Checked0) ->
            %% construct a message with the current raft index
            %% and update acquired count before adding it to the message queue
            Header = update_header(acquired_count, fun incr/1, 1, Header0),
            State0 = add_bytes_return(Header, State00),
            Con = Con0#consumer{checked_out = maps:remove(MsgId, Checked0),
                                credit = increase_credit(Con0, 1)},
            State1 = State0#?STATE{messages = rabbit_fifo_pq:in(4,
                                                                ?MSG(Idx, Header),
                                                                Messages)},
            State2 = update_or_remove_con(Meta, ConsumerKey, Con, State1),
            {State3, Effects} = activate_next_consumer({State2, []}),
            checkout(Meta, State0, State3, Effects);
        _ ->
            {State00, ok, []}
    end;
apply_(Meta, #credit{consumer_key = ConsumerKey} = Credit,
      #?STATE{consumers = Cons} = State) ->
    case Cons of
        #{ConsumerKey := Con} ->
            credit_active_consumer(Credit, Con, Meta, State);
        _ ->
            case lists:keytake(ConsumerKey, 1, State#?STATE.waiting_consumers) of
                {value, {_, Con}, Waiting} ->
                    credit_inactive_consumer(Credit, Con, Waiting, State);
                false ->
                    %% credit for unknown consumer - just ignore
                    {State, ok}
            end
    end;
apply_(_, #checkout{spec = {dequeue, _}},
      #?STATE{cfg = #cfg{consumer_strategy = single_active}} = State0) ->
    {State0, {error, {unsupported, single_active_consumer}}};
apply_(#{index := Index,
         system_time := Ts,
         from := From} = Meta,
       #checkout{spec = {dequeue, Settlement},
                 meta = ConsumerMeta,
                 consumer_id = ConsumerId},
       #?STATE{consumers = Consumers} = State00) ->
    %% dequeue always updates last_active
    State0 = State00#?STATE{last_active = Ts},
    %% all dequeue operations result in keeping the queue from expiring
    Exists = find_consumer(ConsumerId, Consumers) /= undefined,
    case messages_ready(State0) of
        0 ->
            {State0, {dequeue, empty}, []};
        _ when Exists ->
            %% a dequeue using the same consumer_id isn't possible at this point
            {State0, {dequeue, empty}};
        _ ->
            {_, State1} = update_consumer(Meta, ConsumerId, ConsumerId, ConsumerMeta,
                                          {once, {simple_prefetch, 1}}, 0,
                                          State0),
            case checkout_one(Meta, false, State1, []) of
                {success, _, MsgId, Msg, _ExpiredMsg, State2, Effects0} ->
                    RaftIdx = get_msg_idx(Msg),
                    Header = get_msg_header(Msg),
                    {State4, Effects1} =
                        case Settlement of
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
                    Effects2 = [reply_log_effect(RaftIdx, MsgId, Header,
                                                 messages_ready(State4), From)
                                | Effects1],
                    {State, Effects} = evaluate_limit(Index, State0,
                                                      State4, Effects2),
                    {State,  '$ra_no_reply', Effects};
                {nochange, _ExpiredMsg = true, State2, Effects0} ->
                    %% All ready messages expired.
                    State3 = State2#?STATE{consumers =
                                           maps:remove(ConsumerId,
                                                       State2#?STATE.consumers)},
                    {State, Effects} = evaluate_limit(Index, State0,
                                                      State3, Effects0),
                    {State, {dequeue, empty}, Effects}
            end
    end;
apply_(Meta, #checkout{spec = Spec,
                       consumer_id = ConsumerId}, State0)
  when Spec == cancel orelse
       Spec == remove ->
    case consumer_key_from_id(ConsumerId, State0) of
        {ok, ConsumerKey} ->
            {State1, Effects1} = activate_next_consumer(
                                   cancel_consumer(Meta, ConsumerKey, State0, [],
                                                   Spec)),
            Reply = {ok, consumer_cancel_info(ConsumerKey, State1)},
            {State, _, Effects} = checkout(Meta, State0, State1, Effects1),
            {State, Reply, Effects};
        error ->
            {State0, {error, consumer_not_found}, []}
    end;
apply_(#{index := Idx} = Meta,
       #checkout{spec = Spec0,
                 meta = ConsumerMeta,
                 consumer_id = {_, Pid} = ConsumerId}, State0) ->
    %% might be better to check machine_version
    IsV4 = tuple_size(Spec0) == 2,
    %% normalise spec format
    Spec = case Spec0 of
               {_, _} ->
                   Spec0;
               {Life, Prefetch, simple_prefetch} ->
                   {Life, {simple_prefetch, Prefetch}};
               {Life, _Credit, credited} ->
                   {Life, credited}
           end,
    Priority = get_consumer_priority(ConsumerMeta),
    ConsumerKey = case consumer_key_from_id(ConsumerId, State0) of
                      {ok, K} ->
                          K;
                      error when IsV4 ->
                          %% if the consumer does not already exist use the
                          %% raft index as it's unique identifier in future
                          %% settle, credit, return and discard operations
                          Idx;
                      error ->
                          ConsumerId
                  end,
    {Consumer, State1} = update_consumer(Meta, ConsumerKey, ConsumerId,
                                         ConsumerMeta, Spec, Priority, State0),
    {State2, Effs} = activate_next_consumer(State1, []),

    %% reply with a consumer infos
    Reply = {ok, consumer_info(ConsumerKey, Consumer, State2)},
    checkout(Meta, State0, State2, [{monitor, process, Pid} | Effs], Reply);
apply_(#{index := Index}, #purge{},
       #?STATE{messages_total = Total} = State0) ->
    NumReady = messages_ready(State0),
    State1 = State0#?STATE{messages = rabbit_fifo_pq:new(),
                           messages_total = Total - NumReady,
                           returns = lqueue:new(),
                           msg_bytes_enqueue = 0
                          },
    Effects0 = [{aux, force_checkpoint}, garbage_collection],
    Reply = {purge, NumReady},
    {State, Effects} = evaluate_limit(Index, State0, State1, Effects0),
    {State, Reply, Effects};
apply_(#{index := _Idx}, #garbage_collection{}, State) ->
    {State, ok, [{aux, garbage_collection}]};
apply_(Meta, {timeout, expire_msgs}, State) ->
    checkout(Meta, State, State, []);
% apply_(#{system_time := Ts} = Meta,
%        {down, Pid, noconnection},
%        #?STATE{consumers = Cons0,
%                cfg = #cfg{consumer_strategy = single_active},
%                waiting_consumers = Waiting0,
%                enqueuers = Enqs0} = State0) ->
%     Node = node(Pid),
%     %% if the pid refers to an active or cancelled consumer,
%     %% mark it as suspected and return it to the waiting queue
%     {State1, Effects0} =
%         maps:fold(
%           fun(CKey, ?CONSUMER_PID(P) = #consumer{status = Status} = C0, {S0, E0})
%                 when is_atom(Status) andalso node(P) =:= Node ->
%                   %% the consumer should be returned to waiting
%                   %% and checked out messages should be returned
%                   Effs = consumer_update_active_effects(
%                            S0, C0, false, {suspected_down, Status} , E0),
%                   %% TODO: set a timer instead of reaturn all here to allow
%                   %% a disconnected node a configurable bit of time to be
%                   %% reconnected
%                   {St, Effs1} = return_all(Meta, S0, Effs, CKey, C0, false),
%                   %% if the consumer was cancelled there is a chance it got
%                   %% removed when returning hence we need to be defensive here
%                   Waiting = case St#?STATE.consumers of
%                                 #{CKey := C} ->
%                                     Waiting0 ++ [{CKey, C}];
%                                 _ ->
%                                     Waiting0
%                             end,
%                   {St#?STATE{consumers = maps:remove(CKey, St#?STATE.consumers),
%                              waiting_consumers = Waiting,
%                              last_active = Ts},
%                    Effs1};
%              (_, _, S) ->
%                   S
%           end, {State0, []}, maps:iterator(Cons0, ordered)),
%     WaitingConsumers = update_waiting_consumer_status(Node, State1,
%                                                       {suspected_down, up}),

%     %% select a new consumer from the waiting queue and run a checkout
%     State2 = State1#?STATE{waiting_consumers = WaitingConsumers},
%     {State, Effects1} = activate_next_consumer(State2, Effects0),

%     %% mark any enquers as suspected
%     Enqs = maps:map(fun(P, E) when node(P) =:= Node ->
%                             E#enqueuer{status = suspected_down};
%                        (_, E) -> E
%                     end, Enqs0),
%     Effects = [{monitor, node, Node} | Effects1],
%     checkout(Meta, State0, State#?STATE{enqueuers = Enqs}, Effects);
apply_(#{system_time := Ts} = Meta,
       {down, Pid, noconnection},
       #?STATE{consumers = Cons0,
               enqueuers = Enqs0} = State0) ->

    %% A node has been disconnected. This doesn't necessarily mean that
    %% any processes on this node are down, they _may_ come back so here
    %% we just mark them as suspected (effectively deactivated)
    %% and return all checked out messages to the main queue for delivery to any
    %% live consumers

    Node = node(Pid),

    {Cons, Effects1} =
        maps:fold(
          fun(CKey, #consumer{cfg = #consumer_cfg{pid = P},
                              status = Status} = C0,
              {Cns0, Eff}) when P =:= Pid ->
                  TargetStatus = case Status of
                                     {suspected_down, T} -> T;
                                     _ ->
                                         Status
                                 end,
                  C = C0#consumer{status = {suspected_down, TargetStatus}},
                  % down consumer still has messages assigned
                  % TODO: make timeout configurable
                  Eff0 = [{timer, {consumer_down_timeout, CKey}, 10_000} | Eff],
                  Eff1 = consumer_update_active_effects(State0, C, false,
                                                        suspected_down, Eff0),
                  {Cns0#{CKey => C}, Eff1};
             (_, _, St) ->
                  St
          end, {Cons0, []}, maps:iterator(Cons0, ordered)),
    Enqs = case Enqs0 of
               #{Pid := E} ->
                   Enqs0#{Pid := E#enqueuer{status = suspected_down}};
               _ ->
                   Enqs0
           end,

    WaitingConsumers = update_waiting_consumer_status(Pid, State0,
                                                      {suspected_down, up}),
    % Monitor the node so that we can "unsuspect" these processes when the node
    % comes back, then re-issue all monitors and discover the final fate of
    % these processes
    Effects = [{monitor, node, Node} | Effects1],
    checkout(Meta, State0, State0#?STATE{enqueuers = Enqs,
                                         waiting_consumers = WaitingConsumers,
                                         consumers = Cons,
                                         last_active = Ts}, Effects);
apply_(Meta, {timeout, {consumer_down_timeout, CKey}},
       #?STATE{cfg = #cfg{consumer_strategy = competing},
               consumers = Consumers} = State0) ->

    case find_consumer(CKey, Consumers) of
        {_CKey, #consumer{status = {suspected_down, _}} = Consumer} ->
            %% the consumer is still suspected and has timed out
            %% return all messages
            {State1, Effects0} = return_all(Meta, State0, [], CKey,
                                            Consumer, false),
            checkout(Meta, State0, State1, Effects0);
        _ ->
            {State0, []}
    end;
apply_(#{system_time := Ts} = Meta, {timeout, {consumer_down_timeout, CKey}},
       #?STATE{cfg = #cfg{consumer_strategy = single_active},
               waiting_consumers = Waiting0,
               consumers = Consumers} = State0) ->

    case find_consumer(CKey, Consumers) of
        {_CKey, #consumer{status = {suspected_down, Status}} = Consumer} ->
            %% the consumer is still suspected and has timed out
            %% return all messages
            {State1, Effects0} = return_all(Meta, State0, [], CKey,
                                            Consumer, false),
            Waiting = case State1#?STATE.consumers of
                          #{CKey := C} when Status =/= cancelled ->
                              Waiting0 ++
                              [{CKey, C#consumer{status = {suspected_down, up}}}];
                          _ ->
                              Waiting0
                      end,
            State2 = State1#?STATE{consumers = maps:remove(CKey, State1#?STATE.consumers),
                                   waiting_consumers = Waiting,
                                   last_active = Ts},
            {State, Effects1} = activate_next_consumer(State2, Effects0),
            checkout(Meta, State0, State, Effects1);
        _ ->
            {State0, []}
    end;
apply_(Meta, {down, Pid, _Info}, State0) ->
    {State1, Effects1} = activate_next_consumer(handle_down(Meta, Pid, State0)),
    checkout(Meta, State0, State1, Effects1);
apply_(Meta, {nodeup, Node}, #?STATE{consumers = Cons0,
                                     enqueuers = Enqs0} = State0) ->
    %% A node we are monitoring has come back.
    %% If we have suspected any processes of being
    %% down we should now re-issue the monitors for them to detect if they're
    %% actually down or not
    %% send leader change events to all disconnected enqueuers to prompt them
    %% to resend any messages stuck during disconnection,
    %% ofc it may not be a leader change per se but it has the same effect
    Effects0 = lists:flatten([[{monitor, process, P},
                               {send_msg, P, leader_change, ra_event}]
                              || P <- suspected_pids_for(Node, State0)]),

    Enqs1 = maps:map(fun(P, E) when node(P) =:= Node ->
                             E#enqueuer{status = up};
                        (_, E) -> E
                     end, Enqs0),

    ConsumerUpdateActiveFun = consumer_active_flag_update_function(State0),
    %% mark all consumers as up
    {State1, Effects1} =
        maps:fold(
          fun(ConsumerKey,
              ?CONSUMER_PID(P) =
              #consumer{status = {suspected_down, NextStatus}} = C,
              {SAcc, EAcc0})
                when node(P) =:= Node ->
                  EAcc1 = ConsumerUpdateActiveFun(SAcc, ConsumerKey,
                                                  C, true, NextStatus, EAcc0),
                  %% cancel timers
                  EAcc = [{timer,
                           {consumer_down_timeout, ConsumerKey},
                           infinity} | EAcc1],

                  {update_or_remove_con(Meta, ConsumerKey,
                                        C#consumer{status = NextStatus},
                                        SAcc), EAcc};
             (_, _, Acc) ->
                  Acc
          end, {State0, Effects0}, maps:iterator(Cons0, ordered)),
    Waiting = update_waiting_consumer_status(Node, State1, up),
    State2 = State1#?STATE{enqueuers = Enqs1,
                           waiting_consumers = Waiting},
    {State, Effects} = activate_next_consumer(State2, Effects1),
    checkout(Meta, State0, State, Effects);
apply_(_, {nodedown, _Node}, State) ->
    {State, ok};
apply_(Meta, #purge_nodes{nodes = Nodes}, State0) ->
    {State, Effects} = lists:foldl(fun(Node, {S, E}) ->
                                           purge_node(Meta, Node, S, E)
                                   end, {State0, []}, Nodes),
    {State, ok, Effects};
apply_(Meta,
       #update_config{config = #{} = Conf},
       #?STATE{cfg = #cfg{dead_letter_handler = OldDLH,
                          resource = QRes},
               dlx = DlxState0} = State0) ->
    NewDLH = maps:get(dead_letter_handler, Conf, OldDLH),
    {DlxState, Effects0} = update_config(OldDLH, NewDLH, QRes,
                                         DlxState0),
    State1 = update_config(Conf, State0#?STATE{dlx = DlxState}),
    checkout(Meta, State0, State1, Effects0);
apply_(Meta, {dlx, _} = Cmd,
       #?STATE{cfg = #cfg{dead_letter_handler = DLH},
               discarded_bytes = DiscardedBytes0,
               dlx = DlxState0} = State0) ->
    {DlxState, DiscardedBytes, Effects0} = dlx_apply(Meta, Cmd, DLH, DlxState0),
    State1 = State0#?STATE{dlx = DlxState,
                           discarded_bytes = DiscardedBytes0 + DiscardedBytes},
    checkout(Meta, State0, State1, Effects0);
apply_(_Meta, Cmd, State) ->
    %% handle unhandled commands gracefully
    ?LOG_DEBUG("rabbit_fifo: unhandled command ~W", [Cmd, 10]),
    {State, ok, []}.

-spec live_indexes(state()) -> [ra:index()].
live_indexes(#?STATE{cfg = #cfg{},
                     returns = Returns,
                     messages = Messages,
                     consumers = Consumers,
                     dlx = #?DLX{discards = Discards}}) ->
    MsgsIdxs = rabbit_fifo_pq:indexes(Messages),
    DlxIndexes = lqueue:fold(fun (?TUPLE(_, Msg), Acc) ->
                                     I = get_msg_idx(Msg),
                                     [I | Acc]
                             end, MsgsIdxs, Discards),
    RtnIndexes = lqueue:fold(fun(Msg, Acc) -> [get_msg_idx(Msg) | Acc] end,
                             DlxIndexes, Returns),
    maps:fold(fun (_Cid, #consumer{checked_out = Ch}, Acc0) ->
                      maps:fold(
                        fun (_MsgId, Msg, Acc) ->
                                [get_msg_idx(Msg) | Acc]
                        end, Acc0, Ch)
              end, RtnIndexes, Consumers).

-spec snapshot_installed(Meta, State, OldMeta, OldState) ->
    ra_machine:effects() when
      Meta :: ra_snapshot:meta(),
      State :: state(),
      OldMeta :: ra_snapshot:meta(),
      OldState :: state().
snapshot_installed(_Meta, #?MODULE{cfg = #cfg{},
                                   consumers = Consumers} = State,
                   _OldMeta, _OldState) ->
    %% here we need to redliver all pending consumer messages
    %% to local consumers
    %% TODO: with some additional state (raft indexes assigned to consumer)
    %% we could reduce the number of resends but it is questionable if this
    %% complexity is worth the effort. rabbit_fifo_client will de-duplicate
    %% deliveries anyway
    SendAcc = maps:fold(
                fun (_ConsumerKey, #consumer{cfg = #consumer_cfg{tag = Tag,
                                                                 pid = Pid},
                                             checked_out = Checked},
                     Acc) ->
                        case node(Pid) == node() of
                            true ->
                                Acc#{{Tag, Pid} => maps:to_list(Checked)};
                            false ->
                                Acc
                        end
                end, #{}, Consumers),
    Effs = add_delivery_effects([], SendAcc, State),
    Effs.

convert_v7_to_v8(#{} = _Meta, StateV7) ->
    %% the structure is intact for now
    Cons0 = element(#?STATE.consumers, StateV7),
    Cons = maps:map(fun (_CKey, #consumer{status = suspected_down} = C) ->
                            C#consumer{status = {suspected_down, up}};
                        (_CKey, C) ->
                            C
                    end, Cons0),
    Msgs = element(#?STATE.messages, StateV7),
    {Hi, No} = rabbit_fifo_q:to_queues(Msgs),
    Pq0 = queue:fold(fun (I, Acc) ->
                             rabbit_fifo_pq:in(9, I, Acc)
                     end, rabbit_fifo_pq:new(), Hi),
    Pq = queue:fold(fun (I, Acc) ->
                            rabbit_fifo_pq:in(4, I, Acc)
                    end, Pq0, No),
    StateV8 = StateV7,
    StateV8#?STATE{discarded_bytes = 0,
                   messages = Pq,
                   consumers = Cons,
                   unused_0 = ?NIL}.

purge_node(Meta, Node, State, Effects) ->
    lists:foldl(fun(Pid, {S0, E0}) ->
                        {S, E} = handle_down(Meta, Pid, S0),
                        {S, E0 ++ E}
                end, {State, Effects},
                all_pids_for(Node, State)).

%% any downs that are not noconnection
handle_down(Meta, Pid, #?STATE{consumers = Cons0,
                               enqueuers = Enqs0} = State0) ->
    % Remove any enqueuer for the down pid
    State1 = State0#?STATE{enqueuers = maps:remove(Pid, Enqs0)},
    {Effects1, State2} = handle_waiting_consumer_down(Pid, State1),
    % return checked out messages to main queue
    % Find the consumers for the down pid
    DownConsumers = maps:filter(fun(_CKey, ?CONSUMER_PID(P)) ->
                                        P =:= Pid
                                end, Cons0),
    DownConsumerKeys = maps_ordered_keys(DownConsumers),
    lists:foldl(fun(ConsumerKey, {S, E}) ->
                        cancel_consumer(Meta, ConsumerKey, S, E, down)
                end, {State2, Effects1}, DownConsumerKeys).

consumer_active_flag_update_function(
  #?STATE{cfg = #cfg{consumer_strategy = competing}}) ->
    fun(State, _ConsumerKey, Consumer, Active, ActivityStatus, Effects) ->
            consumer_update_active_effects(State, Consumer, Active,
                                           ActivityStatus, Effects)
    end;
consumer_active_flag_update_function(
  #?STATE{cfg = #cfg{consumer_strategy = single_active}}) ->
    fun(_, _, _, _, _, Effects) ->
            Effects
    end.

handle_waiting_consumer_down(_Pid,
                             #?STATE{cfg = #cfg{consumer_strategy = competing}}
                             = State) ->
    {[], State};
handle_waiting_consumer_down(_Pid,
                             #?STATE{cfg = #cfg{consumer_strategy = single_active},
                                     waiting_consumers = []} = State) ->
    {[], State};
handle_waiting_consumer_down(Pid,
                             #?STATE{cfg = #cfg{consumer_strategy = single_active},
                                     waiting_consumers = WaitingConsumers0}
                             = State0) ->
    % get cancel effects for down waiting consumers
    Down = lists:filter(fun({_, ?CONSUMER_PID(P)}) -> P =:= Pid end,
                        WaitingConsumers0),
    Effects = lists:foldl(fun ({_ConsumerKey, Consumer}, Effects) ->
                                  ConsumerId = consumer_id(Consumer),
                                  cancel_consumer_effects(ConsumerId, State0,
                                                          Effects)
                          end, [], Down),
    % update state to have only up waiting consumers
    StillUp = lists:filter(fun({_CKey, ?CONSUMER_PID(P)}) ->
                                   P =/= Pid
                           end,
                           WaitingConsumers0),
    State = State0#?STATE{waiting_consumers = StillUp},
    {Effects, State}.

update_waiting_consumer_status(DownPidOrNode,
                               #?STATE{waiting_consumers = WaitingConsumers},
                               Status) ->
    sort_waiting(
      [if is_pid(DownPidOrNode) andalso DownPidOrNode == Pid ->
              {ConsumerKey, Consumer#consumer{status = Status}};
          is_atom(DownPidOrNode) andalso DownPidOrNode == node(Pid) ->
              {ConsumerKey, Consumer#consumer{status = Status}};
          true ->
              {ConsumerKey, Consumer}
       end || {ConsumerKey, ?CONSUMER_PID(Pid) =  Consumer}
              <- WaitingConsumers, Consumer#consumer.status =/= cancelled]).

-spec state_enter(ra_server:ra_state() | eol, state()) ->
    ra_machine:effects().
state_enter(leader,
            #?STATE{consumers = Cons,
                    enqueuers = Enqs,
                    waiting_consumers = WaitingConsumers,
                    cfg = #cfg{resource = QRes,
                               dead_letter_handler = DLH},
                    dlx = DlxState} = State) ->
    TimerEffs = timer_effect(erlang:system_time(millisecond), State, []),
    % return effects to monitor all current consumers and enqueuers
    Pids = lists:usort(maps:keys(Enqs)
                       ++ [P || ?CONSUMER_PID(P) <- maps:values(Cons)]
                       ++ [P || {_, ?CONSUMER_PID(P)} <- WaitingConsumers]),
    Mons = [{monitor, process, P} || P <- Pids],
    Nots = [{send_msg, P, leader_change, ra_event} || P <- Pids],
    NodeMons = lists:usort([{monitor, node, node(P)} || P <- Pids]),
    NotifyDecs = notify_decorators_startup(QRes),
    Effects = TimerEffs ++ Mons ++ Nots ++ NodeMons ++ [NotifyDecs],

    case DLH of
        at_least_once ->
            ensure_worker_started(QRes, DlxState);
        _ ->
            ok
    end,
    Effects;
state_enter(eol, #?STATE{enqueuers = Enqs,
                         consumers = Cons0,
                         waiting_consumers = WaitingConsumers0}) ->
    Custs = maps:fold(fun(_K, ?CONSUMER_PID(P) = V, S) ->
                              S#{P => V}
                      end, #{}, Cons0),
    WaitingConsumers1 = lists:foldl(fun({_, ?CONSUMER_PID(P) = V}, Acc) ->
                                            Acc#{P => V}
                                    end, #{}, WaitingConsumers0),
    AllConsumers = maps:merge(Custs, WaitingConsumers1),
    [{send_msg, P, eol, ra_event}
     || P <- maps:keys(maps:merge(Enqs, AllConsumers))] ++
    [{aux, eol}];
state_enter(_, #?STATE{cfg = #cfg{dead_letter_handler = DLH,
                                  resource = _QRes},
                       dlx = DlxState}) ->
    case DLH of
        at_least_once ->
            ensure_worker_terminated(DlxState);
        _ ->
            ok
    end,
    %% catch all as not handling all states
    [].

-spec tick(non_neg_integer(), state()) -> ra_machine:effects().
tick(Ts, #?STATE{cfg = #cfg{resource = QName}} = State) ->
    case is_expired(Ts, State) of
        true ->
            [{mod_call, rabbit_quorum_queue, spawn_deleter, [QName]}];
        false ->
            [{aux, {handle_tick, [QName, overview(State), all_nodes(State)]}}]
    end.

-spec overview(state()) -> map().
overview(#?STATE{consumers = Cons,
                 enqueuers = Enqs,
                 msg_bytes_enqueue = EnqueueBytes,
                 msg_bytes_checkout = CheckoutBytes,
                 cfg = Cfg,
                 dlx = DlxState,
                 discarded_bytes = DiscardedBytes,
                 messages = Messages,
                 returns = Returns,
                 waiting_consumers = WaitingConsumers} = State) ->
    Conf = #{name => Cfg#cfg.name,
             resource => Cfg#cfg.resource,
             dead_lettering_enabled => undefined =/= Cfg#cfg.dead_letter_handler,
             dead_letter_handler => Cfg#cfg.dead_letter_handler,
             overflow_strategy => Cfg#cfg.overflow_strategy,
             max_length => Cfg#cfg.max_length,
             max_bytes => Cfg#cfg.max_bytes,
             consumer_strategy => Cfg#cfg.consumer_strategy,
             expires => Cfg#cfg.expires,
             msg_ttl => Cfg#cfg.msg_ttl,
             delivery_limit => Cfg#cfg.delivery_limit},
    SacOverview = case active_consumer(Cons) of
                      {SacConsumerKey, SacCon} ->
                          SacConsumerId = consumer_id(SacCon),
                          NumWaiting = length(WaitingConsumers),
                          #{single_active_consumer_id => SacConsumerId,
                            single_active_consumer_key => SacConsumerKey,
                            single_active_num_waiting_consumers => NumWaiting};
                      _ ->
                          #{}
                  end,
    MsgsRet = lqueue:len(Returns),
    %% TODO emit suitable overview metrics
    #{
      % num_hi := MsgsHi,
      % num_no := MsgsNo
     } = rabbit_fifo_pq:overview(Messages),

    Overview = #{type => ?STATE,
                 config => Conf,
                 num_consumers => map_size(Cons),
                 num_active_consumers => query_consumer_count(State),
                 num_checked_out => num_checked_out(State),
                 num_enqueuers => maps:size(Enqs),
                 num_ready_messages => messages_ready(State),
                 % num_ready_messages_high => MsgsHi,
                 % num_ready_messages_normal => MsgsNo,
                 num_ready_messages_return => MsgsRet,
                 num_messages => messages_total(State),
                 enqueue_message_bytes => EnqueueBytes,
                 checkout_message_bytes => CheckoutBytes,
                 discarded_bytes => DiscardedBytes,
                 smallest_raft_index => smallest_raft_index(State)
                 },
    DlxOverview = dlx_overview(DlxState),
    maps:merge(maps:merge(Overview, DlxOverview), SacOverview).

-spec get_checked_out(consumer_key(), msg_id(), msg_id(), state()) ->
    [delivery_msg()].
get_checked_out(CKey, From, To, #?STATE{consumers = Consumers}) ->
    case find_consumer(CKey, Consumers) of
        {_CKey, #consumer{checked_out = Checked}} ->
            [begin
                 Msg = maps:get(K, Checked),
                 I = get_msg_idx(Msg),
                 H = get_msg_header(Msg),
                 {K, {I, H}}
             end || K <- lists:seq(From, To), maps:is_key(K, Checked)];
        _ ->
            []
    end.

-spec version() -> pos_integer().
version() -> 8.

which_module(0) -> rabbit_fifo_v0;
which_module(1) -> rabbit_fifo_v1;
which_module(2) -> rabbit_fifo_v3;
which_module(3) -> rabbit_fifo_v3;
which_module(4) -> rabbit_fifo_v7;
which_module(5) -> rabbit_fifo_v7;
which_module(6) -> rabbit_fifo_v7;
which_module(7) -> rabbit_fifo_v7;
which_module(8) -> ?MODULE.

-define(AUX, aux_v4).

-record(snapshot, {index :: ra:index(),
                   timestamp :: milliseconds(),
                   messages_total = 0 :: non_neg_integer(),
                   discarded_bytes = 0 :: non_neg_integer()}).
-record(aux_gc, {last_raft_idx = 0 :: ra:index()}).
-record(?AUX, {name :: atom(),
               last_decorators_state :: term(),
               unused_1 :: term(),
               gc = #aux_gc{} :: #aux_gc{},
               tick_pid :: undefined | pid(),
               cache = #{} :: map(),
               last_checkpoint :: tuple() | #snapshot{},
               bytes_in = 0 :: non_neg_integer(),
               bytes_out = 0 :: non_neg_integer()}).

init_aux(Name) when is_atom(Name) ->
    %% TODO: catch specific exception throw if table already exists
    ok = ra_machine_ets:create_table(rabbit_fifo_usage,
                                     [named_table, set, public,
                                      {write_concurrency, true}]),
    #?AUX{name = Name,
          last_checkpoint = #snapshot{index = 0,
                                      timestamp = erlang:system_time(millisecond),
                                      messages_total = 0}}.

handle_aux(RaftState, Tag, Cmd, AuxV2, RaAux)
  when element(1, AuxV2) == aux_v2 ->
    Name = element(2, AuxV2),
    AuxV3 = init_aux(Name),
    handle_aux(RaftState, Tag, Cmd, AuxV3, RaAux);
handle_aux(RaftState, Tag, Cmd, AuxV3, RaAux)
  when element(1, AuxV3) == aux_v3 ->
    AuxV4 = #?AUX{name = element(2, AuxV3),
                  last_decorators_state = element(3, AuxV3),
                  unused_1 = undefined,
                  gc = element(5, AuxV3),
                  tick_pid  = element(6, AuxV3),
                  cache = element(7, AuxV3),
                  last_checkpoint = element(8, AuxV3),
                  bytes_in = element(9, AuxV3),
                  bytes_out = 0},
    handle_aux(RaftState, Tag, Cmd, AuxV4, RaAux);
handle_aux(leader, cast, eval,
           #?AUX{last_decorators_state = LastDec,
                 % bytes_in = BytesIn,
                 % bytes_out = BytesOut,
                 last_checkpoint = Check0} = Aux0,
           RaAux) ->
    #?STATE{cfg = #cfg{resource = QName},
            discarded_bytes = DiscardedBytes} = MacState =
        ra_aux:machine_state(RaAux),

    Ts = erlang:system_time(millisecond),
    EffMacVer = try ra_aux:effective_machine_version(RaAux) of
                    V -> V
                catch _:_ ->
                          %% this function is not available in older aux states.
                          %% this is a guess
                          undefined
                end,
    {Check, Effects0} = do_snapshot(EffMacVer, Ts, Check0, RaAux,
                                    DiscardedBytes, false),

    %% this is called after each batch of commands have been applied
    %% set timer for message expire
    %% should really be the last applied index ts but this will have to do
    Effects1 = timer_effect(Ts, MacState, Effects0),
    case query_notify_decorators_info(MacState) of
        LastDec ->
            {no_reply, Aux0#?AUX{last_checkpoint = Check}, RaAux, Effects1};
        {MaxActivePriority, IsEmpty} = NewLast ->
            Effects = [notify_decorators_effect(QName, MaxActivePriority, IsEmpty)
                       | Effects1],
            {no_reply, Aux0#?AUX{last_checkpoint = Check,
                                 last_decorators_state = NewLast}, RaAux, Effects}
    end;
handle_aux(_RaftState, cast, eval,
           #?AUX{last_checkpoint = Check0} = Aux0, RaAux) ->
    Ts = erlang:system_time(millisecond),
    EffMacVer = ra_aux:effective_machine_version(RaAux),
    #?STATE{discarded_bytes = DiscardedBytes} = ra_aux:machine_state(RaAux),
    {Check, Effects} = do_snapshot(EffMacVer, Ts, Check0, RaAux,
                                      DiscardedBytes, false),
    {no_reply, Aux0#?AUX{last_checkpoint = Check}, RaAux, Effects};
handle_aux(_RaftState, cast, {bytes_in, {MetaSize, BodySize}},
           #?AUX{bytes_in = Bytes} = Aux0,
           RaAux) ->
    {no_reply, Aux0#?AUX{bytes_in = Bytes + MetaSize + BodySize}, RaAux, []};
handle_aux(_RaftState, cast, {bytes_out, BodySize},
           #?AUX{bytes_out = Bytes} = Aux0,
           RaAux) ->
    {no_reply, Aux0#?AUX{bytes_out = Bytes + BodySize}, RaAux, []};
handle_aux(_RaftState, cast, {#return{msg_ids = MsgIds,
                                      consumer_key = Key} = Ret, Corr, Pid},
           Aux0, RaAux0) ->
    case ra_aux:machine_state(RaAux0) of
        #?STATE{cfg = #cfg{delivery_limit = undefined},
                consumers = Consumers} ->
            case find_consumer(Key, Consumers) of
                {ConsumerKey, #consumer{checked_out = Checked}} ->
                    {RaAux, ToReturn} =
                        maps:fold(
                          fun (MsgId, Msg, {RA0, Acc}) ->
                                  Idx = get_msg_idx(Msg),
                                  Header = get_msg_header(Msg),
                                  %% it is possible this is not found if the consumer
                                  %% crashed and the message got removed
                                  case ra_aux:log_fetch(Idx, RA0) of
                                      {{_Term, _Meta, Cmd}, RA} ->
                                          Msg = get_msg_from_cmd(Cmd),
                                          {RA, [{MsgId, Idx, Header, Msg} | Acc]};
                                      {undefined, RA} ->
                                          {RA, Acc}
                                  end
                          end, {RaAux0, []}, maps:with(MsgIds, Checked)),

                    Appends = make_requeue(ConsumerKey, {notify, Corr, Pid},
                                           lists:sort(ToReturn), []),
                    {no_reply, Aux0, RaAux, Appends};
                _ ->
                    {no_reply, Aux0, RaAux0}
            end;
        _ ->
            %% for returns with a delivery limit set we can just return as before
            {no_reply, Aux0, RaAux0, [{append, Ret, {notify, Corr, Pid}}]}
    end;
handle_aux(leader, _, {handle_tick, [QName, Overview0, Nodes]},
           #?AUX{tick_pid = Pid} = Aux, RaAux) ->
    Overview = Overview0#{members_info => ra_aux:members_info(RaAux)},
    NewPid =
        case process_is_alive(Pid) of
            false ->
                %% No active TICK pid
                %% this function spawns and returns the tick process pid
                rabbit_quorum_queue:handle_tick(QName, Overview, Nodes);
            true ->
                %% Active TICK pid, do nothing
                Pid
        end,

    %% TODO: check consumer timeouts
    {no_reply, Aux#?AUX{tick_pid = NewPid}, RaAux, []};
handle_aux(_, _, {get_checked_out, ConsumerKey, MsgIds}, Aux0, RaAux0) ->
    #?STATE{cfg = #cfg{},
            consumers = Consumers} = ra_aux:machine_state(RaAux0),
    case Consumers of
        #{ConsumerKey := #consumer{checked_out = Checked}} ->
            {RaState, IdMsgs} =
                maps:fold(
                  fun (MsgId, Msg, {S0, Acc}) ->
                          Idx = get_msg_idx(Msg),
                          Header = get_msg_idx(Msg),
                          %% it is possible this is not found if the consumer
                          %% crashed and the message got removed
                          case ra_aux:log_fetch(Idx, S0) of
                              {{_Term, _Meta, Cmd}, S} ->
                                  {S, [{MsgId, {Header, get_msg_from_cmd(Cmd)}} | Acc]};
                              {undefined, S} ->
                                  {S, Acc}
                          end
                  end, {RaAux0, []}, maps:with(MsgIds, Checked)),
            {reply, {ok,  IdMsgs}, Aux0, RaState};
        _ ->
            {reply, {error, consumer_not_found}, Aux0, RaAux0}
    end;
handle_aux(_RaState, cast, tick, #?AUX{name = _Name} = State0,
           RaAux) ->
    Aux = eval_gc(RaAux, ra_aux:machine_state(RaAux), State0),
    {no_reply, Aux, RaAux, []};
handle_aux(_RaState, cast, eol, #?AUX{name = Name} = Aux, RaAux) ->
    ets:delete(rabbit_fifo_usage, Name),
    {no_reply, Aux, RaAux};
handle_aux(_RaState, {call, _From}, oldest_entry_timestamp,
           #?AUX{cache = Cache} = Aux0, RaAux0) ->
    {CachedIdx, CachedTs} = maps:get(oldest_entry, Cache,
                                     {undefined, undefined}),
    case smallest_raft_index(ra_aux:machine_state(RaAux0)) of
        %% if there are no entries, we return current timestamp
        %% so that any previously obtained entries are considered
        %% older than this
        undefined ->
            Aux1 = Aux0#?AUX{cache = maps:remove(oldest_entry, Cache)},
            {reply, {ok, erlang:system_time(millisecond)}, Aux1, RaAux0};
        CachedIdx ->
            %% cache hit
            {reply, {ok, CachedTs}, Aux0, RaAux0};
        Idx when is_integer(Idx) ->
            case ra_aux:log_fetch(Idx, RaAux0) of
                {{_Term, #{ts := Timestamp}, _Cmd}, RaAux} ->
                    Aux1 = Aux0#?AUX{cache = Cache#{oldest_entry =>
                                                    {Idx, Timestamp}}},
                    {reply, {ok, Timestamp}, Aux1, RaAux};
                {undefined, RaAux} ->
                    %% fetch failed
                    {reply, {error, failed_to_get_timestamp}, Aux0, RaAux}
            end
    end;
handle_aux(_RaState, {call, _From}, {peek, Pos}, Aux0,
           RaAux0) ->
    MacState = ra_aux:machine_state(RaAux0),
    case query_peek(Pos, MacState) of
        {ok, Msg} ->
            Idx = get_msg_idx(Msg),
            Header = get_msg_header(Msg),
            %% need to re-hydrate from the log
            {{_, _, Cmd}, RaAux} = ra_aux:log_fetch(Idx, RaAux0),
            ActualMsg = get_msg_from_cmd(Cmd),
            {reply, {ok, {Header, ActualMsg}}, Aux0, RaAux};
        Err ->
            {reply, Err, Aux0, RaAux0}
    end;
handle_aux(_, _, garbage_collection, Aux, RaAux) ->
    {no_reply, force_eval_gc(RaAux, Aux), RaAux};
handle_aux(_RaState, _, force_checkpoint,
           #?AUX{last_checkpoint  = Check0} = Aux, RaAux) ->
    Ts = erlang:system_time(millisecond),
    #?STATE{cfg = #cfg{resource = QR},
            discarded_bytes = DiscardedBytes} = ra_aux:machine_state(RaAux),
    ?LOG_DEBUG("~ts: rabbit_fifo: forcing checkpoint at ~b",
               [rabbit_misc:rs(QR), ra_aux:last_applied(RaAux)]),
    EffMacVer = ra_aux:effective_machine_version(RaAux),
    {Check, Effects} = do_snapshot(EffMacVer, Ts, Check0, RaAux,
                                   DiscardedBytes, true),
    {no_reply, Aux#?AUX{last_checkpoint = Check}, RaAux, Effects};
handle_aux(leader, _, {dlx, setup}, Aux, RaAux) ->
    #?STATE{dlx = DlxState,
            cfg = #cfg{dead_letter_handler = DLH,
                       resource = QRes}} = ra_aux:machine_state(RaAux),
    case DLH of
        at_least_once ->
            ensure_worker_started(QRes, DlxState);
        _ ->
            ok
    end,
    {no_reply, Aux, RaAux}.

eval_gc(RaAux, MacState,
        #?AUX{gc = #aux_gc{last_raft_idx = LastGcIdx} = Gc} = AuxState) ->
    {Idx, _} = ra_aux:log_last_index_term(RaAux),
    #?STATE{cfg = #cfg{resource = QR}} = ra_aux:machine_state(RaAux),
    {memory, Mem} = erlang:process_info(self(), memory),
    case messages_total(MacState) of
        0 when Idx > LastGcIdx andalso
               Mem > ?GC_MEM_LIMIT_B ->
            garbage_collect(),
            {memory, MemAfter} = erlang:process_info(self(), memory),
            ?LOG_DEBUG("~ts: full GC sweep complete. "
                            "Process memory changed from ~.2fMB to ~.2fMB.",
                            [rabbit_misc:rs(QR), Mem/?MB, MemAfter/?MB]),
            AuxState#?AUX{gc = Gc#aux_gc{last_raft_idx = Idx}};
        _ ->
            AuxState
    end.

force_eval_gc(RaAux,
              #?AUX{gc = #aux_gc{last_raft_idx = LastGcIdx} = Gc} = AuxState) ->
    {Idx, _} = ra_aux:log_last_index_term(RaAux),
    #?STATE{cfg = #cfg{resource = QR}} = ra_aux:machine_state(RaAux),
    {memory, Mem} = erlang:process_info(self(), memory),
    case Idx > LastGcIdx of
        true ->
            garbage_collect(),
            {memory, MemAfter} = erlang:process_info(self(), memory),
            ?LOG_DEBUG("~ts: full GC sweep complete. "
                            "Process memory changed from ~.2fMB to ~.2fMB.",
                             [rabbit_misc:rs(QR), Mem/?MB, MemAfter/?MB]),
            AuxState#?AUX{gc = Gc#aux_gc{last_raft_idx = Idx}};
        false ->
            AuxState
    end.

process_is_alive(Pid) when is_pid(Pid) ->
    is_process_alive(Pid);
process_is_alive(_) ->
    false.
%%% Queries

query_messages_ready(State) ->
    messages_ready(State).

query_messages_checked_out(#?STATE{consumers = Consumers}) ->
    maps:fold(fun (_, #consumer{checked_out = C}, S) ->
                      maps:size(C) + S
              end, 0, Consumers).

query_messages_total(State) ->
    messages_total(State).

query_processes(#?STATE{enqueuers = Enqs, consumers = Cons0}) ->
    Cons = maps:fold(fun(_, ?CONSUMER_PID(P) = V, S) ->
                             S#{P => V}
                     end, #{}, Cons0),
    maps:keys(maps:merge(Enqs, Cons)).


query_waiting_consumers(#?STATE{waiting_consumers = WaitingConsumers}) ->
    WaitingConsumers.

query_consumer_count(#?STATE{consumers = Consumers,
                             waiting_consumers = WaitingConsumers}) ->
    Up = maps:filter(fun(_ConsumerKey, #consumer{status = Status}) ->
                             %% TODO: should this really not include suspected
                             %% consumers?
                             is_atom(Status)
                     end, Consumers),
    maps:size(Up) + length(WaitingConsumers).

query_consumers(#?STATE{consumers = Consumers,
                        waiting_consumers = WaitingConsumers,
                        cfg = #cfg{consumer_strategy = ConsumerStrategy}}
                = State) ->
    ActiveActivityStatusFun =
    case ConsumerStrategy of
            competing ->
                fun(_ConsumerKey, #consumer{status = Status}) ->
                        case Status of
                            {suspected_down, _}  ->
                                {false, suspected_down};
                            _ ->
                                {true, Status}
                        end
                end;
            single_active ->
                SingleActiveConsumer = query_single_active_consumer(State),
                fun(_, ?CONSUMER_TAG_PID(Tag, Pid)) ->
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
                      (Key,
                       #consumer{cfg = #consumer_cfg{tag = Tag,
                                                     pid = Pid,
                                                     meta = Meta}} = Consumer,
                       Acc) ->
                          {Active, ActivityStatus} =
                              ActiveActivityStatusFun(Key, Consumer),
                          maps:put(Key,
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
            lists:foldl(
              fun ({_, #consumer{status = cancelled}},
                   Acc) ->
                      Acc;
                  ({Key,
                    #consumer{cfg = #consumer_cfg{tag = Tag,
                                                  pid = Pid,
                                                  meta = Meta}} = Consumer},
                   Acc) ->
                      {Active, ActivityStatus} =
                          ActiveActivityStatusFun(Key, Consumer),
                      maps:put(Key,
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


query_single_active_consumer(#?STATE{cfg = #cfg{consumer_strategy = single_active},
                                     consumers = Consumers}) ->
    case active_consumer(Consumers) of
        undefined ->
            {error, no_value};
        {_CKey, ?CONSUMER_TAG_PID(Tag, Pid)} ->
            {value, {Tag, Pid}}
    end;
query_single_active_consumer(_) ->
    disabled.

query_stat(#?STATE{consumers = Consumers} = State) ->
    {messages_ready(State), maps:size(Consumers)}.

query_in_memory_usage(#?STATE{ }) ->
    {0, 0}.

query_stat_dlx(#?STATE{dlx = DlxState}) ->
    dlx_stat(DlxState).

query_peek(Pos, State0) when Pos > 0 ->
    case take_next_msg(State0) of
        empty ->
            {error, no_message_at_pos};
        {Msg, _State}
          when Pos == 1 ->
            {ok, unpack(Msg)};
        {_Msg, State} ->
            query_peek(Pos-1, State)
    end.

unpack(Packed) when ?IS_PACKED(Packed) ->
    ?MSG(?PACKED_IDX(Packed), ?PACKED_SZ(Packed));
unpack(Msg) ->
    Msg.

query_notify_decorators_info(#?STATE{consumers = Consumers} = State) ->
    MaxActivePriority = maps:fold(
                          fun(_, #consumer{credit = C,
                                           status = up,
                                           cfg = #consumer_cfg{priority = P}},
                              MaxP) when C > 0 ->
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

messages_ready(#?STATE{messages = M,
                       returns = R}) ->
    rabbit_fifo_pq:len(M) + lqueue:len(R).

messages_total(#?STATE{messages_total = Total,
                       dlx = DlxState}) ->
    {DlxTotal, _} = dlx_stat(DlxState),
    Total + DlxTotal.

num_checked_out(#?STATE{consumers = Cons}) ->
    maps:fold(fun (_, #consumer{checked_out = C}, Acc) ->
                      maps:size(C) + Acc
              end, 0, Cons).

cancel_consumer(Meta, ConsumerKey,
                #?STATE{cfg = #cfg{consumer_strategy = competing}} = State,
                Effects, Reason) ->
    cancel_consumer0(Meta, ConsumerKey, State, Effects, Reason);
cancel_consumer(Meta, ConsumerKey,
                #?STATE{cfg = #cfg{consumer_strategy = single_active},
                        waiting_consumers = []} = State,
                Effects, Reason) ->
    %% single active consumer on, no consumers are waiting
    cancel_consumer0(Meta, ConsumerKey, State, Effects, Reason);
cancel_consumer(Meta, ConsumerKey,
                #?STATE{consumers = Cons0,
                        cfg = #cfg{consumer_strategy = single_active},
                        waiting_consumers = Waiting0} = State0,
               Effects0, Reason) ->
    %% single active consumer on, consumers are waiting
    case Cons0 of
        #{ConsumerKey := #consumer{status = _}} ->
            % The active consumer is to be removed
            cancel_consumer0(Meta, ConsumerKey, State0,
                             Effects0, Reason);
        _ ->
            % The cancelled consumer is not active or cancelled
            % Just remove it from idle_consumers
            case lists:keyfind(ConsumerKey, 1, Waiting0) of
                {_, ?CONSUMER_TAG_PID(T, P)} ->
                    Waiting = lists:keydelete(ConsumerKey, 1, Waiting0),
                    Effects = cancel_consumer_effects({T, P}, State0, Effects0),
                    % A waiting consumer isn't supposed to have any checked out messages,
                    % so nothing special to do here
                    {State0#?STATE{waiting_consumers = Waiting}, Effects};
                _ ->
                    {State0, Effects0}
            end
    end.

consumer_update_active_effects(#?STATE{cfg = #cfg{resource = QName}},
                               #consumer{cfg = #consumer_cfg{pid = CPid,
                                                             tag = CTag,
                                                             meta = Meta}},
                               Active, ActivityStatus,
                               Effects) ->
    Ack = maps:get(ack, Meta, undefined),
    Prefetch = maps:get(prefetch, Meta, undefined),
    Args = maps:get(args, Meta, []),
    [{mod_call, rabbit_quorum_queue, update_consumer_handler,
      [QName, {CTag, CPid}, false, Ack, Prefetch, Active, ActivityStatus, Args]}
      | Effects].

cancel_consumer0(Meta, ConsumerKey,
                 #?STATE{consumers = C0} = S0, Effects0, Reason) ->
    case C0 of
        #{ConsumerKey := Consumer} ->
            {S, Effects2} = maybe_return_all(Meta, ConsumerKey, Consumer,
                                             S0, Effects0, Reason),

            %% The effects are emitted before the consumer is actually removed
            %% if the consumer has unacked messages. This is a bit weird but
            %% in line with what classic queues do (from an external point of
            %% view)
            Effects = cancel_consumer_effects(consumer_id(Consumer), S, Effects2),
            {S, Effects};
        _ ->
            %% already removed: do nothing
            {S0, Effects0}
    end.

activate_next_consumer({State, Effects}) ->
    activate_next_consumer(State, Effects).

activate_next_consumer(#?STATE{cfg = #cfg{consumer_strategy = competing}} = State,
                       Effects) ->
    {State, Effects};
activate_next_consumer(#?STATE{consumers = Cons0,
                               waiting_consumers = Waiting0} = State0,
                       Effects0) ->
    %% invariant, the waiting list always need to be sorted by consumers that are
    %% up - then by priority
    NextConsumer =
        case Waiting0 of
            [{_, #consumer{status = up}} = Next | _] ->
                Next;
            _ ->
                undefined
        end,

    case {active_consumer(Cons0), NextConsumer} of
        {undefined, {NextCKey, #consumer{cfg = NextCCfg} = NextC}} ->
            Remaining = tl(Waiting0),
            %% TODO: can this happen?
            Consumer = case maps:get(NextCKey, Cons0, undefined) of
                           undefined ->
                               NextC;
                           Existing ->
                               %% there was an exisiting non-active consumer
                               %% just update the existing cancelled consumer
                               %% with the new config
                               Existing#consumer{cfg =  NextCCfg}
                       end,
            #?STATE{service_queue = ServiceQueue} = State0,
            ServiceQueue1 = maybe_queue_consumer(NextCKey,
                                                 Consumer,
                                                 ServiceQueue),
            State = State0#?STATE{consumers = Cons0#{NextCKey => Consumer},
                                  service_queue = ServiceQueue1,
                                  waiting_consumers = Remaining},
            Effects = consumer_update_active_effects(State, Consumer,
                                                     true, single_active,
                                                     Effects0),
            {State, Effects};
        {{ActiveCKey, ?CONSUMER_PRIORITY(ActivePriority) =
                      #consumer{checked_out = ActiveChecked} = Active},
         {NextCKey, ?CONSUMER_PRIORITY(WaitingPriority) = Consumer}}
          when WaitingPriority > ActivePriority andalso
               map_size(ActiveChecked) == 0 ->
            Remaining = tl(Waiting0),
            %% the next consumer is a higher priority and should take over
            %% and this consumer does not have any pending messages
            #?STATE{service_queue = ServiceQueue} = State0,
            ServiceQueue1 = maybe_queue_consumer(NextCKey,
                                                 Consumer,
                                                 ServiceQueue),
            Cons1 = Cons0#{NextCKey => Consumer},
            Cons = maps:remove(ActiveCKey, Cons1),
            Waiting = add_waiting({ActiveCKey, Active}, Remaining),
            State = State0#?STATE{consumers = Cons,
                                  service_queue = ServiceQueue1,
                                  waiting_consumers = Waiting},
            Effects1 = consumer_update_active_effects(State, Active,
                                                      false, waiting,
                                                      Effects0),
            Effects = consumer_update_active_effects(State, Consumer,
                                                     true, single_active,
                                                     Effects1),
            {State, Effects};
        {{ActiveCKey, ?CONSUMER_PRIORITY(ActivePriority) = Active},
         {_NextCKey, ?CONSUMER_PRIORITY(WaitingPriority)}}
          when WaitingPriority > ActivePriority ->
            %% A higher priority consumer has attached but the current one has
            %% pending messages
            Cons = maps:update(ActiveCKey,
                               Active#consumer{status = quiescing},
                               Cons0),
            {State0#?STATE{consumers = Cons}, Effects0};
        _ ->
            %% no activation
            {State0, Effects0}
    end.

active_consumer({CKey, #consumer{status = Status} = Consumer, _I})
  when Status == up orelse
       Status == quiescing orelse
       Status == {suspected_down, up} orelse
       Status == {suspected_down, quiescing} ->
    {CKey, Consumer};
active_consumer({_CKey, #consumer{status = _}, I}) ->
    active_consumer(maps:next(I));
active_consumer(none) ->
    undefined;
active_consumer(M) when is_map(M) ->
    I = maps:iterator(M),
    active_consumer(maps:next(I)).

is_active(_ConsumerKey, #?STATE{cfg = #cfg{consumer_strategy = competing}}) ->
    %% all competing consumers are potentially active
    true;
is_active(ConsumerKey, #?STATE{cfg = #cfg{consumer_strategy = single_active},
                               consumers = Consumers}) ->
    ConsumerKey == active_consumer(Consumers).

maybe_return_all(#{system_time := Ts} = Meta, ConsumerKey,
                 #consumer{cfg = CCfg} = Consumer, S0,
                 Effects0, Reason) ->
    case Reason of
        cancel ->
            {update_or_remove_con(
               Meta, ConsumerKey,
               Consumer#consumer{cfg = CCfg#consumer_cfg{lifetime = once},
                                 credit = 0,
                                 status = cancelled},
               S0), Effects0};
        _ ->
            {S1, Effects} = return_all(Meta, S0, Effects0, ConsumerKey,
                                       Consumer, Reason == down),
            {S1#?STATE{consumers = maps:remove(ConsumerKey, S1#?STATE.consumers),
                       last_active = Ts},
             Effects}
    end.

apply_enqueue(#{index := RaftIdx,
                system_time := Ts} = Meta, From,
              Seq, RawMsg, Size, State0) ->
    case maybe_enqueue(RaftIdx, Ts, From, Seq, RawMsg, Size, [], State0) of
        {ok, State1, Effects1} ->
            checkout(Meta, State0, State1, Effects1);
        {out_of_sequence, State, Effects} ->
            {State, not_enqueued, Effects};
        {duplicate, State, Effects} ->
            {State, ok, Effects}
    end.

decr_total(#?STATE{messages_total = Tot} = State) ->
    State#?STATE{messages_total = Tot - 1}.

drop_head(#?STATE{discarded_bytes = DiscardedBytes0} = State0, Effects) ->
    case take_next_msg(State0) of
        {Msg, State1} ->
            Header = get_msg_header(Msg),
            State = decr_total(add_bytes_drop(Header, State1)),
            #?STATE{cfg = #cfg{dead_letter_handler = DLH},
                    dlx = DlxState} = State,
            {_, _RetainedBytes, DlxEffects} =
                discard_or_dead_letter([Msg], maxlen, DLH, DlxState),
            Size = get_header(size, Header),
            {State#?STATE{discarded_bytes = DiscardedBytes0 + Size + ?ENQ_OVERHEAD},
             add_drop_head_effects(DlxEffects, Effects)};
        empty ->
            {State0, Effects}
    end.

add_drop_head_effects([{mod_call,
                        rabbit_global_counters,
                        messages_dead_lettered,
                        [Reason, rabbit_quorum_queue, Type, NewLen]}],
                      [{mod_call,
                        rabbit_global_counters,
                        messages_dead_lettered,
                        [Reason, rabbit_quorum_queue, Type, PrevLen]} | Rem]) ->
    %% combine global counter update effects to avoid bulding a huge list of
    %% effects if many messages are dropped at the same time as could happen
    %% when the `max_length' is changed via a configuration update.
    [{mod_call,
      rabbit_global_counters,
      messages_dead_lettered,
      [Reason, rabbit_quorum_queue, Type, PrevLen + NewLen]} | Rem];
add_drop_head_effects(New, Old) ->
    New ++ Old.

maybe_set_msg_ttl(Msg, RaCmdTs, Header,
                  #?STATE{cfg = #cfg{msg_ttl = MsgTTL}}) ->
    case mc:is(Msg) of
        true ->
            TTL = min(MsgTTL, mc:ttl(Msg)),
            update_expiry_header(RaCmdTs, TTL, Header);
        false ->
            update_expiry_header(RaCmdTs, MsgTTL, Header)
    end.

maybe_set_msg_delivery_count(Msg, Header) ->
    case mc:is(Msg) of
        true ->
            case mc:get_annotation(delivery_count, Msg) of
                undefined ->
                    Header;
                DelCnt ->
                    update_header(delivery_count, fun (_) -> DelCnt end,
                                  DelCnt, Header)
            end;
        false ->
            Header
    end.

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

make_msg(Idx, Sz)
  when Idx =< ?PACKED_IDX_MAX andalso
       (is_integer(Sz) andalso Sz =< ?PACKED_SZ_MAX) ->
       ?PACK(Idx, Sz);
make_msg(Idx, Hdr) ->
    ?MSG(Idx, Hdr).

maybe_enqueue(RaftIdx, Ts, undefined, undefined, RawMsg,
              {MetaSize, BodySize},
              Effects, #?STATE{msg_bytes_enqueue = Enqueue,
                               messages = Messages,
                               messages_total = Total} = State0) ->
    % direct enqueue without tracking
    Size = MetaSize + BodySize,
    Header0 = maybe_set_msg_ttl(RawMsg, Ts, Size, State0),
    Header = maybe_set_msg_delivery_count(RawMsg, Header0),
    Msg = make_msg(RaftIdx, Header),
    Priority = msg_priority(RawMsg),
    State = State0#?STATE{msg_bytes_enqueue = Enqueue + Size,
                          messages_total = Total + 1,
                          messages = rabbit_fifo_pq:in(Priority, Msg, Messages)
                         },
    {ok, State, Effects};
maybe_enqueue(RaftIdx, Ts, From, MsgSeqNo, RawMsg,
              {MetaSize, BodySize} = MsgSize,
              Effects0, #?STATE{msg_bytes_enqueue = BytesEnqueued,
                                enqueuers = Enqueuers0,
                                messages = Messages,
                                discarded_bytes = DiscardedBytes0,
                                messages_total = Total} = State0) ->
    Size = MetaSize + BodySize,
    case maps:get(From, Enqueuers0, undefined) of
        undefined ->
            State1 = State0#?STATE{enqueuers = Enqueuers0#{From => #enqueuer{}}},
            {Res, State, Effects} = maybe_enqueue(RaftIdx, Ts, From, MsgSeqNo,
                                                  RawMsg, MsgSize, Effects0,
                                                  State1),
            {Res, State, [{monitor, process, From} | Effects]};
        #enqueuer{next_seqno = MsgSeqNo} = Enq0 ->
            % it is the next expected seqno
            Header0 = maybe_set_msg_ttl(RawMsg, Ts, Size, State0),
            Header = maybe_set_msg_delivery_count(RawMsg, Header0),
            Msg = make_msg(RaftIdx, Header),
            Enq = Enq0#enqueuer{next_seqno = MsgSeqNo + 1},
            MsgCache = case can_immediately_deliver(State0) of
                           true ->
                               {RaftIdx, RawMsg};
                           false ->
                               undefined
                       end,
            Priority = msg_priority(RawMsg),
            State = State0#?STATE{msg_bytes_enqueue = BytesEnqueued + Size,
                                  messages_total = Total + 1,
                                  messages = rabbit_fifo_pq:in(Priority, Msg, Messages),
                                  enqueuers = Enqueuers0#{From => Enq},
                                  msg_cache = MsgCache
                                 },
            {ok, State, Effects0};
        #enqueuer{next_seqno = Next}
          when MsgSeqNo > Next ->
            %% TODO: when can this happen?
            State = State0#?STATE{discarded_bytes =
                                  DiscardedBytes0 + Size + ?ENQ_OVERHEAD},
            {out_of_sequence, State, Effects0};
        #enqueuer{next_seqno = Next} when MsgSeqNo =< Next ->
            % duplicate delivery
            State = State0#?STATE{discarded_bytes =
                                  DiscardedBytes0 + Size + ?ENQ_OVERHEAD},
            {duplicate, State, Effects0}
    end.

return(Meta, ConsumerKey,
       MsgIds, IncrDelCount, Anns, Checked, Effects0, State0)
 when is_map(Anns) ->
    %% We requeue in the same order as messages got returned by the client.
    {State1, Effects1} =
        lists:foldl(
          fun(MsgId, Acc = {S0, E0}) ->
                  case Checked of
                      #{MsgId := Msg} ->
                          return_one(Meta, MsgId, Msg, IncrDelCount, Anns,
                                     S0, E0, ConsumerKey);
                      #{} ->
                          Acc
                  end
          end, {State0, Effects0}, MsgIds),
    State2 = case State1#?STATE.consumers of
                 #{ConsumerKey := Con} ->
                     update_or_remove_con(Meta, ConsumerKey, Con, State1);
                 _ ->
                     State1
             end,
    {State3, Effects2} = activate_next_consumer({State2, Effects1}),
    checkout(Meta, State0, State3, Effects2).

% used to process messages that are finished
complete(Meta, ConsumerKey, [MsgId],
         #consumer{checked_out = Checked0} = Con0,
         #?STATE{msg_bytes_checkout = BytesCheckout,
                 discarded_bytes = DiscBytes,
                 messages_total = Tot} = State0,
        Effects) ->
    case maps:take(MsgId, Checked0) of
        {Msg, Checked} ->
            Hdr = get_msg_header(Msg),
            SettledSize = get_header(size, Hdr),
            Con = Con0#consumer{checked_out = Checked,
                                credit = increase_credit(Con0, 1)},
            State1 = update_or_remove_con(Meta, ConsumerKey, Con, State0),
            {State1#?STATE{msg_bytes_checkout = BytesCheckout - SettledSize,
                           discarded_bytes = DiscBytes + SettledSize + ?ENQ_OVERHEAD,
                           messages_total = Tot - 1},
             Effects};
        error ->
            {State0, Effects}
    end;
complete(Meta, ConsumerKey, MsgIds,
         #consumer{checked_out = Checked0} = Con0,
         #?STATE{msg_bytes_checkout = BytesCheckout,
                 discarded_bytes = DiscBytes,
                 messages_total = Tot} = State0, Effects) ->
    {SettledSize, Checked}
        = lists:foldl(
            fun (MsgId, {S0, Ch0}) ->
                    case maps:take(MsgId, Ch0) of
                        {Msg, Ch} ->
                            Hdr = get_msg_header(Msg),
                            S = get_header(size, Hdr) + S0,
                            {S, Ch};
                        error ->
                            {S0, Ch0}
                    end
            end, {0, Checked0}, MsgIds),
    Len = map_size(Checked0) - map_size(Checked),
    Con = Con0#consumer{checked_out = Checked,
                        credit = increase_credit(Con0, Len)},
    State1 = update_or_remove_con(Meta, ConsumerKey, Con, State0),
    {State1#?STATE{msg_bytes_checkout = BytesCheckout - SettledSize,
                   discarded_bytes = DiscBytes + SettledSize + (Len *?ENQ_OVERHEAD),
                   messages_total = Tot - Len},
     Effects}.

increase_credit(#consumer{cfg = #consumer_cfg{lifetime = once},
                          credit = Credit}, _) ->
    %% once consumers cannot increment credit
    Credit;
increase_credit(#consumer{cfg = #consumer_cfg{lifetime = auto,
                                              credit_mode = credited},
                          credit = Credit}, _) ->
    %% credit_mode: `credited' also doesn't automatically increment credit
    Credit;
increase_credit(#consumer{cfg = #consumer_cfg{lifetime = auto,
                                              credit_mode = {credited, _}},
                          credit = Credit}, _) ->
    %% credit_mode: `credited' also doesn't automatically increment credit
    Credit;
increase_credit(#consumer{cfg = #consumer_cfg{credit_mode =
                                              {simple_prefetch, MaxCredit}},
                          credit = Current}, Credit)
  when MaxCredit > 0 ->
    min(MaxCredit, Current + Credit);
increase_credit(#consumer{credit = Current}, Credit) ->
    Current + Credit.

complete_and_checkout(#{} = Meta, MsgIds, ConsumerKey,
                      #consumer{} = Con0,
                      Effects0, State0) ->
    {State1, Effects1} = complete(Meta, ConsumerKey, MsgIds,
                                  Con0, State0, Effects0),
    %% a completion could have removed the active/quiescing consumer
    Effects2 = add_active_effect(Con0, State1, Effects1),
    {State2, Effects} = activate_next_consumer(State1, Effects2),
    checkout(Meta, State0, State2, Effects).

add_active_effect(#consumer{status = quiescing} = Consumer,
                  #?STATE{cfg = #cfg{consumer_strategy = single_active},
                          consumers = Consumers} = State,
                  Effects) ->
    case active_consumer(Consumers) of
        undefined ->
            consumer_update_active_effects(State, Consumer, false, waiting, Effects);
        _ ->
            Effects
    end;
add_active_effect(_, _, Effects) ->
    Effects.

cancel_consumer_effects(ConsumerId,
                        #?STATE{cfg = #cfg{resource = QName}},
                        Effects) when is_tuple(ConsumerId) ->
    [{mod_call, rabbit_quorum_queue,
      cancel_consumer_handler, [QName, ConsumerId]} | Effects].

update_msg_header(Key, Fun, Def, Msg) ->
    ?MSG(get_msg_idx(Msg),
         update_header(Key, Fun, Def, get_msg_header(Msg))).

update_header(expiry, _, Expiry, Size)
  when is_integer(Size) ->
    ?TUPLE(Size, Expiry);
update_header(Key, UpdateFun, Default, Size)
  when is_integer(Size) ->
    update_header(Key, UpdateFun, Default, #{size => Size});
update_header(Key, UpdateFun, Default, ?TUPLE(Size, Expiry))
  when is_integer(Size) andalso
       is_integer(Expiry) ->
    update_header(Key, UpdateFun, Default, #{size => Size,
                                             expiry => Expiry});
update_header(Key, UpdateFun, Default, Header)
  when is_map_key(size, Header) ->
    maps:update_with(Key, UpdateFun, Default, Header).

get_msg_idx(?MSG(Idx, _Header)) ->
    Idx;
get_msg_idx(Packed) when ?IS_PACKED(Packed) ->
    ?PACKED_IDX(Packed).

get_msg_header(?MSG(_Idx, Header)) ->
    Header;
get_msg_header(Packed) when ?IS_PACKED(Packed) ->
    ?PACKED_SZ(Packed).

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

annotate_msg(Header, Msg0) ->
    case mc:is(Msg0) of
        true when is_map(Header) ->
            Msg = maps:fold(fun (K, V, Acc) ->
                                    mc:set_annotation(K, V, Acc)
                            end, Msg0, maps:get(anns, Header, #{})),
            case Header of
                #{delivery_count := DelCount} ->
                    mc:set_annotation(delivery_count, DelCount, Msg);
                _ ->
                    Msg
            end;
        _ ->
            Msg0
    end.

return_one(Meta, MsgId, Msg0, DeliveryFailed, Anns,
           #?STATE{returns = Returns,
                   consumers = Consumers,
                   dlx = DlxState0,
                   discarded_bytes = DiscardedBytes0,
                   cfg = #cfg{delivery_limit = DeliveryLimit,
                              dead_letter_handler = DLH}} = State0,
           Effects0, ConsumerKey) ->
    #consumer{checked_out = Checked0} = Con0 = maps:get(ConsumerKey, Consumers),
    Msg = incr_msg_headers(Msg0, DeliveryFailed, Anns),
    Header = get_msg_header(Msg),
    %% TODO: do not use acquired count here as that includes all deliberate
    %% returns, use delivery_count header instead
    case get_header(delivery_count, Header) of
        DeliveryCount
          when is_integer(DeliveryCount) andalso
               DeliveryCount > DeliveryLimit ->
            {DlxState, RetainedBytes, DlxEffects} =
                discard_or_dead_letter([Msg], delivery_limit, DLH, DlxState0),
            %% subtract retained bytes as complete/6 will add them on irrespective
            %% of dead letter strategy, alt, consider adding a new argument to
            %% indicate if message ids were retained
            State1 = State0#?STATE{dlx = DlxState,
                                   discarded_bytes = DiscardedBytes0 - RetainedBytes},
            {State, Effects} = complete(Meta, ConsumerKey, [MsgId],
                                        Con0, State1, Effects0),
            {State, DlxEffects ++ Effects};
        _ ->
            Checked = maps:remove(MsgId, Checked0),
            Con = Con0#consumer{checked_out = Checked,
                                credit = increase_credit(Con0, 1)},
            {add_bytes_return(
               Header,
               State0#?STATE{consumers = Consumers#{ConsumerKey => Con},
                             returns = lqueue:in(Msg, Returns)}),
             Effects0}
    end.

return_all(Meta, #?STATE{consumers = Cons} = State0, Effects0, ConsumerKey,
           #consumer{checked_out = Checked} = Con, DeliveryFailed) ->
    State = State0#?STATE{consumers = Cons#{ConsumerKey => Con}},
    lists:foldl(fun ({MsgId, Msg}, {S, E}) ->
                        return_one(Meta, MsgId, Msg, DeliveryFailed, #{},
                                   S, E, ConsumerKey)
                end, {State, Effects0}, lists:sort(maps:to_list(Checked))).

checkout(Meta, OldState, State0, Effects0) ->
    checkout(Meta, OldState, State0, Effects0, ok).

checkout(#{index := Index} = Meta,
         #?STATE{} = OldState,
         State0, Effects0, Reply) ->
    {#?STATE{cfg = #cfg{dead_letter_handler = DLH},
             dlx = DlxState0} = State1, _ExpiredMsg, Effects1} =
        checkout0(Meta, checkout_one(Meta, false, State0, Effects0), #{}),
    {DlxState, DlxDeliveryEffects} = dlx_checkout(DLH, DlxState0),
    %% TODO: only update dlx state if it has changed?
    %% by this time the cache should be used
    State2 = State1#?STATE{msg_cache = undefined,
                           dlx = DlxState},
    Effects2 = DlxDeliveryEffects ++ Effects1,
    {State, Effects} = evaluate_limit(Index, OldState, State2, Effects2),
    {State, Reply, Effects}.

checkout0(Meta, {success, ConsumerKey, MsgId,
                 Msg, ExpiredMsg, State, Effects},
          SendAcc0) ->
    DelMsg = {MsgId, Msg},
    SendAcc = case maps:get(ConsumerKey, SendAcc0, undefined) of
                  undefined ->
                      SendAcc0#{ConsumerKey => [DelMsg]};
                  LogMsgs ->
                      SendAcc0#{ConsumerKey => [DelMsg | LogMsgs]}
              end,
    checkout0(Meta, checkout_one(Meta, ExpiredMsg, State, Effects), SendAcc);
checkout0(_Meta, {_Activity, ExpiredMsg, State0, Effects0}, SendAcc) ->
    Effects = add_delivery_effects([], SendAcc, State0),
    {State0, ExpiredMsg, Effects0 ++ lists:reverse(Effects)}.

evaluate_limit(Idx, State1, State2, OuterEffects) ->
    case evaluate_limit0(Idx, State1, State2, []) of
        {State, []} ->
            {State, OuterEffects};
        {State, Effects} ->
            {State, OuterEffects ++ lists:reverse(Effects)}
    end.

evaluate_limit0(_Index,
                #?STATE{cfg = #cfg{max_length = undefined,
                                   max_bytes = undefined}},
                #?STATE{cfg = #cfg{max_length = undefined,
                                   max_bytes = undefined}} = State,
                Effects) ->
    {State, Effects};
evaluate_limit0(_Index, _BeforeState,
                #?STATE{cfg = #cfg{max_length = undefined,
                                   max_bytes = undefined},
                        enqueuers = Enqs0} = State0,
                Effects0) ->
    %% max_length and/or max_bytes policies have just been deleted
    {Enqs, Effects} = unblock_enqueuers(Enqs0, Effects0),
    {State0#?STATE{enqueuers = Enqs}, Effects};
evaluate_limit0(Index, BeforeState,
                #?STATE{cfg = #cfg{overflow_strategy = Strategy},
                        enqueuers = Enqs0} = State0,
                Effects0) ->
    case is_over_limit(State0) of
        true when Strategy == drop_head ->
            {State, Effects} = drop_head(State0, Effects0),
            evaluate_limit0(Index, BeforeState, State, Effects);
        true when Strategy == reject_publish ->
            %% generate send_msg effect for each enqueuer to let them know
            %% they need to block
            {Enqs, Effects} =
                maps:fold(
                  fun (P, #enqueuer{blocked = undefined} = E0, {Enqs, Acc}) ->
                          E = E0#enqueuer{blocked = Index},
                          {Enqs#{P => E},
                           [{send_msg, P, {queue_status, reject_publish},
                             [ra_event]} | Acc]};
                      (_P, _E, Acc) ->
                          Acc
                  end, {Enqs0, Effects0}, Enqs0),
            {State0#?STATE{enqueuers = Enqs}, Effects};
        false when Strategy == reject_publish ->
            %% TODO: optimise as this case gets called for every command
            %% pretty much
            Before = is_below_soft_limit(BeforeState),
            case {Before, is_below_soft_limit(State0)} of
                {false, true} ->
                    %% we have moved below the lower limit
                    {Enqs, Effects} = unblock_enqueuers(Enqs0, Effects0),
                    {State0#?STATE{enqueuers = Enqs}, Effects};
                _ ->
                    {State0, Effects0}
            end;
        false ->
            {State0, Effects0}
    end.

unblock_enqueuers(Enqs0, Effects0) ->
    maps:fold(
      fun (P, #enqueuer{} = E0, {Enqs, Acc})  ->
              E = E0#enqueuer{blocked = undefined},
              {Enqs#{P => E},
               [{send_msg, P, {queue_status, go}, [ra_event]}
               | Acc]};
          (_P, _E, Acc) ->
              Acc
      end, {Enqs0, Effects0}, Enqs0).

%% [6,5,4,3,2,1] -> [[1,2],[3,4],[5,6]]
chunk_disk_msgs([], _Bytes, [[] | Chunks]) ->
    Chunks;
chunk_disk_msgs([], _Bytes, Chunks) ->
    Chunks;
chunk_disk_msgs([{_MsgId, Msg} = ConsumerMsg | Rem],
                Bytes, Chunks)
  when Bytes >= ?DELIVERY_CHUNK_LIMIT_B ->
    Size = get_header(size, get_msg_header(Msg)),
    chunk_disk_msgs(Rem, Size, [[ConsumerMsg] | Chunks]);
chunk_disk_msgs([{_MsgId, Msg} = ConsumerMsg | Rem], Bytes,
                [CurChunk | Chunks]) ->
    Size = get_header(size, get_msg_header(Msg)),
    chunk_disk_msgs(Rem, Bytes + Size, [[ConsumerMsg | CurChunk] | Chunks]).

add_delivery_effects(Effects0, AccMap, _State)
  when map_size(AccMap) == 0 ->
    %% does this ever happen?
    Effects0;
add_delivery_effects(Effects0, AccMap, State) ->
     maps:fold(fun (C, DiskMsgs, Efs)
                     when is_list(DiskMsgs) ->
                       lists:foldl(
                         fun (Msgs, E) ->
                                 [delivery_effect(C, Msgs, State) | E]
                         end, Efs, chunk_disk_msgs(DiskMsgs, 0, [[]]))
               end, Effects0, AccMap).

take_next_msg(#?STATE{returns = Returns0,
                      messages = Messages0} = State) ->
    case lqueue:out(Returns0) of
        {{value, NextMsg}, Returns} ->
            {NextMsg, State#?STATE{returns = Returns}};
        {empty, _} ->
            case rabbit_fifo_pq:out(Messages0) of
                empty ->
                    empty;
                {Msg, Messages} ->
                    {Msg, State#?STATE{messages = Messages}}
            end
    end.

get_next_msg(#?STATE{returns = Returns0,
                     messages = Messages0}) ->
    case lqueue:get(Returns0, empty) of
        empty ->
            rabbit_fifo_pq:get(Messages0);
        Msg ->
            Msg
    end.

delivery_effect(ConsumerKey, [{MsgId, ?MSG(Idx,  Header)}],
                #?STATE{msg_cache = {Idx, RawMsg}} = State) ->
    {CTag, CPid} = consumer_id(ConsumerKey, State),
    {send_msg, CPid, {delivery, CTag, [{MsgId, {Header, RawMsg}}]},
     ?DELIVERY_SEND_MSG_OPTS};
delivery_effect(ConsumerKey, [{MsgId, Msg}],
                #?STATE{msg_cache = {Idx, RawMsg}} = State)
  when is_integer(Msg) andalso ?PACKED_IDX(Msg) == Idx ->
    Header = get_msg_header(Msg),
    {CTag, CPid} = consumer_id(ConsumerKey, State),
    {send_msg, CPid, {delivery, CTag, [{MsgId, {Header, RawMsg}}]},
     ?DELIVERY_SEND_MSG_OPTS};
delivery_effect(ConsumerKey, Msgs, #?STATE{} = State) ->
    {CTag, CPid} = consumer_id(ConsumerKey, State),
    {RaftIdxs, _Num} = lists:foldr(fun ({_, Msg}, {Acc, N}) ->

                                           {[get_msg_idx(Msg) | Acc], N+1}
                                   end, {[], 0}, Msgs),
    {log_ext, RaftIdxs,
     fun (ReadPlan) ->
             case node(CPid) == node() of
                 true ->
                     [{send_msg, CPid, {delivery, CTag, ReadPlan, Msgs},
                       ?DELIVERY_SEND_MSG_OPTS}];
                 false ->
                     %% if we got there we need to read the data on this node
                     %% and send it to the consumer pid as it isn't availble
                     %% locally
                     {DelMsgs, Flru} = exec_read(undefined, ReadPlan, Msgs),
                     %% we need to evict all cached items here
                     _ = ra_flru:evict_all(Flru),
                     [{send_msg, CPid, {delivery, CTag, DelMsgs},
                       ?DELIVERY_SEND_MSG_OPTS}]
             end
     end,
     {local, node(CPid)}}.

reply_log_effect(RaftIdx, MsgId, Header, Ready, From) ->
    {log, [RaftIdx],
     fun ([]) ->
             [];
         ([Cmd]) ->
             [{reply, From, {wrap_reply,
                             {dequeue, {MsgId, {Header, get_msg_from_cmd(Cmd)}}, Ready}}}]
     end}.

checkout_one(#{system_time := Ts} = Meta, ExpiredMsg0, InitState0, Effects0) ->
    %% Before checking out any messsage to any consumer,
    %% first remove all expired messages from the head of the queue.
    {ExpiredMsg, #?STATE{service_queue = SQ0,
                         messages = Messages0,
                         msg_bytes_checkout = BytesCheckout,
                         msg_bytes_enqueue = BytesEnqueue,
                         consumers = Cons0} = InitState, Effects1} =
        expire_msgs(Ts, ExpiredMsg0, InitState0, Effects0),

    case priority_queue:out(SQ0) of
        {{value, ConsumerKey}, SQ1}
          when is_map_key(ConsumerKey, Cons0) ->
            case take_next_msg(InitState) of
                {Msg, State0} ->
                    %% there are consumers waiting to be serviced
                    %% process consumer checkout
                    case maps:get(ConsumerKey, Cons0) of
                        #consumer{credit = Credit,
                                  status = Status}
                          when Credit =:= 0 orelse
                               Status =/= up ->
                            %% not an active consumer but still in the consumers
                            %% map - this can happen when draining
                            %% or when higher priority single active consumers
                            %% take over, recurse without consumer in service
                            %% queue
                            checkout_one(Meta, ExpiredMsg,
                                         InitState#?STATE{service_queue = SQ1},
                                         Effects1);
                        #consumer{checked_out = Checked0,
                                  next_msg_id = Next,
                                  credit = Credit,
                                  delivery_count = DelCnt0,
                                  cfg = Cfg} = Con0 ->
                            Checked = maps:put(Next, Msg, Checked0),
                            DelCnt = case credit_api_v2(Cfg) of
                                         true -> add(DelCnt0, 1);
                                         false -> DelCnt0 + 1
                                     end,
                            Con = Con0#consumer{checked_out = Checked,
                                                next_msg_id = Next + 1,
                                                credit = Credit - 1,
                                                delivery_count = DelCnt},
                            Size = get_header(size, get_msg_header(Msg)),
                            State1 =
                                State0#?STATE{service_queue = SQ1,
                                              msg_bytes_checkout = BytesCheckout + Size,
                                              msg_bytes_enqueue = BytesEnqueue - Size},
                            State = update_or_remove_con(
                                       Meta, ConsumerKey, Con, State1),
                            {success, ConsumerKey, Next, Msg, ExpiredMsg,
                             State, Effects1}
                    end;
                empty ->
                    {nochange, ExpiredMsg, InitState, Effects1}
            end;
        {{value, _ConsumerId}, SQ1} ->
            %% consumer was not active but was queued, recurse
            checkout_one(Meta, ExpiredMsg,
                         InitState#?STATE{service_queue = SQ1}, Effects1);
        {empty, _} ->
            case rabbit_fifo_pq:len(Messages0) of
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
            %% packed messages never have an expiry
            {Result, State, Effects}
    end.

expire(RaCmdTs, State0, Effects) ->
    {Msg,
     #?STATE{cfg = #cfg{dead_letter_handler = DLH},
             dlx = DlxState0,
             messages_total = Tot,
             discarded_bytes = DiscardedBytes0,
             msg_bytes_enqueue = MsgBytesEnqueue} = State1} =
        take_next_msg(State0),
    {DlxState, _RetainedBytes, DlxEffects} =
        discard_or_dead_letter([Msg], expired, DLH, DlxState0),
    Header = get_msg_header(Msg),
    Size = get_header(size, Header),
    DiscardedSize = Size + ?ENQ_OVERHEAD,
    State = State1#?STATE{dlx = DlxState,
                          messages_total = Tot - 1,
                          discarded_bytes = DiscardedBytes0 + DiscardedSize,
                          msg_bytes_enqueue = MsgBytesEnqueue - Size},
    expire_msgs(RaCmdTs, true, State, DlxEffects ++ Effects).

timer_effect(RaCmdTs, State, Effects) ->
    T = case get_next_msg(State) of
            ?MSG(_, ?TUPLE(Size, Expiry))
              when is_integer(Size) andalso
                   is_integer(Expiry) ->
                %% Next message contains 'expiry' header.
                %% (Re)set timer so that message will be dropped or
                %% dead-lettered on time.
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

update_or_remove_con(Meta, ConsumerKey,
                     #consumer{cfg = #consumer_cfg{lifetime = once},
                               checked_out = Checked,
                               credit = 0} = Con,
                     #?STATE{consumers = Cons} = State) ->
    case map_size(Checked) of
        0 ->
            #{system_time := Ts} = Meta,
            % we're done with this consumer
            State#?STATE{consumers = maps:remove(ConsumerKey, Cons),
                         last_active = Ts};
        _ ->
            % there are unsettled items so need to keep around
            State#?STATE{consumers = maps:put(ConsumerKey, Con, Cons)}
    end;
update_or_remove_con(_Meta, ConsumerKey,
                     #consumer{status = quiescing,
                               checked_out = Checked} = Con0,
                     #?STATE{consumers = Cons,
                             waiting_consumers = Waiting} = State)
  when map_size(Checked) == 0 ->
    Con = Con0#consumer{status = up},
    State#?STATE{consumers = maps:remove(ConsumerKey, Cons),
                 waiting_consumers = add_waiting({ConsumerKey, Con}, Waiting)};
update_or_remove_con(_Meta, ConsumerKey,
                     #consumer{} = Con,
                     #?STATE{consumers = Cons,
                             service_queue = ServiceQueue} = State) ->
    State#?STATE{consumers = maps:put(ConsumerKey, Con, Cons),
                 service_queue = maybe_queue_consumer(ConsumerKey, Con,
                                                      ServiceQueue)}.

maybe_queue_consumer(Key, #consumer{credit = Credit,
                                    status = up,
                                    cfg = #consumer_cfg{priority = P}},
                     ServiceQueue)
  when Credit > 0 ->
    % TODO: queue:member could surely be quite expensive, however the practical
    % number of unique consumers may not be large enough for it to matter
    case priority_queue:member(Key, ServiceQueue) of
        true ->
            ServiceQueue;
        false ->
            priority_queue:in(Key, P, ServiceQueue)
    end;
maybe_queue_consumer(_Key, _Consumer, ServiceQueue) ->
    ServiceQueue.

update_consumer(Meta, ConsumerKey, {Tag, Pid}, ConsumerMeta,
                {Life, Mode} = Spec, Priority,
                #?STATE{cfg = #cfg{consumer_strategy = competing},
                        consumers = Cons0} = State0) ->
    Consumer = case Cons0 of
                   #{ConsumerKey := #consumer{} = Consumer0} ->
                       merge_consumer(Meta, Consumer0, ConsumerMeta,
                                      Spec, Priority);
                   _ ->
                       Credit = included_credit(Mode),
                       DeliveryCount = initial_delivery_count(Mode),
                       #consumer{cfg = #consumer_cfg{tag = Tag,
                                                     pid = Pid,
                                                     lifetime = Life,
                                                     meta = ConsumerMeta,
                                                     priority = Priority,
                                                     credit_mode = Mode},
                                 credit = Credit,
                                 delivery_count = DeliveryCount}
               end,
    {Consumer, update_or_remove_con(Meta, ConsumerKey, Consumer, State0)};
update_consumer(Meta, ConsumerKey, {Tag, Pid}, ConsumerMeta,
                {Life, Mode} = Spec, Priority,
                #?STATE{cfg = #cfg{consumer_strategy = single_active},
                        consumers = Cons0,
                        waiting_consumers = Waiting0,
                        service_queue = _ServiceQueue0} = State) ->
    %% if it is the current active consumer, just update
    %% if it is a cancelled active consumer, add to waiting unless it is the only
    %% one, then merge
    case active_consumer(Cons0) of
        {ConsumerKey, #consumer{status = up} = Consumer0} ->
            Consumer = merge_consumer(Meta, Consumer0, ConsumerMeta,
                                      Spec, Priority),
            {Consumer, update_or_remove_con(Meta, ConsumerKey, Consumer, State)};
        undefined when is_map_key(ConsumerKey, Cons0) ->
            %% there is no active consumer and the current consumer is in the
            %% consumers map and thus must be cancelled, in this case we can just
            %% merge and effectively make this the current active one
            Consumer0 = maps:get(ConsumerKey, Cons0),
            Consumer = merge_consumer(Meta, Consumer0, ConsumerMeta,
                                      Spec, Priority),
            {Consumer, update_or_remove_con(Meta, ConsumerKey, Consumer, State)};
        _ ->
            %% add as a new waiting consumer
            Credit = included_credit(Mode),
            DeliveryCount = initial_delivery_count(Mode),
            Consumer = #consumer{cfg = #consumer_cfg{tag = Tag,
                                                     pid = Pid,
                                                     lifetime = Life,
                                                     meta = ConsumerMeta,
                                                     priority = Priority,
                                                     credit_mode = Mode},
                                 credit = Credit,
                                 delivery_count = DeliveryCount},
            Waiting = add_waiting({ConsumerKey, Consumer}, Waiting0),
            {Consumer, State#?STATE{waiting_consumers = Waiting}}
    end.

add_waiting({Key, _} = New, Waiting) ->
    sort_waiting(lists:keystore(Key, 1, Waiting, New)).

sort_waiting(Waiting) ->
    lists:sort(fun
                   ({_, ?CONSUMER_PRIORITY(P1) = #consumer{status = up}},
                    {_, ?CONSUMER_PRIORITY(P2) = #consumer{status = up}})
                     when P1 =/= P2 ->
                       P2 =< P1;
                   ({C1, #consumer{status = up,
                                   credit = Cr1}},
                    {C2, #consumer{status = up,
                                   credit = Cr2}}) ->
                       %% both are up, priority the same
                       if Cr1 == Cr2 ->
                              %% same credit
                              %% sort by key, first attached priority
                              C1 =< C2;
                          true ->
                              %% else sort by credit
                              Cr2 =< Cr1
                       end;
                   (_, {_, #consumer{status = Status}}) ->
                       %% not up
                       Status /= up
               end, Waiting).

merge_consumer(_Meta, #consumer{cfg = CCfg, checked_out = Checked} = Consumer,
               ConsumerMeta, {Life, Mode}, Priority) ->
    Credit = included_credit(Mode),
    NumChecked = map_size(Checked),
    NewCredit = max(0, Credit - NumChecked),
    Consumer#consumer{cfg = CCfg#consumer_cfg{priority = Priority,
                                              meta = ConsumerMeta,
                                              credit_mode = Mode,
                                              lifetime = Life},
                      status = up,
                      credit = NewCredit}.

included_credit({simple_prefetch, Credit}) ->
    Credit;
included_credit({credited, _}) ->
    0;
included_credit(credited) ->
    0.

credit_active_consumer(
  #credit{credit = LinkCreditRcv,
          delivery_count = DeliveryCountRcv,
          drain = Drain,
          consumer_key = ConsumerKey},
  #consumer{delivery_count = DeliveryCountSnd,
            cfg = Cfg} = Con0,
  Meta,
  #?STATE{consumers = Cons0,
          service_queue = ServiceQueue0} = State0) ->
    LinkCreditSnd = link_credit_snd(DeliveryCountRcv, LinkCreditRcv,
                                    DeliveryCountSnd, Cfg),
    %% grant the credit
    Con1 = Con0#consumer{credit = LinkCreditSnd},
    ServiceQueue = maybe_queue_consumer(ConsumerKey, Con1, ServiceQueue0),
    State1 = State0#?STATE{service_queue = ServiceQueue,
                           consumers = maps:update(ConsumerKey, Con1, Cons0)},
    {State2, ok, Effects} = checkout(Meta, State0, State1, []),

    #?STATE{consumers = Cons1 = #{ConsumerKey := Con2}} = State2,
    #consumer{cfg = #consumer_cfg{pid = CPid,
                                  tag = CTag},
              credit = PostCred,
              delivery_count = PostDeliveryCount} = Con2,
    Available = messages_ready(State2),
    case credit_api_v2(Cfg) of
        true ->
            {Credit, DeliveryCount, State} =
            case Drain andalso PostCred > 0 of
                true ->
                    AdvancedDeliveryCount = add(PostDeliveryCount, PostCred),
                    ZeroCredit = 0,
                    Con = Con2#consumer{delivery_count = AdvancedDeliveryCount,
                                        credit = ZeroCredit},
                    Cons = maps:update(ConsumerKey, Con, Cons1),
                    State3 = State2#?STATE{consumers = Cons},
                    {ZeroCredit, AdvancedDeliveryCount, State3};
                false ->
                    {PostCred, PostDeliveryCount, State2}
            end,
            %% We must send the delivery effects to the queue client
            %% before credit_reply such that session process can send to
            %% AMQP 1.0 client TRANSFERs before FLOW.
            {State, ok, Effects ++ [{send_msg, CPid,
                                     {credit_reply, CTag, DeliveryCount,
                                      Credit, Available, Drain},
                                     ?DELIVERY_SEND_MSG_OPTS}]};
        false ->
            %% We must always send a send_credit_reply because basic.credit
            %% is synchronous.
            %% Additionally, we keep the bug of credit API v1 that we
            %% send to queue client the
            %% send_drained reply before the delivery effects (resulting
            %% in the wrong behaviour that the session process sends to
            %% AMQP 1.0 client the FLOW before the TRANSFERs).
            %% We have to keep this bug because old rabbit_fifo_client
            %% implementations expect a send_drained Ra reply
            %% (they can't handle such a Ra effect).
            CreditReply = {send_credit_reply, Available},
            case Drain of
                true ->
                    AdvancedDeliveryCount = PostDeliveryCount + PostCred,
                    Con = Con2#consumer{delivery_count = AdvancedDeliveryCount,
                                        credit = 0},
                    Cons = maps:update(ConsumerKey, Con, Cons1),
                    State = State2#?STATE{consumers = Cons},
                    Reply = {multi, [CreditReply,
                                     {send_drained, {CTag, PostCred}}]},
                    {State, Reply, Effects};
                false ->
                    {State2, CreditReply, Effects}
            end
    end.

credit_inactive_consumer(
  #credit{credit = LinkCreditRcv,
          delivery_count = DeliveryCountRcv,
          drain = Drain,
          consumer_key = ConsumerKey},
  #consumer{cfg = #consumer_cfg{pid = CPid,
                                tag = CTag} = Cfg,
            delivery_count = DeliveryCountSnd} = Con0,
  Waiting0, State0) ->
    %% No messages are available for inactive consumers.
    Available = 0,
    LinkCreditSnd = link_credit_snd(DeliveryCountRcv,
                                    LinkCreditRcv,
                                    DeliveryCountSnd,
                                    Cfg),
    case credit_api_v2(Cfg) of
        true ->
            {Credit, DeliveryCount} =
            case Drain of
                true ->
                    %% By issuing drain=true, the client says "either send a transfer or a flow frame".
                    %% Since there are no messages to send to an inactive consumer, we advance the
                    %% delivery-count consuming all link-credit and send a credit_reply with drain=true
                    %% to the session which causes the session to send a flow frame to the client.
                    AdvancedDeliveryCount = add(DeliveryCountSnd, LinkCreditSnd),
                    {0, AdvancedDeliveryCount};
                false ->
                    {LinkCreditSnd, DeliveryCountSnd}
            end,
            %% Grant the credit.
            Con = Con0#consumer{credit = Credit,
                                delivery_count = DeliveryCount},
            Waiting = add_waiting({ConsumerKey, Con}, Waiting0),
            State = State0#?STATE{waiting_consumers = Waiting},
            {State, ok,
             {send_msg, CPid,
              {credit_reply, CTag, DeliveryCount, Credit, Available, Drain},
              ?DELIVERY_SEND_MSG_OPTS}};
        false ->
            %% Credit API v1 doesn't support draining an inactive consumer.
            %% Grant the credit.
            Con = Con0#consumer{credit = LinkCreditSnd},
            Waiting = add_waiting({ConsumerKey, Con}, Waiting0),
            State = State0#?STATE{waiting_consumers = Waiting},
            {State, {send_credit_reply, Available}}
    end.

is_over_limit(#?STATE{cfg = #cfg{max_length = undefined,
                                  max_bytes = undefined}}) ->
    false;
is_over_limit(#?STATE{cfg = #cfg{max_length = MaxLength,
                                  max_bytes = MaxBytes},
                       msg_bytes_enqueue = BytesEnq,
                       dlx = DlxState} = State) ->
    {NumDlx, BytesDlx} = dlx_stat(DlxState),
    (messages_ready(State) + NumDlx > MaxLength) orelse
    (BytesEnq + BytesDlx > MaxBytes).

is_below_soft_limit(#?STATE{cfg = #cfg{max_length = undefined,
                                        max_bytes = undefined}}) ->
    false;
is_below_soft_limit(#?STATE{cfg = #cfg{max_length = MaxLength,
                                        max_bytes = MaxBytes},
                             msg_bytes_enqueue = BytesEnq,
                             dlx = DlxState} = State) ->
    {NumDlx, BytesDlx} = dlx_stat(DlxState),
    is_below(MaxLength, messages_ready(State) + NumDlx) andalso
    is_below(MaxBytes, BytesEnq + BytesDlx).

is_below(undefined, _Num) ->
    true;
is_below(Val, Num) when is_integer(Val) andalso is_integer(Num) ->
    Num =< trunc(Val * ?LOW_LIMIT).

-spec make_enqueue(option(pid()), option(msg_seqno()), raw_msg()) ->
    protocol().
make_enqueue(undefined, undefined, Msg) ->
    %% need to keep this old version for untracked enqueues
    #enqueue{msg = Msg};
make_enqueue(Pid, Seq, Msg)
  when is_pid(Pid) andalso
       is_integer(Seq) ->
    #?ENQ_V2{seq = Seq,
             msg = Msg,
             size = ?SIZE(Msg)}.

-spec make_register_enqueuer(pid()) -> protocol().
make_register_enqueuer(Pid) ->
    #register_enqueuer{pid = Pid}.

-spec make_checkout(consumer_id(), checkout_spec(), consumer_meta()) ->
    protocol().
make_checkout({_, _} = ConsumerId, Spec, Meta) ->
    #checkout{consumer_id = ConsumerId,
              spec = Spec, meta = Meta}.

-spec make_settle(consumer_key(), [msg_id()]) -> protocol().
make_settle(ConsumerKey, MsgIds) when is_list(MsgIds) ->
    #settle{consumer_key = ConsumerKey, msg_ids = MsgIds}.

-spec make_return(consumer_key(), [msg_id()]) -> protocol().
make_return(ConsumerKey, MsgIds) ->
    #return{consumer_key = ConsumerKey, msg_ids = MsgIds}.

-spec is_return(protocol()) -> boolean().
is_return(Command) ->
    is_record(Command, return).

-spec make_discard(consumer_key(), [msg_id()]) -> protocol().
make_discard(ConsumerKey, MsgIds) ->
    #discard{consumer_key = ConsumerKey, msg_ids = MsgIds}.

-spec make_credit(consumer_key(), rabbit_queue_type:credit(),
                  non_neg_integer(), boolean()) -> protocol().
make_credit(Key, Credit, DeliveryCount, Drain) ->
    #credit{consumer_key = Key,
            credit = Credit,
            delivery_count = DeliveryCount,
            drain = Drain}.

-spec make_modify(consumer_key(), [msg_id()],
                  boolean(), boolean(), mc:annotations()) -> protocol().
make_modify(ConsumerKey, MsgIds, DeliveryFailed, UndeliverableHere, Anns)
  when is_list(MsgIds) andalso
       is_boolean(DeliveryFailed) andalso
       is_boolean(UndeliverableHere) andalso
       is_map(Anns) ->
    #modify{consumer_key = ConsumerKey,
            msg_ids = MsgIds,
            delivery_failed = DeliveryFailed,
            undeliverable_here = UndeliverableHere,
            annotations = Anns}.


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
               #?STATE{msg_bytes_enqueue = Enqueue} = State) ->
    Size = get_header(size, Header),
    State#?STATE{msg_bytes_enqueue = Enqueue - Size}.


add_bytes_return(Header,
                 #?STATE{msg_bytes_checkout = Checkout,
                          msg_bytes_enqueue = Enqueue} = State) ->
    Size = get_header(size, Header),
    State#?STATE{msg_bytes_checkout = Checkout - Size,
                 msg_bytes_enqueue = Enqueue + Size}.

message_size(B) when is_binary(B) ->
    byte_size(B);
message_size(Msg) ->
    case mc:is(Msg) of
        true ->
            mc:size(Msg);
        false ->
            %% probably only hit this for testing so ok to use erts_debug
            {0, erts_debug:size(Msg)}
    end.

all_nodes(#?STATE{consumers = Cons0,
                  enqueuers = Enqs0,
                  waiting_consumers = WaitingConsumers0}) ->
    Nodes0 = maps:fold(fun(_, ?CONSUMER_PID(P), Acc) ->
                               Acc#{node(P) => ok}
                       end, #{}, Cons0),
    Nodes1 = maps:fold(fun(P, _, Acc) ->
                               Acc#{node(P) => ok}
                       end, Nodes0, Enqs0),
    maps:keys(
      lists:foldl(fun({_, ?CONSUMER_PID(P)}, Acc) ->
                          Acc#{node(P) => ok}
                  end, Nodes1, WaitingConsumers0)).

all_pids_for(Node, #?STATE{consumers = Cons0,
                           enqueuers = Enqs0,
                           waiting_consumers = WaitingConsumers0}) ->
    Cons = maps:fold(fun(_, ?CONSUMER_PID(P), Acc)
                           when node(P) =:= Node ->
                             [P | Acc];
                        (_, _, Acc) ->
                             Acc
                     end, [], maps:iterator(Cons0, ordered)),
    Enqs = maps:fold(fun(P, _, Acc)
                           when node(P) =:= Node ->
                             [P | Acc];
                        (_, _, Acc) ->
                             Acc
                     end, Cons, maps:iterator(Enqs0, ordered)),
    lists:foldl(fun({_, ?CONSUMER_PID(P)}, Acc)
                      when node(P) =:= Node ->
                        [P | Acc];
                   (_, Acc) -> Acc
                end, Enqs, WaitingConsumers0).

suspected_pids_for(Node, #?STATE{consumers = Cons0,
                                 enqueuers = Enqs0,
                                 waiting_consumers = WaitingConsumers0}) ->
    Cons = maps:fold(fun(_Key,
                         #consumer{cfg = #consumer_cfg{pid = P},
                                   status = {suspected_down, _}},
                         Acc)
                           when node(P) =:= Node ->
                             [P | Acc];
                        (_, _, Acc) ->
                             Acc
                     end, [], maps:iterator(Cons0, ordered)),
    Enqs = maps:fold(fun(P, #enqueuer{status = suspected_down}, Acc)
                                       when node(P) =:= Node ->
                                         [P | Acc];
                                    (_, _, Acc) ->
                                         Acc
                                 end, Cons, maps:iterator(Enqs0, ordered)),
    lists:foldl(fun({_Key,
                     #consumer{cfg = #consumer_cfg{pid = P},
                               status = {suspected_down, _}}}, Acc)
                      when node(P) =:= Node ->
                        [P | Acc];
                   (_, Acc) -> Acc
                end, Enqs, WaitingConsumers0).

is_expired(Ts, #?STATE{cfg = #cfg{expires = Expires},
                       last_active = LastActive,
                       consumers = Consumers})
  when is_number(LastActive) andalso is_number(Expires) ->
    %% TODO: should it be active consumers?
    Active = maps:filter(fun (_, #consumer{status = {suspected_down, _}}) ->
                                 false;
                             (_, _) ->
                                 true
                         end, Consumers),

    Ts > (LastActive + Expires) andalso maps:size(Active) == 0;
is_expired(_Ts, _State) ->
    false.

get_consumer_priority(#{priority := Priority}) ->
    Priority;
get_consumer_priority(#{args := Args}) ->
    %% fallback, v3 option
    case rabbit_misc:table_lookup(Args, <<"x-priority">>) of
        {_Type, Value} ->
            Value;
        _ ->
            0
    end;
get_consumer_priority(_) ->
    0.

notify_decorators_effect(QName, MaxActivePriority, IsEmpty) ->
    {mod_call, rabbit_quorum_queue, spawn_notify_decorators,
     [QName, consumer_state_changed, [MaxActivePriority, IsEmpty]]}.

notify_decorators_startup(QName) ->
    {mod_call, rabbit_quorum_queue, spawn_notify_decorators,
     [QName, startup, []]}.

convert(_Meta, To, To, State) ->
    State;
convert(Meta, 0, To, State) ->
    convert(Meta, 1, To, rabbit_fifo_v1:convert_v0_to_v1(State));
convert(Meta, 1, To, State) ->
    convert(Meta, 2, To, rabbit_fifo_v3:convert_v1_to_v2(State));
convert(Meta, 2, To, State) ->
    convert(Meta, 3, To, rabbit_fifo_v3:convert_v2_to_v3(State));
convert(Meta, 3, To, State) ->
    convert(Meta, 4, To, rabbit_fifo_v7:convert_v3_to_v4(Meta, State));
convert(Meta, 4, To, State) ->
    %% no conversion needed, this version only includes a logic change
    convert(Meta, 5, To, State);
convert(Meta, 5, To, State) ->
    %% no conversion needed, this version only includes a logic change
    convert(Meta, 6, To, State);
convert(Meta, 6, To, State) ->
    %% no conversion needed, this version only includes a logic change
    convert(Meta, 7, To, State);
convert(Meta, 7, To, State) ->
    convert(Meta, 8, To, convert_v7_to_v8(Meta, State)).

smallest_raft_index(#?STATE{messages = Messages,
                            dlx = #?DLX{discards = Discards}} = State) ->
    SmallestDlxRaIdx = lqueue:fold(fun (?TUPLE(_, Msg), Acc) ->
                                           min(get_msg_idx(Msg), Acc)
                                   end, undefined, Discards),
    SmallestMsgsRaIdx = rabbit_fifo_pq:get_lowest_index(Messages),
    %% scan consumers and returns queue here instead
    smallest_checked_out(State, min(SmallestDlxRaIdx, SmallestMsgsRaIdx)).

smallest_checked_out(#?STATE{returns = Returns,
                             consumers = Consumers}, Min) ->
    SmallestSoFar = lqueue:fold(fun (Msg, Acc) ->
                                        min(get_msg_idx(Msg), Acc)
                                end, Min, Returns),
    maps:fold(fun (_Cid, #consumer{checked_out = Ch}, Acc0) ->
                      maps:fold(
                        fun (_MsgId, Msg, Acc) ->
                                min(get_msg_idx(Msg), Acc)
                        end, Acc0, Ch)
              end, SmallestSoFar, Consumers).

make_requeue(ConsumerKey, Notify, [{MsgId, Idx, Header, Msg}], Acc) ->
    lists:reverse([{append,
                    #requeue{consumer_key = ConsumerKey,
                             index = Idx,
                             header = Header,
                             msg_id = MsgId,
                             msg = Msg},
                    Notify}
                   | Acc]);
make_requeue(ConsumerKey, Notify, [{MsgId, Idx, Header, Msg} | Rem], Acc) ->
    make_requeue(ConsumerKey, Notify, Rem,
                 [{append,
                   #requeue{consumer_key = ConsumerKey,
                            index = Idx,
                            header = Header,
                            msg_id = MsgId,
                            msg = Msg},
                   noreply}
                  | Acc]);
make_requeue(_ConsumerId, _Notify, [], []) ->
    [].

can_immediately_deliver(#?STATE{service_queue = SQ,
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

get_msg_from_cmd(#?ENQ_V2{msg = M}) ->
    M;
get_msg_from_cmd(#enqueue{msg = M}) ->
    M;
get_msg_from_cmd(#requeue{msg = M}) ->
    M.

initial_delivery_count({credited, Count}) ->
    %% credit API v2
    Count;
initial_delivery_count(_) ->
    %% credit API v1
    0.

credit_api_v2(#consumer_cfg{credit_mode = {credited, _}}) ->
    true;
credit_api_v2(_) ->
    false.

link_credit_snd(DeliveryCountRcv, LinkCreditRcv, DeliveryCountSnd, ConsumerCfg) ->
    case credit_api_v2(ConsumerCfg) of
        true ->
            amqp10_util:link_credit_snd(DeliveryCountRcv, LinkCreditRcv, DeliveryCountSnd);
        false ->
            C = DeliveryCountRcv + LinkCreditRcv - DeliveryCountSnd,
            %% C can be negative when receiver decreases credits while messages are in flight.
            max(0, C)
    end.

consumer_id(#consumer{cfg = Cfg}) ->
    {Cfg#consumer_cfg.tag, Cfg#consumer_cfg.pid}.

consumer_id(Key, #?STATE{consumers = Consumers})
  when is_integer(Key) ->
    consumer_id(maps:get(Key, Consumers));
consumer_id({_, _} = ConsumerId, _State) ->
    ConsumerId.


consumer_key_from_id(ConsumerId, #?STATE{consumers = Consumers})
  when is_map_key(ConsumerId, Consumers) ->
    {ok, ConsumerId};
consumer_key_from_id(ConsumerId, #?STATE{consumers = Consumers,
                                         waiting_consumers = Waiting}) ->
    case consumer_key_from_id(ConsumerId, maps:next(maps:iterator(Consumers))) of
        {ok, _} = Res ->
            Res;
        error ->
            %% scan the waiting consumers
            case lists:search(fun ({_K, ?CONSUMER_TAG_PID(T, P)}) ->
                                      {T, P} == ConsumerId
                              end, Waiting) of
                {value, {K, _}} ->
                    {ok, K};
                false ->
                    error
            end
    end;
consumer_key_from_id({CTag, CPid}, {Key, ?CONSUMER_TAG_PID(T, P), _I})
  when T == CTag andalso P == CPid ->
    {ok, Key};
consumer_key_from_id(ConsumerId, {_, _, I}) ->
    consumer_key_from_id(ConsumerId, maps:next(I));
consumer_key_from_id(_ConsumerId, none) ->
    error.

consumer_cancel_info(ConsumerKey, #?STATE{consumers = Consumers}) ->
    case Consumers of
        #{ConsumerKey := #consumer{checked_out = Checked}} ->
            #{key => ConsumerKey,
              num_checked_out => map_size(Checked)};
        _ ->
            #{}
    end.

find_consumer(Key, Consumers) ->
    case Consumers of
        #{Key := Con} ->
            {Key, Con};
        _ when is_tuple(Key) ->
            %% sometimes rabbit_fifo_client may send a settle, return etc
            %% by it's ConsumerId even if it was created with an integer key
            %% as it may have lost it's state after a consumer cancel
            maps_search(fun (_K, ?CONSUMER_TAG_PID(Tag, Pid)) ->
                                Key == {Tag, Pid}
                        end, Consumers);
        _ ->
            undefined
    end.

maps_search(_Pred, none) ->
    undefined;
maps_search(Pred, {K, V, I}) ->
    case Pred(K, V) of
        true ->
            {K, V};
        false ->
            maps_search(Pred, maps:next(I))
    end;
maps_search(Pred, Map) when is_map(Map) ->
    maps_search(Pred, maps:next(maps:iterator(Map))).

-define(DEFAULT_PRIORITY, 4).
-define(MAX_PRIORITY, 31).

msg_priority(Msg) ->
    case mc:is(Msg) of
        true ->
            case mc:priority(Msg) of
                P when is_integer(P) ->
                    min(P, ?MAX_PRIORITY);
                _ ->
                    ?DEFAULT_PRIORITY
            end;
        false ->
            ?DEFAULT_PRIORITY
    end.

do_snapshot(MacVer, Ts, Ch, RaAux, DiscardedBytes, Force)
  when element(1, Ch) == checkpoint andalso
       is_integer(MacVer)  andalso
       MacVer >= 8 ->
    Idx = element(2, Ch),
    LastTs = element(3, Ch),
    do_snapshot(MacVer, Ts, #snapshot{index = Idx, timestamp = LastTs},
                RaAux, DiscardedBytes, Force);
do_snapshot(MacVer, Ts, #snapshot{index = _ChIdx,
                                  timestamp = SnapTime,
                                  discarded_bytes = LastDiscardedBytes} = Snap0,
            RaAux, DiscardedBytes, Force)
  when is_integer(MacVer) andalso MacVer >= 8 ->
    LastAppliedIdx = ra_aux:last_applied(RaAux),
    #?STATE{consumers = Consumers,
            enqueuers = Enqueuers} = MacState = ra_aux:machine_state(RaAux),
    TimeSince = Ts - SnapTime,
    MsgsTot = messages_total(MacState),
    %% if the approximate snapshot size * 2 can be reclaimed it is worth
    %% taking a snapshot
    %% take number of enqueues and consumers into account
    %% message: 32 bytes
    %% enqueuer: 96 bytes
    %% consumer: 256 bytes
    NumEnqueuers = map_size(Enqueuers),
    NumConsumers = map_size(Consumers),
    ApproxSnapSize = 4096 +
                     (MsgsTot * 32) +
                     (NumEnqueuers * 96) +
                     (NumConsumers * 256),

    EnoughDataRemoved = DiscardedBytes - LastDiscardedBytes > (ApproxSnapSize * 3),

    {CheckMinInterval, _CheckMinIndexes, _CheckMaxIndexes} =
        persistent_term:get(quorum_queue_checkpoint_config,
                            {?CHECK_MIN_INTERVAL_MS, ?CHECK_MIN_INDEXES,
                             ?CHECK_MAX_INDEXES}),
    EnoughTimeHasPassed = TimeSince > CheckMinInterval,
    case (EnoughTimeHasPassed andalso
          EnoughDataRemoved) orelse
         Force of
        true ->
            {#snapshot{index = LastAppliedIdx,
                       timestamp = Ts,
                       messages_total = MsgsTot,
                       discarded_bytes = DiscardedBytes},
             [{release_cursor, LastAppliedIdx, MacState}]};
        false ->
            {Snap0, []}
    end.

discard(Meta, MsgIds, ConsumerKey,
        #consumer{checked_out = Checked} = Con,
        DelFailed, Anns,
        #?STATE{cfg = #cfg{dead_letter_handler = DLH},
                discarded_bytes = DiscardedBytes0,
                dlx = DlxState0} = State0) ->
    %% We publish to dead-letter exchange in the same order
    %% as messages got rejected by the client.
    DiscardMsgs = lists:filtermap(
                    fun(Id) ->
                            case maps:get(Id, Checked, undefined) of
                                undefined ->
                                    false;
                                Msg0 ->
                                    {true, incr_msg_headers(Msg0, DelFailed, Anns)}
                            end
                    end, MsgIds),
    {DlxState, RetainedBytes, Effects} =
        discard_or_dead_letter(DiscardMsgs, rejected, DLH, DlxState0),
    State = State0#?STATE{dlx = DlxState,
                          discarded_bytes = DiscardedBytes0 - RetainedBytes},
    complete_and_checkout(Meta, MsgIds, ConsumerKey, Con, Effects, State).

incr_msg_headers(Msg0, DeliveryFailed, Anns) ->
    Msg1 = update_msg_header(acquired_count, fun incr/1, 1, Msg0),
    Msg2 = case map_size(Anns) > 0 of
               true ->
                   update_msg_header(anns, fun(A) ->
                                                   maps:merge(A, Anns)
                                           end, Anns,
                                     Msg1);
               false ->
                   Msg1
           end,
    case DeliveryFailed of
        true ->
            update_msg_header(delivery_count, fun incr/1, 1, Msg2);
        false ->
            Msg2
    end.

exec_read(Flru0, ReadPlan, Msgs) ->
    try ra_log_read_plan:execute(ReadPlan, Flru0) of
        {Entries, Flru} ->
            %% return a list in original order
            {lists:map(fun ({MsgId, Msg}) ->
                               Idx = get_msg_idx(Msg),
                               Header = get_msg_header(Msg),
                               Cmd = maps:get(Idx, Entries),
                               {MsgId, {Header, get_msg_from_cmd(Cmd)}}
                       end, Msgs), Flru}
    catch exit:{missing_key, _}
            when Flru0 =/= undefined ->
              %% this segment has most likely been appended to but the
              %% cached index doesn't know about new items and need to be
              %% re-generated
              _ = ra_flru:evict_all(Flru0),
              %% retry without segment cache
              exec_read(undefined, ReadPlan, Msgs)
    end.

maps_ordered_keys(Map) ->
    lists:sort(maps:keys(Map)).

%% enqueue overhead: 256b + message size

estimate_discarded_size(#?ENQ_V2{}) ->
    0;
estimate_discarded_size(Cmd)
  when is_record(Cmd, settle) orelse
       is_record(Cmd, return) orelse
       is_record(Cmd, discard) orelse
       is_record(Cmd, credit) ->
    128;
estimate_discarded_size(#checkout{}) ->
    300;
estimate_discarded_size(#register_enqueuer{}) ->
    200;
estimate_discarded_size(#modify{}) ->
    256;
estimate_discarded_size(#update_config{}) ->
    512;
estimate_discarded_size(#purge{}) ->
    64;
estimate_discarded_size(#purge_nodes{}) ->
    64;
estimate_discarded_size(#requeue{}) ->
    0;
estimate_discarded_size(#enqueue{}) ->
    0;
estimate_discarded_size({nodeup, _}) ->
    96;
estimate_discarded_size({down, _, _}) ->
    96;
estimate_discarded_size({dlx, _Cmd}) ->
    64;
estimate_discarded_size(_Cmd) ->
    %% something is better than nothing
    64.


dlx_apply(_Meta, {dlx, {settle, MsgIds}}, at_least_once,
          #?DLX{consumer = #dlx_consumer{checked_out = Checked0}} = State0) ->
    Acked = maps:with(MsgIds, Checked0),
    {DBytes, State} =
        maps:fold(
          fun(MsgId, ?TUPLE(_Rsn, Msg),
              {Sz, #?DLX{consumer = #dlx_consumer{checked_out = Checked} = C,
                         msg_bytes_checkout = BytesCheckout,
                         ra_indexes = Indexes0} = S}) ->
                  Idx = get_msg_idx(Msg),
                  Hdr = get_msg_header(Msg),
                  Indexes = rabbit_fifo_index:delete(Idx, Indexes0),
                  Size = get_header(size, Hdr),
                  {Sz + Size + ?ENQ_OVERHEAD, 
                   S#?DLX{consumer = C#dlx_consumer{checked_out =
                                                    maps:remove(MsgId, Checked)},
                          msg_bytes_checkout = BytesCheckout - Size,
                          ra_indexes = Indexes}}
          end, {0, State0}, Acked),
    {State, DBytes,
     [{mod_call, rabbit_global_counters, messages_dead_lettered_confirmed,
       [rabbit_quorum_queue, at_least_once, maps:size(Acked)]}]};
dlx_apply(_, {dlx, {checkout, Pid, Prefetch}},
          at_least_once,
          #?DLX{consumer = undefined} = State0) ->
    State = State0#?DLX{consumer = #dlx_consumer{pid = Pid,
                                                 prefetch = Prefetch}},
    {State, 0, []};
dlx_apply(_, {dlx, {checkout, ConsumerPid, Prefetch}},
          at_least_once,
          #?DLX{consumer = #dlx_consumer{pid = OldConsumerPid,
                                         checked_out = CheckedOutOldConsumer},
                discards = Discards0,
                msg_bytes = Bytes,
                msg_bytes_checkout = BytesCheckout} = State0) ->
    %% Since we allow only a single consumer, the new consumer replaces the old consumer.
    case ConsumerPid of
        OldConsumerPid ->
            ok;
        _ ->
            ?LOG_DEBUG("Terminating ~p since ~p becomes active rabbit_fifo_dlx_worker",
                       [OldConsumerPid, ConsumerPid]),
            %% turn into aux command
            ensure_worker_terminated(State0)
    end,
    %% All checked out messages to the old consumer need to be returned to the discards queue
    %% such that these messages will be re-delivered to the new consumer.
    %% When inserting back into the discards queue, we respect the original order in which messages
    %% were discarded.
    Checked0 = maps:to_list(CheckedOutOldConsumer),
    Checked1 = lists:keysort(1, Checked0),
    {Discards, BytesMoved} = lists:foldr(
                               fun({_Id, ?TUPLE(_, Msg) = RsnMsg}, {D, B}) ->
                                       Size = get_header(size, get_msg_header(Msg)),
                                       {lqueue:in_r(RsnMsg, D), B + Size}
                               end, {Discards0, 0}, Checked1),
    State = State0#?DLX{consumer = #dlx_consumer{pid = ConsumerPid,
                                                 prefetch = Prefetch},
                        discards = Discards,
                        msg_bytes = Bytes + BytesMoved,
                        msg_bytes_checkout = BytesCheckout - BytesMoved},
    {State, 0, []};
dlx_apply(_, Cmd, DLH, State) ->
    ?LOG_DEBUG("Ignoring command ~tp for dead_letter_handler ~tp", [Cmd, DLH]),
    {State, 0, []}.

%% nodeup: 74 bytes
%% down: 90 bytes
%% enqueue overhead 210

ensure_worker_started(QRef, #?DLX{consumer = undefined}) ->
    start_worker(QRef);
ensure_worker_started(QRef, #?DLX{consumer = #dlx_consumer{pid = Pid}}) ->
    case is_local_and_alive(Pid) of
        true ->
            ?LOG_DEBUG("rabbit_fifo_dlx_worker ~tp already started for ~ts",
                             [Pid, rabbit_misc:rs(QRef)]);
        false ->
            start_worker(QRef)
    end.

%% Ensure that starting the rabbit_fifo_dlx_worker succeeds.
%% Therefore, do not use an effect.
%% Also therefore, if starting the rabbit_fifo_dlx_worker fails, let the
%% Ra server process crash in which case another Ra node will become leader.
start_worker(QRef) ->
    {ok, Pid} = supervisor:start_child(rabbit_fifo_dlx_sup, [QRef]),
    ?LOG_DEBUG("started rabbit_fifo_dlx_worker ~tp for ~ts",
                     [Pid, rabbit_misc:rs(QRef)]).

ensure_worker_terminated(#?DLX{consumer = undefined}) ->
    ok;
ensure_worker_terminated(#?DLX{consumer = #dlx_consumer{pid = Pid}}) ->
    case is_local_and_alive(Pid) of
        true ->
            %% Note that we can't return a mod_call effect here
            %% because mod_call is executed on the leader only.
            ok = supervisor:terminate_child(rabbit_fifo_dlx_sup, Pid),
            ?LOG_DEBUG("terminated rabbit_fifo_dlx_worker ~tp", [Pid]);
        false ->
            ok
    end.

local_alive_consumer_pid(#?DLX{consumer = undefined}) ->
    undefined;
local_alive_consumer_pid(#?DLX{consumer = #dlx_consumer{pid = Pid}}) ->
    case is_local_and_alive(Pid) of
        true ->
            Pid;
        false ->
            undefined
    end.

is_local_and_alive(Pid)
  when node(Pid) =:= node() ->
    is_process_alive(Pid);
is_local_and_alive(_) ->
    false.

update_config(at_least_once, at_least_once, _, State) ->
    case local_alive_consumer_pid(State) of
        undefined ->
            {State, []};
        Pid ->
            %% Notify rabbit_fifo_dlx_worker about potentially updated policies.
            {State, [{send_msg, Pid, {dlx_event, self(), lookup_topology}, cast}]}
    end;
update_config(SameDLH, SameDLH, _, State) ->
    {State, []};
update_config(OldDLH, NewDLH, QRes, State0) ->
    LogOnLeader = {mod_call, logger, debug,
                   ["Switching dead_letter_handler from ~tp to ~tp for ~ts",
                    [OldDLH, NewDLH, rabbit_misc:rs(QRes)]]},
    {State1, Effects0} = switch_from(OldDLH, QRes, State0),
    {State, Effects} = switch_to(NewDLH, State1, Effects0),
    {State, [LogOnLeader|Effects]}.

switch_from(at_least_once, QRes, State) ->
    %% Switch from at-least-once to some other strategy.
    %% TODO: do worker teardown in aux handler
    ensure_worker_terminated(State),
    {Num, Bytes} = dlx_stat(State),
    %% Log only on leader.
    {#?DLX{}, [{mod_call, logger, info,
               ["Deleted ~b dead-lettered messages (with total messages size of ~b bytes) in ~ts",
                [Num, Bytes, rabbit_misc:rs(QRes)]]}]};
switch_from(_, _, State) ->
    {State, []}.

switch_to(at_least_once, _, Effects) ->
    %% Switch from some other strategy to at-least-once.
    %% Dlx worker needs to be started on the leader.
    %% The cleanest way to determine the Ra state of this node is delegation to handle_aux.
    {#?DLX{}, [{aux, {dlx, setup}} | Effects]};
switch_to(_, State, Effects) ->
    {State, Effects}.


dlx_stat(#?DLX{consumer = Con,
               discards = Discards,
               msg_bytes = MsgBytes,
               msg_bytes_checkout = MsgBytesCheckout}) ->
    Num0 = lqueue:len(Discards),
    Num = case Con of
              undefined ->
                  Num0;
              #dlx_consumer{checked_out = Checked} ->
                  %% O(1) because Erlang maps maintain their own size
                  Num0 + maps:size(Checked)
          end,
    Bytes = MsgBytes + MsgBytesCheckout,
    {Num, Bytes}.


dlx_overview(#?DLX{consumer = undefined,
                  msg_bytes = MsgBytes,
                  msg_bytes_checkout = 0,
                  discards = Discards}) ->
    dlx_overview0(Discards, #{}, MsgBytes, 0);
dlx_overview(#?DLX{consumer = #dlx_consumer{checked_out = Checked},
                  msg_bytes = MsgBytes,
                  msg_bytes_checkout = MsgBytesCheckout,
                  discards = Discards}) ->
    dlx_overview0(Discards, Checked, MsgBytes, MsgBytesCheckout).

dlx_overview0(Discards, Checked, MsgBytes, MsgBytesCheckout) ->
    #{num_discarded => lqueue:len(Discards),
      num_discard_checked_out => maps:size(Checked),
      discard_message_bytes => MsgBytes,
      discard_checkout_message_bytes => MsgBytesCheckout}.


discard_or_dead_letter(Msgs, Reason, undefined, State) ->
    {State, 0,
     [{mod_call, rabbit_global_counters, messages_dead_lettered,
       [Reason, rabbit_quorum_queue, disabled, length(Msgs)]}]};
discard_or_dead_letter(Msgs0, Reason, {at_most_once, {Mod, Fun, Args}}, State) ->
    Idxs = lists:map(fun get_msg_idx/1, Msgs0),
    %% TODO: this could be turned into a log_ext effect instead to avoid
    %% reading from disk inside the qq process
    Effect = {log, Idxs,
              fun (Log) ->
                      Lookup = maps:from_list(lists:zip(Idxs, Log)),
                      Msgs = [begin
                                  Idx = get_msg_idx(Msg),
                                  Hdr = get_msg_header(Msg),
                                  Cmd = maps:get(Idx, Lookup),
                                  %% ensure header delivery count
                                  %% is copied to the message container
                                  annotate_msg(Hdr, rabbit_fifo:get_msg_from_cmd(Cmd))
                              end || Msg <- Msgs0],
                      [{mod_call, Mod, Fun, Args ++ [Reason, Msgs]}]
              end},
    {State, 0, [Effect]};
discard_or_dead_letter(Msgs, Reason, at_least_once, State0)
  when Reason =/= maxlen ->
    RetainedBytes = lists:foldl(fun (M, Acc) ->
                                        Acc + size_in_bytes(M) + ?ENQ_OVERHEAD
                                end, 0, Msgs),
    State = lists:foldl(fun(Msg, #?DLX{discards = D0,
                                       msg_bytes = B0,
                                       ra_indexes = I0} = S0) ->
                                MsgSize = size_in_bytes(Msg),
                                D = lqueue:in(?TUPLE(Reason, Msg), D0),
                                B = B0 + MsgSize,
                                Idx = get_msg_idx(Msg),
                                I = rabbit_fifo_index:append(Idx, I0),
                                S0#?DLX{discards = D,
                                        msg_bytes = B,
                                        ra_indexes = I}
                        end, State0, Msgs),
    {State, RetainedBytes,
     [{mod_call, rabbit_global_counters, messages_dead_lettered,
       [Reason, rabbit_quorum_queue, at_least_once, length(Msgs)]}]}.


size_in_bytes(Msg) ->
    Header = get_msg_header(Msg),
    get_header(size, Header).

dlx_checkout(at_least_once, #?DLX{consumer = #dlx_consumer{}} = State) ->
    dlx_checkout0(dlx_checkout_one(State), []);
dlx_checkout(_, State) ->
    {State, []}.

dlx_checkout0({success, MsgId, ?TUPLE(Reason, Msg), State}, SendAcc) ->
    Idx = get_msg_idx(Msg),
    Hdr = get_msg_header(Msg),
    DelMsg = {Idx, {Reason, Hdr, MsgId}},
    dlx_checkout0(dlx_checkout_one(State), [DelMsg | SendAcc]);
dlx_checkout0(#?DLX{consumer = #dlx_consumer{pid = Pid}} = State, SendAcc) ->
    Effects = dlx_delivery_effects(Pid, SendAcc),
    {State, Effects}.

dlx_checkout_one(#?DLX{consumer = #dlx_consumer{checked_out = Checked,
                                               prefetch = Prefetch}} = State)
  when map_size(Checked) >= Prefetch ->
    State;
dlx_checkout_one(#?DLX{discards = Discards0,
                       msg_bytes = Bytes,
                       msg_bytes_checkout = BytesCheckout,
                       consumer = #dlx_consumer{checked_out = Checked0,
                                                next_msg_id = Next} = Con0} = State0) ->
    case lqueue:out(Discards0) of
        {{value, ?TUPLE(_, Msg) = ReasonMsg}, Discards} ->
            Checked = maps:put(Next, ReasonMsg, Checked0),
            Size = size_in_bytes(Msg),
            State = State0#?DLX{discards = Discards,
                                msg_bytes = Bytes - Size,
                                msg_bytes_checkout = BytesCheckout + Size,
                                consumer = Con0#dlx_consumer{checked_out = Checked,
                                                             next_msg_id = Next + 1}},
            {success, Next, ReasonMsg, State};
        {empty, _} ->
            State0
    end.

dlx_delivery_effects(_CPid, []) ->
    [];
dlx_delivery_effects(CPid, Msgs0) ->
    Msgs1 = lists:reverse(Msgs0),
    {RaftIdxs, RsnIds} = lists:unzip(Msgs1),
    [{log, RaftIdxs,
      fun(Log) ->
              Msgs = lists:zipwith(
                       fun (Cmd, {Reason, H, MsgId}) ->
                               {MsgId, {Reason,
                                        annotate_msg(H, rabbit_fifo:get_msg_from_cmd(Cmd))}}
                       end, Log, RsnIds),
              [{send_msg, CPid, {dlx_event, self(), {dlx_delivery, Msgs}}, [cast]}]
      end}].

consumer_info(ConsumerKey,
              #consumer{checked_out = Checked,
                        credit = Credit,
                        delivery_count = DeliveryCount,
                        next_msg_id = NextMsgId},
              State) ->
    #{next_msg_id => NextMsgId,
      credit => Credit,
      key => ConsumerKey,
      delivery_count => DeliveryCount,
      is_active => is_active(ConsumerKey, State),
      num_checked_out => map_size(Checked)}.
