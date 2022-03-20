%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_fifo_v0).

-behaviour(ra_machine).

-compile(inline_list_funcs).
-compile(inline).
-compile({no_auto_import, [apply/3]}).

-include("rabbit_fifo_v0.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-export([
         init/1,
         apply/3,
         state_enter/2,
         tick/2,
         overview/1,
         get_checked_out/4,
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
         query_single_active_consumer/1,
         query_in_memory_usage/1,
         usage/1,

         zero/1,

         %% misc
         dehydrate_state/1,
         normalize/1,
         normalize_for_v1/1,
         %% getters for coversions
         get_field/2,
         get_cfg_field/2,

         %% protocol helpers
         make_enqueue/3,
         make_checkout/3,
         make_settle/2,
         make_return/2,
         make_discard/2,
         make_credit/4,
         make_purge/0,
         make_purge_nodes/1,
         make_update_config/1
        ]).

%% command records representing all the protocol actions that are supported
-record(enqueue, {pid :: option(pid()),
                  seq :: option(msg_seqno()),
                  msg :: raw_msg()}).
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

-opaque protocol() ::
    #enqueue{} |
    #checkout{} |
    #settle{} |
    #return{} |
    #discard{} |
    #credit{} |
    #purge{} |
    #purge_nodes{} |
    #update_config{}.

-type command() :: protocol() | ra_machine:builtin_command().
%% all the command types supported by ra fifo

-type client_msg() :: delivery().
%% the messages `rabbit_fifo' can send to consumers.

-opaque state() :: #?STATE{}.

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

-spec init(config()) -> state().
init(#{name := Name,
       queue_resource := Resource} = Conf) ->
    update_config(Conf, #?STATE{cfg = #cfg{name = Name,
                                            resource = Resource}}).

update_config(Conf, State) ->
    DLH = maps:get(dead_letter_handler, Conf, undefined),
    BLH = maps:get(become_leader_handler, Conf, undefined),
    SHI = maps:get(release_cursor_interval, Conf, ?RELEASE_CURSOR_EVERY),
    MaxLength = maps:get(max_length, Conf, undefined),
    MaxBytes = maps:get(max_bytes, Conf, undefined),
    MaxMemoryLength = maps:get(max_in_memory_length, Conf, undefined),
    MaxMemoryBytes = maps:get(max_in_memory_bytes, Conf, undefined),
    DeliveryLimit = maps:get(delivery_limit, Conf, undefined),
    ConsumerStrategy = case maps:get(single_active_consumer_on, Conf, false) of
                           true ->
                               single_active;
                           false ->
                               competing
                       end,
    Cfg = State#?STATE.cfg,
    SHICur = case State#?STATE.cfg of
                 #cfg{release_cursor_interval = {_, C}} ->
                     C;
                 #cfg{release_cursor_interval = undefined} ->
                     SHI;
                 #cfg{release_cursor_interval = C} ->
                     C
             end,

    State#?STATE{cfg = Cfg#cfg{release_cursor_interval = {SHI, SHICur},
                                dead_letter_handler = DLH,
                                become_leader_handler = BLH,
                                max_length = MaxLength,
                                max_bytes = MaxBytes,
                                max_in_memory_length = MaxMemoryLength,
                                max_in_memory_bytes = MaxMemoryBytes,
                                consumer_strategy = ConsumerStrategy,
                                delivery_limit = DeliveryLimit}}.

zero(_) ->
    0.

% msg_ids are scoped per consumer
% ra_indexes holds all raft indexes for enqueues currently on queue
-spec apply(ra_machine:command_meta_data(), command(), state()) ->
    {state(), Reply :: term(), ra_machine:effects()} |
    {state(), Reply :: term()}.
apply(Metadata, #enqueue{pid = From, seq = Seq,
                         msg = RawMsg}, State00) ->
    apply_enqueue(Metadata, From, Seq, RawMsg, State00);
apply(Meta,
      #settle{msg_ids = MsgIds, consumer_id = ConsumerId},
      #?STATE{consumers = Cons0} = State) ->
    case Cons0 of
        #{ConsumerId := Con0} ->
            % need to increment metrics before completing as any snapshot
            % states taken need to include them
            complete_and_checkout(Meta, MsgIds, ConsumerId,
                                  Con0, [], State);
        _ ->
            {State, ok}

    end;
apply(Meta, #discard{msg_ids = MsgIds, consumer_id = ConsumerId},
      #?STATE{consumers = Cons0} = State0) ->
    case Cons0 of
        #{ConsumerId := Con0} ->
            Discarded = maps:with(MsgIds, Con0#consumer.checked_out),
            Effects = dead_letter_effects(rejected, Discarded, State0, []),
            complete_and_checkout(Meta, MsgIds, ConsumerId, Con0,
                                  Effects, State0);
        _ ->
            {State0, ok}
    end;
apply(Meta, #return{msg_ids = MsgIds, consumer_id = ConsumerId},
      #?STATE{consumers = Cons0} = State) ->
    case Cons0 of
        #{ConsumerId := #consumer{checked_out = Checked0}} ->
            Returned = maps:with(MsgIds, Checked0),
            return(Meta, ConsumerId, Returned, [], State);
        _ ->
            {State, ok}
    end;
apply(Meta, #credit{credit = NewCredit, delivery_count = RemoteDelCnt,
                    drain = Drain, consumer_id = ConsumerId},
      #?STATE{consumers = Cons0,
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
                checkout(Meta, State0#?STATE{service_queue = ServiceQueue,
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
                        maps:get(ConsumerId, State1#?STATE.consumers),
                    %% add the outstanding credit to the delivery count
                    DeliveryCount = Con#consumer.delivery_count + PostCred,
                    Consumers = maps:put(ConsumerId,
                                         Con#consumer{delivery_count = DeliveryCount,
                                                      credit = 0},
                                         State1#?STATE.consumers),
                    Drained = Con#consumer.credit,
                    {CTag, _} = ConsumerId,
                    {State1#?STATE{consumers = Consumers},
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
                    State = State0#?STATE{waiting_consumers =
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
      #?STATE{cfg = #cfg{consumer_strategy = single_active}} = State0) ->
    {State0, {error, unsupported}};
apply(#{from := From} = Meta, #checkout{spec = {dequeue, Settlement},
                                        meta = ConsumerMeta,
                                        consumer_id = ConsumerId},
      #?STATE{consumers = Consumers} = State0) ->
    Exists = maps:is_key(ConsumerId, Consumers),
    case messages_ready(State0) of
        0 ->
            {State0, {dequeue, empty}};
        _ when Exists ->
            %% a dequeue using the same consumer_id isn't possible at this point
            {State0, {dequeue, empty}};
        Ready ->
            State1 = update_consumer(ConsumerId, ConsumerMeta,
                                     {once, 1, simple_prefetch},
                                     State0),
            {success, _, MsgId, Msg, State2} = checkout_one(State1),
            {State, Effects} = case Settlement of
                                   unsettled ->
                                       {_, Pid} = ConsumerId,
                                       {State2, [{monitor, process, Pid}]};
                                   settled ->
                                       %% immediately settle the checkout
                                       {State3, _, Effects0} =
                                           apply(Meta, make_settle(ConsumerId, [MsgId]),
                                                 State2),
                                       {State3, Effects0}
                               end,
            case Msg of
                {RaftIdx, {Header, 'empty'}} ->
                    %% TODO add here new log effect with reply
                    {State, '$ra_no_reply',
                     reply_log_effect(RaftIdx, MsgId, Header, Ready - 1, From)};
                _ ->
                    {State, {dequeue, {MsgId, Msg}, Ready-1}, Effects}
            end
    end;
apply(Meta, #checkout{spec = cancel, consumer_id = ConsumerId}, State0) ->
    {State, Effects} = cancel_consumer(ConsumerId, State0, [], consumer_cancel),
    checkout(Meta, State, Effects);
apply(Meta, #checkout{spec = Spec, meta = ConsumerMeta,
                      consumer_id = {_, Pid} = ConsumerId},
      State0) ->
    State1 = update_consumer(ConsumerId, ConsumerMeta, Spec, State0),
    checkout(Meta, State1, [{monitor, process, Pid}]);
apply(#{index := RaftIdx}, #purge{},
      #?STATE{ra_indexes = Indexes0,
               returns = Returns,
               messages = Messages} = State0) ->
    Total = messages_ready(State0),
    Indexes1 = lists:foldl(fun rabbit_fifo_index:delete/2, Indexes0,
                          [I || {I, _} <- lists:sort(maps:values(Messages))]),
    Indexes = lists:foldl(fun rabbit_fifo_index:delete/2, Indexes1,
                          [I || {_, {I, _}} <- lqueue:to_list(Returns)]),
    {State, _, Effects} =
        update_smallest_raft_index(RaftIdx,
                                   State0#?STATE{ra_indexes = Indexes,
                                                  messages = #{},
                                                  returns = lqueue:new(),
                                                  msg_bytes_enqueue = 0,
                                                  prefix_msgs = {0, [], 0, []},
                                                  low_msg_num = undefined,
                                                  msg_bytes_in_memory = 0,
                                                  msgs_ready_in_memory = 0},
                                   []),
    %% as we're not checking out after a purge (no point) we have to
    %% reverse the effects ourselves
    {State, {purge, Total},
     lists:reverse([garbage_collection | Effects])};
apply(Meta, {down, Pid, noconnection},
      #?STATE{consumers = Cons0,
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
                          {St, Effs1} = return_all(S0, Effs,
                                                   Cid, C0#consumer{credit = Credit}),
                          %% if the consumer was cancelled there is a chance it got
                          %% removed when returning hence we need to be defensive here
                          Waiting = case St#?STATE.consumers of
                                        #{Cid := C} ->
                                            Waiting0 ++ [{Cid, C}];
                                        _ ->
                                            Waiting0
                                    end,
                          {St#?STATE{consumers = maps:remove(Cid, St#?STATE.consumers),
                                      waiting_consumers = Waiting},
                           Effs1};
                     (_, _, S) ->
                          S
                  end, {State0, []}, Cons0),
    WaitingConsumers = update_waiting_consumer_status(Node, State1,
                                                      suspected_down),

    %% select a new consumer from the waiting queue and run a checkout
    State2 = State1#?STATE{waiting_consumers = WaitingConsumers},
    {State, Effects1} = activate_next_consumer(State2, Effects0),

    %% mark any enquers as suspected
    Enqs = maps:map(fun(P, E) when node(P) =:= Node ->
                            E#enqueuer{status = suspected_down};
                       (_, E) -> E
                    end, Enqs0),
    Effects = [{monitor, node, Node} | Effects1],
    checkout(Meta, State#?STATE{enqueuers = Enqs}, Effects);
apply(Meta, {down, Pid, noconnection},
      #?STATE{consumers = Cons0,
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
                  {St, Eff0} = return_all(St0, Eff, Cid, C),
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
    Effects = case maps:size(State#?STATE.consumers) of
                  0 ->
                      [{aux, inactive}, {monitor, node, Node}];
                  _ ->
                      [{monitor, node, Node}]
              end ++ Effects1,
    checkout(Meta, State#?STATE{enqueuers = Enqs}, Effects);
apply(Meta, {down, Pid, _Info}, State0) ->
    {State, Effects} = handle_down(Pid, State0),
    checkout(Meta, State, Effects);
apply(Meta, {nodeup, Node}, #?STATE{consumers = Cons0,
                                     enqueuers = Enqs0,
                                     service_queue = SQ0} = State0) ->
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
    {Cons1, SQ, Effects1} =
        maps:fold(fun({_, P} = ConsumerId, C, {CAcc, SQAcc, EAcc})
                        when (node(P) =:= Node) and
                             (C#consumer.status =/= cancelled) ->
                          EAcc1 = ConsumerUpdateActiveFun(State0, ConsumerId,
                                                          C, true, up, EAcc),
                          update_or_remove_sub(ConsumerId,
                                               C#consumer{status = up}, CAcc,
                                               SQAcc, EAcc1);
                     (_, _, Acc) ->
                          Acc
                  end, {Cons0, SQ0, Monitors}, Cons0),
    Waiting = update_waiting_consumer_status(Node, State0, up),
    State1 = State0#?STATE{consumers = Cons1,
                            enqueuers = Enqs1,
                            service_queue = SQ,
                            waiting_consumers = Waiting},
    {State, Effects} = activate_next_consumer(State1, Effects1),
    checkout(Meta, State, Effects);
apply(_, {nodedown, _Node}, State) ->
    {State, ok};
apply(_, #purge_nodes{nodes = Nodes}, State0) ->
    {State, Effects} = lists:foldl(fun(Node, {S, E}) ->
                                           purge_node(Node, S, E)
                                   end, {State0, []}, Nodes),
    {State, ok, Effects};
apply(Meta, #update_config{config = Conf}, State) ->
    checkout(Meta, update_config(Conf, State), []).

purge_node(Node, State, Effects) ->
    lists:foldl(fun(Pid, {S0, E0}) ->
                        {S, E} = handle_down(Pid, S0),
                        {S, E0 ++ E}
                end, {State, Effects}, all_pids_for(Node, State)).

%% any downs that re not noconnection
handle_down(Pid, #?STATE{consumers = Cons0,
                          enqueuers = Enqs0} = State0) ->
    % Remove any enqueuer for the same pid and enqueue any pending messages
    % This should be ok as we won't see any more enqueues from this pid
    State1 = case maps:take(Pid, Enqs0) of
                 {#enqueuer{pending = Pend}, Enqs} ->
                     lists:foldl(fun ({_, RIdx, RawMsg}, S) ->
                                         enqueue(RIdx, RawMsg, S)
                                 end, State0#?STATE{enqueuers = Enqs}, Pend);
                 error ->
                     State0
             end,
    {Effects1, State2} = handle_waiting_consumer_down(Pid, State1),
    % return checked out messages to main queue
    % Find the consumers for the down pid
    DownConsumers = maps:keys(
                      maps:filter(fun({_, P}, _) -> P =:= Pid end, Cons0)),
    lists:foldl(fun(ConsumerId, {S, E}) ->
                        cancel_consumer(ConsumerId, S, E, down)
                end, {State2, Effects1}, DownConsumers).

consumer_active_flag_update_function(#?STATE{cfg = #cfg{consumer_strategy = competing}}) ->
    fun(State, ConsumerId, Consumer, Active, ActivityStatus, Effects) ->
        consumer_update_active_effects(State, ConsumerId, Consumer, Active,
                                       ActivityStatus, Effects)
    end;
consumer_active_flag_update_function(#?STATE{cfg = #cfg{consumer_strategy = single_active}}) ->
    fun(_, _, _, _, _, Effects) ->
        Effects
    end.

handle_waiting_consumer_down(_Pid,
                             #?STATE{cfg = #cfg{consumer_strategy = competing}} = State) ->
    {[], State};
handle_waiting_consumer_down(_Pid,
                             #?STATE{cfg = #cfg{consumer_strategy = single_active},
                                      waiting_consumers = []} = State) ->
    {[], State};
handle_waiting_consumer_down(Pid,
                             #?STATE{cfg = #cfg{consumer_strategy = single_active},
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
    State = State0#?STATE{waiting_consumers = StillUp},
    {Effects, State}.

update_waiting_consumer_status(Node,
                               #?STATE{waiting_consumers = WaitingConsumers},
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
state_enter(leader, #?STATE{consumers = Cons,
                             enqueuers = Enqs,
                             waiting_consumers = WaitingConsumers,
                             cfg = #cfg{name = Name,
                                        resource = Resource,
                                        become_leader_handler = BLH},
                             prefix_msgs = {0, [], 0, []}
                            }) ->
    % return effects to monitor all current consumers and enqueuers
    Pids = lists:usort(maps:keys(Enqs)
        ++ [P || {_, P} <- maps:keys(Cons)]
        ++ [P || {{_, P}, _} <- WaitingConsumers]),
    Mons = [{monitor, process, P} || P <- Pids],
    Nots = [{send_msg, P, leader_change, ra_event} || P <- Pids],
    NodeMons = lists:usort([{monitor, node, node(P)} || P <- Pids]),
    FHReservation = [{mod_call, rabbit_quorum_queue, file_handle_leader_reservation, [Resource]}],
    Effects = Mons ++ Nots ++ NodeMons ++ FHReservation,
    case BLH of
        undefined ->
            Effects;
        {Mod, Fun, Args} ->
            [{mod_call, Mod, Fun, Args ++ [Name]} | Effects]
    end;
state_enter(eol, #?STATE{enqueuers = Enqs,
                          consumers = Custs0,
                          waiting_consumers = WaitingConsumers0}) ->
    Custs = maps:fold(fun({_, P}, V, S) -> S#{P => V} end, #{}, Custs0),
    WaitingConsumers1 = lists:foldl(fun({{_, P}, V}, Acc) -> Acc#{P => V} end,
                                    #{}, WaitingConsumers0),
    AllConsumers = maps:merge(Custs, WaitingConsumers1),
    [{send_msg, P, eol, ra_event}
     || P <- maps:keys(maps:merge(Enqs, AllConsumers))] ++
        [{mod_call, rabbit_quorum_queue, file_handle_release_reservation, []}];
state_enter(State, #?STATE{cfg = #cfg{resource = _Resource}}) when State =/= leader ->
    FHReservation = {mod_call, rabbit_quorum_queue, file_handle_other_reservation, []},
    [FHReservation];
 state_enter(_, _) ->
    %% catch all as not handling all states
    [].


-spec tick(non_neg_integer(), state()) -> ra_machine:effects().
tick(_Ts, #?STATE{cfg = #cfg{name = Name,
                              resource = QName},
                   msg_bytes_enqueue = EnqueueBytes,
                   msg_bytes_checkout = CheckoutBytes} = State) ->
    Metrics = {Name,
               messages_ready(State),
               num_checked_out(State), % checked out
               messages_total(State),
               query_consumer_count(State), % Consumers
               EnqueueBytes,
               CheckoutBytes},
    [{mod_call, rabbit_quorum_queue,
      handle_tick, [QName, Metrics, all_nodes(State)]}].

-spec overview(state()) -> map().
overview(#?STATE{consumers = Cons,
                 enqueuers = Enqs,
                 release_cursors = Cursors,
                 enqueue_count = EnqCount,
                 msg_bytes_enqueue = EnqueueBytes,
                 msg_bytes_checkout = CheckoutBytes,
                 cfg = Cfg} = State) ->
    Conf = #{name => Cfg#cfg.name,
             resource => Cfg#cfg.resource,
             release_cursor_interval => Cfg#cfg.release_cursor_interval,
             dead_lettering_enabled => undefined =/= Cfg#cfg.dead_letter_handler,
             max_length => Cfg#cfg.max_length,
             max_bytes => Cfg#cfg.max_bytes,
             consumer_strategy => Cfg#cfg.consumer_strategy,
             max_in_memory_length => Cfg#cfg.max_in_memory_length,
             max_in_memory_bytes => Cfg#cfg.max_in_memory_bytes},
    #{type => ?MODULE,
      config => Conf,
      num_consumers => maps:size(Cons),
      num_checked_out => num_checked_out(State),
      num_enqueuers => maps:size(Enqs),
      num_ready_messages => messages_ready(State),
      num_messages => messages_total(State),
      num_release_cursors => lqueue:len(Cursors),
      release_crusor_enqueue_counter => EnqCount,
      enqueue_message_bytes => EnqueueBytes,
      checkout_message_bytes => CheckoutBytes}.

-spec get_checked_out(consumer_id(), msg_id(), msg_id(), state()) ->
    [delivery_msg()].
get_checked_out(Cid, From, To, #?STATE{consumers = Consumers}) ->
    case Consumers of
        #{Cid := #consumer{checked_out = Checked}} ->
            [{K, snd(snd(maps:get(K, Checked)))}
             || K <- lists:seq(From, To),
                maps:is_key(K, Checked)];
        _ ->
            []
    end.

-record(aux_gc, {last_raft_idx = 0 :: ra:index()}).
-record(aux, {name :: atom(),
              utilisation :: term(),
              gc = #aux_gc{} :: #aux_gc{}}).

init_aux(Name) when is_atom(Name) ->
    %% TODO: catch specific exception throw if table already exists
    ok = ra_machine_ets:create_table(rabbit_fifo_usage,
                                     [named_table, set, public,
                                      {write_concurrency, true}]),
    Now = erlang:monotonic_time(micro_seconds),
    #aux{name = Name,
         utilisation = {inactive, Now, 1, 1.0}}.

handle_aux(_RaState, cast, Cmd, #aux{name = Name,
                                     utilisation = Use0} = State0,
           Log, MacState) ->
    State = case Cmd of
              _ when Cmd == active orelse Cmd == inactive ->
                  State0#aux{utilisation = update_use(Use0, Cmd)};
              tick ->
                  true = ets:insert(rabbit_fifo_usage,
                                    {Name, utilisation(Use0)}),
                  eval_gc(Log, MacState, State0);
              eval ->
                  State0
          end,
    {no_reply, State, Log}.

eval_gc(Log, #?STATE{cfg = #cfg{resource = QR}} = MacState,
        #aux{gc = #aux_gc{last_raft_idx = LastGcIdx} = Gc} = AuxState) ->
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
            AuxState#aux{gc = Gc#aux_gc{last_raft_idx = Idx}};
        _ ->
            AuxState
    end.

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
    Cons = maps:fold(fun({_, P}, V, S) -> S#{P => V} end, #{}, Cons0),
    maps:keys(maps:merge(Enqs, Cons)).


query_ra_indexes(#?STATE{ra_indexes = RaIndexes}) ->
    RaIndexes.

query_consumer_count(#?STATE{consumers = Consumers,
                              waiting_consumers = WaitingConsumers}) ->
    maps:size(Consumers) + length(WaitingConsumers).

query_consumers(#?STATE{consumers = Consumers,
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

query_single_active_consumer(#?STATE{cfg = #cfg{consumer_strategy = single_active},
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

query_stat(#?STATE{consumers = Consumers} = State) ->
    {messages_ready(State), maps:size(Consumers)}.

query_in_memory_usage(#?STATE{msg_bytes_in_memory = Bytes,
                               msgs_ready_in_memory = Length}) ->
    {Length, Bytes}.

-spec usage(atom()) -> float().
usage(Name) when is_atom(Name) ->
    case ets:lookup(rabbit_fifo_usage, Name) of
        [] -> 0.0;
        [{_, Use}] -> Use
    end.

%%% Internal

messages_ready(#?STATE{messages = M,
                        prefix_msgs = {RCnt, _R, PCnt, _P},
                        returns = R}) ->

    %% prefix messages will rarely have anything in them during normal
    %% operations so length/1 is fine here
    maps:size(M) + lqueue:len(R) + RCnt + PCnt.

messages_total(#?STATE{ra_indexes = I,
                        prefix_msgs = {RCnt, _R, PCnt, _P}}) ->
    rabbit_fifo_index:size(I) + RCnt + PCnt.

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

utilisation({active, Since, Avg}) ->
    use_avg(erlang:monotonic_time(micro_seconds) - Since, 0, Avg);
utilisation({inactive, Since, Active, Avg}) ->
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

num_checked_out(#?STATE{consumers = Cons}) ->
    maps:fold(fun (_, #consumer{checked_out = C}, Acc) ->
                      maps:size(C) + Acc
              end, 0, Cons).

cancel_consumer(ConsumerId,
                #?STATE{cfg = #cfg{consumer_strategy = competing}} = State,
                Effects, Reason) ->
    cancel_consumer0(ConsumerId, State, Effects, Reason);
cancel_consumer(ConsumerId,
                #?STATE{cfg = #cfg{consumer_strategy = single_active},
                         waiting_consumers = []} = State,
                Effects, Reason) ->
    %% single active consumer on, no consumers are waiting
    cancel_consumer0(ConsumerId, State, Effects, Reason);
cancel_consumer(ConsumerId,
                #?STATE{consumers = Cons0,
                         cfg = #cfg{consumer_strategy = single_active},
                         waiting_consumers = Waiting0} = State0,
               Effects0, Reason) ->
    %% single active consumer on, consumers are waiting
    case maps:is_key(ConsumerId, Cons0) of
        true ->
            % The active consumer is to be removed
            {State1, Effects1} = cancel_consumer0(ConsumerId, State0,
                                                  Effects0, Reason),
            activate_next_consumer(State1, Effects1);
        false ->
            % The cancelled consumer is not active or cancelled
            % Just remove it from idle_consumers
            Waiting = lists:keydelete(ConsumerId, 1, Waiting0),
            Effects = cancel_consumer_effects(ConsumerId, State0, Effects0),
            % A waiting consumer isn't supposed to have any checked out messages,
            % so nothing special to do here
            {State0#?STATE{waiting_consumers = Waiting}, Effects}
    end.

consumer_update_active_effects(#?STATE{cfg = #cfg{resource = QName}},
                               ConsumerId, #consumer{meta = Meta},
                               Active, ActivityStatus,
                               Effects) ->
    Ack = maps:get(ack, Meta, undefined),
    Prefetch = maps:get(prefetch, Meta, undefined),
    Args = maps:get(args, Meta, []),
    [{mod_call, rabbit_quorum_queue, update_consumer_handler,
      [QName, ConsumerId, false, Ack, Prefetch, Active, ActivityStatus, Args]}
      | Effects].

cancel_consumer0(ConsumerId, #?STATE{consumers = C0} = S0, Effects0, Reason) ->
    case C0 of
        #{ConsumerId := Consumer} ->
            {S, Effects2} = maybe_return_all(ConsumerId, Consumer, S0,
                                             Effects0, Reason),
            %% The effects are emitted before the consumer is actually removed
            %% if the consumer has unacked messages. This is a bit weird but
            %% in line with what classic queues do (from an external point of
            %% view)
            Effects = cancel_consumer_effects(ConsumerId, S, Effects2),
            case maps:size(S#?STATE.consumers) of
                0 ->
                    {S, [{aux, inactive} | Effects]};
                _ ->
                    {S, Effects}
            end;
        _ ->
            %% already removed: do nothing
            {S0, Effects0}
    end.

activate_next_consumer(#?STATE{consumers = Cons,
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
                    #?STATE{service_queue = ServiceQueue} = State0,
                    ServiceQueue1 = maybe_queue_consumer(NextConsumerId,
                                                         NextConsumer,
                                                         ServiceQueue),
                    State = State0#?STATE{consumers = Cons#{NextConsumerId => NextConsumer},
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



maybe_return_all(ConsumerId, Consumer,
                 #?STATE{consumers = C0,
                          service_queue = SQ0} = S0,
                 Effects0, Reason) ->
    case Reason of
        consumer_cancel ->
            {Cons, SQ, Effects1} =
                update_or_remove_sub(ConsumerId,
                                     Consumer#consumer{lifetime = once,
                                                       credit = 0,
                                                       status = cancelled},
                                     C0, SQ0, Effects0),
            {S0#?STATE{consumers = Cons,
                        service_queue = SQ}, Effects1};
        down ->
            {S1, Effects1} = return_all(S0, Effects0, ConsumerId, Consumer),
            {S1#?STATE{consumers = maps:remove(ConsumerId, S1#?STATE.consumers)},
             Effects1}
    end.

apply_enqueue(#{index := RaftIdx} = Meta, From, Seq, RawMsg, State0) ->
    case maybe_enqueue(RaftIdx, From, Seq, RawMsg, [], State0) of
        {ok, State1, Effects1} ->
            State2 = append_to_master_index(RaftIdx, State1),
            {State, ok, Effects} = checkout(Meta, State2, Effects1),
            {maybe_store_dehydrated_state(RaftIdx, State), ok, Effects};
        {duplicate, State, Effects} ->
            {State, ok, Effects}
    end.

drop_head(#?STATE{ra_indexes = Indexes0} = State0, Effects0) ->
    case take_next_msg(State0) of
        {FullMsg = {_MsgId, {RaftIdxToDrop, {Header, Msg}}},
         State1} ->
            Indexes = rabbit_fifo_index:delete(RaftIdxToDrop, Indexes0),
            State2 = add_bytes_drop(Header, State1#?STATE{ra_indexes = Indexes}),
            State = case Msg of
                        'empty' -> State2;
                        _ -> subtract_in_memory_counts(Header, State2)
                    end,
            Effects = dead_letter_effects(maxlen, #{none => FullMsg},
                                          State, Effects0),
            {State, Effects};
        {{'$prefix_msg', Header}, State1} ->
            State2 = subtract_in_memory_counts(Header, add_bytes_drop(Header, State1)),
            {State2, Effects0};
        {{'$empty_msg', Header}, State1} ->
            State2 = add_bytes_drop(Header, State1),
            {State2, Effects0};
        empty ->
            {State0, Effects0}
    end.

enqueue(RaftIdx, RawMsg, #?STATE{messages = Messages,
                                  low_msg_num = LowMsgNum,
                                  next_msg_num = NextMsgNum} = State0) ->
    %% the initial header is an integer only - it will get expanded to a map
    %% when the next required key is added
    Header = message_size(RawMsg),
    {State1, Msg} =
        case evaluate_memory_limit(Header, State0) of
            true ->
                % indexed message with header map
                {State0, {RaftIdx, {Header, 'empty'}}};
            false ->
                {add_in_memory_counts(Header, State0),
                 {RaftIdx, {Header, RawMsg}}} % indexed message with header map
        end,
    State = add_bytes_enqueue(Header, State1),
    State#?STATE{messages = Messages#{NextMsgNum => Msg},
                  %% this is probably only done to record it when low_msg_num
                  %% is undefined
                  low_msg_num = min(LowMsgNum, NextMsgNum),
                  next_msg_num = NextMsgNum + 1}.

append_to_master_index(RaftIdx,
                       #?STATE{ra_indexes = Indexes0} = State0) ->
    State = incr_enqueue_count(State0),
    Indexes = rabbit_fifo_index:append(RaftIdx, Indexes0),
    State#?STATE{ra_indexes = Indexes}.


incr_enqueue_count(#?STATE{enqueue_count = C,
                            cfg = #cfg{release_cursor_interval = {_Base, C}}
                            } = State0) ->
    %% this will trigger a dehydrated version of the state to be stored
    %% at this raft index for potential future snapshot generation
    %% Q: Why don't we just stash the release cursor here?
    %% A: Because it needs to be the very last thing we do and we
    %% first needs to run the checkout logic.
    State0#?STATE{enqueue_count = 0};
incr_enqueue_count(#?STATE{cfg = #cfg{release_cursor_interval = C} = Cfg}
                   = State0)
  when is_integer(C) ->
    %% conversion to new release cursor interval format
    State = State0#?STATE{cfg = Cfg#cfg{release_cursor_interval = {C, C}}},
    incr_enqueue_count(State);
incr_enqueue_count(#?STATE{enqueue_count = C} = State) ->
    State#?STATE{enqueue_count = C + 1}.

maybe_store_dehydrated_state(RaftIdx,
                             #?STATE{cfg =
                                      #cfg{release_cursor_interval = {Base, _}}
                                      = Cfg,
                                      ra_indexes = Indexes,
                                      enqueue_count = 0,
                                      release_cursors = Cursors0} = State0) ->
    case rabbit_fifo_index:exists(RaftIdx, Indexes) of
        false ->
            %% the incoming enqueue must already have been dropped
            State0;
        true ->
            Interval = case Base of
                           0 -> 0;
                           _ ->
                               Total = messages_total(State0),
                               min(max(Total, Base),
                                   ?RELEASE_CURSOR_EVERY_MAX)
                       end,
            State = convert_prefix_msgs(
                      State0#?STATE{cfg = Cfg#cfg{release_cursor_interval =
                                                   {Base, Interval}}}),
            Dehydrated = dehydrate_state(State),
            Cursor = {release_cursor, RaftIdx, Dehydrated},
            Cursors = lqueue:in(Cursor, Cursors0),
            State#?STATE{release_cursors = Cursors}
    end;
maybe_store_dehydrated_state(RaftIdx,
                             #?STATE{cfg =
                                      #cfg{release_cursor_interval = C} = Cfg}
                                      = State0)
  when is_integer(C) ->
    %% convert to new format
    State = State0#?STATE{cfg = Cfg#cfg{release_cursor_interval = {C, C}}},
    maybe_store_dehydrated_state(RaftIdx, State);
maybe_store_dehydrated_state(_RaftIdx, State) ->
    State.

enqueue_pending(From,
                #enqueuer{next_seqno = Next,
                          pending = [{Next, RaftIdx, RawMsg} | Pending]} = Enq0,
                State0) ->
            State = enqueue(RaftIdx, RawMsg, State0),
            Enq = Enq0#enqueuer{next_seqno = Next + 1, pending = Pending},
            enqueue_pending(From, Enq, State);
enqueue_pending(From, Enq, #?STATE{enqueuers = Enqueuers0} = State) ->
    State#?STATE{enqueuers = Enqueuers0#{From => Enq}}.

maybe_enqueue(RaftIdx, undefined, undefined, RawMsg, Effects, State0) ->
    % direct enqueue without tracking
    State = enqueue(RaftIdx, RawMsg, State0),
    {ok, State, Effects};
maybe_enqueue(RaftIdx, From, MsgSeqNo, RawMsg, Effects0,
              #?STATE{enqueuers = Enqueuers0} = State0) ->
    case maps:get(From, Enqueuers0, undefined) of
        undefined ->
            State1 = State0#?STATE{enqueuers = Enqueuers0#{From => #enqueuer{}}},
            {ok, State, Effects} = maybe_enqueue(RaftIdx, From, MsgSeqNo,
                                                 RawMsg, Effects0, State1),
            {ok, State, [{monitor, process, From} | Effects]};
        #enqueuer{next_seqno = MsgSeqNo} = Enq0 ->
            % it is the next expected seqno
            State1 = enqueue(RaftIdx, RawMsg, State0),
            Enq = Enq0#enqueuer{next_seqno = MsgSeqNo + 1},
            State = enqueue_pending(From, Enq, State1),
            {ok, State, Effects0};
        #enqueuer{next_seqno = Next,
                  pending = Pending0} = Enq0
          when MsgSeqNo > Next ->
            % out of order delivery
            Pending = [{MsgSeqNo, RaftIdx, RawMsg} | Pending0],
            Enq = Enq0#enqueuer{pending = lists:sort(Pending)},
            {ok, State0#?STATE{enqueuers = Enqueuers0#{From => Enq}}, Effects0};
        #enqueuer{next_seqno = Next} when MsgSeqNo =< Next ->
            % duplicate delivery - remove the raft index from the ra_indexes
            % map as it was added earlier
            {duplicate, State0, Effects0}
    end.

snd(T) ->
    element(2, T).

return(#{index := IncomingRaftIdx} = Meta, ConsumerId, Returned,
       Effects0, #?STATE{service_queue = SQ0} = State0) ->
    {State1, Effects1} = maps:fold(
                           fun(MsgId, {Tag, _} = Msg, {S0, E0})
                                 when Tag == '$prefix_msg';
                                      Tag == '$empty_msg'->
                                  return_one(MsgId, 0, Msg, S0, E0, ConsumerId);
                             (MsgId, {MsgNum, Msg}, {S0, E0}) ->
                                  return_one(MsgId, MsgNum, Msg, S0, E0,
                                             ConsumerId)
                          end, {State0, Effects0}, Returned),
    {State2, Effects3} =
        case State1#?STATE.consumers of
            #{ConsumerId := Con0} = Cons0 ->
                Con = Con0#consumer{credit = increase_credit(Con0,
                                                             map_size(Returned))},
                {Cons, SQ, Effects2} = update_or_remove_sub(ConsumerId, Con,
                                                            Cons0, SQ0, Effects1),
                {State1#?STATE{consumers = Cons,
                               service_queue = SQ}, Effects2};
            _ ->
                {State1, Effects1}
        end,
    {State, ok, Effects} = checkout(Meta, State2, Effects3),
    update_smallest_raft_index(IncomingRaftIdx, State, Effects).

% used to processes messages that are finished
complete(ConsumerId, Discarded,
         #consumer{checked_out = Checked} = Con0, Effects0,
         #?STATE{consumers = Cons0, service_queue = SQ0,
                  ra_indexes = Indexes0} = State0) ->
    %% TODO optimise use of Discarded map here
    MsgRaftIdxs = [RIdx || {_, {RIdx, _}} <- maps:values(Discarded)],
    %% credit_mode = simple_prefetch should automatically top-up credit
    %% as messages are simple_prefetch or otherwise returned
    Con = Con0#consumer{checked_out = maps:without(maps:keys(Discarded), Checked),
                        credit = increase_credit(Con0, map_size(Discarded))},
    {Cons, SQ, Effects} = update_or_remove_sub(ConsumerId, Con, Cons0,
                                               SQ0, Effects0),
    Indexes = lists:foldl(fun rabbit_fifo_index:delete/2, Indexes0,
                          MsgRaftIdxs),
    %% TODO: use maps:fold instead
    State1 = lists:foldl(fun({_, {_, {Header, _}}}, Acc) ->
                                 add_bytes_settle(Header, Acc);
                            ({'$prefix_msg', Header}, Acc) ->
                                 add_bytes_settle(Header, Acc);
                            ({'$empty_msg', Header}, Acc) ->
                                 add_bytes_settle(Header, Acc)
                         end, State0, maps:values(Discarded)),
    {State1#?STATE{consumers = Cons,
                    ra_indexes = Indexes,
                    service_queue = SQ}, Effects}.

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
                      #consumer{checked_out = Checked0} = Con0,
                      Effects0, State0) ->
    Discarded = maps:with(MsgIds, Checked0),
    {State2, Effects1} = complete(ConsumerId, Discarded, Con0,
                                  Effects0, State0),
    {State, ok, Effects} = checkout(Meta, State2, Effects1),
    update_smallest_raft_index(IncomingRaftIdx, State, Effects).

dead_letter_effects(_Reason, _Discarded,
                    #?STATE{cfg = #cfg{dead_letter_handler = undefined}},
                    Effects) ->
    Effects;
dead_letter_effects(Reason, Discarded,
                    #?STATE{cfg = #cfg{dead_letter_handler = {Mod, Fun, Args}}},
                    Effects) ->
    RaftIdxs = maps:fold(
                    fun (_, {_, {RaftIdx, {_Header, 'empty'}}}, Acc) ->
                            [RaftIdx | Acc];
                        (_, _, Acc) ->
                            Acc
                    end, [], Discarded),
    [{log, RaftIdxs,
      fun (Log) ->
              Lookup = maps:from_list(lists:zip(RaftIdxs, Log)),
              DeadLetters = maps:fold(
                              fun (_, {_, {RaftIdx, {_Header, 'empty'}}}, Acc) ->
                                      {enqueue, _, _, Msg} = maps:get(RaftIdx, Lookup),
                                      [{Reason, Msg} | Acc];
                                  (_, {_, {_, {_Header, Msg}}}, Acc) ->
                                      [{Reason, Msg} | Acc];
                                  (_, _, Acc) ->
                                      Acc
                              end, [], Discarded),
              [{mod_call, Mod, Fun, Args ++ [DeadLetters]}]
      end} | Effects].

cancel_consumer_effects(ConsumerId,
                        #?STATE{cfg = #cfg{resource = QName}}, Effects) ->
    [{mod_call, rabbit_quorum_queue,
      cancel_consumer_handler, [QName, ConsumerId]} | Effects].

update_smallest_raft_index(IncomingRaftIdx,
                           #?STATE{ra_indexes = Indexes,
                                    release_cursors = Cursors0} = State0,
                           Effects) ->
    case rabbit_fifo_index:size(Indexes) of
        0 ->
            % there are no messages on queue anymore and no pending enqueues
            % we can forward release_cursor all the way until
            % the last received command, hooray
            State = State0#?STATE{release_cursors = lqueue:new()},
            {State, ok, Effects ++ [{release_cursor, IncomingRaftIdx, State}]};
        _ ->
            Smallest = rabbit_fifo_index:smallest(Indexes),
            case find_next_cursor(Smallest, Cursors0) of
                {empty, Cursors} ->
                    {State0#?STATE{release_cursors = Cursors},
                     ok, Effects};
                {Cursor, Cursors} ->
                    %% we can emit a release cursor we've passed the smallest
                    %% release cursor available.
                    {State0#?STATE{release_cursors = Cursors}, ok,
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
        _ ->
            {Potential, Cursors0}
    end.

update_header(Key, UpdateFun, Default, Header)
  when is_integer(Header) ->
    update_header(Key, UpdateFun, Default, #{size => Header});
update_header(Key, UpdateFun, Default, Header) ->
    maps:update_with(Key, UpdateFun, Default, Header).


return_one(MsgId, 0, {Tag, Header0},
           #?STATE{returns = Returns,
                    consumers = Consumers,
                    cfg = #cfg{delivery_limit = DeliveryLimit}} = State0,
           Effects0, ConsumerId)
  when Tag == '$prefix_msg'; Tag == '$empty_msg' ->
    #consumer{checked_out = Checked} = Con0 = maps:get(ConsumerId, Consumers),
    Header = update_header(delivery_count, fun (C) -> C+1 end, 1, Header0),
    Msg0 = {Tag, Header},
    case maps:get(delivery_count, Header) of
        DeliveryCount when DeliveryCount > DeliveryLimit ->
            complete(ConsumerId, #{MsgId => Msg0}, Con0, Effects0, State0);
        _ ->
            %% this should not affect the release cursor in any way
            Con = Con0#consumer{checked_out = maps:remove(MsgId, Checked)},
            {Msg, State1} = case Tag of
                                '$empty_msg' ->
                                    {Msg0, State0};
                                _ -> case evaluate_memory_limit(Header, State0) of
                                         true ->
                                             {{'$empty_msg', Header}, State0};
                                         false ->
                                             {Msg0, add_in_memory_counts(Header, State0)}
                                     end
                            end,
            {add_bytes_return(
               Header,
               State1#?STATE{consumers = Consumers#{ConsumerId => Con},
                              returns = lqueue:in(Msg, Returns)}),
             Effects0}
    end;
return_one(MsgId, MsgNum, {RaftId, {Header0, RawMsg}},
           #?STATE{returns = Returns,
                    consumers = Consumers,
                    cfg = #cfg{delivery_limit = DeliveryLimit}} = State0,
           Effects0, ConsumerId) ->
    #consumer{checked_out = Checked} = Con0 = maps:get(ConsumerId, Consumers),
    Header = update_header(delivery_count, fun (C) -> C+1 end, 1, Header0),
    Msg0 = {RaftId, {Header, RawMsg}},
    case maps:get(delivery_count, Header) of
        DeliveryCount when DeliveryCount > DeliveryLimit ->
            DlMsg = {MsgNum, Msg0},
            Effects = dead_letter_effects(delivery_limit, #{none => DlMsg},
                                          State0, Effects0),
            complete(ConsumerId, #{MsgId => DlMsg}, Con0, Effects, State0);
        _ ->
            Con = Con0#consumer{checked_out = maps:remove(MsgId, Checked)},
            %% this should not affect the release cursor in any way
            {Msg, State1} = case RawMsg of
                                'empty' ->
                                    {Msg0, State0};
                                _ ->
                                    case evaluate_memory_limit(Header, State0) of
                                        true ->
                                            {{RaftId, {Header, 'empty'}}, State0};
                                        false ->
                                            {Msg0, add_in_memory_counts(Header, State0)}
                                    end
                            end,
            {add_bytes_return(
               Header,
               State1#?STATE{consumers = Consumers#{ConsumerId => Con},
                              returns = lqueue:in({MsgNum, Msg}, Returns)}),
             Effects0}
    end.

return_all(#?STATE{consumers = Cons} = State0, Effects0, ConsumerId,
           #consumer{checked_out = Checked0} = Con) ->
    %% need to sort the list so that we return messages in the order
    %% they were checked out
    Checked = lists:sort(maps:to_list(Checked0)),
    State = State0#?STATE{consumers = Cons#{ConsumerId => Con}},
    lists:foldl(fun ({MsgId, {'$prefix_msg', _} = Msg}, {S, E}) ->
                        return_one(MsgId, 0, Msg, S, E, ConsumerId);
                    ({MsgId, {'$empty_msg', _} = Msg}, {S, E}) ->
                        return_one(MsgId, 0, Msg, S, E, ConsumerId);
                    ({MsgId, {MsgNum, Msg}}, {S, E}) ->
                        return_one(MsgId, MsgNum, Msg, S, E, ConsumerId)
                end, {State, Effects0}, Checked).

%% checkout new messages to consumers
checkout(#{index := Index}, State0, Effects0) ->
    {State1, _Result, Effects1} = checkout0(checkout_one(State0),
                                            Effects0, {#{}, #{}}),
    case evaluate_limit(false, State1, Effects1) of
        {State, true, Effects} ->
            update_smallest_raft_index(Index, State, Effects);
        {State, false, Effects} ->
            {State, ok, Effects}
    end.

checkout0({success, ConsumerId, MsgId, {RaftIdx, {Header, 'empty'}}, State},
          Effects, {SendAcc, LogAcc0}) ->
    DelMsg = {RaftIdx, {MsgId, Header}},
    LogAcc = maps:update_with(ConsumerId,
                              fun (M) -> [DelMsg | M] end,
                              [DelMsg], LogAcc0),
    checkout0(checkout_one(State), Effects, {SendAcc, LogAcc});
checkout0({success, ConsumerId, MsgId, Msg, State}, Effects,
          {SendAcc0, LogAcc}) ->
    DelMsg = {MsgId, Msg},
    SendAcc = maps:update_with(ConsumerId,
                               fun (M) -> [DelMsg | M] end,
                               [DelMsg], SendAcc0),
    checkout0(checkout_one(State), Effects, {SendAcc, LogAcc});
checkout0({Activity, State0}, Effects0, {SendAcc, LogAcc}) ->
    Effects1 = case Activity of
                   nochange ->
                       append_send_msg_effects(
                         append_log_effects(Effects0, LogAcc), SendAcc);
                   inactive ->
                       [{aux, inactive}
                        | append_send_msg_effects(
                            append_log_effects(Effects0, LogAcc), SendAcc)]
               end,
    {State0, ok, lists:reverse(Effects1)}.

evaluate_limit(Result,
               #?STATE{cfg = #cfg{max_length = undefined,
                                   max_bytes = undefined}} = State,
               Effects) ->
    {State, Result, Effects};
evaluate_limit(Result, State00, Effects0) ->
    State0 = convert_prefix_msgs(State00),
    case is_over_limit(State0) of
        true ->
            {State, Effects} = drop_head(State0, Effects0),
            evaluate_limit(true, State, Effects);
        false ->
            {State0, Result, Effects0}
    end.

evaluate_memory_limit(_Header,
                      #?STATE{cfg = #cfg{max_in_memory_length = undefined,
                                          max_in_memory_bytes = undefined}}) ->
    false;
evaluate_memory_limit(#{size := Size}, State) ->
    evaluate_memory_limit(Size, State);
evaluate_memory_limit(Size,
                      #?STATE{cfg = #cfg{max_in_memory_length = MaxLength,
                                          max_in_memory_bytes = MaxBytes},
                               msg_bytes_in_memory = Bytes,
                               msgs_ready_in_memory = Length})
  when is_integer(Size) ->
    (Length >= MaxLength) orelse ((Bytes + Size) > MaxBytes).

append_send_msg_effects(Effects, AccMap) when map_size(AccMap) == 0 ->
    Effects;
append_send_msg_effects(Effects0, AccMap) ->
    Effects = maps:fold(fun (C, Msgs, Ef) ->
                                [send_msg_effect(C, lists:reverse(Msgs)) | Ef]
                        end, Effects0, AccMap),
    [{aux, active} | Effects].

append_log_effects(Effects0, AccMap) ->
    maps:fold(fun (C, Msgs, Ef) ->
                      [send_log_effect(C, lists:reverse(Msgs)) | Ef]
              end, Effects0, AccMap).

%% next message is determined as follows:
%% First we check if there are are prefex returns
%% Then we check if there are current returns
%% then we check prefix msgs
%% then we check current messages
%%
%% When we return it is always done to the current return queue
%% for both prefix messages and current messages
take_next_msg(#?STATE{prefix_msgs = {R, P}} = State) ->
    %% conversion
    take_next_msg(State#?STATE{prefix_msgs = {length(R), R, length(P), P}});
take_next_msg(#?STATE{prefix_msgs = {NumR, [{'$empty_msg', _} = Msg | Rem],
                                      NumP, P}} = State) ->
    %% there are prefix returns, these should be served first
    {Msg, State#?STATE{prefix_msgs = {NumR-1, Rem, NumP, P}}};
take_next_msg(#?STATE{prefix_msgs = {NumR, [Header | Rem], NumP, P}} = State) ->
    %% there are prefix returns, these should be served first
    {{'$prefix_msg', Header},
     State#?STATE{prefix_msgs = {NumR-1, Rem, NumP, P}}};
take_next_msg(#?STATE{returns = Returns,
                       low_msg_num = Low0,
                       messages = Messages0,
                       prefix_msgs = {NumR, R, NumP, P}} = State) ->
    %% use peek rather than out there as the most likely case is an empty
    %% queue
    case lqueue:peek(Returns) of
        {value, NextMsg} ->
            {NextMsg,
             State#?STATE{returns = lqueue:drop(Returns)}};
        empty when P == [] ->
            case Low0 of
                undefined ->
                    empty;
                _ ->
                    {Msg, Messages} = maps:take(Low0, Messages0),
                    case maps:size(Messages) of
                        0 ->
                            {{Low0, Msg},
                             State#?STATE{messages = Messages,
                                           low_msg_num = undefined}};
                        _ ->
                            {{Low0, Msg},
                             State#?STATE{messages = Messages,
                                           low_msg_num = Low0 + 1}}
                    end
            end;
        empty ->
            [Msg | Rem] = P,
            case Msg of
                {Header, 'empty'} ->
                    %% There are prefix msgs
                    {{'$empty_msg', Header},
                     State#?STATE{prefix_msgs = {NumR, R, NumP-1, Rem}}};
                Header ->
                    {{'$prefix_msg', Header},
                     State#?STATE{prefix_msgs = {NumR, R, NumP-1, Rem}}}
            end
    end.

send_msg_effect({CTag, CPid}, Msgs) ->
    {send_msg, CPid, {delivery, CTag, Msgs}, [local, ra_event]}.

send_log_effect({CTag, CPid}, IdxMsgs) ->
    {RaftIdxs, Data} = lists:unzip(IdxMsgs),
    {log, RaftIdxs,
     fun(Log) ->
             Msgs = lists:zipwith(fun ({enqueue, _, _, Msg}, {MsgId, Header}) ->
                                          {MsgId, {Header, Msg}}
                                  end, Log, Data),
             [{send_msg, CPid, {delivery, CTag, Msgs}, [local, ra_event]}]
     end,
     {local, node(CPid)}}.

reply_log_effect(RaftIdx, MsgId, Header, Ready, From) ->
    {log, [RaftIdx],
     fun([{enqueue, _, _, Msg}]) ->
             [{reply, From, {wrap_reply,
                             {dequeue, {MsgId, {Header, Msg}}, Ready}}}]
     end}.

checkout_one(#?STATE{service_queue = SQ0,
                      messages = Messages0,
                      consumers = Cons0} = InitState) ->
    case queue:peek(SQ0) of
        {value, ConsumerId} ->
            case take_next_msg(InitState) of
                {ConsumerMsg, State0} ->
                    SQ1 = queue:drop(SQ0),
                    %% there are consumers waiting to be serviced
                    %% process consumer checkout
                    case maps:find(ConsumerId, Cons0) of
                        {ok, #consumer{credit = 0}} ->
                            %% no credit but was still on queue
                            %% can happen when draining
                            %% recurse without consumer on queue
                            checkout_one(InitState#?STATE{service_queue = SQ1});
                        {ok, #consumer{status = cancelled}} ->
                            checkout_one(InitState#?STATE{service_queue = SQ1});
                        {ok, #consumer{status = suspected_down}} ->
                            checkout_one(InitState#?STATE{service_queue = SQ1});
                        {ok, #consumer{checked_out = Checked0,
                                       next_msg_id = Next,
                                       credit = Credit,
                                       delivery_count = DelCnt} = Con0} ->
                            Checked = maps:put(Next, ConsumerMsg, Checked0),
                            Con = Con0#consumer{checked_out = Checked,
                                                next_msg_id = Next + 1,
                                                credit = Credit - 1,
                                                delivery_count = DelCnt + 1},
                            {Cons, SQ, []} = % we expect no effects
                                update_or_remove_sub(ConsumerId, Con,
                                                     Cons0, SQ1, []),
                            State1 = State0#?STATE{service_queue = SQ,
                                                    consumers = Cons},
                            {State, Msg} =
                                case ConsumerMsg of
                                    {'$prefix_msg', Header} ->
                                        {subtract_in_memory_counts(
                                           Header, add_bytes_checkout(Header, State1)),
                                         ConsumerMsg};
                                    {'$empty_msg', Header} ->
                                        {add_bytes_checkout(Header, State1),
                                         ConsumerMsg};
                                    {_, {_, {Header, 'empty'}} = M} ->
                                        {add_bytes_checkout(Header, State1),
                                         M};
                                    {_, {_, {Header, _} = M}} ->
                                        {subtract_in_memory_counts(
                                           Header,
                                           add_bytes_checkout(Header, State1)),
                                         M}
                                end,
                            {success, ConsumerId, Next, Msg, State};
                        error ->
                            %% consumer did not exist but was queued, recurse
                            checkout_one(InitState#?STATE{service_queue = SQ1})
                    end;
                empty ->
                    {nochange, InitState}
            end;
        empty ->
            case maps:size(Messages0) of
                0 -> {nochange, InitState};
                _ -> {inactive, InitState}
            end
    end.

update_or_remove_sub(ConsumerId, #consumer{lifetime = auto,
                                           credit = 0} = Con,
                     Cons, ServiceQueue, Effects) ->
    {maps:put(ConsumerId, Con, Cons), ServiceQueue, Effects};
update_or_remove_sub(ConsumerId, #consumer{lifetime = auto} = Con,
                     Cons, ServiceQueue, Effects) ->
    {maps:put(ConsumerId, Con, Cons),
     uniq_queue_in(ConsumerId, ServiceQueue), Effects};
update_or_remove_sub(ConsumerId, #consumer{lifetime = once,
                                           checked_out = Checked,
                                           credit = 0} = Con,
                     Cons, ServiceQueue, Effects) ->
    case maps:size(Checked)  of
        0 ->
            % we're done with this consumer
            % TODO: demonitor consumer pid but _only_ if there are no other
            % monitors for this pid
            {maps:remove(ConsumerId, Cons), ServiceQueue, Effects};
        _ ->
            % there are unsettled items so need to keep around
            {maps:put(ConsumerId, Con, Cons), ServiceQueue, Effects}
    end;
update_or_remove_sub(ConsumerId, #consumer{lifetime = once} = Con,
                     Cons, ServiceQueue, Effects) ->
    {maps:put(ConsumerId, Con, Cons),
     uniq_queue_in(ConsumerId, ServiceQueue), Effects}.

uniq_queue_in(Key, Queue) ->
    % TODO: queue:member could surely be quite expensive, however the practical
    % number of unique consumers may not be large enough for it to matter
    case queue:member(Key, Queue) of
        true ->
            Queue;
        false ->
            queue:in(Key, Queue)
    end.

update_consumer(ConsumerId, Meta, Spec,
                #?STATE{cfg = #cfg{consumer_strategy = competing}} = State0) ->
    %% general case, single active consumer off
    update_consumer0(ConsumerId, Meta, Spec, State0);
update_consumer(ConsumerId, Meta, Spec,
                #?STATE{consumers = Cons0,
                         cfg = #cfg{consumer_strategy = single_active}} = State0)
  when map_size(Cons0) == 0 ->
    %% single active consumer on, no one is consuming yet
    update_consumer0(ConsumerId, Meta, Spec, State0);
update_consumer(ConsumerId, Meta, {Life, Credit, Mode},
                #?STATE{cfg = #cfg{consumer_strategy = single_active},
                         waiting_consumers = WaitingConsumers0} = State0) ->
    %% single active consumer on and one active consumer already
    %% adding the new consumer to the waiting list
    Consumer = #consumer{lifetime = Life, meta = Meta,
                         credit = Credit, credit_mode = Mode},
    WaitingConsumers1 = WaitingConsumers0 ++ [{ConsumerId, Consumer}],
    State0#?STATE{waiting_consumers = WaitingConsumers1}.

update_consumer0(ConsumerId, Meta, {Life, Credit, Mode},
                 #?STATE{consumers = Cons0,
                          service_queue = ServiceQueue0} = State0) ->
    %% TODO: this logic may not be correct for updating a pre-existing consumer
    Init = #consumer{lifetime = Life, meta = Meta,
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
    State0#?STATE{consumers = Cons, service_queue = ServiceQueue}.

maybe_queue_consumer(ConsumerId, #consumer{credit = Credit},
                     ServiceQueue0) ->
    case Credit > 0 of
        true ->
            % consumerect needs service - check if already on service queue
            uniq_queue_in(ConsumerId, ServiceQueue0);
        false ->
            ServiceQueue0
    end.

convert_prefix_msgs(#?STATE{prefix_msgs = {R, P}} = State) ->
    State#?STATE{prefix_msgs = {length(R), R, length(P), P}};
convert_prefix_msgs(State) ->
    State.

%% creates a dehydrated version of the current state to be cached and
%% potentially used to for a snaphot at a later point
dehydrate_state(#?STATE{messages = Messages,
                         consumers = Consumers,
                         returns = Returns,
                         low_msg_num = Low,
                         next_msg_num = Next,
                         prefix_msgs = {PRCnt, PrefRet0, PPCnt, PrefMsg0},
                         waiting_consumers = Waiting0} = State) ->
    RCnt = lqueue:len(Returns),
    %% TODO: optimise this function as far as possible
    PrefRet1 = lists:foldr(fun ({'$prefix_msg', Header}, Acc) ->
                                  [Header | Acc];
                              ({'$empty_msg', _} = Msg, Acc) ->
                                  [Msg | Acc];
                              ({_, {_, {Header, 'empty'}}}, Acc) ->
                                  [{'$empty_msg', Header} | Acc];
                              ({_, {_, {Header, _}}}, Acc) ->
                                  [Header | Acc]
                          end,
                          [],
                          lqueue:to_list(Returns)),
    PrefRet = PrefRet0 ++ PrefRet1,
    PrefMsgsSuff = dehydrate_messages(Low, Next - 1, Messages, []),
    %% prefix messages are not populated in normal operation only after
    %% recovering from a snapshot
    PrefMsgs = PrefMsg0 ++ PrefMsgsSuff,
    Waiting = [{Cid, dehydrate_consumer(C)} || {Cid, C} <- Waiting0],
    State#?STATE{messages = #{},
                  ra_indexes = rabbit_fifo_index:empty(),
                  release_cursors = lqueue:new(),
                  low_msg_num = undefined,
                  consumers = maps:map(fun (_, C) ->
                                               dehydrate_consumer(C)
                                       end, Consumers),
                  returns = lqueue:new(),
                  prefix_msgs = {PRCnt + RCnt, PrefRet,
                                 PPCnt + maps:size(Messages), PrefMsgs},
                  waiting_consumers = Waiting}.

dehydrate_messages(Low, Next, _Msgs, Acc)
  when Next < Low  ->
    Acc;
dehydrate_messages(Low, Next, Msgs, Acc0)  ->
    Acc = case maps:get(Next, Msgs) of
              {_RaftIdx, {_, 'empty'} = Msg} ->
                  [Msg | Acc0];
              {_RaftIdx, {Header, _}} ->
                  [Header | Acc0]
          end,
    dehydrate_messages(Low, Next - 1, Msgs, Acc).

dehydrate_consumer(#consumer{checked_out = Checked0} = Con) ->
    Checked = maps:map(fun (_, {'$prefix_msg', _} = M) ->
                               M;
                           (_, {'$empty_msg', _} = M) ->
                               M;
                           (_, {_, {_, {Header, 'empty'}}}) ->
                               {'$empty_msg', Header};
                           (_, {_, {_, {Header, _}}}) ->
                               {'$prefix_msg', Header}
                       end, Checked0),
    Con#consumer{checked_out = Checked}.

%% make the state suitable for equality comparison
normalize(#?STATE{release_cursors = Cursors} = State) ->
    State#?STATE{release_cursors = lqueue:from_list(lqueue:to_list(Cursors))}.

is_over_limit(#?STATE{cfg = #cfg{max_length = undefined,
                                  max_bytes = undefined}}) ->
    false;
is_over_limit(#?STATE{cfg = #cfg{max_length = MaxLength,
                                  max_bytes = MaxBytes},
                       msg_bytes_enqueue = BytesEnq} = State) ->

    messages_ready(State) > MaxLength orelse (BytesEnq > MaxBytes).

normalize_for_v1(#?STATE{cfg = Cfg} = State) ->
    %% run all v0 conversions so that v1 does not have to have this code
    RCI = case Cfg of
              #cfg{release_cursor_interval = {_, _} = R} ->
                  R;
              #cfg{release_cursor_interval = undefined} ->
                  {?RELEASE_CURSOR_EVERY, ?RELEASE_CURSOR_EVERY};
              #cfg{release_cursor_interval = C} ->
                  {?RELEASE_CURSOR_EVERY, C}
          end,
    convert_prefix_msgs(
      State#?STATE{cfg = Cfg#cfg{release_cursor_interval = RCI}}).

get_field(Field, State) ->
    Fields = record_info(fields, ?STATE),
    Index = record_index_of(Field, Fields),
    element(Index, State).

get_cfg_field(Field, #?STATE{cfg = Cfg} ) ->
    Fields = record_info(fields, cfg),
    Index = record_index_of(Field, Fields),
    element(Index, Cfg).

record_index_of(F, Fields) ->
    index_of(2, F, Fields).

index_of(_, F, []) ->
    exit({field_not_found, F});
index_of(N, F, [F | _]) ->
   N;
index_of(N, F, [_ | T]) ->
    index_of(N+1, F, T).

-spec make_enqueue(option(pid()), option(msg_seqno()), raw_msg()) -> protocol().
make_enqueue(Pid, Seq, Msg) ->
    #enqueue{pid = Pid, seq = Seq, msg = Msg}.
-spec make_checkout(consumer_id(),
                    checkout_spec(), consumer_meta()) -> protocol().
make_checkout(ConsumerId, Spec, Meta) ->
    #checkout{consumer_id = ConsumerId,
              spec = Spec, meta = Meta}.

-spec make_settle(consumer_id(), [msg_id()]) -> protocol().
make_settle(ConsumerId, MsgIds) ->
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

-spec make_purge_nodes([node()]) -> protocol().
make_purge_nodes(Nodes) ->
    #purge_nodes{nodes = Nodes}.

-spec make_update_config(config()) -> protocol().
make_update_config(Config) ->
    #update_config{config = Config}.

add_bytes_enqueue(Bytes,
                  #?STATE{msg_bytes_enqueue = Enqueue} = State)
 when is_integer(Bytes) ->
    State#?STATE{msg_bytes_enqueue = Enqueue + Bytes};
add_bytes_enqueue(#{size := Bytes}, State) ->
    add_bytes_enqueue(Bytes, State).

add_bytes_drop(Bytes,
               #?STATE{msg_bytes_enqueue = Enqueue} = State)
  when is_integer(Bytes) ->
    State#?STATE{msg_bytes_enqueue = Enqueue - Bytes};
add_bytes_drop(#{size := Bytes}, State) ->
    add_bytes_drop(Bytes, State).

add_bytes_checkout(Bytes,
                   #?STATE{msg_bytes_checkout = Checkout,
                            msg_bytes_enqueue = Enqueue } = State)
  when is_integer(Bytes) ->
    State#?STATE{msg_bytes_checkout = Checkout + Bytes,
                  msg_bytes_enqueue = Enqueue - Bytes};
add_bytes_checkout(#{size := Bytes}, State) ->
    add_bytes_checkout(Bytes, State).

add_bytes_settle(Bytes,
                 #?STATE{msg_bytes_checkout = Checkout} = State)
  when is_integer(Bytes) ->
    State#?STATE{msg_bytes_checkout = Checkout - Bytes};
add_bytes_settle(#{size := Bytes}, State) ->
    add_bytes_settle(Bytes, State).

add_bytes_return(Bytes,
                 #?STATE{msg_bytes_checkout = Checkout,
                               msg_bytes_enqueue = Enqueue} = State)
  when is_integer(Bytes) ->
    State#?STATE{msg_bytes_checkout = Checkout - Bytes,
                  msg_bytes_enqueue = Enqueue + Bytes};
add_bytes_return(#{size := Bytes}, State) ->
    add_bytes_return(Bytes, State).

add_in_memory_counts(Bytes,
                     #?STATE{msg_bytes_in_memory = InMemoryBytes,
                              msgs_ready_in_memory = InMemoryCount} = State)
  when is_integer(Bytes) ->
    State#?STATE{msg_bytes_in_memory = InMemoryBytes + Bytes,
                  msgs_ready_in_memory = InMemoryCount + 1};
add_in_memory_counts(#{size := Bytes}, State) ->
    add_in_memory_counts(Bytes, State).

subtract_in_memory_counts(Bytes,
                          #?STATE{msg_bytes_in_memory = InMemoryBytes,
                                   msgs_ready_in_memory = InMemoryCount} = State)
  when is_integer(Bytes) ->
    State#?STATE{msg_bytes_in_memory = InMemoryBytes - Bytes,
                  msgs_ready_in_memory = InMemoryCount - 1};
subtract_in_memory_counts(#{size := Bytes}, State) ->
    subtract_in_memory_counts(Bytes, State).

message_size(#basic_message{content = Content}) ->
    #content{payload_fragments_rev = PFR} = Content,
    iolist_size(PFR);
message_size({'$prefix_msg', H}) ->
    get_size_from_header(H);
message_size({'$empty_msg', H}) ->
    get_size_from_header(H);
message_size(B) when is_binary(B) ->
    byte_size(B);
message_size(Msg) ->
    %% probably only hit this for testing so ok to use erts_debug
    erts_debug:size(Msg).

get_size_from_header(Size) when is_integer(Size) ->
    Size;
get_size_from_header(#{size := B}) ->
    B.


all_nodes(#?STATE{consumers = Cons0,
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

all_pids_for(Node, #?STATE{consumers = Cons0,
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

suspected_pids_for(Node, #?STATE{consumers = Cons0,
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
