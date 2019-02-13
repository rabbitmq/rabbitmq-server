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
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_fifo).

-behaviour(ra_machine).

-compile(inline_list_funcs).
-compile(inline).
-compile({no_auto_import, [apply/3]}).

-include_lib("ra/include/ra.hrl").
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
         usage/1,

         zero/1,

         %% misc
         dehydrate_state/1,
         normalize/1,

         %% protocol helpers
         make_enqueue/3,
         make_checkout/3,
         make_settle/2,
         make_return/2,
         make_discard/2,
         make_credit/4,
         make_purge/0,
         make_update_config/1
        ]).

-type raw_msg() :: term().
%% The raw message. It is opaque to rabbit_fifo.

-type msg_in_id() :: non_neg_integer().
% a queue scoped monotonically incrementing integer used to enforce order
% in the unassigned messages map

-type msg_id() :: non_neg_integer().
%% A consumer-scoped monotonically incrementing integer included with a
%% {@link delivery/0.}. Used to settle deliveries using
%% {@link rabbit_fifo_client:settle/3.}

-type msg_seqno() :: non_neg_integer().
%% A sender process scoped monotonically incrementing integer included
%% in enqueue messages. Used to ensure ordering of messages send from the
%% same process

-type msg_header() :: #{delivery_count => non_neg_integer()}.
%% The message header map:
%% delivery_count: the number of unsuccessful delivery attempts.
%%                 A non-zero value indicates a previous attempt.

-type msg() :: {msg_header(), raw_msg()}.
%% message with a header map.

-type msg_size() :: non_neg_integer().
%% the size in bytes of the msg payload

-type indexed_msg() :: {ra_index(), msg()}.

-type prefix_msg() :: {'$prefix_msg', msg_size()}.

-type delivery_msg() :: {msg_id(), msg()}.
%% A tuple consisting of the message id and the headered message.

-type consumer_tag() :: binary().
%% An arbitrary binary tag used to distinguish between different consumers
%% set up by the same process. See: {@link rabbit_fifo_client:checkout/3.}

-type delivery() :: {delivery, consumer_tag(), [delivery_msg()]}.
%% Represents the delivery of one or more rabbit_fifo messages.

-type consumer_id() :: {consumer_tag(), pid()}.
%% The entity that receives messages. Uniquely identifies a consumer.

-type credit_mode() :: simple_prefetch | credited.
%% determines how credit is replenished

-type checkout_spec() :: {once | auto, Num :: non_neg_integer(),
                          credit_mode()} |
                         {dequeue, settled | unsettled} |
                         cancel.

-type consumer_meta() :: #{ack => boolean(),
                           username => binary(),
                           prefetch => non_neg_integer(),
                           args => list()}.
%% static meta data associated with a consumer

%% command records representing all the protocol actions that are supported
-record(enqueue, {pid :: maybe(pid()),
                  seq :: maybe(msg_seqno()),
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
-record(update_config, {config :: config()}).



-opaque protocol() ::
    #enqueue{} |
    #checkout{} |
    #settle{} |
    #return{} |
    #discard{} |
    #credit{} |
    #purge{} |
    #update_config{}.

-type command() :: protocol() | ra_machine:builtin_command().
%% all the command types supported by ra fifo

-type client_msg() :: delivery().
%% the messages `rabbit_fifo' can send to consumers.

-type applied_mfa() :: {module(), atom(), list()}.
% represents a partially applied module call

-define(RELEASE_CURSOR_EVERY, 64000).
-define(USE_AVG_HALF_LIFE, 10000.0).

-record(consumer,
        {meta = #{} :: consumer_meta(),
         checked_out = #{} :: #{msg_id() => {msg_in_id(), indexed_msg()}},
         next_msg_id = 0 :: msg_id(), % part of snapshot data
         %% max number of messages that can be sent
         %% decremented for each delivery
         credit = 0 : non_neg_integer(),
         %% total number of checked out messages - ever
         %% incremented for each delivery
         delivery_count = 0 :: non_neg_integer(),
         %% the mode of how credit is incremented
         %% simple_prefetch: credit is re-filled as deliveries are settled
         %% or returned.
         %% credited: credit can only be changed by receiving a consumer_credit
         %% command: `{consumer_credit, ReceiverDeliveryCount, Credit}'
         credit_mode = simple_prefetch :: credit_mode(), % part of snapshot data
         lifetime = once :: once | auto,
         status = up :: up | suspected_down | cancelled
        }).

-type consumer() :: #consumer{}.

-record(enqueuer,
        {next_seqno = 1 :: msg_seqno(),
         % out of order enqueues - sorted list
         pending = [] :: [{msg_seqno(), ra_index(), raw_msg()}],
         status = up :: up | suspected_down
        }).

-record(state,
        {name :: atom(),
         queue_resource :: rabbit_types:r('queue'),
         release_cursor_interval = ?RELEASE_CURSOR_EVERY :: non_neg_integer(),
         % unassigned messages
         messages = #{} :: #{msg_in_id() => indexed_msg()},
         % defines the lowest message in id available in the messages map
         % that isn't a return
         low_msg_num :: msg_in_id() | undefined,
         % defines the next message in id to be added to the messages map
         next_msg_num = 1 :: msg_in_id(),
         % list of returned msg_in_ids - when checking out it picks from
         % this list first before taking low_msg_num
         returns = lqueue:new() :: lqueue:lqueue(prefix_msg() |
                                                 {msg_in_id(), indexed_msg()}),
         % a counter of enqueues - used to trigger shadow copy points
         enqueue_count = 0 :: non_neg_integer(),
         % a map containing all the live processes that have ever enqueued
         % a message to this queue as well as a cached value of the smallest
         % ra_index of all pending enqueues
         enqueuers = #{} :: #{pid() => #enqueuer{}},
         % master index of all enqueue raft indexes including pending
         % enqueues
         % rabbit_fifo_index can be slow when calculating the smallest
         % index when there are large gaps but should be faster than gb_trees
         % for normal appending operations as it's backed by a map
         ra_indexes = rabbit_fifo_index:empty() :: rabbit_fifo_index:state(),
         release_cursors = lqueue:new() :: lqueue:lqueue({release_cursor,
                                                          ra_index(), state()}),
         % consumers need to reflect consumer state at time of snapshot
         % needs to be part of snapshot
         consumers = #{} :: #{consumer_id() => #consumer{}},
         % consumers that require further service are queued here
         % needs to be part of snapshot
         service_queue = queue:new() :: queue:queue(consumer_id()),
         dead_letter_handler :: maybe(applied_mfa()),
         become_leader_handler :: maybe(applied_mfa()),
         %% This is a special field that is only used for snapshots
         %% It represents the queued messages at the time the
         %% dehydrated snapshot state was cached.
         %% As release_cursors are only emitted for raft indexes where all
         %% prior messages no longer contribute to the current state we can
         %% replace all message payloads with their sizes (to be used for
         %% overflow calculations).
         %% This is done so that consumers are still served in a deterministic
         %% order on recovery.
         prefix_msgs = {[], []} :: {Return :: [msg_size()],
                                    PrefixMsgs :: [msg_size()]},
         msg_bytes_enqueue = 0 :: non_neg_integer(),
         msg_bytes_checkout = 0 :: non_neg_integer(),
         max_length :: maybe(non_neg_integer()),
         max_bytes :: maybe(non_neg_integer()),
         %% whether single active consumer is on or not for this queue
         consumer_strategy = default :: default | single_active,
         %% waiting consumers, one is picked active consumer is cancelled or dies
         %% used only when single active consumer is on
         waiting_consumers = [] :: [{consumer_id(), consumer()}]
        }).

-opaque state() :: #state{}.

-type config() :: #{name := atom(),
                    queue_resource := rabbit_types:r('queue'),
                    dead_letter_handler => applied_mfa(),
                    become_leader_handler => applied_mfa(),
                    release_cursor_interval => non_neg_integer(),
                    max_length => non_neg_integer(),
                    max_bytes => non_neg_integer(),
                    single_active_consumer_on => boolean()}.

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
    update_config(Conf, #state{name = Name,
                               queue_resource = Resource}).

update_config(Conf, State) ->
    DLH = maps:get(dead_letter_handler, Conf, undefined),
    BLH = maps:get(become_leader_handler, Conf, undefined),
    SHI = maps:get(release_cursor_interval, Conf, ?RELEASE_CURSOR_EVERY),
    MaxLength = maps:get(max_length, Conf, undefined),
    MaxBytes = maps:get(max_bytes, Conf, undefined),
    ConsumerStrategy = case maps:get(single_active_consumer_on, Conf, false) of
                           true ->
                               single_active;
                           false ->
                               default
                       end,
    State#state{dead_letter_handler = DLH,
                become_leader_handler = BLH,
                release_cursor_interval = SHI,
                max_length = MaxLength,
                max_bytes = MaxBytes,
                consumer_strategy = ConsumerStrategy}.

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
      #state{consumers = Cons0} = State) ->
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
      #state{consumers = Cons0} = State0) ->
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
      #state{consumers = Cons0} = State) ->
    case Cons0 of
        #{ConsumerId := Con0 = #consumer{checked_out = Checked0}} ->
            Checked = maps:without(MsgIds, Checked0),
            Returned = maps:with(MsgIds, Checked0),
            MsgNumMsgs = maps:values(Returned),
            return(Meta, ConsumerId, MsgNumMsgs, Con0, Checked, [], State);
        _ ->
            {State, ok}
    end;
apply(Meta, #credit{credit = NewCredit, delivery_count = RemoteDelCnt,
                    drain = Drain, consumer_id = ConsumerId},
      #state{consumers = Cons0,
             service_queue = ServiceQueue0} = State0) ->
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
                checkout(Meta, State0#state{service_queue = ServiceQueue,
                                      consumers = Cons}, []),
            Response = {send_credit_reply, maps:size(State1#state.messages)},
            %% by this point all checkouts for the updated credit value
            %% should be processed so we can evaluate the drain
            case Drain of
                false ->
                    %% just return the result of the checkout
                    {State1, Response, Effects};
                true ->
                    Con = #consumer{credit = PostCred} =
                        maps:get(ConsumerId, State1#state.consumers),
                    %% add the outstanding credit to the delivery count
                    DeliveryCount = Con#consumer.delivery_count + PostCred,
                    Consumers = maps:put(ConsumerId,
                                         Con#consumer{delivery_count = DeliveryCount,
                                                      credit = 0},
                                         State1#state.consumers),
                    Drained = Con#consumer.credit,
                    {CTag, _} = ConsumerId,
                    {State1#state{consumers = Consumers},
                     %% returning a multi response with two client actions
                     %% for the channel to execute
                     {multi, [Response, {send_drained, [{CTag, Drained}]}]},
                     Effects}
            end;
        _ ->
            %% credit for unknown consumer - just ignore
            {State0, ok}
    end;
apply(Meta, #checkout{spec = {dequeue, Settlement},
                      meta = ConsumerMeta,
                      consumer_id = ConsumerId},
      #state{consumers = Consumers} = State0) ->
    Exists = maps:is_key(ConsumerId, Consumers),
    case messages_ready(State0) of
        0 ->
            {State0, {dequeue, empty}};
        _ when Exists ->
            %% a dequeue using the same consumer_id isn't possible at this point
            {State0, {dequeue, empty}};
        Ready ->
            State1 = update_consumer(ConsumerId, ConsumerMeta,
                                     {once, 1, simple_prefetch}, State0),
            {success, _, MsgId, Msg, State2} = checkout_one(State1),
            case Settlement of
                unsettled ->
                    {_, Pid} = ConsumerId,
                    {State2, {dequeue, {MsgId, Msg}, Ready-1},
                     [{monitor, process, Pid}]};
                settled ->
                    %% immediately settle the checkout
                    {State, _, Effects} = apply(Meta,
                                                make_settle(ConsumerId,
                                                            [MsgId]),
                                                State2),
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
      #state{ra_indexes = Indexes0,
             returns = Returns,
             messages = Messages} = State0) ->
    Total = messages_ready(State0),
    Indexes1 = lists:foldl(fun rabbit_fifo_index:delete/2, Indexes0,
                          [I || {I, _} <- lists:sort(maps:values(Messages))]),
    Indexes = lists:foldl(fun rabbit_fifo_index:delete/2, Indexes1,
                          [I || {_, {I, _}} <- lqueue:to_list(Returns)]),
    {State, _, Effects} =
        update_smallest_raft_index(RaftIdx,
                                   State0#state{ra_indexes = Indexes,
                                                messages = #{},
                                                returns = lqueue:new(),
                                                msg_bytes_enqueue = 0,
                                                prefix_msgs = {[], []},
                                                low_msg_num = undefined},
                                   []),
    %% as we're not checking out after a purge (no point) we have to
    %% reverse the effects ourselves
    {State, {purge, Total},
     lists:reverse([garbage_collection | Effects])};
apply(_, {down, ConsumerPid, noconnection},
      #state{consumers = Cons0,
             enqueuers = Enqs0} = State0) ->
    Node = node(ConsumerPid),
    ConsumerUpdateActiveFun = consumer_active_flag_update_function(State0),
    % mark all consumers and enqueuers as suspected down
    % and monitor the node so that we can find out the final state of the
    % process at some later point
    {Cons, State, Effects1} =
        maps:fold(fun({_, P} = K,
                      #consumer{checked_out = Checked0} = C,
                      {Co, St0, Eff}) when (node(P) =:= Node) and
                                           (C#consumer.status =/= cancelled)->
                          St = return_all(St0, Checked0),
                          Credit = increase_credit(C, maps:size(Checked0)),
                          Eff1 = ConsumerUpdateActiveFun(St, K, C, false,
                                                         suspected_down, Eff),
                          {maps:put(K,
                                    C#consumer{status = suspected_down,
                                               credit = Credit,
                                               checked_out = #{}}, Co),
                           St, Eff1};
                     (K, C, {Co, St, Eff}) ->
                          {maps:put(K, C, Co), St, Eff}
                  end, {#{}, State0, []}, Cons0),
    Enqs = maps:map(fun(P, E) when node(P) =:= Node ->
                            E#enqueuer{status = suspected_down};
                       (_, E) -> E
                    end, Enqs0),
    % mark waiting consumers as suspected if necessary
    WaitingConsumers = update_waiting_consumer_status(Node, State0,
                                                            suspected_down),

    Effects2 = case maps:size(Cons) of
                   0 ->
                       [{aux, inactive}, {monitor, node, Node}];
                   _ ->
                       [{monitor, node, Node}]
               end ++ Effects1,
    %% TODO: should we run a checkout here?
    {State#state{consumers = Cons, enqueuers = Enqs,
                 waiting_consumers = WaitingConsumers}, ok, Effects2};
apply(Meta, {down, Pid, _Info}, #state{consumers = Cons0,
                                       enqueuers = Enqs0} = State0) ->
    % Remove any enqueuer for the same pid and enqueue any pending messages
    % This should be ok as we won't see any more enqueues from this pid
    State1 = case maps:take(Pid, Enqs0) of
                 {#enqueuer{pending = Pend}, Enqs} ->
                     lists:foldl(fun ({_, RIdx, RawMsg}, S) ->
                                         enqueue(RIdx, RawMsg, S)
                                 end, State0#state{enqueuers = Enqs}, Pend);
                 error ->
                     State0
             end,
    {Effects1, State2} = handle_waiting_consumer_down(Pid, State1),
    % return checked out messages to main queue
    % Find the consumers for the down pid
    DownConsumers = maps:keys(
                      maps:filter(fun({_, P}, _) -> P =:= Pid end, Cons0)),
    {State, Effects} = lists:foldl(fun(ConsumerId, {S, E}) ->
                                           cancel_consumer(ConsumerId, S, E, down)
                                   end, {State2, Effects1}, DownConsumers),
    checkout(Meta, State, Effects);
apply(Meta, {nodeup, Node}, #state{consumers = Cons0,
                                   enqueuers = Enqs0,
                                   service_queue = SQ0} = State0) ->
    %% A node we are monitoring has come back.
    %% If we have suspected any processes of being
    %% down we should now re-issue the monitors for them to detect if they're
    %% actually down or not
    Monitors = [{monitor, process, P}
                || P <- suspected_pids_for(Node, State0)],

    % un-suspect waiting consumers when necessary
    WaitingConsumers = update_waiting_consumer_status(Node, State0, up),

    Enqs1 = maps:map(fun(P, E) when node(P) =:= Node ->
                             E#enqueuer{status = up};
                        (_, E) -> E
                     end, Enqs0),
    ConsumerUpdateActiveFun = consumer_active_flag_update_function(State0),
    {Cons1, SQ, Effects} =
        maps:fold(fun({_, P} = ConsumerId, C, {CAcc, SQAcc, EAcc})
                        when (node(P) =:= Node) and
                             (C#consumer.status =/= cancelled) ->
                          EAcc1 = ConsumerUpdateActiveFun(State0, ConsumerId, C, true, up, EAcc),
                          update_or_remove_sub(
                            ConsumerId, C#consumer{status = up},
                            CAcc, SQAcc, EAcc1);
                     (_, _, Acc) ->
                          Acc
                  end, {Cons0, SQ0, Monitors}, Cons0),

    checkout(Meta, State0#state{consumers = Cons1, enqueuers = Enqs1,
                                service_queue = SQ,
                                waiting_consumers = WaitingConsumers}, Effects);
apply(_, {nodedown, _Node}, State) ->
    {State, ok};
apply(Meta, #update_config{config = Conf}, State) ->
    checkout(Meta, update_config(Conf, State), []).

consumer_active_flag_update_function(#state{consumer_strategy = default}) ->
    fun(State, ConsumerId, Consumer, Active, ActivityStatus, Effects) ->
        consumer_update_active_effects(State, ConsumerId, Consumer, Active, ActivityStatus, Effects)
    end;
consumer_active_flag_update_function(#state{consumer_strategy = single_active}) ->
    fun(_, _, _, _, _, Effects) ->
        Effects
    end.

handle_waiting_consumer_down(_Pid,
                             #state{consumer_strategy = default} = State) ->
    {[], State};
handle_waiting_consumer_down(_Pid,
                             #state{consumer_strategy = single_active,
                                    waiting_consumers = []} = State) ->
    {[], State};
handle_waiting_consumer_down(Pid,
                             #state{consumer_strategy = single_active,
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
    State = State0#state{waiting_consumers = StillUp},
    {Effects, State}.

update_waiting_consumer_status(_Node, #state{consumer_strategy = default},
                               _Status) ->
    [];
update_waiting_consumer_status(_Node,
                               #state{consumer_strategy = single_active,
                                      waiting_consumers = []},
                               _Status) ->
    [];
update_waiting_consumer_status(Node,
                               #state{consumer_strategy = single_active,
                                      waiting_consumers = WaitingConsumers},
                               Status) ->
    [begin
         case node(P) of
             Node ->
                 {ConsumerId, Consumer#consumer{status = Status}};
             _ ->
                 {ConsumerId, Consumer}
         end
     end || {{_, P} = ConsumerId, Consumer} <- WaitingConsumers,
            Consumer#consumer.status =/= cancelled].

-spec state_enter(ra_server:ra_state(), state()) -> ra_machine:effects().
state_enter(leader, #state{consumers = Cons,
                           enqueuers = Enqs,
                           waiting_consumers = WaitingConsumers,
                           name = Name,
                           prefix_msgs = {[], []},
                           become_leader_handler = BLH}) ->
    % return effects to monitor all current consumers and enqueuers
    Pids = lists:usort(maps:keys(Enqs)
        ++ [P || {_, P} <- maps:keys(Cons)]
        ++ [P || {{_, P}, _} <- WaitingConsumers]),
    Mons = [{monitor, process, P} || P <- Pids],
    Nots = [{send_msg, P, leader_change, ra_event} || P <- Pids],
    NodeMons = lists:usort([{monitor, node, node(P)} || P <- Pids]),
    Effects = Mons ++ Nots ++ NodeMons,
    case BLH of
        undefined ->
            Effects;
        {Mod, Fun, Args} ->
            [{mod_call, Mod, Fun, Args ++ [Name]} | Effects]
    end;
state_enter(recovered, #state{prefix_msgs = PrefixMsgCounts})
  when PrefixMsgCounts =/= {[], []} ->
    %% TODO: remove assertion?
    exit({rabbit_fifo, unexpected_prefix_msgs, PrefixMsgCounts});
state_enter(eol, #state{enqueuers = Enqs,
                        consumers = Custs0,
                        waiting_consumers = WaitingConsumers0}) ->
    Custs = maps:fold(fun({_, P}, V, S) -> S#{P => V} end, #{}, Custs0),
    WaitingConsumers1 = lists:foldl(fun({{_, P}, V}, Acc) -> Acc#{P => V} end,
                                    #{}, WaitingConsumers0),
    AllConsumers = maps:merge(Custs, WaitingConsumers1),
    [{send_msg, P, eol, ra_event}
     || P <- maps:keys(maps:merge(Enqs, AllConsumers))];
state_enter(_, _) ->
    %% catch all as not handling all states
    [].


-spec tick(non_neg_integer(), state()) -> ra_machine:effects().
tick(_Ts, #state{name = Name,
                 queue_resource = QName,
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
      handle_tick, [QName, Metrics]}, {aux, emit}].

-spec overview(state()) -> map().
overview(#state{consumers = Cons,
                enqueuers = Enqs,
                release_cursors = Cursors,
                msg_bytes_enqueue = EnqueueBytes,
                msg_bytes_checkout = CheckoutBytes} = State) ->
    #{type => ?MODULE,
      num_consumers => maps:size(Cons),
      num_checked_out => num_checked_out(State),
      num_enqueuers => maps:size(Enqs),
      num_ready_messages => messages_ready(State),
      num_messages => messages_total(State),
      num_release_cursors => lqueue:len(Cursors),
      enqueue_message_bytes => EnqueueBytes,
      checkout_message_bytes => CheckoutBytes}.

-spec get_checked_out(consumer_id(), msg_id(), msg_id(), state()) ->
    [delivery_msg()].
get_checked_out(Cid, From, To, #state{consumers = Consumers}) ->
    case Consumers of
        #{Cid := #consumer{checked_out = Checked}} ->
            [{K, snd(snd(maps:get(K, Checked)))}
             || K <- lists:seq(From, To),
                maps:is_key(K, Checked)];
        _ ->
            []
    end.

init_aux(Name) when is_atom(Name) ->
    %% TODO: catch specific exception throw if table already exists
    ok = ra_machine_ets:create_table(rabbit_fifo_usage,
                                     [named_table, set, public,
                                      {write_concurrency, true}]),
    Now = erlang:monotonic_time(micro_seconds),
    {Name, {inactive, Now, 1, 1.0}}.

handle_aux(_, cast, Cmd, {Name, Use0}, Log, _) ->
    Use = case Cmd of
              _ when Cmd == active orelse Cmd == inactive ->
                  update_use(Use0, Cmd);
              emit ->
                  true = ets:insert(rabbit_fifo_usage,
                                    {Name, utilisation(Use0)}),
                  Use0
          end,
    {no_reply, {Name, Use}, Log}.

%%% Queries

query_messages_ready(State) ->
    messages_ready(State).

query_messages_checked_out(#state{consumers = Consumers}) ->
    maps:fold(fun (_, #consumer{checked_out = C}, S) ->
                      maps:size(C) + S
              end, 0, Consumers).

query_messages_total(State) ->
    messages_total(State).

query_processes(#state{enqueuers = Enqs, consumers = Cons0}) ->
    Cons = maps:fold(fun({_, P}, V, S) -> S#{P => V} end, #{}, Cons0),
    maps:keys(maps:merge(Enqs, Cons)).


query_ra_indexes(#state{ra_indexes = RaIndexes}) ->
    RaIndexes.

query_consumer_count(#state{consumers = Consumers,
                            waiting_consumers = WaitingConsumers}) ->
    maps:size(Consumers) + length(WaitingConsumers).

query_consumers(#state{consumers = Consumers,
                       waiting_consumers = WaitingConsumers,
                       consumer_strategy = ConsumerStrategy } = State) ->
    ActiveActivityStatusFun = case ConsumerStrategy of
                                  default ->
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
    FromConsumers = maps:fold(fun (_, #consumer{status = cancelled}, Acc) ->
                                      Acc;
                                   ({Tag, Pid}, #consumer{meta = Meta} = Consumer, Acc) ->
                                      {Active, ActivityStatus} = ActiveActivityStatusFun({Tag, Pid}, Consumer),
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
                            {Active, ActivityStatus} = ActiveActivityStatusFun({Tag, Pid}, Consumer),
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

query_single_active_consumer(#state{consumer_strategy = single_active,
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

query_stat(#state{consumers = Consumers} = State) ->
    {messages_ready(State), maps:size(Consumers)}.

-spec usage(atom()) -> float().
usage(Name) when is_atom(Name) ->
    case ets:lookup(rabbit_fifo_usage, Name) of
        [] -> 0.0;
        [{_, Use}] -> Use
    end.

%%% Internal

messages_ready(#state{messages = M,
                      prefix_msgs = {PreR, PreM},
                      returns = R}) ->

    %% TODO: optimise to avoid length/1 call
    maps:size(M) + lqueue:len(R) + length(PreR) + length(PreM).

messages_total(#state{ra_indexes = I,
                      prefix_msgs = {PreR, PreM}}) ->
    rabbit_fifo_index:size(I) + length(PreR) + length(PreM).

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

num_checked_out(#state{consumers = Cons}) ->
    lists:foldl(fun (#consumer{checked_out = C}, Acc) ->
                        maps:size(C) + Acc
                end, 0, maps:values(Cons)).

cancel_consumer(ConsumerId,
                #state{consumer_strategy = default} = State, Effects, Reason) ->
    %% general case, single active consumer off
    cancel_consumer0(ConsumerId, State, Effects, Reason);
cancel_consumer(ConsumerId,
                #state{consumer_strategy = single_active,
                       waiting_consumers = []} = State,
                Effects, Reason) ->
    %% single active consumer on, no consumers are waiting
    cancel_consumer0(ConsumerId, State, Effects, Reason);
cancel_consumer(ConsumerId,
                #state{consumers = Cons0,
                       consumer_strategy = single_active,
                       waiting_consumers = WaitingConsumers0} = State0,
               Effects0, Reason) ->
    %% single active consumer on, consumers are waiting
    case maps:take(ConsumerId, Cons0) of
        {Consumer, Cons1} ->
            % The active consumer is to be removed
            % Cancel it
            {State1, Effects1} = maybe_return_all(ConsumerId, Consumer, Cons1, State0, Effects0, Reason),
            Effects2 = cancel_consumer_effects(ConsumerId, State1, Effects1),
            % Take another one from the waiting consumers and put it in consumers
            [{NewActiveConsumerId, NewActiveConsumer}
             | RemainingWaitingConsumers] = WaitingConsumers0,
            #state{service_queue = ServiceQueue} = State1,
            ServiceQueue1 = maybe_queue_consumer(NewActiveConsumerId,
                                                 NewActiveConsumer,
                                                 ServiceQueue),
            State = State1#state{consumers = maps:put(NewActiveConsumerId,
                                                      NewActiveConsumer, State1#state.consumers),
                                 service_queue = ServiceQueue1,
                                 waiting_consumers = RemainingWaitingConsumers},
            Effects = consumer_update_active_effects(State, NewActiveConsumerId,
                                                      NewActiveConsumer, true,
                                                      single_active, Effects2),
            {State, Effects};
        error ->
            % The cancelled consumer is not the active one
            % Just remove it from idle_consumers
            WaitingConsumers = lists:keydelete(ConsumerId, 1,
                                               WaitingConsumers0),
            Effects = cancel_consumer_effects(ConsumerId, State0, Effects0),
            % A waiting consumer isn't supposed to have any checked out messages,
            % so nothing special to do here
            {State0#state{waiting_consumers = WaitingConsumers}, Effects}
    end.

consumer_update_active_effects(#state{queue_resource = QName },
                               ConsumerId, #consumer{meta = Meta},
                               Active, ActivityStatus,
                               Effects) ->
    Ack = maps:get(ack, Meta, undefined),
    Prefetch = maps:get(prefetch, Meta, undefined),
    Args = maps:get(args, Meta, []),
    [{mod_call,
      rabbit_quorum_queue,
      update_consumer_handler,
      [QName, ConsumerId, false, Ack, Prefetch, Active, ActivityStatus, Args]}
      | Effects].

cancel_consumer0(ConsumerId, #state{consumers = C0} = S0, Effects0, Reason) ->
    case maps:take(ConsumerId, C0) of
        {Consumer, Cons1} ->
            {S, Effects2} = maybe_return_all(ConsumerId, Consumer, Cons1, S0, Effects0, Reason),
            Effects = cancel_consumer_effects(ConsumerId, S, Effects2),
            case maps:size(S#state.consumers) of
                0 ->
                    {S, [{aux, inactive} | Effects]};
                _ ->
                    {S, Effects}
            end;
        error ->
            %% already removed: do nothing
            {S0, Effects0}
    end.

maybe_return_all(ConsumerId, #consumer{checked_out = Checked0} = Consumer, Cons1,
                 #state{consumers = C0,
                        service_queue = SQ0} = S0, Effects0, Reason) ->
    case Reason of
        consumer_cancel ->
            {Cons, SQ, Effects1} =
                update_or_remove_sub(ConsumerId,
                                     Consumer#consumer{lifetime = once,
                                                       credit = 0,
                                                       status = cancelled},
                                     C0, SQ0, Effects0),
            {S0#state{consumers = Cons, service_queue = SQ}, Effects1};
        down ->
            S1 = return_all(S0, Checked0),
            {S1#state{consumers = Cons1}, Effects0}
    end.

apply_enqueue(#{index := RaftIdx} = Meta, From, Seq, RawMsg, State0) ->
    Bytes = message_size(RawMsg),
    case maybe_enqueue(RaftIdx, From, Seq, RawMsg, [], State0) of
        {ok, State1, Effects1} ->
            State2 = append_to_master_index(RaftIdx,
                                            add_bytes_enqueue(Bytes, State1)),
            {State, ok, Effects} = checkout(Meta, State2, Effects1),
            {maybe_store_dehydrated_state(RaftIdx, State), ok, Effects};
        {duplicate, State, Effects} ->
            {State, ok, Effects}
    end.

drop_head(#state{ra_indexes = Indexes0} = State0, Effects0) ->
    case take_next_msg(State0) of
        {FullMsg = {_MsgId, {RaftIdxToDrop, {_Header, Msg}}},
         State1} ->
            Indexes = rabbit_fifo_index:delete(RaftIdxToDrop, Indexes0),
            Bytes = message_size(Msg),
            State = add_bytes_drop(Bytes, State1#state{ra_indexes = Indexes}),
            Effects = dead_letter_effects(maxlen, maps:put(none, FullMsg, #{}),
                                          State, Effects0),
            {State, Effects};
        {{'$prefix_msg', Bytes}, State1} ->
            State = add_bytes_drop(Bytes, State1),
            {State, Effects0};
        empty ->
            {State0, Effects0}
    end.

enqueue(RaftIdx, RawMsg, #state{messages = Messages,
                                low_msg_num = LowMsgNum,
                                next_msg_num = NextMsgNum} = State0) ->
    Msg = {RaftIdx, {#{}, RawMsg}}, % indexed message with header map
    State0#state{messages = Messages#{NextMsgNum => Msg},
                 % this is probably only done to record it when low_msg_num
                 % is undefined
                 low_msg_num = min(LowMsgNum, NextMsgNum),
                 next_msg_num = NextMsgNum + 1}.

append_to_master_index(RaftIdx,
                       #state{ra_indexes = Indexes0} = State0) ->
    State = incr_enqueue_count(State0),
    Indexes = rabbit_fifo_index:append(RaftIdx, Indexes0),
    State#state{ra_indexes = Indexes}.

incr_enqueue_count(#state{enqueue_count = C,
                          release_cursor_interval = C} = State0) ->
    % this will trigger a dehydrated version of the state to be stored
    % at this raft index for potential future snapshot generation
    State0#state{enqueue_count = 0};
incr_enqueue_count(#state{enqueue_count = C} = State) ->
    State#state{enqueue_count = C + 1}.

maybe_store_dehydrated_state(RaftIdx,
                             #state{ra_indexes = Indexes,
                                    enqueue_count = 0,
                                    release_cursors = Cursors} = State) ->
    case rabbit_fifo_index:exists(RaftIdx, Indexes) of
        false ->
            %% the incoming enqueue must already have been dropped
            State;
        true ->
            Dehydrated = dehydrate_state(State),
            Cursor = {release_cursor, RaftIdx, Dehydrated},
            State#state{release_cursors = lqueue:in(Cursor, Cursors)}
    end;
maybe_store_dehydrated_state(_RaftIdx, State) ->
    State.

enqueue_pending(From,
                #enqueuer{next_seqno = Next,
                          pending = [{Next, RaftIdx, RawMsg} | Pending]} = Enq0,
                State0) ->
            State = enqueue(RaftIdx, RawMsg, State0),
            Enq = Enq0#enqueuer{next_seqno = Next + 1, pending = Pending},
            enqueue_pending(From, Enq, State);
enqueue_pending(From, Enq, #state{enqueuers = Enqueuers0} = State) ->
    State#state{enqueuers = Enqueuers0#{From => Enq}}.

maybe_enqueue(RaftIdx, undefined, undefined, RawMsg, Effects, State0) ->
    % direct enqueue without tracking
    State = enqueue(RaftIdx, RawMsg, State0),
    {ok, State, Effects};
maybe_enqueue(RaftIdx, From, MsgSeqNo, RawMsg, Effects0,
              #state{enqueuers = Enqueuers0} = State0) ->
    case maps:get(From, Enqueuers0, undefined) of
        undefined ->
            State1 = State0#state{enqueuers = Enqueuers0#{From => #enqueuer{}}},
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
            {ok, State0#state{enqueuers = Enqueuers0#{From => Enq}}, Effects0};
        #enqueuer{next_seqno = Next} when MsgSeqNo =< Next ->
            % duplicate delivery - remove the raft index from the ra_indexes
            % map as it was added earlier
            {duplicate, State0, Effects0}
    end.

snd(T) ->
    element(2, T).

return(Meta, ConsumerId, MsgNumMsgs, Con0, Checked,
       Effects0, #state{consumers = Cons0, service_queue = SQ0} = State0) ->
    Con = Con0#consumer{checked_out = Checked,
                        credit = increase_credit(Con0, length(MsgNumMsgs))},
    {Cons, SQ, Effects} = update_or_remove_sub(ConsumerId, Con, Cons0,
                                               SQ0, Effects0),
    State1 = lists:foldl(fun({'$prefix_msg', _} = Msg, S0) ->
                                 return_one(0, Msg, S0);
                            ({MsgNum, Msg}, S0) ->
                                 return_one(MsgNum, Msg, S0)
                         end, State0, MsgNumMsgs),
    checkout(Meta, State1#state{consumers = Cons,
                                service_queue = SQ},
             Effects).

% used to processes messages that are finished
complete(ConsumerId, MsgRaftIdxs, NumDiscarded,
         Con0, Checked, Effects0,
         #state{consumers = Cons0, service_queue = SQ0,
                ra_indexes = Indexes0} = State0) ->
    %% credit_mode = simple_prefetch should automatically top-up credit
    %% as messages are simple_prefetch or otherwise returned
    Con = Con0#consumer{checked_out = Checked,
                        credit = increase_credit(Con0, NumDiscarded)},
    {Cons, SQ, Effects} = update_or_remove_sub(ConsumerId, Con, Cons0,
                                               SQ0, Effects0),
    Indexes = lists:foldl(fun rabbit_fifo_index:delete/2, Indexes0,
                          MsgRaftIdxs),
    {State0#state{consumers = Cons,
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
    Checked = maps:without(MsgIds, Checked0),
    Discarded = maps:with(MsgIds, Checked0),
    MsgRaftIdxs = [RIdx || {_, {RIdx, _}} <- maps:values(Discarded)],
    State1 = lists:foldl(fun({_, {_, {_, RawMsg}}}, Acc) ->
                                 add_bytes_settle(RawMsg, Acc);
                            ({'$prefix_msg', _} = M, Acc) ->
                                 add_bytes_settle(M, Acc)
                         end, State0, maps:values(Discarded)),
    %% need to pass the length of discarded as $prefix_msgs would be filtered
    %% by the above list comprehension
    {State2, Effects1} = complete(ConsumerId, MsgRaftIdxs,
                                  maps:size(Discarded),
                                  Con0, Checked, Effects0, State1),
    {State, ok, Effects} = checkout(Meta, State2, Effects1),
    % settle metrics are incremented separately
    update_smallest_raft_index(IncomingRaftIdx, State, Effects).

dead_letter_effects(_Reason, _Discarded,
                    #state{dead_letter_handler = undefined},
                    Effects) ->
    Effects;
dead_letter_effects(Reason, Discarded,
                    #state{dead_letter_handler = {Mod, Fun, Args}}, Effects) ->
    DeadLetters = maps:fold(fun(_, {_, {_, {_Header, Msg}}},
                                % MsgId, MsgIdID, RaftId, Header
                                Acc) -> [{Reason, Msg} | Acc]
                            end, [], Discarded),
    [{mod_call, Mod, Fun, Args ++ [DeadLetters]} | Effects].

cancel_consumer_effects(ConsumerId, #state{queue_resource = QName}, Effects) ->
    [{mod_call, rabbit_quorum_queue,
      cancel_consumer_handler, [QName, ConsumerId]} | Effects].

update_smallest_raft_index(IncomingRaftIdx,
                           #state{ra_indexes = Indexes,
                                  release_cursors = Cursors0} = State0,
                           Effects) ->
    case rabbit_fifo_index:size(Indexes) of
        0 ->
            % there are no messages on queue anymore and no pending enqueues
            % we can forward release_cursor all the way until
            % the last received command, hooray
            State = State0#state{release_cursors = lqueue:new()},
            {State, ok,
             [{release_cursor, IncomingRaftIdx, State} | Effects]};
        _ ->
            Smallest = rabbit_fifo_index:smallest(Indexes),
            case find_next_cursor(Smallest, Cursors0) of
                {empty, Cursors} ->
                    {State0#state{release_cursors = Cursors},
                     ok, Effects};
                {Cursor, Cursors} ->
                    %% we can emit a release cursor we've passed the smallest
                    %% release cursor available.
                    {State0#state{release_cursors = Cursors}, ok,
                     [Cursor | Effects]}
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

return_one(0, {'$prefix_msg', _} = Msg,
           #state{returns = Returns} = State0) ->
    add_bytes_return(Msg,
                     State0#state{returns = lqueue:in(Msg, Returns)});
return_one(MsgNum, {RaftId, {Header0, RawMsg}},
           #state{returns = Returns} = State0) ->
    Header = maps:update_with(delivery_count,
                              fun (C) -> C+1 end,
                              1, Header0),
    Msg = {RaftId, {Header, RawMsg}},
    % this should not affect the release cursor in any way
    add_bytes_return(RawMsg,
                     State0#state{returns = lqueue:in({MsgNum, Msg}, Returns)}).

return_all(State0, Checked0) ->
    %% need to sort the list so that we return messages in the order
    %% they were checked out
    Checked = lists:sort(maps:to_list(Checked0)),
    lists:foldl(fun ({_, {'$prefix_msg', _} = Msg}, S) ->
                        return_one(0, Msg, S);
                    ({_, {MsgNum, Msg}}, S) ->
                        return_one(MsgNum, Msg, S)
                end, State0, Checked).

%% checkout new messages to consumers
%% reverses the effects list
checkout(#{index := Index}, State0, Effects0) ->
    {State1, _Result, Effects1} = checkout0(checkout_one(State0),
                                            Effects0, #{}),
    case evaluate_limit(State0#state.ra_indexes, false,
                        State1, Effects1) of
        {State, true, Effects} ->
            update_smallest_raft_index(Index, State, Effects);
        {State, false, Effects} ->
            {State, ok, Effects}
    end.

checkout0({success, ConsumerId, MsgId, Msg, State}, Effects, Acc0) ->
    DelMsg = {MsgId, Msg},
    Acc = maps:update_with(ConsumerId,
                           fun (M) -> [DelMsg | M] end,
                           [DelMsg], Acc0),
    checkout0(checkout_one(State), Effects, Acc);
checkout0({Activity, State0}, Effects0, Acc) ->
    Effects1 = case Activity of
                   nochange ->
                       append_send_msg_effects(Effects0, Acc);
                   inactive ->
                       [{aux, inactive}
                        | append_send_msg_effects(Effects0, Acc)]
               end,
    {State0, ok, lists:reverse(Effects1)}.

evaluate_limit(_OldIndexes, Result,
               #state{max_length = undefined,
                      max_bytes = undefined} = State,
               Effects) ->
    {State, Result, Effects};
evaluate_limit(OldIndexes, Result,
               State0, Effects0) ->
    case is_over_limit(State0) of
        true ->
            {State, Effects} = drop_head(State0, Effects0),
            evaluate_limit(OldIndexes, true, State, Effects);
        false ->
            {State0, Result, Effects0}
    end.

append_send_msg_effects(Effects, AccMap) when map_size(AccMap) == 0 ->
    Effects;
append_send_msg_effects(Effects0, AccMap) ->
    Effects = maps:fold(fun (C, Msgs, Ef) ->
                                [send_msg_effect(C, lists:reverse(Msgs)) | Ef]
                        end, Effects0, AccMap),
    [{aux, active} | Effects].

%% next message is determined as follows:
%% First we check if there are are prefex returns
%% Then we check if there are current returns
%% then we check prefix msgs
%% then we check current messages
%%
%% When we return it is always done to the current return queue
%% for both prefix messages and current messages
take_next_msg(#state{prefix_msgs = {[Bytes | Rem], P}} = State) ->
    %% there are prefix returns, these should be served first
    {{'$prefix_msg', Bytes},
     State#state{prefix_msgs = {Rem, P}}};
take_next_msg(#state{returns = Returns,
                     low_msg_num = Low0,
                     messages = Messages0,
                     prefix_msgs = {R, P}} = State) ->
    %% use peek rather than out there as the most likely case is an empty
    %% queue
    case lqueue:peek(Returns) of
        {value, NextMsg} ->
            {NextMsg,
             State#state{returns = lqueue:drop(Returns)}};
        empty when P == [] ->
            case Low0 of
                undefined ->
                    empty;
                _ ->
                    {Msg, Messages} = maps:take(Low0, Messages0),
                    case maps:size(Messages) of
                        0 ->
                            {{Low0, Msg},
                             State#state{messages = Messages,
                                         low_msg_num = undefined}};
                        _ ->
                            {{Low0, Msg},
                             State#state{messages = Messages,
                                         low_msg_num = Low0 + 1}}
                    end
            end;
        empty ->
            [Bytes | Rem] = P,
            %% There are prefix msgs
            {{'$prefix_msg', Bytes},
             State#state{prefix_msgs = {R, Rem}}}
    end.

send_msg_effect({CTag, CPid}, Msgs) ->
    {send_msg, CPid, {delivery, CTag, Msgs}, ra_event}.

checkout_one(#state{service_queue = SQ0,
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
                            checkout_one(InitState#state{service_queue = SQ1});
                        {ok, #consumer{status = cancelled}} ->
                            checkout_one(InitState#state{service_queue = SQ1});
                        {ok, #consumer{status = suspected_down}} ->
                            checkout_one(InitState#state{service_queue = SQ1});
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
                            State1 = State0#state{service_queue = SQ,
                                                  consumers = Cons},
                            {State, Msg} =
                                case ConsumerMsg of
                                    {'$prefix_msg', _} ->
                                        {add_bytes_checkout(ConsumerMsg, State1),
                                         ConsumerMsg};
                                    {_, {_, {_, RawMsg} = M}} ->
                                        {add_bytes_checkout(RawMsg, State1),
                                         M}
                                end,
                            {success, ConsumerId, Next, Msg, State};
                        error ->
                            %% consumer did not exist but was queued, recurse
                            checkout_one(InitState#state{service_queue = SQ1})
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
                #state{consumer_strategy = default} = State0) ->
    %% general case, single active consumer off
    update_consumer0(ConsumerId, Meta, Spec, State0);
update_consumer(ConsumerId, Meta, Spec,
                #state{consumers = Cons0,
                       consumer_strategy = single_active} = State0)
  when map_size(Cons0) == 0 ->
    %% single active consumer on, no one is consuming yet
    update_consumer0(ConsumerId, Meta, Spec, State0);
update_consumer(ConsumerId, Meta, {Life, Credit, Mode},
                #state{consumer_strategy = single_active,
                       waiting_consumers = WaitingConsumers0} = State0) ->
    %% single active consumer on and one active consumer already
    %% adding the new consumer to the waiting list
    Consumer = #consumer{lifetime = Life, meta = Meta,
                         credit = Credit, credit_mode = Mode},
    WaitingConsumers1 = WaitingConsumers0 ++ [{ConsumerId, Consumer}],
    State0#state{waiting_consumers = WaitingConsumers1}.

update_consumer0(ConsumerId, Meta, {Life, Credit, Mode},
                 #state{consumers = Cons0,
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

    State0#state{consumers = Cons, service_queue = ServiceQueue}.

maybe_queue_consumer(ConsumerId, #consumer{credit = Credit},
                     ServiceQueue0) ->
    case Credit > 0 of
        true ->
            % consumerect needs service - check if already on service queue
            uniq_queue_in(ConsumerId, ServiceQueue0);
        false ->
            ServiceQueue0
    end.


%% creates a dehydrated version of the current state to be cached and
%% potentially used to for a snaphot at a later point
dehydrate_state(#state{messages = Messages,
                       consumers = Consumers,
                       returns = Returns,
                       prefix_msgs = {PrefRet0, PrefMsg0}} = State) ->
    %% TODO: optimise this function as far as possible
    PrefRet = lists:foldl(fun ({'$prefix_msg', Bytes}, Acc) ->
                                  [Bytes | Acc];
                              ({_, {_, {_, Raw}}}, Acc) ->
                                  [message_size(Raw) | Acc]
                          end,
                          lists:reverse(PrefRet0),
                          lqueue:to_list(Returns)),
    PrefMsgs = lists:foldl(fun ({_, {_RaftIdx, {_H, Raw}}}, Acc) ->
                                   [message_size(Raw) | Acc]
                           end,
                           lists:reverse(PrefMsg0),
                           lists:sort(maps:to_list(Messages))),
    State#state{messages = #{},
                ra_indexes = rabbit_fifo_index:empty(),
                release_cursors = lqueue:new(),
                low_msg_num = undefined,
                consumers = maps:map(fun (_, C) ->
                                             dehydrate_consumer(C)
                                     end, Consumers),
                returns = lqueue:new(),
                prefix_msgs = {lists:reverse(PrefRet),
                               lists:reverse(PrefMsgs)}}.

dehydrate_consumer(#consumer{checked_out = Checked0} = Con) ->
    Checked = maps:map(fun (_, {'$prefix_msg', _} = M) ->
                               M;
                           (_, {_, {_, {_, Raw}}}) ->
                               {'$prefix_msg', message_size(Raw)}
                       end, Checked0),
    Con#consumer{checked_out = Checked}.

%% make the state suitable for equality comparison
normalize(#state{release_cursors = Cursors} = State) ->
    State#state{release_cursors = lqueue:from_list(lqueue:to_list(Cursors))}.

is_over_limit(#state{max_length = undefined,
                     max_bytes = undefined}) ->
    false;
is_over_limit(#state{max_length = MaxLength,
                     max_bytes = MaxBytes,
                     msg_bytes_enqueue = BytesEnq} = State) ->

    messages_ready(State) > MaxLength orelse (BytesEnq > MaxBytes).

-spec make_enqueue(maybe(pid()), maybe(msg_seqno()), raw_msg()) -> protocol().
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

-spec make_update_config(config()) -> protocol().
make_update_config(Config) ->
    #update_config{config = Config}.

add_bytes_enqueue(Bytes, #state{msg_bytes_enqueue = Enqueue} = State) ->
    State#state{msg_bytes_enqueue = Enqueue + Bytes}.

add_bytes_drop(Bytes, #state{msg_bytes_enqueue = Enqueue} = State) ->
    State#state{msg_bytes_enqueue = Enqueue - Bytes}.

add_bytes_checkout(Msg, #state{msg_bytes_checkout = Checkout,
                               msg_bytes_enqueue = Enqueue } = State) ->
    Bytes = message_size(Msg),
    State#state{msg_bytes_checkout = Checkout + Bytes,
                msg_bytes_enqueue = Enqueue - Bytes}.

add_bytes_settle(Msg, #state{msg_bytes_checkout = Checkout} = State) ->
    Bytes = message_size(Msg),
    State#state{msg_bytes_checkout = Checkout - Bytes}.

add_bytes_return(Msg, #state{msg_bytes_checkout = Checkout,
                             msg_bytes_enqueue = Enqueue} = State) ->
    Bytes = message_size(Msg),
    State#state{msg_bytes_checkout = Checkout - Bytes,
                msg_bytes_enqueue = Enqueue + Bytes}.

message_size(#basic_message{content = Content}) ->
    #content{payload_fragments_rev = PFR} = Content,
    iolist_size(PFR);
message_size({'$prefix_msg', B}) ->
    B;
message_size(B) when is_binary(B) ->
    byte_size(B);
message_size(Msg) ->
    %% probably only hit this for testing so ok to use erts_debug
    erts_debug:size(Msg).

suspected_pids_for(Node, #state{consumers = Cons0,
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

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-define(ASSERT_EFF(EfxPat, Effects),
        ?ASSERT_EFF(EfxPat, true, Effects)).

-define(ASSERT_EFF(EfxPat, Guard, Effects),
    ?assert(lists:any(fun (EfxPat) when Guard -> true;
                          (_) -> false
                      end, Effects))).

-define(ASSERT_NO_EFF(EfxPat, Effects),
    ?assert(not lists:any(fun (EfxPat) -> true;
                          (_) -> false
                      end, Effects))).

-define(assertNoEffect(EfxPat, Effects),
    ?assert(not lists:any(fun (EfxPat) -> true;
                          (_) -> false
                      end, Effects))).

test_init(Name) ->
    init(#{name => Name,
           queue_resource => rabbit_misc:r("/", queue,
                                           atom_to_binary(Name, utf8)),
           release_cursor_interval => 0}).

% To launch these tests: make eunit EUNIT_MODS="rabbit_fifo"

enq_enq_checkout_test() ->
    Cid = {<<"enq_enq_checkout_test">>, self()},
    {State1, _} = enq(1, 1, first, test_init(test)),
    {State2, _} = enq(2, 2, second, State1),
    {_State3, _, Effects} =
        apply(meta(3),
              make_checkout(Cid, {once, 2, simple_prefetch}, #{}),
              State2),
    ?ASSERT_EFF({monitor, _, _}, Effects),
    ?ASSERT_EFF({send_msg, _, {delivery, _, _}, _}, Effects),
    ok.

credit_enq_enq_checkout_settled_credit_test() ->
    Cid = {?FUNCTION_NAME, self()},
    {State1, _} = enq(1, 1, first, test_init(test)),
    {State2, _} = enq(2, 2, second, State1),
    {State3, _, Effects} =
        apply(meta(3), make_checkout(Cid, {auto, 1, credited}, #{}), State2),
    ?ASSERT_EFF({monitor, _, _}, Effects),
    Deliveries = lists:filter(fun ({send_msg, _, {delivery, _, _}, _}) -> true;
                                  (_) -> false
                              end, Effects),
    ?assertEqual(1, length(Deliveries)),
    %% settle the delivery this should _not_ result in further messages being
    %% delivered
    {State4, SettledEffects} = settle(Cid, 4, 1, State3),
    ?assertEqual(false, lists:any(fun ({send_msg, _, {delivery, _, _}, _}) ->
                                          true;
                                      (_) -> false
                                  end, SettledEffects)),
    %% granting credit (3) should deliver the second msg if the receivers
    %% delivery count is (1)
    {State5, CreditEffects} = credit(Cid, 5, 1, 1, false, State4),
    % ?debugFmt("CreditEffects  ~p ~n~p", [CreditEffects, State4]),
    ?ASSERT_EFF({send_msg, _, {delivery, _, _}, _}, CreditEffects),
    {_State6, FinalEffects} = enq(6, 3, third, State5),
    ?assertEqual(false, lists:any(fun ({send_msg, _, {delivery, _, _}, _}) ->
                                          true;
                                      (_) -> false
                                  end, FinalEffects)),
    ok.

credit_with_drained_test() ->
    Cid = {?FUNCTION_NAME, self()},
    State0 = test_init(test),
    %% checkout with a single credit
    {State1, _, _} =
        apply(meta(1), make_checkout(Cid, {auto, 1, credited},#{}),
              State0),
    ?assertMatch(#state{consumers = #{Cid := #consumer{credit = 1,
                                                       delivery_count = 0}}},
                 State1),
    {State, Result, _} =
         apply(meta(3), make_credit(Cid, 0, 5, true), State1),
    ?assertMatch(#state{consumers = #{Cid := #consumer{credit = 0,
                                                       delivery_count = 5}}},
                 State),
    ?assertEqual({multi, [{send_credit_reply, 0},
                          {send_drained, [{?FUNCTION_NAME, 5}]}]},
                           Result),
    ok.

credit_and_drain_test() ->
    Cid = {?FUNCTION_NAME, self()},
    {State1, _} = enq(1, 1, first, test_init(test)),
    {State2, _} = enq(2, 2, second, State1),
    %% checkout without any initial credit (like AMQP 1.0 would)
    {State3, _, CheckEffs} =
        apply(meta(3), make_checkout(Cid, {auto, 0, credited}, #{}),
              State2),

    ?ASSERT_NO_EFF({send_msg, _, {delivery, _, _}}, CheckEffs),
    {State4, {multi, [{send_credit_reply, 0},
                      {send_drained, [{?FUNCTION_NAME, 2}]}]},
    Effects} = apply(meta(4), make_credit(Cid, 4, 0, true), State3),
    ?assertMatch(#state{consumers = #{Cid := #consumer{credit = 0,
                                                       delivery_count = 4}}},
                 State4),

    ?ASSERT_EFF({send_msg, _, {delivery, _, [{_, {_, first}},
                                             {_, {_, second}}]}, _}, Effects),
    {_State5, EnqEffs} = enq(5, 2, third, State4),
    ?ASSERT_NO_EFF({send_msg, _, {delivery, _, _}}, EnqEffs),
    ok.



enq_enq_deq_test() ->
    Cid = {?FUNCTION_NAME, self()},
    {State1, _} = enq(1, 1, first, test_init(test)),
    {State2, _} = enq(2, 2, second, State1),
    % get returns a reply value
    NumReady = 1,
    {_State3, {dequeue, {0, {_, first}}, NumReady}, [{monitor, _, _}]} =
        apply(meta(3), make_checkout(Cid, {dequeue, unsettled}, #{}),
              State2),
    ok.

enq_enq_deq_deq_settle_test() ->
    Cid = {?FUNCTION_NAME, self()},
    {State1, _} = enq(1, 1, first, test_init(test)),
    {State2, _} = enq(2, 2, second, State1),
    % get returns a reply value
    {State3, {dequeue, {0, {_, first}}, 1}, [{monitor, _, _}]} =
        apply(meta(3), make_checkout(Cid, {dequeue, unsettled}, #{}),
              State2),
    {_State4, {dequeue, empty}} =
        apply(meta(4), make_checkout(Cid, {dequeue, unsettled}, #{}),
              State3),
    ok.

enq_enq_checkout_get_settled_test() ->
    Cid = {?FUNCTION_NAME, self()},
    {State1, _} = enq(1, 1, first, test_init(test)),
    % get returns a reply value
    {_State2, {dequeue, {0, {_, first}}, _}, _Effs} =
        apply(meta(3), make_checkout(Cid, {dequeue, settled}, #{}),
              State1),
    ok.

checkout_get_empty_test() ->
    Cid = {?FUNCTION_NAME, self()},
    State = test_init(test),
    {_State2, {dequeue, empty}} =
        apply(meta(1), make_checkout(Cid, {dequeue, unsettled}, #{}), State),
    ok.

untracked_enq_deq_test() ->
    Cid = {?FUNCTION_NAME, self()},
    State0 = test_init(test),
    {State1, _, _} = apply(meta(1),
                           make_enqueue(undefined, undefined, first),
                           State0),
    {_State2, {dequeue, {0, {_, first}}, _}, _} =
        apply(meta(3), make_checkout(Cid, {dequeue, settled}, #{}), State1),
    ok.

release_cursor_test() ->
    Cid = {?FUNCTION_NAME, self()},
    {State1, _} = enq(1, 1, first,  test_init(test)),
    {State2, _} = enq(2, 2, second, State1),
    {State3, _} = check(Cid, 3, 10, State2),
    % no release cursor effect at this point
    {State4, _} = settle(Cid, 4, 1, State3),
    {_Final, Effects1} = settle(Cid, 5, 0, State4),
    % empty queue forwards release cursor all the way
    ?ASSERT_EFF({release_cursor, 5, _}, Effects1),
    ok.

checkout_enq_settle_test() ->
    Cid = {?FUNCTION_NAME, self()},
    {State1, [{monitor, _, _} | _]} = check(Cid, 1, test_init(test)),
    {State2, Effects0} = enq(2, 1,  first, State1),
    ?ASSERT_EFF({send_msg, _,
                 {delivery, ?FUNCTION_NAME,
                  [{0, {_, first}}]}, _},
                Effects0),
    {State3, [_Inactive]} = enq(3, 2, second, State2),
    {_, _Effects} = settle(Cid, 4, 0, State3),
    % the release cursor is the smallest raft index that does not
    % contribute to the state of the application
    % ?ASSERT_EFF({release_cursor, 2, _}, Effects),
    ok.

out_of_order_enqueue_test() ->
    Cid = {?FUNCTION_NAME, self()},
    {State1, [{monitor, _, _} | _]} = check_n(Cid, 5, 5, test_init(test)),
    {State2, Effects2} = enq(2, 1, first, State1),
    ?ASSERT_EFF({send_msg, _, {delivery, _, [{_, {_, first}}]}, _}, Effects2),
    % assert monitor was set up
    ?ASSERT_EFF({monitor, _, _}, Effects2),
    % enqueue seq num 3 and 4 before 2
    {State3, Effects3} = enq(3, 3, third, State2),
    ?assertNoEffect({send_msg, _, {delivery, _, _}, _}, Effects3),
    {State4, Effects4} = enq(4, 4, fourth, State3),
    % assert no further deliveries where made
    ?assertNoEffect({send_msg, _, {delivery, _, _}, _}, Effects4),
    {_State5, Effects5} = enq(5, 2, second, State4),
    % assert two deliveries were now made
    ?ASSERT_EFF({send_msg, _, {delivery, _, [{_, {_, second}},
                                               {_, {_, third}},
                                               {_, {_, fourth}}]}, _},
                Effects5),
    ok.

out_of_order_first_enqueue_test() ->
    Cid = {?FUNCTION_NAME, self()},
    {State1, _} = check_n(Cid, 5, 5, test_init(test)),
    {_State2, Effects2} = enq(2, 10, first, State1),
    ?ASSERT_EFF({monitor, process, _}, Effects2),
    ?assertNoEffect({send_msg, _, {delivery, _, [{_, {_, first}}]}, _},
                    Effects2),
    ok.

duplicate_enqueue_test() ->
    Cid = {<<"duplicate_enqueue_test">>, self()},
    {State1, [{monitor, _, _} | _]} = check_n(Cid, 5, 5, test_init(test)),
    {State2, Effects2} = enq(2, 1, first, State1),
    ?ASSERT_EFF({send_msg, _, {delivery, _, [{_, {_, first}}]}, _}, Effects2),
    {_State3, Effects3} = enq(3, 1, first, State2),
    ?assertNoEffect({send_msg, _, {delivery, _, [{_, {_, first}}]}, _}, Effects3),
    ok.

return_non_existent_test() ->
    Cid = {<<"cid">>, self()},
    {State0, [_, _Inactive]} = enq(1, 1, second, test_init(test)),
    % return non-existent
    {_State2, _} = apply(meta(3), make_return(Cid, [99]), State0),
    ok.

return_checked_out_test() ->
    Cid = {<<"cid">>, self()},
    {State0, [_, _]} = enq(1, 1, first, test_init(test)),
    {State1, [_Monitor,
              {send_msg, _, {delivery, _, [{MsgId, _}]}, ra_event},
              {aux, active} | _ ]} = check_auto(Cid, 2, State0),
    % returning immediately checks out the same message again
    {_, ok, [{send_msg, _, {delivery, _, [{_, _}]}, ra_event},
             {aux, active}]} =
        apply(meta(3), make_return(Cid, [MsgId]), State1),
    ok.

return_auto_checked_out_test() ->
    Cid = {<<"cid">>, self()},
    {State00, [_, _]} = enq(1, 1, first, test_init(test)),
    {State0, [_]} = enq(2, 2, second, State00),
    % it first active then inactive as the consumer took on but cannot take
    % any more
    {State1, [_Monitor,
              {send_msg, _, {delivery, _, [{MsgId, _}]}, _},
              {aux, active},
              {aux, inactive}
             ]} = check_auto(Cid, 2, State0),
    % return should include another delivery
    {_State2, _, Effects} = apply(meta(3), make_return(Cid, [MsgId]), State1),
    ?ASSERT_EFF({send_msg, _,
                 {delivery, _, [{_, {#{delivery_count := 1}, first}}]}, _},
                Effects),
    ok.


cancelled_checkout_out_test() ->
    Cid = {<<"cid">>, self()},
    {State00, [_, _]} = enq(1, 1, first, test_init(test)),
    {State0, [_]} = enq(2, 2, second, State00),
    {State1, _} = check_auto(Cid, 2, State0),
    % cancelled checkout should not return pending messages to queue
    {State2, _, _} = apply(meta(3), make_checkout(Cid, cancel, #{}), State1),
    ?assertEqual(1, maps:size(State2#state.messages)),
    ?assertEqual(0, lqueue:len(State2#state.returns)),

    {State3, {dequeue, empty}} =
        apply(meta(3), make_checkout(Cid, {dequeue, settled}, #{}), State2),
    %% settle
    {State4, ok, _} =
        apply(meta(4), make_settle(Cid, [0]), State3),

    {_State, {dequeue, {_, {_, second}}, _}, _} =
        apply(meta(5), make_checkout(Cid, {dequeue, settled}, #{}), State4),
    ok.

down_with_noproc_consumer_returns_unsettled_test() ->
    Cid = {<<"down_consumer_returns_unsettled_test">>, self()},
    {State0, [_, _]} = enq(1, 1, second, test_init(test)),
    {State1, [{monitor, process, Pid} | _]} = check(Cid, 2, State0),
    {State2, _, _} = apply(meta(3), {down, Pid, noproc}, State1),
    {_State, Effects} = check(Cid, 4, State2),
    ?ASSERT_EFF({monitor, process, _}, Effects),
    ok.

down_with_noconnection_marks_suspect_and_node_is_monitored_test() ->
    Pid = spawn(fun() -> ok end),
    Cid = {<<"down_with_noconnect">>, Pid},
    Self = self(),
    Node = node(Pid),
    {State0, Effects0} = enq(1, 1, second, test_init(test)),
    ?ASSERT_EFF({monitor, process, P}, P =:= Self, Effects0),
    {State1, Effects1} = check_auto(Cid, 2, State0),
    #consumer{credit = 0} = maps:get(Cid, State1#state.consumers),
    ?ASSERT_EFF({monitor, process, P}, P =:= Pid, Effects1),
    % monitor both enqueuer and consumer
    % because we received a noconnection we now need to monitor the node
    {State2a, _, _} = apply(meta(3), {down, Pid, noconnection}, State1),
    #consumer{credit = 1} = maps:get(Cid, State2a#state.consumers),
    %% validate consumer has credit
    {State2, _, Effects2} = apply(meta(3), {down, Self, noconnection}, State2a),
    ?ASSERT_EFF({monitor, node, _}, Effects2),
    ?assertNoEffect({demonitor, process, _}, Effects2),
    % when the node comes up we need to retry the process monitors for the
    % disconnected processes
    {_State3, _, Effects3} = apply(meta(3), {nodeup, Node}, State2),
    % try to re-monitor the suspect processes
    ?ASSERT_EFF({monitor, process, P}, P =:= Pid, Effects3),
    ?ASSERT_EFF({monitor, process, P}, P =:= Self, Effects3),
    ok.

down_with_noconnection_returns_unack_test() ->
    Pid = spawn(fun() -> ok end),
    Cid = {<<"down_with_noconnect">>, Pid},
    {State0, _} = enq(1, 1, second, test_init(test)),
    ?assertEqual(1, maps:size(State0#state.messages)),
    ?assertEqual(0, lqueue:len(State0#state.returns)),
    {State1, {_, _}} = deq(2, Cid, unsettled, State0),
    ?assertEqual(0, maps:size(State1#state.messages)),
    ?assertEqual(0, lqueue:len(State1#state.returns)),
    {State2a, _, _} = apply(meta(3), {down, Pid, noconnection}, State1),
    ?assertEqual(0, maps:size(State2a#state.messages)),
    ?assertEqual(1, lqueue:len(State2a#state.returns)),
    ok.

down_with_noproc_enqueuer_is_cleaned_up_test() ->
    State00 = test_init(test),
    Pid = spawn(fun() -> ok end),
    {State0, _, Effects0} = apply(meta(1), make_enqueue(Pid, 1, first), State00),
    ?ASSERT_EFF({monitor, process, _}, Effects0),
    {State1, _, _} = apply(meta(3), {down, Pid, noproc}, State0),
    % ensure there are no enqueuers
    ?assert(0 =:= maps:size(State1#state.enqueuers)),
    ok.

discarded_message_without_dead_letter_handler_is_removed_test() ->
    Cid = {<<"completed_consumer_yields_demonitor_effect_test">>, self()},
    {State0, [_, _]} = enq(1, 1, first, test_init(test)),
    {State1, Effects1} = check_n(Cid, 2, 10, State0),
    ?ASSERT_EFF({send_msg, _,
                 {delivery, _, [{0, {#{}, first}}]}, _},
                Effects1),
    {_State2, _, Effects2} = apply(meta(1), make_discard(Cid, [0]), State1),
    ?assertNoEffect({send_msg, _,
                     {delivery, _, [{0, {#{}, first}}]}, _},
                    Effects2),
    ok.

discarded_message_with_dead_letter_handler_emits_mod_call_effect_test() ->
    Cid = {<<"completed_consumer_yields_demonitor_effect_test">>, self()},
    State00 = init(#{name => test,
                     queue_resource => rabbit_misc:r(<<"/">>, queue, <<"test">>),
                     dead_letter_handler =>
                     {somemod, somefun, [somearg]}}),
    {State0, [_, _]} = enq(1, 1, first, State00),
    {State1, Effects1} = check_n(Cid, 2, 10, State0),
    ?ASSERT_EFF({send_msg, _,
                 {delivery, _, [{0, {#{}, first}}]}, _},
                Effects1),
    {_State2, _, Effects2} = apply(meta(1), make_discard(Cid, [0]), State1),
    % assert mod call effect with appended reason and message
    ?ASSERT_EFF({mod_call, somemod, somefun, [somearg, [{rejected, first}]]},
                Effects2),
    ok.

tick_test() ->
    Cid = {<<"c">>, self()},
    Cid2 = {<<"c2">>, self()},
    {S0, _} = enq(1, 1, <<"fst">>, test_init(?FUNCTION_NAME)),
    {S1, _} = enq(2, 2, <<"snd">>, S0),
    {S2, {MsgId, _}} = deq(3, Cid, unsettled, S1),
    {S3, {_, _}} = deq(4, Cid2, unsettled, S2),
    {S4, _, _} = apply(meta(5), make_return(Cid, [MsgId]), S3),

    [{mod_call, _, _,
      [#resource{},
       {?FUNCTION_NAME, 1, 1, 2, 1, 3, 3}]}, {aux, emit}] = tick(1, S4),
    ok.

enq_deq_snapshot_recover_test() ->
    Tag = atom_to_binary(?FUNCTION_NAME, utf8),
    Cid = {Tag, self()},
    Commands = [
                make_enqueue(self(), 1, one),
                make_enqueue(self(), 2, two),
                make_checkout(Cid, {dequeue, settled}, #{}),
                make_enqueue(self(), 3, three),
                make_enqueue(self(), 4, four),
                make_checkout(Cid, {dequeue, settled}, #{}),
                make_enqueue(self(), 5, five),
                make_checkout(Cid, {dequeue, settled}, #{})
              ],
    run_snapshot_test(?FUNCTION_NAME, Commands).

enq_deq_settle_snapshot_recover_test() ->
    Tag = atom_to_binary(?FUNCTION_NAME, utf8),
    Cid = {Tag, self()},
    % OthPid = spawn(fun () -> ok end),
    % Oth = {<<"oth">>, OthPid},
    Commands = [
                make_enqueue(self(), 1, one),
                make_enqueue(self(), 2, two),
                make_checkout(Cid, {dequeue, unsettled}, #{}),
                make_settle(Cid, [0])
              ],
    run_snapshot_test(?FUNCTION_NAME, Commands).

enq_deq_settle_snapshot_recover_2_test() ->
    Tag = atom_to_binary(?FUNCTION_NAME, utf8),
    Cid = {Tag, self()},
    OthPid = spawn(fun () -> ok end),
    Oth = {<<"oth">>, OthPid},
    Commands = [
                make_enqueue(self(), 1, one),
                make_enqueue(self(), 2, two),
                make_checkout(Cid, {dequeue, unsettled}, #{}),
                make_settle(Cid, [0]),
                make_enqueue(self(), 3, two),
                make_checkout(Cid, {dequeue, unsettled}, #{}),
                make_settle(Oth, [0])
              ],
    run_snapshot_test(?FUNCTION_NAME, Commands).

snapshot_recover_test() ->
    Tag = atom_to_binary(?FUNCTION_NAME, utf8),
    Cid = {Tag, self()},
    Commands = [
                make_checkout(Cid, {auto, 2, simple_prefetch}, #{}),
                make_enqueue(self(), 1, one),
                make_enqueue(self(), 2, two),
                make_enqueue(self(), 3, three),
                make_purge()
              ],
    run_snapshot_test(?FUNCTION_NAME, Commands).

enq_deq_return_settle_snapshot_test() ->
    Tag = atom_to_binary(?FUNCTION_NAME, utf8),
    Cid = {Tag, self()},
    Commands = [
                make_enqueue(self(), 1, one), %% to Cid
                make_checkout(Cid, {auto, 1, simple_prefetch}, #{}),
                make_return(Cid, [0]), %% should be re-delivered to Cid
                make_enqueue(self(), 2, two), %% Cid prefix_msg_count: 2
                make_settle(Cid, [1]),
                make_settle(Cid, [2])
              ],
    run_snapshot_test(?FUNCTION_NAME, Commands).

return_prefix_msg_count_test() ->
    Tag = atom_to_binary(?FUNCTION_NAME, utf8),
    Cid = {Tag, self()},
    Commands = [
                make_enqueue(self(), 1, one),
                make_checkout(Cid, {auto, 1, simple_prefetch}, #{}),
                make_checkout(Cid, cancel, #{}),
                make_enqueue(self(), 2, two) %% Cid prefix_msg_count: 2
               ],
    Indexes = lists:seq(1, length(Commands)),
    Entries = lists:zip(Indexes, Commands),
    {_State, _Effects} = run_log(test_init(?FUNCTION_NAME), Entries),
    ok.


return_settle_snapshot_test() ->
    Tag = atom_to_binary(?FUNCTION_NAME, utf8),
    Cid = {Tag, self()},
    Commands = [
                make_enqueue(self(), 1, one), %% to Cid
                make_checkout(Cid, {auto, 1, simple_prefetch}, #{}),
                make_return(Cid, [0]), %% should be re-delivered to Oth
                make_enqueue(self(), 2, two), %% Cid prefix_msg_count: 2
                make_settle(Cid, [1]),
                make_return(Cid, [2]),
                make_settle(Cid, [3]),
                make_enqueue(self(), 3, three),
                make_purge(),
                make_enqueue(self(), 4, four)
              ],
    run_snapshot_test(?FUNCTION_NAME, Commands).

enq_check_settle_snapshot_recover_test() ->
    Tag = atom_to_binary(?FUNCTION_NAME, utf8),
    Cid = {Tag, self()},
    Commands = [
                make_checkout(Cid, {auto, 2, simple_prefetch}, #{}),
                make_enqueue(self(), 1, one),
                make_enqueue(self(), 2, two),
                make_settle(Cid, [1]),
                make_settle(Cid, [0]),
                make_enqueue(self(), 3, three),
                make_settle(Cid, [2])
              ],
         % ?debugFmt("~w running commands ~w~n", [?FUNCTION_NAME, C]),
    run_snapshot_test(?FUNCTION_NAME, Commands).

enq_check_settle_snapshot_purge_test() ->
    Tag = atom_to_binary(?FUNCTION_NAME, utf8),
    Cid = {Tag, self()},
    Commands = [
                make_checkout(Cid, {auto, 2, simple_prefetch},#{}),
                make_enqueue(self(), 1, one),
                make_enqueue(self(), 2, two),
                make_settle(Cid, [1]),
                make_settle(Cid, [0]),
                make_enqueue(self(), 3, three),
                make_purge()
              ],
         % ?debugFmt("~w running commands ~w~n", [?FUNCTION_NAME, C]),
    run_snapshot_test(?FUNCTION_NAME, Commands).

enq_check_settle_duplicate_test() ->
    %% duplicate settle commands are likely
    Tag = atom_to_binary(?FUNCTION_NAME, utf8),
    Cid = {Tag, self()},
    Commands = [
                make_checkout(Cid, {auto, 2, simple_prefetch}, #{}),
                make_enqueue(self(), 1, one), %% 0
                make_enqueue(self(), 2, two), %% 0
                make_settle(Cid, [0]),
                make_settle(Cid, [1]),
                make_settle(Cid, [1]),
                make_enqueue(self(), 3, three),
                make_settle(Cid, [2])
              ],
         % ?debugFmt("~w running commands ~w~n", [?FUNCTION_NAME, C]),
    run_snapshot_test(?FUNCTION_NAME, Commands).

run_snapshot_test(Name, Commands) ->
    %% create every incremental permutation of the commands lists
    %% and run the snapshot tests against that
    [begin
         run_snapshot_test0(Name, C)
     end || C <- prefixes(Commands, 1, [])].

run_snapshot_test0(Name, Commands) ->
    Indexes = lists:seq(1, length(Commands)),
    Entries = lists:zip(Indexes, Commands),
    {State, Effects} = run_log(test_init(Name), Entries),

    [begin
         Filtered = lists:dropwhile(fun({X, _}) when X =< SnapIdx -> true;
                                       (_) -> false
                                    end, Entries),
         {S, _} = run_log(SnapState, Filtered),
         % assert log can be restored from any release cursor index
         ?assertEqual(normalize(State), normalize(S))
     end || {release_cursor, SnapIdx, SnapState} <- Effects],
    ok.

prefixes(Source, N, Acc) when N > length(Source) ->
    lists:reverse(Acc);
prefixes(Source, N, Acc) ->
    {X, _} = lists:split(N, Source),
    prefixes(Source, N+1, [X | Acc]).

delivery_query_returns_deliveries_test() ->
    Tag = atom_to_binary(?FUNCTION_NAME, utf8),
    Cid = {Tag, self()},
    Commands = [
                make_checkout(Cid, {auto, 5, simple_prefetch}, #{}),
                make_enqueue(self(), 1, one),
                make_enqueue(self(), 2, two),
                make_enqueue(self(), 3, tre),
                make_enqueue(self(), 4, for)
              ],
    Indexes = lists:seq(1, length(Commands)),
    Entries = lists:zip(Indexes, Commands),
    {State, _Effects} = run_log(test_init(help), Entries),
    % 3 deliveries are returned
    [{0, {#{}, one}}] = get_checked_out(Cid, 0, 0, State),
    [_, _, _] = get_checked_out(Cid, 1, 3, State),
    ok.

pending_enqueue_is_enqueued_on_down_test() ->
    Cid = {<<"cid">>, self()},
    Pid = self(),
    {State0, _} = enq(1, 2, first, test_init(test)),
    {State1, _, _} = apply(meta(2), {down, Pid, noproc}, State0),
    {_State2, {dequeue, {0, {_, first}}, 0}, _} =
        apply(meta(3), make_checkout(Cid, {dequeue, settled}, #{}), State1),
    ok.

duplicate_delivery_test() ->
    {State0, _} = enq(1, 1, first, test_init(test)),
    {#state{ra_indexes = RaIdxs,
            messages = Messages}, _} = enq(2, 1, first, State0),
    ?assertEqual(1, rabbit_fifo_index:size(RaIdxs)),
    ?assertEqual(1, maps:size(Messages)),
    ok.

state_enter_test() ->
    S0 = init(#{name => the_name,
                queue_resource => rabbit_misc:r(<<"/">>, queue, <<"test">>),
                become_leader_handler => {m, f, [a]}}),
    [{mod_call, m, f, [a, the_name]}] = state_enter(leader, S0),
    ok.

state_enter_monitors_and_notifications_test() ->
    Oth = spawn(fun () -> ok end),
    {State0, _} = enq(1, 1, first, test_init(test)),
    Cid = {<<"adf">>, self()},
    OthCid = {<<"oth">>, Oth},
    {State1, _} = check(Cid, 2, State0),
    {State, _} = check(OthCid, 3, State1),
    Self = self(),
    Effects = state_enter(leader, State),

    %% monitor all enqueuers and consumers
    [{monitor, process, Self},
     {monitor, process, Oth}] =
        lists:filter(fun ({monitor, process, _}) -> true;
                         (_) -> false
                     end, Effects),
    [{send_msg, Self, leader_change, ra_event},
     {send_msg, Oth, leader_change, ra_event}] =
        lists:filter(fun ({send_msg, _, leader_change, ra_event}) -> true;
                         (_) -> false
                     end, Effects),
    ?ASSERT_EFF({monitor, process, _}, Effects),
    ok.

purge_test() ->
    Cid = {<<"purge_test">>, self()},
    {State1, _} = enq(1, 1, first, test_init(test)),
    {State2, {purge, 1}, _} = apply(meta(2), make_purge(), State1),
    {State3, _} = enq(3, 2, second, State2),
    % get returns a reply value
    {_State4, {dequeue, {0, {_, second}}, _}, [{monitor, _, _}]} =
        apply(meta(4), make_checkout(Cid, {dequeue, unsettled}, #{}), State3),
    ok.

purge_with_checkout_test() ->
    Cid = {<<"purge_test">>, self()},
    {State0, _} = check_auto(Cid, 1, test_init(?FUNCTION_NAME)),
    {State1, _} = enq(2, 1, <<"first">>, State0),
    {State2, _} = enq(3, 2, <<"second">>, State1),
    %% assert message bytes are non zero
    ?assert(State2#state.msg_bytes_checkout > 0),
    ?assert(State2#state.msg_bytes_enqueue > 0),
    {State3, {purge, 1}, _} = apply(meta(2), make_purge(), State2),
    ?assert(State2#state.msg_bytes_checkout > 0),
    ?assertEqual(0, State3#state.msg_bytes_enqueue),
    ?assertEqual(1, rabbit_fifo_index:size(State3#state.ra_indexes)),
    #consumer{checked_out = Checked} = maps:get(Cid, State3#state.consumers),
    ?assertEqual(1, maps:size(Checked)),
    ok.

down_returns_checked_out_in_order_test() ->
    S0 = test_init(?FUNCTION_NAME),
    %% enqueue 100
    S1 = lists:foldl(fun (Num, FS0) ->
                         {FS, _} = enq(Num, Num, Num, FS0),
                         FS
                     end, S0, lists:seq(1, 100)),
    ?assertEqual(100, maps:size(S1#state.messages)),
    Cid = {<<"cid">>, self()},
    {S2, _} = check(Cid, 101, 1000, S1),
    #consumer{checked_out = Checked} = maps:get(Cid, S2#state.consumers),
    ?assertEqual(100, maps:size(Checked)),
    %% simulate down
    {S, _, _} = apply(meta(102), {down, self(), noproc}, S2),
    Returns = lqueue:to_list(S#state.returns),
    ?assertEqual(100, length(Returns)),
    %% validate returns are in order
    ?assertEqual(lists:sort(Returns), Returns),
    ok.

single_active_consumer_test() ->
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => rabbit_misc:r("/", queue,
                        atom_to_binary(?FUNCTION_NAME, utf8)),
                    release_cursor_interval => 0,
                    single_active_consumer_on => true}),
    ?assertEqual(single_active, State0#state.consumer_strategy),
    ?assertEqual(0, map_size(State0#state.consumers)),

    % adding some consumers
    AddConsumer = fun(CTag, State) ->
                      {NewState, _, _} = apply(
                          meta(1),
                          #checkout{spec = {once, 1, simple_prefetch},
                                    meta = #{},
                                    consumer_id = {CTag, self()}},
                          State),
                      NewState
                  end,
    State1 = lists:foldl(AddConsumer, State0, [<<"ctag1">>, <<"ctag2">>, <<"ctag3">>, <<"ctag4">>]),

    % the first registered consumer is the active one, the others are waiting
    ?assertEqual(1, map_size(State1#state.consumers)),
    ?assert(maps:is_key({<<"ctag1">>, self()}, State1#state.consumers)),
    ?assertEqual(3, length(State1#state.waiting_consumers)),
    ?assertNotEqual(false, lists:keyfind({<<"ctag2">>, self()}, 1, State1#state.waiting_consumers)),
    ?assertNotEqual(false, lists:keyfind({<<"ctag3">>, self()}, 1, State1#state.waiting_consumers)),
    ?assertNotEqual(false, lists:keyfind({<<"ctag4">>, self()}, 1, State1#state.waiting_consumers)),

    % cancelling a waiting consumer
    {State2, _, Effects1} = apply(meta(2),
                                  #checkout{spec = cancel, consumer_id = {<<"ctag3">>, self()}}, State1),
    % the active consumer should still be in place
    ?assertEqual(1, map_size(State2#state.consumers)),
    ?assert(maps:is_key({<<"ctag1">>, self()}, State2#state.consumers)),
    % the cancelled consumer has been removed from waiting consumers
    ?assertEqual(2, length(State2#state.waiting_consumers)),
    ?assertNotEqual(false, lists:keyfind({<<"ctag2">>, self()}, 1, State2#state.waiting_consumers)),
    ?assertNotEqual(false, lists:keyfind({<<"ctag4">>, self()}, 1, State2#state.waiting_consumers)),
    % there are some effects to unregister the consumer
    ?assertEqual(1, length(Effects1)),

    % cancelling the active consumer
    {State3, _, Effects2} = apply(meta(3),
                                  #checkout{spec = cancel,
                                            consumer_id = {<<"ctag1">>, self()}},
                                  State2),
    % the second registered consumer is now the active one
    ?assertEqual(1, map_size(State3#state.consumers)),
    ?assert(maps:is_key({<<"ctag2">>, self()}, State3#state.consumers)),
    % the new active consumer is no longer in the waiting list
    ?assertEqual(1, length(State3#state.waiting_consumers)),
    ?assertNotEqual(false, lists:keyfind({<<"ctag4">>, self()}, 1, State3#state.waiting_consumers)),
    % there are some effects to unregister the consumer and to update the new active one (metrics)
    ?assertEqual(2, length(Effects2)),

    % cancelling the active consumer
    {State4, _, Effects3} = apply(meta(4), #checkout{spec = cancel, consumer_id = {<<"ctag2">>, self()}}, State3),
    % the last waiting consumer became the active one
    ?assertEqual(1, map_size(State4#state.consumers)),
    ?assert(maps:is_key({<<"ctag4">>, self()}, State4#state.consumers)),
    % the waiting consumer list is now empty
    ?assertEqual(0, length(State4#state.waiting_consumers)),
    % there are some effects to unregister the consumer and to update the new active one (metrics)
    ?assertEqual(2, length(Effects3)),

    % cancelling the last consumer
    {State5, _, Effects4} = apply(meta(5), #checkout{spec = cancel, consumer_id = {<<"ctag4">>, self()}}, State4),
    % no active consumer anymore
    ?assertEqual(0, map_size(State5#state.consumers)),
    % still nothing in the waiting list
    ?assertEqual(0, length(State5#state.waiting_consumers)),
    % there is an effect to unregister the consumer + queue inactive effect
    ?assertEqual(1 + 1, length(Effects4)),

    ok.

single_active_consumer_cancel_consumer_when_channel_is_down_test() ->
    State0 = init(#{name => ?FUNCTION_NAME,
        queue_resource => rabbit_misc:r("/", queue,
            atom_to_binary(?FUNCTION_NAME, utf8)),
        release_cursor_interval => 0,
        single_active_consumer_on => true}),

    DummyFunction = fun() -> ok  end,
    Pid1 = spawn(DummyFunction),
    Pid2 = spawn(DummyFunction),
    Pid3 = spawn(DummyFunction),

    % adding some consumers
    AddConsumer = fun({CTag, ChannelId}, State) ->
        {NewState, _, _} = apply(
            #{index => 1},
            #checkout{spec = {once, 1, simple_prefetch},
                meta = #{},
                consumer_id = {CTag, ChannelId}},
            State),
        NewState
                  end,
    State1 = lists:foldl(AddConsumer, State0,
        [{<<"ctag1">>, Pid1}, {<<"ctag2">>, Pid2}, {<<"ctag3">>, Pid2}, {<<"ctag4">>, Pid3}]),

    % the channel of the active consumer goes down
    {State2, _, Effects} = apply(#{index => 2}, {down, Pid1, doesnotmatter}, State1),
    % fell back to another consumer
    ?assertEqual(1, map_size(State2#state.consumers)),
    % there are still waiting consumers
    ?assertEqual(2, length(State2#state.waiting_consumers)),
    % effects to unregister the consumer and
    % to update the new active one (metrics) are there
    ?assertEqual(2, length(Effects)),

    % the channel of the active consumer and a waiting consumer goes down
    {State3, _, Effects2} = apply(#{index => 3}, {down, Pid2, doesnotmatter}, State2),
    % fell back to another consumer
    ?assertEqual(1, map_size(State3#state.consumers)),
    % no more waiting consumer
    ?assertEqual(0, length(State3#state.waiting_consumers)),
    % effects to cancel both consumers of this channel + effect to update the new active one (metrics)
    ?assertEqual(3, length(Effects2)),

    % the last channel goes down
    {State4, _, Effects3} = apply(#{index => 4}, {down, Pid3, doesnotmatter}, State3),
    % no more consumers
    ?assertEqual(0, map_size(State4#state.consumers)),
    ?assertEqual(0, length(State4#state.waiting_consumers)),
    % there is an effect to unregister the consumer + queue inactive effect
    ?assertEqual(1 + 1, length(Effects3)),

    ok.

single_active_consumer_mark_waiting_consumers_as_suspected_when_down_noconnnection_test() ->
    State0 = init(#{name => ?FUNCTION_NAME,
        queue_resource => rabbit_misc:r("/", queue,
            atom_to_binary(?FUNCTION_NAME, utf8)),
        release_cursor_interval => 0,
        single_active_consumer_on => true}),

    Meta = #{index => 1},
    % adding some consumers
    AddConsumer = fun(CTag, State) ->
        {NewState, _, _} = apply(
            Meta,
            #checkout{spec = {once, 1, simple_prefetch},
                meta = #{},
                consumer_id = {CTag, self()}},
            State),
        NewState
                  end,
    State1 = lists:foldl(AddConsumer, State0,
                         [<<"ctag1">>, <<"ctag2">>, <<"ctag3">>, <<"ctag4">>]),

    % simulate node goes down
    {State2, _, _} = apply(#{}, {down, self(), noconnection}, State1),

    % all the waiting consumers should be suspected down
    ?assertEqual(3, length(State2#state.waiting_consumers)),
    lists:foreach(fun({_, #consumer{status = Status}}) ->
                      ?assert(Status == suspected_down)
                  end, State2#state.waiting_consumers),

    % simulate node goes back up
    {State3, _, _} = apply(#{index => 2}, {nodeup, node(self())}, State2),

    % all the waiting consumers should be un-suspected
    ?assertEqual(3, length(State3#state.waiting_consumers)),
    lists:foreach(fun({_, #consumer{status = Status}}) ->
                      ?assert(Status /= suspected_down)
                  end, State3#state.waiting_consumers),

    ok.

single_active_consumer_state_enter_leader_include_waiting_consumers_test() ->
    State0 = init(#{name => ?FUNCTION_NAME,
        queue_resource => rabbit_misc:r("/", queue,
            atom_to_binary(?FUNCTION_NAME, utf8)),
        release_cursor_interval => 0,
        single_active_consumer_on => true}),

    DummyFunction = fun() -> ok  end,
    Pid1 = spawn(DummyFunction),
    Pid2 = spawn(DummyFunction),
    Pid3 = spawn(DummyFunction),

    Meta = #{index => 1},
    % adding some consumers
    AddConsumer = fun({CTag, ChannelId}, State) ->
        {NewState, _, _} = apply(
            Meta,
            #checkout{spec = {once, 1, simple_prefetch},
                meta = #{},
                consumer_id = {CTag, ChannelId}},
            State),
        NewState
                  end,
    State1 = lists:foldl(AddConsumer, State0,
        [{<<"ctag1">>, Pid1}, {<<"ctag2">>, Pid2}, {<<"ctag3">>, Pid2}, {<<"ctag4">>, Pid3}]),

    Effects = state_enter(leader, State1),
    % 2 effects for each consumer process (channel process), 1 effect for the node
    ?assertEqual(2 * 3 + 1, length(Effects)).

single_active_consumer_state_enter_eol_include_waiting_consumers_test() ->
    State0 = init(#{name => ?FUNCTION_NAME,
        queue_resource => rabbit_misc:r("/", queue,
            atom_to_binary(?FUNCTION_NAME, utf8)),
        release_cursor_interval => 0,
        single_active_consumer_on => true}),

    DummyFunction = fun() -> ok  end,
    Pid1 = spawn(DummyFunction),
    Pid2 = spawn(DummyFunction),
    Pid3 = spawn(DummyFunction),

    Meta = #{index => 1},
    % adding some consumers
    AddConsumer = fun({CTag, ChannelId}, State) ->
        {NewState, _, _} = apply(
            Meta,
            #checkout{spec = {once, 1, simple_prefetch},
                meta = #{},
                consumer_id = {CTag, ChannelId}},
            State),
        NewState
                  end,
    State1 = lists:foldl(AddConsumer, State0,
        [{<<"ctag1">>, Pid1}, {<<"ctag2">>, Pid2}, {<<"ctag3">>, Pid2}, {<<"ctag4">>, Pid3}]),

    Effects = state_enter(eol, State1),
    % 1 effect for each consumer process (channel process)
    ?assertEqual(3, length(Effects)).

query_consumers_test() ->
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => rabbit_misc:r("/", queue,
                        atom_to_binary(?FUNCTION_NAME, utf8)),
                    release_cursor_interval => 0,
                    single_active_consumer_on => false}),

    % adding some consumers
    AddConsumer = fun(CTag, State) ->
        {NewState, _, _} = apply(
            #{index => 1},
            #checkout{spec = {once, 1, simple_prefetch},
                meta = #{},
                consumer_id = {CTag, self()}},
            State),
        NewState
                  end,
    State1 = lists:foldl(AddConsumer, State0, [<<"ctag1">>, <<"ctag2">>, <<"ctag3">>, <<"ctag4">>]),
    Consumers0 = State1#state.consumers,
    Consumer = maps:get({<<"ctag2">>, self()}, Consumers0),
    Consumers1 = maps:put({<<"ctag2">>, self()},
                          Consumer#consumer{status = suspected_down}, Consumers0),
    State2 = State1#state{consumers = Consumers1},

    ?assertEqual(4, query_consumer_count(State2)),
    Consumers2 = query_consumers(State2),
    ?assertEqual(4, maps:size(Consumers2)),
    maps:fold(fun(_Key, {Pid, Tag, _, _, Active, ActivityStatus, _, _}, _Acc) ->
        ?assertEqual(self(), Pid),
        case Tag of
            <<"ctag2">> ->
                ?assertNot(Active),
                ?assertEqual(suspected_down, ActivityStatus);
            _ ->
                ?assert(Active),
                ?assertEqual(up, ActivityStatus)
        end
              end, [], Consumers2).

query_consumers_when_single_active_consumer_is_on_test() ->
    State0 = init(#{name => ?FUNCTION_NAME,
                    queue_resource => rabbit_misc:r("/", queue,
                        atom_to_binary(?FUNCTION_NAME, utf8)),
                    release_cursor_interval => 0,
                    single_active_consumer_on => true}),
    Meta = #{index => 1},
    % adding some consumers
    AddConsumer = fun(CTag, State) ->
                    {NewState, _, _} = apply(
                        Meta,
                        #checkout{spec = {once, 1, simple_prefetch},
                                  meta = #{},
                                  consumer_id = {CTag, self()}},
                        State),
                    NewState
                  end,
    State1 = lists:foldl(AddConsumer, State0, [<<"ctag1">>, <<"ctag2">>, <<"ctag3">>, <<"ctag4">>]),

    ?assertEqual(4, query_consumer_count(State1)),
    Consumers = query_consumers(State1),
    ?assertEqual(4, maps:size(Consumers)),
    maps:fold(fun(_Key, {Pid, Tag, _, _, Active, ActivityStatus, _, _}, _Acc) ->
                  ?assertEqual(self(), Pid),
                  case Tag of
                     <<"ctag1">> ->
                         ?assert(Active),
                         ?assertEqual(single_active, ActivityStatus);
                     _ ->
                         ?assertNot(Active),
                         ?assertEqual(waiting, ActivityStatus)
                  end
              end, [], Consumers).

active_flag_updated_when_consumer_suspected_unsuspected_test() ->
    State0 = init(#{name => ?FUNCTION_NAME,
        queue_resource => rabbit_misc:r("/", queue,
            atom_to_binary(?FUNCTION_NAME, utf8)),
        release_cursor_interval => 0,
        single_active_consumer_on => false}),

    DummyFunction = fun() -> ok  end,
    Pid1 = spawn(DummyFunction),
    Pid2 = spawn(DummyFunction),
    Pid3 = spawn(DummyFunction),

    % adding some consumers
    AddConsumer = fun({CTag, ChannelId}, State) ->
        {NewState, _, _} = apply(
            #{index => 1},
            #checkout{spec = {once, 1, simple_prefetch},
                meta = #{},
                consumer_id = {CTag, ChannelId}},
            State),
        NewState
                  end,
    State1 = lists:foldl(AddConsumer, State0,
        [{<<"ctag1">>, Pid1}, {<<"ctag2">>, Pid2}, {<<"ctag3">>, Pid2}, {<<"ctag4">>, Pid3}]),

    {State2, _, Effects2} = apply(#{}, {down, Pid1, noconnection}, State1),
    % 1 effect to update the metrics of each consumer (they belong to the same node), 1 more effect to monitor the node
    ?assertEqual(4 + 1, length(Effects2)),

    {_, _, Effects3} = apply(#{index => 1}, {nodeup, node(self())}, State2),
    % for each consumer: 1 effect to update the metrics, 1 effect to monitor the consumer PID
    ?assertEqual(4 + 4, length(Effects3)).

active_flag_not_updated_when_consumer_suspected_unsuspected_and_single_active_consumer_is_on_test() ->
    State0 = init(#{name => ?FUNCTION_NAME,
        queue_resource => rabbit_misc:r("/", queue,
            atom_to_binary(?FUNCTION_NAME, utf8)),
        release_cursor_interval => 0,
        single_active_consumer_on => true}),

    DummyFunction = fun() -> ok  end,
    Pid1 = spawn(DummyFunction),
    Pid2 = spawn(DummyFunction),
    Pid3 = spawn(DummyFunction),

    % adding some consumers
    AddConsumer = fun({CTag, ChannelId}, State) ->
        {NewState, _, _} = apply(
            #{index => 1},
            #checkout{spec = {once, 1, simple_prefetch},
                meta = #{},
                consumer_id = {CTag, ChannelId}},
            State),
        NewState
                  end,
    State1 = lists:foldl(AddConsumer, State0,
        [{<<"ctag1">>, Pid1}, {<<"ctag2">>, Pid2}, {<<"ctag3">>, Pid2}, {<<"ctag4">>, Pid3}]),

    {State2, _, Effects2} = apply(#{}, {down, Pid1, noconnection}, State1),
    % only 1 effect to monitor the node
    ?assertEqual(1, length(Effects2)),

    {_, _, Effects3} = apply(#{index => 1}, {nodeup, node(self())}, State2),
    % for each consumer: 1 effect to monitor the consumer PID
    ?assertEqual(4, length(Effects3)).

meta(Idx) ->
    #{index => Idx, term => 1}.

enq(Idx, MsgSeq, Msg, State) ->
    strip_reply(
        apply(meta(Idx), make_enqueue(self(), MsgSeq, Msg), State)).

deq(Idx, Cid, Settlement, State0) ->
    {State, {dequeue, {MsgId, Msg}, _}, _} =
        apply(meta(Idx),
              make_checkout(Cid, {dequeue, Settlement}, #{}),
              State0),
    {State, {MsgId, Msg}}.

check_n(Cid, Idx, N, State) ->
    strip_reply(
      apply(meta(Idx),
            make_checkout(Cid, {auto, N, simple_prefetch}, #{}),
            State)).

check(Cid, Idx, State) ->
    strip_reply(
      apply(meta(Idx),
            make_checkout(Cid, {once, 1, simple_prefetch}, #{}),
            State)).

check_auto(Cid, Idx, State) ->
    strip_reply(
      apply(meta(Idx),
            make_checkout(Cid, {auto, 1, simple_prefetch}, #{}),
            State)).

check(Cid, Idx, Num, State) ->
    strip_reply(
      apply(meta(Idx),
            make_checkout(Cid, {auto, Num, simple_prefetch}, #{}),
            State)).

settle(Cid, Idx, MsgId, State) ->
    strip_reply(apply(meta(Idx), make_settle(Cid, [MsgId]), State)).

credit(Cid, Idx, Credit, DelCnt, Drain, State) ->
    strip_reply(apply(meta(Idx), make_credit(Cid, Credit, DelCnt, Drain),
                      State)).

strip_reply({State, _, Effects}) ->
    {State, Effects}.

run_log(InitState, Entries) ->
    lists:foldl(fun ({Idx, E}, {Acc0, Efx0}) ->
                        case apply(meta(Idx), E, Acc0) of
                            {Acc, _, Efx} when is_list(Efx) ->
                                {Acc, Efx0 ++ Efx};
                            {Acc, _, Efx}  ->
                                {Acc, Efx0 ++ [Efx]};
                            {Acc, _}  ->
                                {Acc, Efx0}
                        end
                end, {InitState, []}, Entries).


%% AUX Tests

aux_test() ->
    _ = ra_machine_ets:start_link(),
    Aux0 = init_aux(aux_test),
    MacState = init(#{name => aux_test,
                      queue_resource =>
                      rabbit_misc:r(<<"/">>, queue, <<"test">>)}),
    Log = undefined,
    {no_reply, Aux, undefined} = handle_aux(leader, cast, active, Aux0,
                                            Log, MacState),
    {no_reply, _Aux, undefined} = handle_aux(leader, cast, emit, Aux,
                                             Log, MacState),
    [X] = ets:lookup(rabbit_fifo_usage, aux_test),
    ?assert(X > 0.0),
    ok.


-endif.

