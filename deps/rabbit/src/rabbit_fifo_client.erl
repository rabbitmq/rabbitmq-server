%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @doc Provides an easy to consume API for interacting with the {@link rabbit_fifo.}
%% state machine implementation running inside a `ra' raft system.
%%
%% Handles command tracking and other non-functional concerns.
-module(rabbit_fifo_client).

-export([
         init/2,
         init/3,
         init/5,
         checkout/5,
         cancel_checkout/2,
         enqueue/2,
         enqueue/3,
         dequeue/3,
         settle/3,
         return/3,
         discard/3,
         credit/4,
         handle_ra_event/3,
         untracked_enqueue/2,
         purge/1,
         cluster_name/1,
         update_machine_state/2,
         pending_size/1,
         stat/1,
         stat/2,
         query_single_active_consumer/1
         ]).

-include_lib("rabbit_common/include/rabbit.hrl").

-define(SOFT_LIMIT, 32).
-define(TIMER_TIME, 10000).
-define(COMMAND_TIMEOUT, 30000).

-type seq() :: non_neg_integer().
-type action() :: {send_credit_reply, Available :: non_neg_integer()} |
                  {send_drained, CTagCredit ::
                   {rabbit_fifo:consumer_tag(), non_neg_integer()}}.
-type actions() :: [action()].

-type cluster_name() :: rabbit_types:r(queue).

-record(consumer, {last_msg_id :: seq() | -1,
                   ack = false :: boolean(),
                   delivery_count = 0 :: non_neg_integer()}).

-record(cfg, {cluster_name :: cluster_name(),
              servers = [] :: [ra:server_id()],
              soft_limit = ?SOFT_LIMIT :: non_neg_integer(),
              block_handler = fun() -> ok end :: fun(() -> term()),
              unblock_handler = fun() -> ok end :: fun(() -> ok),
              timeout :: non_neg_integer(),
              version = 0 :: non_neg_integer()}).

-record(state, {cfg :: #cfg{},
                leader :: undefined | ra:server_id(),
                queue_status  :: undefined | go | reject_publish,
                next_seq = 0 :: seq(),
                next_enqueue_seq = 1 :: seq(),
                %% indicates that we've exceeded the soft limit
                slow = false :: boolean(),
                unsent_commands = #{} :: #{rabbit_fifo:consumer_id() =>
                                           {[seq()], [seq()], [seq()]}},
                pending = #{} :: #{seq() =>
                                   {term(), rabbit_fifo:command()}},
                consumer_deliveries = #{} :: #{rabbit_fifo:consumer_tag() =>
                                               #consumer{}},
                timer_state :: term()
               }).

-opaque state() :: #state{}.

-export_type([
              state/0,
              actions/0
             ]).


%% @doc Create the initial state for a new rabbit_fifo sessions. A state is needed
%% to interact with a rabbit_fifo queue using @module.
%% @param ClusterName the id of the cluster to interact with
%% @param Servers The known servers of the queue. If the current leader is known
%% ensure the leader node is at the head of the list.
-spec init(cluster_name(), [ra:server_id()]) -> state().
init(ClusterName, Servers) ->
    init(ClusterName, Servers, ?SOFT_LIMIT).

%% @doc Create the initial state for a new rabbit_fifo sessions. A state is needed
%% to interact with a rabbit_fifo queue using @module.
%% @param ClusterName the id of the cluster to interact with
%% @param Servers The known servers of the queue. If the current leader is known
%% ensure the leader node is at the head of the list.
%% @param MaxPending size defining the max number of pending commands.
-spec init(cluster_name(), [ra:server_id()], non_neg_integer()) -> state().
init(ClusterName = #resource{}, Servers, SoftLimit) ->
    Timeout = application:get_env(kernel, net_ticktime, 60) + 5,
    #state{cfg = #cfg{cluster_name = ClusterName,
                      servers = Servers,
                      soft_limit = SoftLimit,
                      timeout = Timeout * 1000}}.

-spec init(cluster_name(), [ra:server_id()], non_neg_integer(), fun(() -> ok),
           fun(() -> ok)) -> state().
init(ClusterName = #resource{}, Servers, SoftLimit, BlockFun, UnblockFun) ->
    %% net ticktime is in seconds
    Timeout = application:get_env(kernel, net_ticktime, 60) + 5,
    #state{cfg = #cfg{cluster_name = ClusterName,
                      servers = Servers,
                      block_handler = BlockFun,
                      unblock_handler = UnblockFun,
                      soft_limit = SoftLimit,
                      timeout = Timeout * 1000}}.


%% @doc Enqueues a message.
%% @param Correlation an arbitrary erlang term used to correlate this
%% command when it has been applied.
%% @param Msg an arbitrary erlang term representing the message.
%% @param State the current {@module} state.
%% @returns
%% `{ok | slow, State}' if the command was successfully sent. If the return
%% tag is `slow' it means the limit is approaching and it is time to slow down
%% the sending rate.
%% {@module} assigns a sequence number to every raft command it issues. The
%% SequenceNumber can be correlated to the applied sequence numbers returned
%% by the {@link handle_ra_event/2. handle_ra_event/2} function.
-spec enqueue(Correlation :: term(), Msg :: term(), State :: state()) ->
    {ok | slow | reject_publish, state()}.
enqueue(Correlation, Msg,
        #state{queue_status = undefined,
               next_enqueue_seq = 1,
               cfg = #cfg{servers = Servers,
                          timeout = Timeout}} = State0) ->
    %% it is the first enqueue, check the version
    {_, Node} = pick_server(State0),
    case rpc:call(Node, ra_machine, version, [{machine, rabbit_fifo, #{}}]) of
        0 ->
            %% the leader is running the old version
            enqueue(Correlation, Msg, State0#state{queue_status = go});
        N when is_integer(N) ->
            %% were running the new version on the leader do sync initialisation
            %% of enqueuer session
            Reg = rabbit_fifo:make_register_enqueuer(self()),
            case ra:process_command(Servers, Reg, Timeout) of
                {ok, reject_publish, Leader} ->
                    {reject_publish, State0#state{leader = Leader,
                                                  queue_status = reject_publish}};
                {ok, ok, Leader} ->
                    enqueue(Correlation, Msg, State0#state{leader = Leader,
                                                           queue_status = go});
                {error, {no_more_servers_to_try, _Errs}} ->
                    %% if we are not able to process the register command
                    %% it is safe to reject the message as we never attempted
                    %% to send it
                    {reject_publish, State0};
                %% TODO: not convinced this can ever happen when using
                %% a list of servers
                {timeout, _} ->
                    {reject_publish, State0};
                Err ->
                    exit(Err)
            end;
        {badrpc, nodedown} ->
            {reject_publish, State0}
    end;
enqueue(_Correlation, _Msg,
        #state{queue_status = reject_publish,
               cfg = #cfg{}} = State) ->
    {reject_publish, State};
enqueue(Correlation, Msg,
        #state{slow = Slow,
               pending = Pending,
               queue_status = go,
               next_seq = Seq,
               next_enqueue_seq = EnqueueSeq,
               cfg = #cfg{soft_limit = SftLmt,
                          block_handler = BlockFun}} = State0) ->
    Server = pick_server(State0),
    % by default there is no correlation id
    Cmd = rabbit_fifo:make_enqueue(self(), EnqueueSeq, Msg),
    ok = ra:pipeline_command(Server, Cmd, Seq, low),
    Tag = case map_size(Pending) >= SftLmt of
              true -> slow;
              false -> ok
          end,
    State = State0#state{pending = Pending#{Seq => {Correlation, Cmd}},
                         next_seq = Seq + 1,
                         next_enqueue_seq = EnqueueSeq + 1,
                         slow = Tag == slow},
    case Tag of
        slow when not Slow ->
            BlockFun(),
            {slow, set_timer(State)};
        _ ->
            {ok, State}
    end.

%% @doc Enqueues a message.
%% @param Msg an arbitrary erlang term representing the message.
%% @param State the current {@module} state.
%% @returns
%% `{ok | slow, State}' if the command was successfully sent. If the return
%% tag is `slow' it means the limit is approaching and it is time to slow down
%% the sending rate.
%% {@module} assigns a sequence number to every raft command it issues. The
%% SequenceNumber can be correlated to the applied sequence numbers returned
%% by the {@link handle_ra_event/2. handle_ra_event/2} function.
%%
-spec enqueue(Msg :: term(), State :: state()) ->
    {ok | slow | reject_publish, state()}.
enqueue(Msg, State) ->
    enqueue(undefined, Msg, State).

%% @doc Dequeue a message from the queue.
%%
%% This is a synchronous call. I.e. the call will block until the command
%% has been accepted by the ra process or it times out.
%%
%% @param ConsumerTag a unique tag to identify this particular consumer.
%% @param Settlement either `settled' or `unsettled'. When `settled' no
%% further settlement needs to be done.
%% @param State The {@module} state.
%%
%% @returns `{ok, IdMsg, State}' or `{error | timeout, term()}'
-spec dequeue(rabbit_fifo:consumer_tag(),
              Settlement :: settled | unsettled, state()) ->
    {ok, non_neg_integer(), term(), non_neg_integer()}
     | {empty, state()} | {error | timeout, term()}.
dequeue(ConsumerTag, Settlement,
        #state{cfg = #cfg{timeout = Timeout,
                          cluster_name = QName}} = State0) ->
    Node = pick_server(State0),
    ConsumerId = consumer_id(ConsumerTag),
    case ra:process_command(Node,
                            rabbit_fifo:make_checkout(ConsumerId,
                                                      {dequeue, Settlement},
                                                      #{}),
                            Timeout) of
        {ok, {dequeue, empty}, Leader} ->
            {empty, State0#state{leader = Leader}};
        {ok, {dequeue, {MsgId, {MsgHeader, Msg0}}, MsgsReady}, Leader} ->
            Count = case MsgHeader of
                        #{delivery_count := C} -> C;
                       _ -> 0
                    end,
            IsDelivered = Count > 0,
            Msg = add_delivery_count_header(Msg0, Count),
            {ok, MsgsReady,
             {QName, qref(Leader), MsgId, IsDelivered, Msg},
             State0#state{leader = Leader}};
        {ok, {error, _} = Err, _Leader} ->
            Err;
        Err ->
            Err
    end.

add_delivery_count_header(#basic_message{} = Msg0, Count)
  when is_integer(Count) ->
    rabbit_basic:add_header(<<"x-delivery-count">>, long, Count, Msg0);
add_delivery_count_header(Msg, _Count) ->
    Msg.


%% @doc Settle a message. Permanently removes message from the queue.
%% @param ConsumerTag the tag uniquely identifying the consumer.
%% @param MsgIds the message ids received with the {@link rabbit_fifo:delivery/0.}
%% @param State the {@module} state
%% @returns
%% `{ok | slow, State}' if the command was successfully sent. If the return
%% tag is `slow' it means the limit is approaching and it is time to slow down
%% the sending rate.
%%
-spec settle(rabbit_fifo:consumer_tag(), [rabbit_fifo:msg_id()], state()) ->
    {state(), list()}.
settle(ConsumerTag, [_|_] = MsgIds, #state{slow = false} = State0) ->
    Node = pick_server(State0),
    Cmd = rabbit_fifo:make_settle(consumer_id(ConsumerTag), MsgIds),
    {send_command(Node, undefined, Cmd, normal, State0), []};
settle(ConsumerTag, [_|_] = MsgIds,
       #state{unsent_commands = Unsent0} = State0) ->
    ConsumerId = consumer_id(ConsumerTag),
    %% we've reached the soft limit so will stash the command to be
    %% sent once we have seen enough notifications
    Unsent = maps:update_with(ConsumerId,
                              fun ({Settles, Returns, Discards}) ->
                                      {Settles ++ MsgIds, Returns, Discards}
                              end, {MsgIds, [], []}, Unsent0),
    {State0#state{unsent_commands = Unsent}, []}.

%% @doc Return a message to the queue.
%% @param ConsumerTag the tag uniquely identifying the consumer.
%% @param MsgIds the message ids to return received
%% from {@link rabbit_fifo:delivery/0.}
%% @param State the {@module} state
%% @returns
%% `{State, list()}' if the command was successfully sent. If the return
%% tag is `slow' it means the limit is approaching and it is time to slow down
%% the sending rate.
%%
-spec return(rabbit_fifo:consumer_tag(), [rabbit_fifo:msg_id()], state()) ->
    {state(), list()}.
return(ConsumerTag, [_|_] = MsgIds, #state{slow = false} = State0) ->
    Node = pick_server(State0),
    % TODO: make rabbit_fifo return support lists of message ids
    Cmd = rabbit_fifo:make_return(consumer_id(ConsumerTag), MsgIds),
    {send_command(Node, undefined, Cmd, normal, State0), []};
return(ConsumerTag, [_|_] = MsgIds,
       #state{unsent_commands = Unsent0} = State0) ->
    ConsumerId = consumer_id(ConsumerTag),
    %% we've reached the soft limit so will stash the command to be
    %% sent once we have seen enough notifications
    Unsent = maps:update_with(ConsumerId,
                              fun ({Settles, Returns, Discards}) ->
                                      {Settles, Returns ++ MsgIds, Discards}
                              end, {[], MsgIds, []}, Unsent0),
    State1 = State0#state{unsent_commands = Unsent},
    {State1, []}.

%% @doc Discards a checked out message.
%% If the queue has a dead_letter_handler configured this will be called.
%% @param ConsumerTag the tag uniquely identifying the consumer.
%% @param MsgIds the message ids to discard
%% from {@link rabbit_fifo:delivery/0.}
%% @param State the {@module} state
%% @returns
%% `{ok | slow, State}' if the command was successfully sent. If the return
%% tag is `slow' it means the limit is approaching and it is time to slow down
%% the sending rate.
-spec discard(rabbit_fifo:consumer_tag(), [rabbit_fifo:msg_id()], state()) ->
    {state(), list()}.
discard(ConsumerTag, [_|_] = MsgIds, #state{slow = false} = State0) ->
    Node = pick_server(State0),
    Cmd = rabbit_fifo:make_discard(consumer_id(ConsumerTag), MsgIds),
    {send_command(Node, undefined, Cmd, normal, State0), []};
discard(ConsumerTag, [_|_] = MsgIds,
        #state{unsent_commands = Unsent0} = State0) ->
    ConsumerId = consumer_id(ConsumerTag),
    %% we've reached the soft limit so will stash the command to be
    %% sent once we have seen enough notifications
    Unsent = maps:update_with(ConsumerId,
                              fun ({Settles, Returns, Discards}) ->
                                      {Settles, Returns, Discards ++ MsgIds}
                              end, {[], [], MsgIds}, Unsent0),
    {State0#state{unsent_commands = Unsent}, []}.

%% @doc Register with the rabbit_fifo queue to "checkout" messages as they
%% become available.
%%
%% This is a synchronous call. I.e. the call will block until the command
%% has been accepted by the ra process or it times out.
%%
%% @param ConsumerTag a unique tag to identify this particular consumer.
%% @param NumUnsettled the maximum number of in-flight messages. Once this
%% number of messages has been received but not settled no further messages
%% will be delivered to the consumer.
%% @param CreditMode The credit mode to use for the checkout.
%%     simple_prefetch: credit is auto topped up as deliveries are settled
%%     credited: credit is only increased by sending credit to the queue
%% @param State The {@module} state.
%%
%% @returns `{ok, State}' or `{error | timeout, term()}'
-spec checkout(rabbit_fifo:consumer_tag(),
               NumUnsettled :: non_neg_integer(),
               CreditMode :: rabbit_fifo:credit_mode(),
               Meta :: rabbit_fifo:consumer_meta(),
               state()) -> {ok, state()} | {error | timeout, term()}.
checkout(ConsumerTag, NumUnsettled, CreditMode, Meta,
         #state{consumer_deliveries = CDels0} = State0) ->
    Servers = sorted_servers(State0),
    ConsumerId = {ConsumerTag, self()},
    Cmd = rabbit_fifo:make_checkout(ConsumerId,
                                    {auto, NumUnsettled, CreditMode},
                                    Meta),
    %% ???
    Ack = maps:get(ack, Meta, true),

    SDels = maps:update_with(ConsumerTag,
                             fun (V) ->
                                     V#consumer{ack = Ack}
                             end,
                             #consumer{last_msg_id = -1,
                                       ack = Ack}, CDels0),
    try_process_command(Servers, Cmd, State0#state{consumer_deliveries = SDels}).


-spec query_single_active_consumer(state()) ->
    {ok, term()} | {error, term()} | {timeout, term()}.
query_single_active_consumer(#state{leader = undefined}) ->
    {error, leader_not_known};
query_single_active_consumer(#state{leader = Leader}) ->
    case ra:local_query(Leader, fun rabbit_fifo:query_single_active_consumer/1,
                        ?COMMAND_TIMEOUT) of
        {ok, {_, Reply}, _} ->
            {ok, Reply};
        Err ->
            Err
    end.

%% @doc Provide credit to the queue
%%
%% This only has an effect if the consumer uses credit mode: credited
%% @param ConsumerTag a unique tag to identify this particular consumer.
%% @param Credit the amount of credit to provide to theq queue
%% @param Drain tells the queue to use up any credit that cannot be immediately
%% fulfilled. (i.e. there are not enough messages on queue to use up all the
%% provided credit).
-spec credit(rabbit_fifo:consumer_tag(),
             Credit :: non_neg_integer(),
             Drain :: boolean(),
             state()) ->
          {state(), actions()}.
credit(ConsumerTag, Credit, Drain,
       #state{consumer_deliveries = CDels} = State0) ->
    ConsumerId = consumer_id(ConsumerTag),
    %% the last received msgid provides us with the delivery count if we
    %% add one as it is 0 indexed
    C = maps:get(ConsumerTag, CDels, #consumer{last_msg_id = -1}),
    Node = pick_server(State0),
    Cmd = rabbit_fifo:make_credit(ConsumerId, Credit,
                                  C#consumer.last_msg_id + 1, Drain),
    {send_command(Node, undefined, Cmd, normal, State0), []}.

%% @doc Cancels a checkout with the rabbit_fifo queue  for the consumer tag
%%
%% This is a synchronous call. I.e. the call will block until the command
%% has been accepted by the ra process or it times out.
%%
%% @param ConsumerTag a unique tag to identify this particular consumer.
%% @param State The {@module} state.
%%
%% @returns `{ok, State}' or `{error | timeout, term()}'
-spec cancel_checkout(rabbit_fifo:consumer_tag(), state()) ->
    {ok, state()} | {error | timeout, term()}.
cancel_checkout(ConsumerTag, #state{consumer_deliveries = CDels} = State0) ->
    Servers = sorted_servers(State0),
    ConsumerId = {ConsumerTag, self()},
    Cmd = rabbit_fifo:make_checkout(ConsumerId, cancel, #{}),
    State = State0#state{consumer_deliveries = maps:remove(ConsumerTag, CDels)},
    try_process_command(Servers, Cmd, State).

%% @doc Purges all the messages from a rabbit_fifo queue and returns the number
%% of messages purged.
-spec purge(ra:server_id()) -> {ok, non_neg_integer()} | {error | timeout, term()}.
purge(Server) ->
    case ra:process_command(Server, rabbit_fifo:make_purge(), ?COMMAND_TIMEOUT) of
        {ok, {purge, Reply}, _} ->
            {ok, Reply};
        Err ->
            Err
    end.

-spec pending_size(state()) -> non_neg_integer().
pending_size(#state{pending = Pend}) ->
    maps:size(Pend).

-spec stat(ra:server_id()) ->
    {ok, non_neg_integer(), non_neg_integer()}
    | {error | timeout, term()}.
stat(Leader) ->
    %% short timeout as we don't want to spend too long if it is going to
    %% fail anyway
    stat(Leader, 250).

-spec stat(ra:server_id(), non_neg_integer()) ->
    {ok, non_neg_integer(), non_neg_integer()}
    | {error | timeout, term()}.
stat(Leader, Timeout) ->
    %% short timeout as we don't want to spend too long if it is going to
    %% fail anyway
    case ra:local_query(Leader, fun rabbit_fifo:query_stat/1, Timeout) of
      {ok, {_, {R, C}}, _} -> {ok, R, C};
      {error, _} = Error   -> Error;
      {timeout, _} = Error -> Error
    end.

%% @doc returns the cluster name
-spec cluster_name(state()) -> cluster_name().
cluster_name(#state{cfg = #cfg{cluster_name = ClusterName}}) ->
    ClusterName.

update_machine_state(Server, Conf) ->
    case ra:process_command(Server, rabbit_fifo:make_update_config(Conf), ?COMMAND_TIMEOUT) of
        {ok, ok, _} ->
            ok;
        Err ->
            Err
    end.

%% @doc Handles incoming `ra_events'. Events carry both internal "bookeeping"
%% events emitted by the `ra' leader as well as `rabbit_fifo' emitted events such
%% as message deliveries. All ra events need to be handled by {@module}
%% to ensure bookeeping, resends and flow control is correctly handled.
%%
%% If the `ra_event' contains a `rabbit_fifo' generated message it will be returned
%% for further processing.
%%
%% Example:
%%
%% ```
%%  receive
%%     {ra_event, From, Evt} ->
%%         case rabbit_fifo_client:handle_ra_event(From, Evt, State0) of
%%             {internal, _Seq, State} -> State;
%%             {{delivery, _ConsumerTag, Msgs}, State} ->
%%                  handle_messages(Msgs),
%%                  ...
%%         end
%%  end
%% '''
%%
%% @param From the {@link ra:server_id().} of the sending process.
%% @param Event the body of the `ra_event'.
%% @param State the current {@module} state.
%%
%% @returns
%% `{internal, AppliedCorrelations, State}' if the event contained an internally
%% handled event such as a notification and a correlation was included with
%% the command (e.g. in a call to `enqueue/3' the correlation terms are returned
%% here).
%%
%% `{RaFifoEvent, State}' if the event contained a client message generated by
%% the `rabbit_fifo' state machine such as a delivery.
%%
%% The type of `rabbit_fifo' client messages that can be received are:
%%
%% `{delivery, ConsumerTag, [{MsgId, {MsgHeader, Msg}}]}'
%%
%% <li>`ConsumerTag' the binary tag passed to {@link checkout/3.}</li>
%% <li>`MsgId' is a consumer scoped monotonically incrementing id that can be
%% used to {@link settle/3.} (roughly: AMQP 0.9.1 ack) message once finished
%% with them.</li>
-spec handle_ra_event(ra:server_id(), ra_server_proc:ra_event_body(), state()) ->
    {internal, Correlators :: [term()], actions(), state()} |
    {rabbit_fifo:client_msg(), state()} | eol.
handle_ra_event(From, {applied, Seqs},
                #state{cfg = #cfg{cluster_name = QRef,
                                  soft_limit = SftLmt,
                                  unblock_handler = UnblockFun}} = State0) ->

    {Corrs, Actions0, State1} = lists:foldl(fun seq_applied/2,
                                           {[], [], State0#state{leader = From}},
                                           Seqs),
    Actions = case Corrs of
                  [] ->
                      lists:reverse(Actions0);
                  _ ->
                      [{settled, QRef, Corrs}
                       | lists:reverse(Actions0)]
              end,
    case maps:size(State1#state.pending) < SftLmt of
        true when State1#state.slow == true ->
            % we have exited soft limit state
            % send any unsent commands and cancel the time as
            % TODO: really the timer should only be cancelled when the channel
            % exits flow state (which depends on the state of all queues the
            % channel is interacting with)
            % but the fact the queue has just applied suggests
            % it's ok to cancel here anyway
            State2 = cancel_timer(State1#state{slow = false,
                                               unsent_commands = #{}}),
            % build up a list of commands to issue
            Commands = maps:fold(
                         fun (Cid, {Settled, Returns, Discards}, Acc) ->
                                 add_command(Cid, settle, Settled,
                                             add_command(Cid, return, Returns,
                                                         add_command(Cid, discard,
                                                                     Discards, Acc)))
                         end, [], State1#state.unsent_commands),
            Node = pick_server(State2),
            %% send all the settlements and returns
            State = lists:foldl(fun (C, S0) ->
                                        send_command(Node, undefined, C,
                                                     normal, S0)
                                end, State2, Commands),
            UnblockFun(),
            {ok, State, Actions};
        _ ->
            {ok, State1, Actions}
    end;
handle_ra_event(From, {machine, {delivery, _ConsumerTag, _} = Del}, State0) ->
    handle_delivery(From, Del, State0);
handle_ra_event(_, {machine, {queue_status, Status}},
                #state{} = State) ->
    %% just set the queue status
    {ok, State#state{queue_status = Status}, []};
handle_ra_event(Leader, {machine, leader_change},
                #state{leader = OldLeader} = State0) ->
    %% we need to update leader
    %% and resend any pending commands
    rabbit_log:debug("~s: Detected QQ leader change from ~w to ~w",
                     [?MODULE, OldLeader, Leader]),
    State = resend_all_pending(State0#state{leader = Leader}),
    {ok, State, []};
handle_ra_event(_From, {rejected, {not_leader, Leader, _Seq}},
                #state{leader = Leader} = State) ->
    {ok, State, []};
handle_ra_event(_From, {rejected, {not_leader, Leader, _Seq}},
                #state{leader = OldLeader} = State0) ->
    rabbit_log:debug("~s: Detected QQ leader change (rejection) from ~w to ~w",
                     [?MODULE, OldLeader, Leader]),
    State = resend_all_pending(State0#state{leader = Leader}),
    {ok, cancel_timer(State), []};
handle_ra_event(_From, {rejected, {not_leader, _UndefinedMaybe, _Seq}}, State0) ->
    % TODO: how should these be handled? re-sent on timer or try random
    {ok, State0, []};
handle_ra_event(_, timeout, #state{cfg = #cfg{servers = Servers}} = State0) ->
    case find_leader(Servers) of
        undefined ->
            %% still no leader, set the timer again
            {ok, set_timer(State0), []};
        Leader ->
            State = resend_all_pending(State0#state{leader = Leader}),
            {ok, State, []}
    end;
handle_ra_event(_Leader, {machine, eol}, _State0) ->
    eol.

%% @doc Attempts to enqueue a message using cast semantics. This provides no
%% guarantees or retries if the message fails to achieve consensus or if the
%% servers sent to happens not to be available. If the message is sent to a
%% follower it will attempt the deliver it to the leader, if known. Else it will
%% drop the messages.
%%
%% NB: only use this for non-critical enqueues where a full rabbit_fifo_client state
%% cannot be maintained.
%%
%% @param CusterId  the cluster id.
%% @param Servers the known servers in the cluster.
%% @param Msg the message to enqueue.
%%
%% @returns `ok'
-spec untracked_enqueue([ra:server_id()], term()) ->
    ok.
untracked_enqueue([Node | _], Msg) ->
    Cmd = rabbit_fifo:make_enqueue(undefined, undefined, Msg),
    ok = ra:pipeline_command(Node, Cmd),
    ok.

%% Internal

try_process_command([Server | Rem], Cmd,
                    #state{cfg = #cfg{timeout = Timeout}} = State) ->
    case ra:process_command(Server, Cmd, Timeout) of
        {ok, _, Leader} ->
            {ok, State#state{leader = Leader}};
        Err when length(Rem) =:= 0 ->
            Err;
        _ ->
            try_process_command(Rem, Cmd, State)
    end.

seq_applied({Seq, Response},
            {Corrs, Actions0, #state{} = State0}) ->
    %% sequences aren't guaranteed to be applied in order as enqueues are
    %% low priority commands and may be overtaken by others with a normal priority.
    {Actions, State} = maybe_add_action(Response, Actions0, State0),
    case maps:take(Seq, State#state.pending) of
        {{undefined, _}, Pending} ->
            {Corrs, Actions, State#state{pending = Pending}};
        {{Corr, _}, Pending}
          when Response /= not_enqueued ->
            {[Corr | Corrs], Actions, State#state{pending = Pending}};
        _ ->
            {Corrs, Actions, State#state{}}
    end;
seq_applied(_Seq, Acc) ->
    Acc.

maybe_add_action(ok, Acc, State) ->
    {Acc, State};
maybe_add_action(not_enqueued, Acc, State) ->
    {Acc, State};
maybe_add_action({multi, Actions}, Acc0, State0) ->
    lists:foldl(fun (Act, {Acc, State}) ->
                        maybe_add_action(Act, Acc, State)
                end, {Acc0, State0}, Actions);
maybe_add_action({send_drained, {Tag, Credit}} = Action, Acc,
                 #state{consumer_deliveries = CDels} = State) ->
    %% add credit to consumer delivery_count
    C = maps:get(Tag, CDels),
    {[Action | Acc],
     State#state{consumer_deliveries =
                 update_consumer(Tag, C#consumer.last_msg_id,
                                 Credit, C, CDels)}};
maybe_add_action(Action, Acc, State) ->
    %% anything else is assumed to be an action
    {[Action | Acc], State}.

% resends a command with a new sequence number
resend(OldSeq, #state{pending = Pending0, leader = Leader} = State) ->
    case maps:take(OldSeq, Pending0) of
        {{Corr, Cmd}, Pending} ->
            %% resends aren't subject to flow control here
            resend_command(Leader, Corr, Cmd, State#state{pending = Pending});
        error ->
            State
    end.

resend_all_pending(#state{pending = Pend} = State) ->
    Seqs = lists:sort(maps:keys(Pend)),
    lists:foldl(fun resend/2, State, Seqs).

maybe_auto_ack(true, Deliver, State0) ->
    %% manual ack is enabled
    {ok, State0, [Deliver]};
maybe_auto_ack(false, {deliver, Tag, _Ack, Msgs} = Deliver, State0) ->
    %% we have to auto ack these deliveries
    MsgIds = [I || {_, _, I, _, _} <- Msgs],
    {State, Actions} = settle(Tag, MsgIds, State0),
    {ok, State, [Deliver] ++ Actions}.

handle_delivery(Leader, {delivery, Tag, [{FstId, _} | _] = IdMsgs},
                #state{cfg = #cfg{cluster_name = QName},
                       consumer_deliveries = CDels0} = State0)
  when is_map_key(Tag, CDels0) ->
    QRef = qref(Leader),
    {LastId, _} = lists:last(IdMsgs),
    Consumer = #consumer{ack = Ack} = maps:get(Tag, CDels0),
    %% format as a deliver action
    Del = {deliver, Tag, Ack, transform_msgs(QName, QRef, IdMsgs)},
    %% TODO: remove potential default allocation
    case Consumer of
        #consumer{last_msg_id = Prev} = C
          when FstId =:= Prev+1 ->
            maybe_auto_ack(Ack, Del,
                           State0#state{consumer_deliveries =
                                        update_consumer(Tag, LastId,
                                                        length(IdMsgs), C,
                                                        CDels0)});
        #consumer{last_msg_id = Prev} = C
          when FstId > Prev+1 ->
            NumMissing = FstId - Prev + 1,
            %% there may actually be fewer missing messages returned than expected
            %% This can happen when a node the channel is on gets disconnected
            %% from the node the leader is on and then reconnected afterwards.
            %% When the node is disconnected the leader will return all checked
            %% out messages to the main queue to ensure they don't get stuck in
            %% case the node never comes back.
            case get_missing_deliveries(Leader, Prev+1, FstId-1, Tag) of
                {protocol_error, _, _, _} = Err ->
                    Err;
                Missing ->
                    XDel = {deliver, Tag, Ack, transform_msgs(QName, QRef,
                                                              Missing ++ IdMsgs)},
                    maybe_auto_ack(Ack, XDel,
                                   State0#state{consumer_deliveries =
                                                    update_consumer(Tag, LastId,
                                                                    length(IdMsgs) + NumMissing,
                                                                    C, CDels0)})
            end;
        #consumer{last_msg_id = Prev}
          when FstId =< Prev ->
            case lists:dropwhile(fun({Id, _}) -> Id =< Prev end, IdMsgs) of
                [] ->
                    {ok, State0, []};
                IdMsgs2 ->
                    handle_delivery(Leader, {delivery, Tag, IdMsgs2}, State0)
            end;
        C when FstId =:= 0 ->
            % the very first delivery
            maybe_auto_ack(Ack, Del,
                           State0#state{consumer_deliveries =
                                        update_consumer(Tag, LastId,
                                                        length(IdMsgs),
                                                        C#consumer{last_msg_id = LastId},
                                                        CDels0)})
    end;
handle_delivery(_Leader, {delivery, Tag, [_ | _] = IdMsgs},
                #state{consumer_deliveries = CDels0} = State0)
  when not is_map_key(Tag, CDels0) ->
    %% Note:
    %% https://github.com/rabbitmq/rabbitmq-server/issues/3729
    %% If the consumer is no longer in the deliveries map,
    %% we should return all messages.
    MsgIntIds = [Id || {Id, _} <- IdMsgs],
    {State1, Deliveries} = return(Tag, MsgIntIds, State0),
    {ok, State1, Deliveries}.

transform_msgs(QName, QRef, Msgs) ->
    lists:map(
      fun({MsgId, {MsgHeader, Msg0}}) ->
              {Msg, Redelivered} = case MsgHeader of
                                       #{delivery_count := C} ->
                                           {add_delivery_count_header(Msg0, C), true};
                                       _ ->
                                           {Msg0, false}
                                   end,
              {QName, QRef, MsgId, Redelivered, Msg}
      end, Msgs).

update_consumer(Tag, LastId, DelCntIncr,
                #consumer{delivery_count = D} = C, Consumers) ->
    maps:put(Tag,
             C#consumer{last_msg_id = LastId,
                        delivery_count = D + DelCntIncr},
             Consumers).


get_missing_deliveries(Leader, From, To, ConsumerTag) ->
    ConsumerId = consumer_id(ConsumerTag),
    % ?INFO("get_missing_deliveries for ~w from ~b to ~b",
    %       [ConsumerId, From, To]),
    Query = fun (State) ->
                    rabbit_fifo:get_checked_out(ConsumerId, From, To, State)
            end,
    case ra:local_query(Leader, Query, ?COMMAND_TIMEOUT) of
        {ok, {_, Missing}, _} ->
            Missing;
        {error, Error} ->
            {protocol_error, internal_error, "Cannot query missing deliveries from ~p: ~p",
             [Leader, Error]};
        {timeout, _} ->
            {protocol_error, internal_error, "Cannot query missing deliveries from ~p: timeout",
             [Leader]}
    end.

pick_server(#state{leader = undefined,
                   cfg = #cfg{servers = [N | _]}}) ->
    %% TODO: pick random rather that first?
    N;
pick_server(#state{leader = Leader}) ->
    Leader.

% servers sorted by last known leader
sorted_servers(#state{leader = undefined,
                      cfg = #cfg{servers = Servers}}) ->
    Servers;
sorted_servers(#state{leader = Leader,
                      cfg = #cfg{servers = Servers}}) ->
    [Leader | lists:delete(Leader, Servers)].

consumer_id(ConsumerTag) ->
    {ConsumerTag, self()}.

send_command(Server, Correlation, Command, _Priority,
             #state{pending = Pending,
                    next_seq = Seq,
                    cfg = #cfg{soft_limit = SftLmt}} = State)
  when element(1, Command) == return ->
    %% returns are sent to the aux machine for pre-evaluation
    ok = ra:cast_aux_command(Server, {Command, Seq, self()}),
    Tag = case map_size(Pending) >= SftLmt of
              true -> slow;
              false -> ok
          end,
    State#state{pending = Pending#{Seq => {Correlation, Command}},
                next_seq = Seq + 1,
                slow = Tag == slow};
send_command(Server, Correlation, Command, Priority,
             #state{pending = Pending,
                    next_seq = Seq,
                    cfg = #cfg{soft_limit = SftLmt}} = State) ->
    ok = ra:pipeline_command(Server, Command, Seq, Priority),
    Tag = case map_size(Pending) >= SftLmt of
              true -> slow;
              false -> ok
          end,
    State#state{pending = Pending#{Seq => {Correlation, Command}},
                next_seq = Seq + 1,
                slow = Tag == slow}.

resend_command(Node, Correlation, Command,
               #state{pending = Pending,
                      next_seq = Seq} = State) ->
    ok = ra:pipeline_command(Node, Command, Seq),
    State#state{pending = Pending#{Seq => {Correlation, Command}},
                next_seq = Seq + 1}.

add_command(_, _, [], Acc) ->
    Acc;
add_command(Cid, settle, MsgIds, Acc) ->
    [rabbit_fifo:make_settle(Cid, MsgIds) | Acc];
add_command(Cid, return, MsgIds, Acc) ->
    [rabbit_fifo:make_return(Cid, MsgIds) | Acc];
add_command(Cid, discard, MsgIds, Acc) ->
    [rabbit_fifo:make_discard(Cid, MsgIds) | Acc].

set_timer(#state{leader = Leader0,
                 cfg = #cfg{servers = [Server | _],
                            cluster_name = QName}} = State) ->
    Leader = case Leader0 of
                 undefined -> Server;
                 _ ->
                     Leader0
             end,
    Ref = erlang:send_after(?TIMER_TIME, self(),
                            {'$gen_cast',
                             {queue_event, QName, {Leader, timeout}}}),
    State#state{timer_state = Ref}.

cancel_timer(#state{timer_state = undefined} = State) ->
    State;
cancel_timer(#state{timer_state = Ref} = State) ->
    erlang:cancel_timer(Ref, [{async, true}, {info, false}]),
    State#state{timer_state = undefined}.

find_leader([]) ->
    undefined;
find_leader([Server | Servers]) ->
    case ra:members(Server, 500) of
        {ok, _, Leader} -> Leader;
        _ ->
            find_leader(Servers)
    end.

qref({Ref, _}) -> Ref;
qref(Ref) -> Ref.
