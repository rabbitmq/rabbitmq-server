%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% @doc Provides an easy to consume API for interacting with the {@link rabbit_fifo.}
%% state machine implementation running inside a `ra' raft system.
%%
%% Handles command tracking and other non-functional concerns.
-module(rabbit_fifo_client).

-export([
         init/1,
         init/2,
         close/1,
         checkout/4,
         cancel_checkout/3,
         enqueue/3,
         enqueue/4,
         dequeue/4,
         settle/3,
         return/3,
         discard/3,
         modify/6,
         credit_v1/4,
         credit/5,
         handle_ra_event/4,
         untracked_enqueue/2,
         purge/1,
         update_machine_state/2,
         pending_size/1,
         num_cached_segments/1,
         stat/1,
         stat/2,
         query_single_active_consumer/1,
         cluster_name/1
         ]).

-define(SOFT_LIMIT, 32).
-define(TIMER_TIME, 10000).
-define(COMMAND_TIMEOUT, 30000).
-define(UNLIMITED_PREFETCH_COUNT, 2000). %% something large for ra
%% controls the timer for closing cached segments
-define(CACHE_SEG_TIMEOUT, 5000).

-type seq() :: non_neg_integer().
-type milliseconds() :: non_neg_integer().


-record(consumer, {key :: rabbit_fifo:consumer_key(),
                   % status = up :: up | cancelled,
                   last_msg_id :: seq() | -1 | undefined,
                   ack = false :: boolean(),
                   %% Remove this field when feature flag rabbitmq_4.0.0 becomes required.
                   delivery_count :: {credit_api_v1, rabbit_queue_type:delivery_count()} |
                                     credit_api_v2
                  }).

-record(cfg, {servers = [] :: [ra:server_id()],
              soft_limit = ?SOFT_LIMIT :: non_neg_integer(),
              timeout :: non_neg_integer()
             }).

-record(state, {cfg :: #cfg{},
                leader :: undefined | ra:server_id(),
                queue_status  :: undefined | go | reject_publish,
                next_seq = 0 :: seq(),
                next_enqueue_seq = 1 :: seq(),
                %% indicates that we've exceeded the soft limit
                slow = false :: boolean(),
                unsent_commands = #{} :: #{rabbit_fifo:consumer_key() =>
                                           {[seq()], [seq()], [seq()]}},
                pending = #{} :: #{seq() =>
                                   {term(), rabbit_fifo:command()}},
                consumers = #{} :: #{rabbit_types:ctag() => #consumer{}},
                timer_state :: term(),
                cached_segments :: undefined |
                                  {undefined | reference(),
                                   LastSeenMs :: milliseconds(),
                                   ra_flru:state()}
               }).

-opaque state() :: #state{}.

-export_type([state/0]).

%% @doc Create the initial state for a new rabbit_fifo sessions. A state is needed
%% to interact with a rabbit_fifo queue using @module.
%% @param Servers The known servers of the queue. If the current leader is known
%% ensure the leader node is at the head of the list.
-spec init([ra:server_id()]) -> state().
init(Servers) ->
    init(Servers, ?SOFT_LIMIT).

%% @doc Create the initial state for a new rabbit_fifo sessions. A state is needed
%% to interact with a rabbit_fifo queue using @module.
%% @param Servers The known servers of the queue. If the current leader is known
%% ensure the leader node is at the head of the list.
%% @param MaxPending size defining the max number of pending commands.
-spec init([ra:server_id()], non_neg_integer()) -> state().
init(Servers, SoftLimit) ->
    Timeout = application:get_env(kernel, net_ticktime, 60) + 5,
    #state{cfg = #cfg{servers = Servers,
                      soft_limit = SoftLimit,
                      timeout = Timeout * 1000}}.

%% @doc Enqueues a message.
%% @param QueueName Name of the queue.
%% @param Correlation an arbitrary erlang term used to correlate this
%% command when it has been applied.
%% @param Msg an arbitrary erlang term representing the message.
%% @param State the current {@module} state.
%% @returns
%% `{ok, State, Actions}' if the command was successfully sent
%% {@module} assigns a sequence number to every raft command it issues. The
%% SequenceNumber can be correlated to the applied sequence numbers returned
%% by the {@link handle_ra_event/2. handle_ra_event/2} function.
-spec enqueue(rabbit_amqqueue:name(), Correlation :: term(),
              Msg :: term(), State :: state()) ->
    {ok, state(), rabbit_queue_type:actions()} | {reject_publish, state()}.
enqueue(QName, Correlation, Msg,
        #state{queue_status = undefined,
               next_enqueue_seq = 1,
               cfg = #cfg{servers = Servers,
                          timeout = Timeout}} = State0) ->
    %% the first publish, register and enqueuer for this process.
    %% TODO: we _only_ need to pre-register an enqueuer to discover if the
    %% queue overflow is `reject_publish` and the queue can accept new messages
    %% if the queue does not have `reject_publish` set we can skip this step
    Reg = rabbit_fifo:make_register_enqueuer(self()),
    case ra:process_command(Servers, Reg, Timeout) of
        {ok, reject_publish, Leader} ->
            {reject_publish, State0#state{leader = Leader,
                                          queue_status = reject_publish}};
        {ok, ok, Leader} ->
            enqueue(QName, Correlation, Msg, State0#state{leader = Leader,
                                                          queue_status = go});
        {error, {no_more_servers_to_try, _Errs}} ->
            %% if we are not able to process the register command
            %% it is safe to reject the message as we never attempted
            %% to send it
            {reject_publish, State0};
        {error, {shutdown, delete}} ->
            rabbit_log:debug("~ts: QQ ~ts tried to register enqueuer during delete shutdown",
                             [?MODULE, rabbit_misc:rs(QName)]),
            {reject_publish, State0};
        {timeout, _} ->
            {reject_publish, State0};
        Err ->
            rabbit_log:debug("~ts: QQ ~ts error when registering enqueuer ~p",
                             [?MODULE, rabbit_misc:rs(QName), Err]),
            exit(Err)
    end;
enqueue(_QName, _Correlation, _Msg,
        #state{queue_status = reject_publish,
               cfg = #cfg{}} = State) ->
    {reject_publish, State};
enqueue(QName, Correlation, Msg,
        #state{slow = WasSlow,
               pending = Pending,
               queue_status = go,
               next_seq = Seq,
               next_enqueue_seq = EnqueueSeq,
               cfg = #cfg{soft_limit = SftLmt}} = State0) ->
    ServerId = pick_server(State0),
    % by default there is no correlation id
    Cmd = rabbit_fifo:make_enqueue(self(), EnqueueSeq, Msg),
    ok = ra:pipeline_command(ServerId, Cmd, Seq, low),
    IsSlow = map_size(Pending) >= SftLmt,
    State = State0#state{pending = Pending#{Seq => {Correlation, Cmd}},
                         next_seq = Seq + 1,
                         next_enqueue_seq = EnqueueSeq + 1,
                         slow = IsSlow},
    if IsSlow andalso not WasSlow ->
           {ok, set_timer(QName, State), [{block, cluster_name(State)}]};
       true ->
           {ok, State, []}
    end.

%% @doc Enqueues a message.
%% @param QueueName Name of the queue.
%% @param Msg an arbitrary erlang term representing the message.
%% @param State the current {@module} state.
%% @return's
%% `{ok, State, Actions}' if the command was successfully sent.
%% {@module} assigns a sequence number to every raft command it issues. The
%% SequenceNumber can be correlated to the applied sequence numbers returned
%% by the {@link handle_ra_event/2. handle_ra_event/2} function.
%%
-spec enqueue(rabbit_amqqueue:name(), Msg :: term(), State :: state()) ->
    {ok, state(), rabbit_queue_type:actions()} | {reject_publish, state()}.
enqueue(QName, Msg, State) ->
    enqueue(QName, undefined, Msg, State).

%% @doc Dequeue a message from the queue.
%%
%% This is a synchronous call. I.e. the call will block until the command
%% has been accepted by the ra process or it times out.
%%
%% @param QueueName Name of the queue.
%% @param ConsumerTag a unique tag to identify this particular consumer.
%% @param Settlement either `settled' or `unsettled'. When `settled' no
%% further settlement needs to be done.
%% @param State The {@module} state.
%%
%% @returns `{ok, IdMsg, State}' or `{error | timeout, term()}'
-spec dequeue(rabbit_amqqueue:name(), rabbit_types:ctag(),
              Settlement :: settled | unsettled, state()) ->
    {ok, non_neg_integer(), term(), non_neg_integer()}
     | {empty, state()} | {error | timeout, term()}.
dequeue(QueueName, ConsumerTag, Settlement,
        #state{cfg = #cfg{timeout = Timeout}} = State0) ->
    ServerId = pick_server(State0),
    %% dequeue never really needs to assign a consumer key so we just use
    %% the old ConsumerId format here
    ConsumerId = consumer_id(ConsumerTag),
    case ra:process_command(ServerId,
                            rabbit_fifo:make_checkout(ConsumerId,
                                                      {dequeue, Settlement},
                                                      #{}),
                            Timeout) of
        {ok, {dequeue, empty}, Leader} ->
            {empty, State0#state{leader = Leader}};
        {ok, {dequeue, {MsgId, {MsgHeader, Msg0}}, MsgsReady}, Leader} ->
            {Msg, Redelivered} = add_delivery_count_header(Msg0, MsgHeader),
            {ok, MsgsReady,
             {QueueName, qref(Leader), MsgId, Redelivered, Msg},
             State0#state{leader = Leader}};
        {ok, {error, _} = Err, _Leader} ->
            Err;
        Err ->
            Err
    end.

add_delivery_count_header(Msg0, #{acquired_count := AcqCount} = Header)
  when is_integer(AcqCount) ->
    Msg = case mc:is(Msg0) of
              true ->
                  Msg1 = mc:set_annotation(<<"x-delivery-count">>, AcqCount, Msg0),
                  %% the "delivery-count" header in the AMQP spec does not include
                  %% returns (released outcomes)
                  rabbit_fifo:annotate_msg(Header, Msg1);
              false ->
                  Msg0
          end,
    Redelivered = AcqCount > 0,
    {Msg, Redelivered};
add_delivery_count_header(Msg, #{delivery_count := DC} = Header) ->
    %% there was a delivery count but no acquired count, this means the message
    %% was delivered from a quorum queue running v3 so we patch this up here
    add_delivery_count_header(Msg, Header#{acquired_count => DC});
add_delivery_count_header(Msg, _Header) ->
    {Msg, false}.

%% @doc Settle a message. Permanently removes message from the queue.
%% @param ConsumerTag the tag uniquely identifying the consumer.
%% @param MsgIds the message ids received with the {@link rabbit_fifo:delivery/0.}
%% @param State the {@module} state
%%
-spec settle(rabbit_types:ctag(), [rabbit_fifo:msg_id()], state()) ->
    {state(), list()}.
settle(ConsumerTag, [_|_] = MsgIds, #state{slow = false} = State0) ->
    ConsumerKey = consumer_key(ConsumerTag, State0),
    ServerId = pick_server(State0),
    Cmd = rabbit_fifo:make_settle(ConsumerKey, MsgIds),
    {send_command(ServerId, undefined, Cmd, normal, State0), []};
settle(ConsumerTag, [_|_] = MsgIds,
       #state{unsent_commands = Unsent0} = State0) ->
    ConsumerKey = consumer_key(ConsumerTag, State0),
    %% we've reached the soft limit so will stash the command to be
    %% sent once we have seen enough notifications
    Unsent = maps:update_with(ConsumerKey,
                              fun ({Settles, Returns, Discards}) ->
                                      %% MsgIds has fewer elements than Settles.
                                      %% Therefore put it on the left side of the ++ operator.
                                      %% The order in which messages are settled does not matter.
                                      {MsgIds ++ Settles, Returns, Discards}
                              end, {MsgIds, [], []}, Unsent0),
    {State0#state{unsent_commands = Unsent}, []}.

%% @doc Return a message to the queue.
%% @param ConsumerTag the tag uniquely identifying the consumer.
%% @param MsgIds the message ids to return received
%% from {@link rabbit_fifo:delivery/0.}
%% @param State the {@module} state
%% @returns
%% `{State, list()}' if the command was successfully sent.
%%
-spec return(rabbit_types:ctag(), [rabbit_fifo:msg_id()], state()) ->
    {state(), list()}.
return(ConsumerTag, [_|_] = MsgIds, #state{slow = false} = State0) ->
    ConsumerKey = consumer_key(ConsumerTag, State0),
    ServerId = pick_server(State0),
    Cmd = rabbit_fifo:make_return(ConsumerKey, MsgIds),
    {send_command(ServerId, undefined, Cmd, normal, State0), []};
return(ConsumerTag, [_|_] = MsgIds,
       #state{unsent_commands = Unsent0} = State0) ->
    ConsumerKey = consumer_key(ConsumerTag, State0),
    %% we've reached the soft limit so will stash the command to be
    %% sent once we have seen enough notifications
    Unsent = maps:update_with(ConsumerKey,
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
-spec discard(rabbit_types:ctag(), [rabbit_fifo:msg_id()], state()) ->
    {state(), list()}.
discard(ConsumerTag, [_|_] = MsgIds, #state{slow = false} = State0) ->
    ConsumerKey = consumer_key(ConsumerTag, State0),
    ServerId = pick_server(State0),
    Cmd = rabbit_fifo:make_discard(ConsumerKey, MsgIds),
    {send_command(ServerId, undefined, Cmd, normal, State0), []};
discard(ConsumerTag, [_|_] = MsgIds,
        #state{unsent_commands = Unsent0} = State0) ->
    ConsumerKey = consumer_key(ConsumerTag, State0),
    %% we've reached the soft limit so will stash the command to be
    %% sent once we have seen enough notifications
    Unsent = maps:update_with(ConsumerKey,
                              fun ({Settles, Returns, Discards}) ->
                                      {Settles, Returns, Discards ++ MsgIds}
                              end, {[], [], MsgIds}, Unsent0),
    {State0#state{unsent_commands = Unsent}, []}.

-spec modify(rabbit_types:ctag(), [rabbit_fifo:msg_id()],
             boolean(), boolean(), mc:annotations(), state()) ->
    {state(), list()}.
modify(ConsumerTag, [_|_] = MsgIds, DelFailed, Undel, Anns,
       #state{} = State0) ->
    ConsumerKey = consumer_key(ConsumerTag, State0),
    %% we need to send any pending settles, discards or returns before we
    %% send the modify as this cannot be batched
    %% as it contains message specific annotations
    State1 = send_pending(ConsumerKey, State0),
    ServerId = pick_server(State1),
    Cmd = rabbit_fifo:make_modify(ConsumerKey, MsgIds, DelFailed, Undel, Anns),
    {send_command(ServerId, undefined, Cmd, normal, State1), []}.

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
-spec checkout(rabbit_types:ctag(),
               CreditMode :: rabbit_fifo:credit_mode(),
               Meta :: rabbit_fifo:consumer_meta(),
               state()) ->
    {ok, ConsumerInfos :: map(), state()} |
    {error | timeout, term()}.
checkout(ConsumerTag, CreditMode, #{} = Meta,
         #state{consumers = CDels0} = State0)
  when is_binary(ConsumerTag) andalso
       is_tuple(CreditMode) ->
    Servers = sorted_servers(State0),
    ConsumerId = consumer_id(ConsumerTag),
    Spec = case rabbit_fifo:is_v4() of
               true ->
                   case CreditMode of
                       {simple_prefetch, 0} ->
                           {auto, {simple_prefetch,
                                   ?UNLIMITED_PREFETCH_COUNT}};
                       _ ->
                           {auto, CreditMode}
                   end;
               false ->
                   case CreditMode of
                       {credited, _} ->
                           {auto, 0, credited};
                       {simple_prefetch, 0} ->
                           {auto, ?UNLIMITED_PREFETCH_COUNT, simple_prefetch};
                       {simple_prefetch, Num} ->
                           {auto, Num, simple_prefetch}
                   end
           end,
    Cmd = rabbit_fifo:make_checkout(ConsumerId, Spec, Meta),
    %% ???
    Ack = maps:get(ack, Meta, true),

    case try_process_command(Servers, Cmd, State0) of
        {ok, {ok, Reply}, Leader} ->
            LastMsgId = case Reply of
                            #{num_checked_out := NumChecked,
                              next_msg_id := NextMsgId} ->
                                case NumChecked > 0 of
                                    true ->
                                        %% we cannot know if the pending messages
                                        %% have been delivered to the client or they
                                        %% are on their way to the current process.
                                        %% We set `undefined' to signal this uncertainty
                                        %% and will just accept the next arriving message
                                        %% irrespective of message id
                                        undefined;
                                    false ->
                                        NextMsgId - 1
                                end
                        end,
            DeliveryCount = case rabbit_fifo:is_v4() of
                                true -> credit_api_v2;
                                false -> {credit_api_v1, 0}
                            end,
            ConsumerKey = maps:get(key, Reply, ConsumerId),
            SDels = maps:update_with(
                      ConsumerTag,
                      fun (C) -> C#consumer{ack = Ack} end,
                      #consumer{key = ConsumerKey,
                                last_msg_id = LastMsgId,
                                ack = Ack,
                                delivery_count = DeliveryCount},
                      CDels0),
            {ok, Reply, State0#state{leader = Leader,
                                     consumers = SDels}};
        Err ->
            Err
    end.

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

-spec credit_v1(rabbit_types:ctag(),
                Credit :: non_neg_integer(),
                Drain :: boolean(),
                state()) ->
    {state(), rabbit_queue_type:actions()}.
credit_v1(ConsumerTag, Credit, Drain,
          #state{consumers = CDels} = State) ->
    #consumer{delivery_count = {credit_api_v1, Count}} = maps:get(ConsumerTag, CDels),
    credit(ConsumerTag, Count, Credit, Drain, State).

%% @doc Provide credit to the queue
%%
%% This only has an effect if the consumer uses credit mode: credited
%% @param ConsumerTag a unique tag to identify this particular consumer.
%% @param Credit the amount of credit to provide to the queue
%% @param Drain tells the queue to use up any credit that cannot be immediately
%% fulfilled. (i.e. there are not enough messages on queue to use up all the
%% provided credit).
%% @param Reply true if the queue client requests a credit_reply queue action
-spec credit(rabbit_types:ctag(),
             rabbit_queue_type:delivery_count(),
             rabbit_queue_type:credit(),
             Drain :: boolean(),
             state()) ->
    {state(), rabbit_queue_type:actions()}.
credit(ConsumerTag, DeliveryCount, Credit, Drain, State) ->
    ConsumerKey = consumer_key(ConsumerTag, State),
    ServerId = pick_server(State),
    Cmd = rabbit_fifo:make_credit(ConsumerKey, Credit, DeliveryCount, Drain),
    {send_command(ServerId, undefined, Cmd, normal, State), []}.

%% @doc Cancels a checkout with the rabbit_fifo queue for the consumer tag
%%
%% This is a synchronous call. I.e. the call will block until the command
%% has been accepted by the ra process or it times out.
%%
%% @param ConsumerTag a unique tag to identify this particular consumer.
%% @param State The {@module} state.
%%
%% @returns `{ok, State}' or `{error | timeout, term()}'
-spec cancel_checkout(rabbit_types:ctag(), rabbit_queue_type:cancel_reason(), state()) ->
    {ok, state()} | {error | timeout, term()}.
cancel_checkout(ConsumerTag, Reason,
                #state{consumers = Consumers} = State0)
  when is_atom(Reason) ->
    case Consumers of
        #{ConsumerTag := #consumer{key = Cid}} ->
            Servers = sorted_servers(State0),
            ConsumerId = {ConsumerTag, self()},
            State1 = send_pending(Cid, State0),
            Cmd = rabbit_fifo:make_checkout(ConsumerId, Reason, #{}),
            State = State1#state{consumers = maps:remove(ConsumerTag, Consumers)},
            case try_process_command(Servers, Cmd, State) of
                {ok, _, Leader} ->
                    {ok, State#state{leader = Leader}};
                Err ->
                    Err
            end;
        _ ->
            %% TODO: when we implement the `delete' checkout spec we could
            %% fallback to that to make sure there is little chance a consumer
            %% sticks around in the machine
            {ok, State0}
    end.

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

-spec num_cached_segments(state()) -> non_neg_integer().
num_cached_segments(#state{cached_segments = CachedSegments}) ->
    case CachedSegments of
        undefined ->
            0;
        {_, _, Cached} ->
            ra_flru:size(Cached)
    end.

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
%% @param QName Name of the queue.
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
-spec handle_ra_event(rabbit_amqqueue:name(), ra:server_id(),
                      ra_server_proc:ra_event_body(), state()) ->
    {internal, Correlators :: [term()], rabbit_queue_type:actions(), state()} |
    {rabbit_fifo:client_msg(), state()} | {eol, rabbit_queue_type:actions()}.
handle_ra_event(QName, Leader, {applied, Seqs},
                #state{leader = OldLeader,
                       cfg = #cfg{soft_limit = SftLmt}} = State0) ->

    {Corrs, ActionsRev, State1} = lists:foldl(fun seq_applied/2,
                                              {[], [], State0#state{leader = Leader}},
                                              Seqs),

    %% if the leader has changed we need to resend any pending commands remaining
    %% after the applied processing
    State2 = if OldLeader =/= Leader ->
                    %% double check before resending as applied notifications
                    %% can arrive from old leaders in any order
                    case ra:members(Leader) of
                        {ok, _, ActualLeader}
                          when ActualLeader =/= OldLeader ->
                            %% there is a new leader
                            rabbit_log:debug("~ts: Detected QQ leader change (applied) "
                                             "from ~w to ~w, "
                                             "resending ~b pending commands",
                                             [?MODULE, OldLeader, ActualLeader,
                                              maps:size(State1#state.pending)]),
                            resend_all_pending(State1#state{leader = ActualLeader});
                        _ ->
                            State1
                    end;
                true ->
                    State1
             end,

    Actions0 = lists:reverse(ActionsRev),
    Actions = case Corrs of
                  [] ->
                      Actions0;
                  _ ->
                      %%TODO: consider using lists:foldr/3 above because
                      %% Corrs is returned in the wrong order here.
                      %% The wrong order does not matter much because the channel sorts the
                      %% sequence numbers before confirming to the client. But rabbit_fifo_client
                      %% is sequence numer agnostic: it handles any correlation terms.
                      [{settled, QName, Corrs} | Actions0]
              end,
    case map_size(State2#state.pending) < SftLmt of
        true when State2#state.slow == true ->
            % we have exited soft limit state
            % send any unsent commands and cancel the time as
            % TODO: really the timer should only be cancelled when the channel
            % exits flow state (which depends on the state of all queues the
            % channel is interacting with)
            % but the fact the queue has just applied suggests
            % it's ok to cancel here anyway
            State3 = cancel_timer(State2#state{slow = false,
                                               unsent_commands = #{}}),
            % build up a list of commands to issue
            Commands = maps:fold(
                         fun (Cid, {Settled, Returns, Discards}, Acc) ->
                                 add_command(Cid, settle, Settled,
                                             add_command(Cid, return, Returns,
                                                         add_command(Cid, discard,
                                                                     Discards, Acc)))
                         end, [], State2#state.unsent_commands),
            ServerId = pick_server(State3),
            %% send all the settlements and returns
            State = lists:foldl(fun (C, S0) ->
                                        send_command(ServerId, undefined, C,
                                                     normal, S0)
                                end, State3, Commands),
            {ok, State, [{unblock, cluster_name(State)} | Actions]};
        _ ->
            {ok, State2, Actions}
    end;
handle_ra_event(QName, From, {machine, Del}, State0)
      when element(1, Del) == delivery ->
    handle_delivery(QName, From, Del, State0);
handle_ra_event(_QName, _From, {machine, Action}, State)
  when element(1, Action) =:= credit_reply orelse
       element(1, Action) =:= credit_reply_v1 ->
    {ok, State, [Action]};
handle_ra_event(_QName, _, {machine, {queue_status, Status}},
                #state{} = State) ->
    %% just set the queue status
    {ok, State#state{queue_status = Status}, []};
handle_ra_event(QName, Leader, {machine, leader_change},
                #state{leader = OldLeader,
                       pending = Pending} = State0) ->
    %% we need to update leader
    %% and resend any pending commands
    rabbit_log:debug("~ts: ~s Detected QQ leader change from ~w to ~w, "
                     "resending ~b pending commands",
                     [rabbit_misc:rs(QName), ?MODULE, OldLeader,
                      Leader, maps:size(Pending)]),
    State = resend_all_pending(State0#state{leader = Leader}),
    {ok, State, []};
handle_ra_event(_QName, _From, {rejected, {not_leader, Leader, _Seq}},
                #state{leader = Leader} = State) ->
    {ok, State, []};
handle_ra_event(QName, _From, {rejected, {not_leader, Leader, _Seq}},
                #state{leader = OldLeader,
                       pending = Pending} = State0) ->
    rabbit_log:debug("~ts: ~s Detected QQ leader change (rejection) from ~w to ~w, "
                     "resending ~b pending commands",
                     [rabbit_misc:rs(QName), ?MODULE, OldLeader,
                      Leader, maps:size(Pending)]),
    State = resend_all_pending(State0#state{leader = Leader}),
    {ok, cancel_timer(State), []};
handle_ra_event(_QName, _From,
                {rejected, {not_leader, _UndefinedMaybe, _Seq}}, State0) ->
    % TODO: how should these be handled? re-sent on timer or try random
    {ok, State0, []};
handle_ra_event(QName, _, timeout, #state{cfg = #cfg{servers = Servers}} = State0) ->
    case find_leader(Servers) of
        undefined ->
            %% still no leader, set the timer again
            {ok, set_timer(QName, State0), []};
        Leader ->
            State = resend_all_pending(State0#state{leader = Leader}),
            {ok, State, []}
    end;
handle_ra_event(QName, Leader, close_cached_segments,
                #state{cached_segments = CachedSegments} = State) ->
    {ok,
     case CachedSegments of
         undefined ->
             %% timer didn't get cancelled so just ignore this
             State;
         {_TRef, Last, Cache} ->
             case now_ms() > Last + ?CACHE_SEG_TIMEOUT of
                 true ->
                     rabbit_log:debug("~ts: closing_cached_segments",
                                      [rabbit_misc:rs(QName)]),
                     %% its been long enough, evict all
                     _ = ra_flru:evict_all(Cache),
                     State#state{cached_segments = undefined};
                 false ->
                     %% set another timer
                     Ref = erlang:send_after(?CACHE_SEG_TIMEOUT, self(),
                                             {'$gen_cast',
                                              {queue_event, QName,
                                               {Leader, close_cached_segments}}}),
                     State#state{cached_segments = {Ref, Last, Cache}}
             end
     end, []};
handle_ra_event(_QName, _Leader, {machine, eol}, State) ->
    {eol, [{unblock, cluster_name(State)}]}.

-spec close(rabbit_fifo_client:state()) -> ok.
close(#state{cached_segments = undefined}) ->
    ok;
close(#state{cached_segments = {_, _, Flru}}) ->
    _ = ra_flru:evict_all(Flru),
    ok.

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
untracked_enqueue([ServerId | _], Msg) ->
    Cmd = rabbit_fifo:make_enqueue(undefined, undefined, Msg),
    ok = ra:pipeline_command(ServerId, Cmd),
    ok.

%% Internal

try_process_command([Server | Rem], Cmd,
                    #state{cfg = #cfg{timeout = Timeout}} = State) ->
    case ra:process_command(Server, Cmd, Timeout) of
        {ok, _, _} = Res ->
            Res;
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
            {Corrs, Actions, State}
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
maybe_add_action({send_drained, {Tag, Credit}}, Acc, State0) ->
    %% This function clause should be deleted when
    %% feature flag rabbitmq_4.0.0 becomes required.
    State = add_delivery_count(Credit, Tag, State0),
    Action = {credit_reply_v1, Tag, Credit, _Avail = 0, _Drain = true},
    {[Action | Acc], State};
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

handle_delivery(QName, Leader, {delivery, Tag, [{FstId, _} | _] = IdMsgs},
                #state{consumers = CDels0} = State0)
  when is_map_key(Tag, CDels0) ->
    QRef = qref(Leader),
    {LastId, _} = lists:last(IdMsgs),
    Consumer = #consumer{ack = Ack} = maps:get(Tag, CDels0),
    %% format as a deliver action
    Del = {deliver, Tag, Ack, transform_msgs(QName, QRef, IdMsgs)},
    %% TODO: remove potential default allocation
    case Consumer of
        #consumer{last_msg_id = Prev} = C
          when Prev =:= undefined orelse FstId =:= Prev+1 ->
            %% Prev =:= undefined is a special case where a checkout was done
            %% for a previously cancelled consumer that still had pending messages
            %% In this case we can't reliably know what the next expected message
            %% id should be so have to accept whatever message comes next
            maybe_auto_ack(Ack, Del,
                           State0#state{consumers =
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
            case get_missing_deliveries(State0, Prev+1, FstId-1, Tag) of
                {protocol_error, _, _, _} = Err ->
                    Err;
                Missing ->
                    XDel = {deliver, Tag, Ack, transform_msgs(QName, QRef,
                                                              Missing ++ IdMsgs)},
                    maybe_auto_ack(Ack, XDel,
                                   State0#state{consumers =
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
                    handle_delivery(QName, Leader, {delivery, Tag, IdMsgs2}, State0)
            end;
        C when FstId =:= 0 ->
            % the very first delivery
            maybe_auto_ack(Ack, Del,
                           State0#state{consumers =
                                        update_consumer(Tag, LastId,
                                                        length(IdMsgs),
                                                        C#consumer{last_msg_id = LastId},
                                                        CDels0)})
    end;
handle_delivery(_QName, _Leader, {delivery, Tag, [_ | _] = IdMsgs},
                #state{consumers = CDels0} = State0)
  when not is_map_key(Tag, CDels0) ->
    %% Note:
    %% https://github.com/rabbitmq/rabbitmq-server/issues/3729
    %% If the consumer is no longer in the deliveries map,
    %% we should return all messages.
    MsgIntIds = [Id || {Id, _} <- IdMsgs],
    {State1, Deliveries} = return(Tag, MsgIntIds, State0),
    {ok, State1, Deliveries};
handle_delivery(QName, Leader, {delivery, Tag, ReadPlan, Msgs},
                #state{cached_segments = CachedSegments} = State) ->
    {TRef, Cached0} = case CachedSegments of
                          undefined ->
                              {undefined, undefined};
                          {R, _, C} ->
                              {R, C}
                      end,
    {MsgIds, Cached1} = rabbit_fifo:exec_read(Cached0, ReadPlan, Msgs),
    %% if there are cached segments after a read and there
    %% is no current timer set, set a timer
    %% send a message to evict cache after some time
    Cached = case ra_flru:size(Cached1) > 0 of
                 true when TRef == undefined ->
                     Ref = erlang:send_after(?CACHE_SEG_TIMEOUT, self(),
                                             {'$gen_cast',
                                              {queue_event, QName,
                                               {Leader, close_cached_segments}}}),
                     {Ref, now_ms(), Cached1};
                 true ->
                     {TRef, now_ms(), Cached1};
                 false when is_reference(TRef) ->
                     %% the time is (potentially) alive and may as well be
                     %% cancelled here
                     _ = erlang:cancel_timer(TRef, [{async, true},
                                                    {info, false}]),
                     undefined;
                 false  ->
                     undefined
             end,
    handle_delivery(QName, Leader, {delivery, Tag, MsgIds},
                    State#state{cached_segments = Cached}).

transform_msgs(QName, QRef, Msgs) ->
    lists:map(
      fun({MsgId, {MsgHeader, Msg0}}) ->
              {Msg, Redelivered} = add_delivery_count_header(Msg0, MsgHeader),
              {QName, QRef, MsgId, Redelivered, Msg}
      end, Msgs).

update_consumer(Tag, LastId, DelCntIncr, Consumer, Consumers) ->
    D = case Consumer#consumer.delivery_count of
            credit_api_v2 -> credit_api_v2;
            {credit_api_v1, Count} -> {credit_api_v1, Count + DelCntIncr}
        end,
    maps:update(Tag,
                Consumer#consumer{last_msg_id = LastId,
                                  delivery_count = D},
                Consumers).

add_delivery_count(DelCntIncr, Tag, #state{consumers = CDels0} = State) ->
    Con = #consumer{last_msg_id = LastMsgId} = maps:get(Tag, CDels0),
    CDels = update_consumer(Tag, LastMsgId, DelCntIncr, Con, CDels0),
    State#state{consumers = CDels}.

get_missing_deliveries(State, From, To, ConsumerTag) ->
    %% find local server
    ConsumerKey = consumer_key(ConsumerTag, State),
    rabbit_log:debug("get_missing_deliveries for consumer '~s' from ~b to ~b",
                     [ConsumerTag, From, To]),
    Cmd = {get_checked_out, ConsumerKey, lists:seq(From, To)},
    ServerId = find_local_or_leader(State),
    case ra:aux_command(ServerId, Cmd) of
        {ok, Missing} ->
            Missing;
        {error, Error} ->
            {protocol_error, internal_error, "Cannot query missing deliveries from ~tp: ~tp",
             [ServerId, Error]};
        {timeout, _} ->
            {protocol_error, internal_error, "Cannot query missing deliveries from ~tp: timeout",
             [ServerId]}
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

consumer_key(ConsumerTag, #state{consumers = Consumers}) ->
    case Consumers of
        #{ConsumerTag := #consumer{key = Key}} ->
            Key;
        _ ->
            %% if no consumer found fall back to using the ConsumerId
            consumer_id(ConsumerTag)
    end.

consumer_id(ConsumerTag) when is_binary(ConsumerTag) ->
    {ConsumerTag, self()}.

send_command(Server, Correlation, Command, Priority,
             #state{pending = Pending,
                    next_seq = Seq,
                    cfg = #cfg{soft_limit = SftLmt}} = State) ->
    ok = case rabbit_fifo:is_return(Command) of
             true ->
                 %% returns are sent to the aux machine for pre-evaluation
                 ra:cast_aux_command(Server, {Command, Seq, self()});
             _ ->
                 ra:pipeline_command(Server, Command, Seq, Priority)
         end,
    State#state{pending = Pending#{Seq => {Correlation, Command}},
                next_seq = Seq + 1,
                slow = map_size(Pending) >= SftLmt}.

resend_command(ServerId, Correlation, Command,
               #state{pending = Pending,
                      next_seq = Seq} = State) ->
    ok = ra:pipeline_command(ServerId, Command, Seq),
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

set_timer(QName, #state{leader = Leader0,
                        cfg = #cfg{servers = [Server | _]}} = State) ->
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
    _ = erlang:cancel_timer(Ref, [{async, true}, {info, false}]),
    State#state{timer_state = undefined}.

find_local_or_leader(#state{leader = Leader,
                            cfg = #cfg{servers = Servers}}) ->
    case find_local(Servers) of
        undefined ->
            Leader;
        ServerId ->
            ServerId
    end.

find_local([{_, N} = ServerId | _]) when N == node() ->
    ServerId;
find_local([_ | Rem]) ->
    find_local(Rem);
find_local([]) ->
    undefined.


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

-spec cluster_name(state()) ->
    atom().
cluster_name(#state{cfg = #cfg{servers = [{Name, _Node} | _]}}) ->
    Name.

send_pending(Cid, #state{unsent_commands = Unsent} = State0) ->
    Commands = case Unsent of
                   #{Cid := {Settled, Returns, Discards}} ->
                       add_command(Cid, settle, Settled,
                                   add_command(Cid, return, Returns,
                                               add_command(Cid, discard,
                                                           Discards, [])));
                   _ ->
                       []
               end,
    ServerId = pick_server(State0),
    %% send all the settlements, discards and returns
    State1 = lists:foldl(fun (C, S0) ->
                                 send_command(ServerId, undefined, C,
                                              normal, S0)
                         end, State0, Commands),
    State1#state{unsent_commands = maps:remove(Cid, Unsent)}.

now_ms() ->
    erlang:system_time(millisecond).
