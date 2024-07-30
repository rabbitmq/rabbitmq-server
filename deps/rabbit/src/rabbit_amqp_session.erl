%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_amqp_session).

-compile({inline, [maps_update_with/4]}).

-behaviour(gen_server).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("amqp10_common/include/amqp10_types.hrl").
-include("rabbit_amqp.hrl").
-include("mc.hrl").

-rabbit_deprecated_feature(
   {amqp_address_v1,
    #{deprecation_phase => permitted_by_default,
      doc_url => "https://www.rabbitmq.com/docs/next/amqp#address",
      messages =>
      #{when_permitted =>
        "RabbitMQ AMQP address version 1 is deprecated. "
        "Clients should use RabbitMQ AMQP address version 2.",
        when_denied =>
        "RabbitMQ AMQP address version 1 is unsupported. "
        "Clients must use RabbitMQ AMQP address version 2."
       }}
   }).

%% This is the link credit that we grant to sending clients.
%% We are free to choose whatever we want, sending clients must obey.
%% Default soft limits / credits in deps/rabbit/Makefile are:
%% 32 for quorum queues
%% 256 for streams
%% 400 for classic queues
%% If link target is a queue (rather than an exchange), we could use one of these depending
%% on target queue type. For the time being just use a static value that's something in between.
%% In future, we could dynamically grow (or shrink) the link credit we grant depending on how fast
%% target queue(s) actually confirm messages: see paper "Credit-Based Flow Control for ATM Networks"
%% from 1995, section 4.2 "Static vs. adaptive credit control" for pros and cons.
-define(DEFAULT_MAX_LINK_CREDIT, 128).
%% Initial and maximum link credit that we grant to a sending queue.
%% Only when we sent sufficient messages to the writer proc, we will again grant
%% credits to the sending queue. We have this limit in place to ensure that our
%% session proc won't be flooded with messages by the sending queue, especially
%% if we are throttled sending messages to the client either by the writer proc
%% or by remote-incoming window (i.e. session flow control).
-define(DEFAULT_MAX_QUEUE_CREDIT, 256).
-define(DEFAULT_MAX_INCOMING_WINDOW, 400).
-define(MAX_LINK_CREDIT, persistent_term:get(max_link_credit)).
-define(MAX_MANAGEMENT_LINK_CREDIT, 8).
-define(MANAGEMENT_NODE_ADDRESS, <<"/management">>).
-define(UINT_OUTGOING_WINDOW, {uint, ?UINT_MAX}).
%% "The next-outgoing-id MAY be initialized to an arbitrary value" [2.5.6]
-define(INITIAL_OUTGOING_TRANSFER_ID, ?UINT_MAX - 3).
%% "Note that, despite its name, the delivery-count is not a count but a
%% sequence number initialized at an arbitrary point by the sender." [2.6.7]
-define(INITIAL_DELIVERY_COUNT, ?UINT_MAX - 4).
-define(INITIAL_OUTGOING_DELIVERY_ID, 0).
-define(DEFAULT_MAX_HANDLE, ?UINT_MAX).
-define(UINT(N), {uint, N}).
%% [3.4]
-define(OUTCOMES, [?V_1_0_SYMBOL_ACCEPTED,
                   ?V_1_0_SYMBOL_REJECTED,
                   ?V_1_0_SYMBOL_RELEASED,
                   ?V_1_0_SYMBOL_MODIFIED]).
-define(DEFAULT_EXCHANGE_NAME, <<>>).
-define(PROTOCOL, amqp10).
-define(PROCESS_GROUP_NAME, amqp_sessions).
-define(MAX_PERMISSION_CACHE_SIZE, 12).
-define(HIBERNATE_AFTER, 6_000).
-define(CREDIT_REPLY_TIMEOUT, 30_000).

-export([start_link/8,
         process_frame/2,
         list_local/0,
         conserve_resources/3,
         check_resource_access/4,
         check_read_permitted_on_topic/4
        ]).

-export([init/1,
         terminate/2,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         format_status/1]).

-import(rabbit_amqp_util,
        [protocol_error/3]).
-import(serial_number,
        [add/2,
         diff/2,
         compare/2]).
-import(rabbit_misc,
        [queue_resource/2,
         exchange_resource/2]).

-type permission_cache() :: [{rabbit_types:r(exchange | queue),
                              rabbit_types:permission_atom()}].
-type topic_permission_cache() :: [{rabbit_types:r(topic),
                                    rabbit_types:routing_key(),
                                    rabbit_types:permission_atom()}].

-type transfer_frame_body() :: [Performative :: #'v1_0.transfer'{} |
                                Payload :: iolist()].

-export_type([permission_cache/0,
              topic_permission_cache/0]).

%% incoming multi transfer delivery [2.6.14]
-record(multi_transfer_msg, {
          payload_fragments_rev :: [binary(),...],
          delivery_id :: delivery_number(),
          settled :: boolean()
         }).

%% For AMQP management operations, we require a link pair as described in
%% https://docs.oasis-open.org/amqp/linkpair/v1.0/cs01/linkpair-v1.0-cs01.html
-record(management_link_pair, {
          client_terminus_address,
          incoming_half :: unattached | link_handle(),
          outgoing_half :: unattached | link_handle()
         }).

%% Incoming or outgoing half of the link pair.
-record(management_link, {
          name :: binary(),
          delivery_count :: sequence_no(),
          credit :: rabbit_queue_type:credit(),
          max_message_size :: unlimited | pos_integer()
         }).

-record(incoming_link, {
          %% The exchange is either defined in the ATTACH frame and static for
          %% the life time of the link or dynamically provided in each message's
          %% "to" field (address v2).
          exchange :: rabbit_types:exchange() | rabbit_exchange:name() | to,
          %% The routing key is either defined in the ATTACH frame and static for
          %% the life time of the link or dynamically provided in each message's
          %% "to" field (address v2) or "subject" field (address v1).
          routing_key :: rabbit_types:routing_key() | to | subject,
          %% queue_name_bin is only set if the link target address refers to a queue.
          queue_name_bin :: undefined | rabbit_misc:resource_name(),
          delivery_count :: sequence_no(),
          credit :: rabbit_queue_type:credit(),
          %% TRANSFER delivery IDs published to queues but not yet confirmed by queues
          incoming_unconfirmed_map = #{} :: #{delivery_number() =>
                                              {#{rabbit_amqqueue:name() := ok},
                                               IsTransferSettled :: boolean(),
                                               AtLeastOneQueueConfirmed :: boolean()}},
          multi_transfer_msg :: undefined | #multi_transfer_msg{}
         }).

%% A credit request from the client (receiver) as sent in the FLOW frame.
-record(credit_req, {
          delivery_count :: sequence_no(),
          credit :: rabbit_queue_type:credit(),
          drain :: boolean(),
          echo :: boolean()
         }).

%% Link flow control state for link between client (receiver) and us (sender).
-record(client_flow_ctl, {
          delivery_count :: sequence_no(),
          credit :: rabbit_queue_type:credit(),
          echo :: boolean()
         }).

%% Link flow control state for link between us (receiver) and queue (sender).
-record(queue_flow_ctl, {
          delivery_count :: sequence_no(),
          %% We cap the actual credit we grant to the sending queue.
          %% If client_flow_ctl.credit is larger than max_queue_credit,
          %% we will top up in batches to the sending queue.
          credit :: rabbit_queue_type:credit(),
          drain :: boolean()
         }).

-record(outgoing_link, {
          %% Although the source address of a link might be an exchange name and binding key
          %% or a topic filter, an outgoing link will always consume from a queue.
          queue_name :: rabbit_amqqueue:name(),
          queue_type :: rabbit_queue_type:queue_type(),
          send_settled :: boolean(),
          max_message_size :: unlimited | pos_integer(),

          %% When feature flag credit_api_v2 becomes required,
          %% the following 2 fields should be deleted.
          credit_api_version :: 1 | 2,
          %% When credit API v1 is used, our session process holds the delivery-count
          delivery_count :: sequence_no() | credit_api_v2,
          %% We use a dual link approach for messages we send to the client.
          %% We hold link flow control state for the link to the receiving
          %% client and for the link to the sending queue.
          client_flow_ctl :: #client_flow_ctl{} | credit_api_v1,
          queue_flow_ctl :: #queue_flow_ctl{} | credit_api_v1,
          %% 'true' means:
          %% * we haven't processed a credit reply yet since we last sent
          %%   a credit request to the sending queue.
          %% * a credit request is certainly in flight
          %% * possibly multiple credit requests are in flight (e.g. rabbit_fifo_client
          %%   will re-send credit requests on our behalf on quorum queue leader changes)
          %% 'false' means:
          %% * we processed a credit reply since we last sent a credit request to the sending queue
          %% * probably no credit request is in flight, but there might be
          %%   (we aren't sure since we don't use correlations for credit requests)
          at_least_one_credit_req_in_flight :: boolean() | credit_api_v1,
          %% While at_least_one_credit_req_in_flight is true, we stash the
          %% latest credit request from the receiving client.
          stashed_credit_req :: none | #credit_req{} | credit_api_v1
         }).

-record(outgoing_unsettled, {
          %% The queue sent us this consumer scoped sequence number.
          msg_id :: rabbit_amqqueue:msg_id(),
          consumer_tag :: rabbit_types:ctag(),
          queue_name :: rabbit_amqqueue:name()
         }).

-record(pending_delivery, {
          %% A large message can be split into multiple transfer frames.
          frames :: [transfer_frame_body(), ...],
          queue_ack_required :: boolean(),
          %% Queue that sent us this message.
          %% When feature flag credit_api_v2 becomes required, this field should be deleted.
          queue_pid :: pid() | credit_api_v2,
          delivery_id :: delivery_number(),
          outgoing_unsettled :: #outgoing_unsettled{}
         }).

-record(pending_management_delivery, {
          %% A large message can be split into multiple transfer frames.
          frames :: [transfer_frame_body(), ...]
         }).

-record(cfg, {
          outgoing_max_frame_size :: unlimited | pos_integer(),
          reader_pid :: rabbit_types:connection(),
          writer_pid :: pid(),
          user :: rabbit_types:user(),
          vhost :: rabbit_types:vhost(),
          %% We just use the incoming (AMQP 1.0) channel number.
          channel_num :: non_neg_integer(),
          %% We tolerate our incoming_window to be violated by up to this number of
          %% excess TRANSFERs. If the client sends us even more TRANSFERs, we will
          %% close the session with session error window-violation.
          %% Unless we decrease our incoming_window dynamically, we are strict by
          %% default and don't allow for any excess TRANSFERs.
          incoming_window_margin = 0 :: non_neg_integer(),
          resource_alarms :: sets:set(rabbit_alarm:resource_alarm_source()),
          trace_state :: rabbit_trace:state(),
          conn_name :: binary(),
          max_incoming_window :: pos_integer()
         }).

-record(state, {
          cfg :: #cfg{},

          %% The following 5 fields are state for session flow control.
          %% See section 2.5.6.
          %%
          %% We omit outgoing-window. We keep the outgoing-window always large and don't
          %% restrict ourselves delivering messages fast to AMQP clients because keeping an
          %% #outgoing_unsettled{} entry in the outgoing_unsettled_map requires far less
          %% memory than holding the message payload in the outgoing_pending queue.
          %%
          %% expected implicit transfer-id of next incoming TRANSFER
          next_incoming_id :: transfer_number(),
          %% Defines the maximum number of incoming transfer frames that we can currently receive.
          %% This value is chosen by us.
          %% Purpose:
          %% 1. It protects our session process from being overloaded, and
          %% 2. Since frames have a maximum size for a given connection, this provides flow control based
          %% on the number of bytes transmitted, and therefore protects our platform, i.e. RabbitMQ as a
          %% whole. We will set this window to 0 if a cluster wide memory or disk alarm occurs (see module
          %% rabbit_alarm) to stop receiving incoming TRANSFERs.
          %% (It's an optional feature: If we wanted we could always keep that window huge, i.e. not
          %% shrinking the window when we receive a TRANSFER. However, we do want to use that feature
          %% due to aforementioned purposes.)
          %% Can become negative up to -incoming_window_margin when client overshoots our window.
          incoming_window :: integer(),
          %% implicit transfer-id of our next outgoing TRANSFER
          next_outgoing_id :: transfer_number(),
          %% Defines the maximum number of outgoing transfer frames that we are
          %% currently allowed to send. This value is chosen by the AMQP client.
          remote_incoming_window :: non_neg_integer(),
          %% This field is informational.
          %% It reflects the maximum number of incoming TRANSFERs that may arrive without exceeding
          %% the AMQP client's own outgoing-window.
          %% When this window shrinks, it is an indication of outstanding transfers (from AMQP client
          %% to us) which we need to settle (after receiving confirmations from target queues) for
          %% the window to grow again.
          remote_outgoing_window :: non_neg_integer(),

          %% These messages were received from queues thanks to sufficient link credit.
          %% However, they are buffered here due to session flow control
          %% (when remote_incoming_window <= 0) before being sent to the AMQP client.
          %%
          %% FLOW frames (and credit reply actions) are stored here as well because for a specific outgoing link
          %% the order in which we send TRANSFER and FLOW frames is important. An outgoing FLOW frame with link
          %% flow control information must not overtake a TRANSFER frame for the same link just because
          %% we are throttled by session flow control. (However, we can still send outgoing FLOW frames
          %% that contain only session flow control information, i.e. where the FLOW's 'handle' field is not set.)
          %% Example:
          %% A receiver grants our queue 2 credits with drain=true and the queue only has 1 message available.
          %% Even when we are limited by session flow control, we must make sure to first send the TRANSFER to the
          %% client (once the remote_incoming_window got opened) followed by the FLOW with drain=true and credit=0
          %% and advanced delivery count. Otherwise, we would violate the AMQP protocol spec.
          outgoing_pending = queue:new() :: queue:queue(#pending_delivery{} |
                                                        rabbit_queue_type:credit_reply_action() |
                                                        #pending_management_delivery{} |
                                                        #'v1_0.flow'{}),

          %% The link or session endpoint assigns each message a unique delivery-id
          %% from a session scoped sequence number.
          %%
          %% Do not confuse this field with next_outgoing_id:
          %% Both are session scoped sequence numbers, but initialised at different arbitrary values.
          %%
          %% next_outgoing_id is an implicit ID, i.e. not sent in the TRANSFER frame.
          %% outgoing_delivery_id is an explicit ID, i.e. sent in the TRANSFER frame.
          %%
          %% next_outgoing_id is incremented per TRANSFER frame.
          %% outgoing_delivery_id is incremented per message.
          %% Remember that a large message can be split up into multiple TRANSFER frames.
          outgoing_delivery_id :: delivery_number(),

          %% Links are unidirectional.
          %% We receive messages from clients on incoming links.
          incoming_links = #{} :: #{link_handle() => #incoming_link{}},
          %% We send messages to clients on outgoing links.
          outgoing_links = #{} :: #{link_handle() => #outgoing_link{}},

          management_link_pairs = #{} :: #{LinkName :: binary() => #management_link_pair{}},
          incoming_management_links = #{} :: #{link_handle() => #management_link{}},
          outgoing_management_links = #{} :: #{link_handle() => #management_link{}},

          %% TRANSFER delivery IDs published to consuming clients but not yet acknowledged by clients.
          outgoing_unsettled_map = #{} :: #{delivery_number() => #outgoing_unsettled{}},

          %% Queue actions that we will process later such that we can confirm and reject
          %% delivery IDs in ranges to reduce the number of DISPOSITION frames sent to the client.
          stashed_rejected = [] :: [{rejected, rabbit_amqqueue:name(), [delivery_number(),...]}],
          stashed_settled = [] :: [{settled, rabbit_amqqueue:name(), [delivery_number(),...]}],
          %% Classic queues that are down.
          stashed_down = []:: [rabbit_amqqueue:name()],
          %% Queues that got deleted.
          stashed_eol = [] :: [rabbit_amqqueue:name()],

          queue_states = rabbit_queue_type:init() :: rabbit_queue_type:state(),
          permission_cache = [] :: permission_cache(),
          topic_permission_cache = [] :: topic_permission_cache()
         }).

-type state() :: #state{}.

start_link(ReaderPid, WriterPid, ChannelNum, FrameMax, User, Vhost, ConnName, BeginFrame) ->
    Args = {ReaderPid, WriterPid, ChannelNum, FrameMax, User, Vhost, ConnName, BeginFrame},
    Opts = [{hibernate_after, ?HIBERNATE_AFTER}],
    gen_server:start_link(?MODULE, Args, Opts).

process_frame(Pid, FrameBody) ->
    gen_server:cast(Pid, {frame_body, FrameBody}).

init({ReaderPid, WriterPid, ChannelNum, MaxFrameSize, User, Vhost, ConnName,
      #'v1_0.begin'{next_outgoing_id = ?UINT(RemoteNextOutgoingId),
                    incoming_window = ?UINT(RemoteIncomingWindow),
                    outgoing_window = ?UINT(RemoteOutgoingWindow),
                    handle_max = HandleMax0}}) ->
    process_flag(trap_exit, true),
    process_flag(message_queue_data, off_heap),
    ok = pg:join(node(), ?PROCESS_GROUP_NAME, self()),

    Alarms0 = rabbit_alarm:register(self(), {?MODULE, conserve_resources, []}),
    Alarms = sets:from_list(Alarms0, [{version, 2}]),

    MaxLinkCredit = application:get_env(
                      rabbit, max_link_credit, ?DEFAULT_MAX_LINK_CREDIT),
    MaxQueueCredit = application:get_env(
                       rabbit, max_queue_credit, ?DEFAULT_MAX_QUEUE_CREDIT),
    MaxIncomingWindow = application:get_env(
                          rabbit, max_incoming_window, ?DEFAULT_MAX_INCOMING_WINDOW),
    true = is_valid_max(MaxLinkCredit),
    true = is_valid_max(MaxQueueCredit),
    true = is_valid_max(MaxIncomingWindow),
    ok = persistent_term:put(max_link_credit, MaxLinkCredit),
    ok = persistent_term:put(max_queue_credit, MaxQueueCredit),
    IncomingWindow = case sets:is_empty(Alarms) of
                         true -> MaxIncomingWindow;
                         false -> 0
                     end,
    NextOutgoingId = ?INITIAL_OUTGOING_TRANSFER_ID,

    HandleMax = case HandleMax0 of
                    ?UINT(Max) -> Max;
                    _ -> ?DEFAULT_MAX_HANDLE
                end,
    Reply = #'v1_0.begin'{remote_channel = {ushort, ChannelNum},
                          handle_max = ?UINT(HandleMax),
                          next_outgoing_id = ?UINT(NextOutgoingId),
                          incoming_window = ?UINT(IncomingWindow),
                          outgoing_window = ?UINT_OUTGOING_WINDOW},
    rabbit_amqp_writer:send_command(WriterPid, ChannelNum, Reply),

    {ok, #state{next_incoming_id = RemoteNextOutgoingId,
                next_outgoing_id = NextOutgoingId,
                incoming_window = IncomingWindow,
                remote_incoming_window = RemoteIncomingWindow,
                remote_outgoing_window = RemoteOutgoingWindow,
                outgoing_delivery_id = ?INITIAL_OUTGOING_DELIVERY_ID,
                cfg = #cfg{reader_pid = ReaderPid,
                           writer_pid = WriterPid,
                           outgoing_max_frame_size = MaxFrameSize,
                           user = User,
                           vhost = Vhost,
                           channel_num = ChannelNum,
                           resource_alarms = Alarms,
                           trace_state = rabbit_trace:init(Vhost),
                           conn_name = ConnName,
                           max_incoming_window = MaxIncomingWindow
                          }}}.

terminate(_Reason, #state{incoming_links = IncomingLinks,
                          outgoing_links = OutgoingLinks,
                          queue_states = QStates}) ->
    maps:foreach(
      fun (_, _) ->
              rabbit_global_counters:publisher_deleted(?PROTOCOL)
      end, IncomingLinks),
    maps:foreach(
      fun (_, _) ->
              rabbit_global_counters:consumer_deleted(?PROTOCOL)
      end, OutgoingLinks),
    ok = rabbit_queue_type:close(QStates).

-spec list_local() -> [pid()].
list_local() ->
    pg:get_local_members(node(), ?PROCESS_GROUP_NAME).

-spec conserve_resources(pid(),
                         rabbit_alarm:resource_alarm_source(),
                         rabbit_alarm:resource_alert()) -> ok.
conserve_resources(Pid, Source, {_, Conserve, _}) ->
    gen_server:cast(Pid, {conserve_resources, Source, Conserve}).

handle_call(Msg, _From, State) ->
    Reply = {error, {not_understood, Msg}},
    reply(Reply, State).

handle_info(timeout, State) ->
    noreply(State);
handle_info({bump_credit, Msg}, State) ->
    %% We are receiving credit from the writer proc.
    credit_flow:handle_bump_msg(Msg),
    noreply(State);
handle_info({{'DOWN', QName}, _MRef, process, QPid, Reason},
            #state{queue_states = QStates0,
                   stashed_eol = Eol} = State0) ->
    case rabbit_queue_type:handle_down(QPid, QName, Reason, QStates0) of
        {ok, QStates, Actions} ->
            State1 = State0#state{queue_states = QStates},
            State = handle_queue_actions(Actions, State1),
            noreply(State);
        {eol, QStates, QRef} ->
            State = State0#state{queue_states = QStates,
                                 stashed_eol = [QRef | Eol]},
            noreply(State)
    end.

handle_cast({frame_body, FrameBody},
            #state{cfg = #cfg{writer_pid = WriterPid,
                              channel_num = Ch}} = State0) ->
    try handle_control(FrameBody, State0) of
        {reply, Replies, State} when is_list(Replies) ->
            lists:foreach(fun (Reply) ->
                                  rabbit_amqp_writer:send_command(WriterPid, Ch, Reply)
                          end, Replies),
            noreply(State);
        {reply, Reply, State} ->
            rabbit_amqp_writer:send_command(WriterPid, Ch, Reply),
            noreply(State);
        {noreply, State} ->
            noreply(State);
        {stop, _, _} = Stop ->
            Stop
    catch exit:#'v1_0.error'{} = Error ->
              log_error_and_close_session(Error, State0);
          exit:normal ->
              {stop, normal, State0};
          _:Reason:Stacktrace ->
              {stop, {Reason, Stacktrace}, State0}
    end;
handle_cast({queue_event, _, _} = QEvent, State0) ->
    try handle_queue_event(QEvent, State0) of
        State ->
            noreply_coalesce(State)
    catch exit:#'v1_0.error'{} = Error ->
              log_error_and_close_session(Error, State0)
    end;
handle_cast({conserve_resources, Alarm, Conserve},
            #state{incoming_window = IncomingWindow0,
                   cfg = #cfg{resource_alarms = Alarms0,
                              incoming_window_margin = Margin0,
                              writer_pid = WriterPid,
                              channel_num = Ch,
                              max_incoming_window = MaxIncomingWindow
                             } = Cfg
                  } = State0) ->
    Alarms = case Conserve of
                 true -> sets:add_element(Alarm, Alarms0);
                 false -> sets:del_element(Alarm, Alarms0)
             end,
    {SendFlow, IncomingWindow, Margin} =
    case {sets:is_empty(Alarms0), sets:is_empty(Alarms)} of
        {true, false} ->
            %% Alarm kicked in.
            %% Notify the client to not send us any more TRANSFERs. Since we decrase
            %% our incoming window dynamically, there might be incoming in-flight
            %% TRANSFERs. So, let's be lax and allow for some excess TRANSFERs.
            {true, 0, MaxIncomingWindow};
        {false, true} ->
            %% All alarms cleared.
            %% Notify the client that it can resume sending us TRANSFERs.
            {true, MaxIncomingWindow, 0};
        _ ->
            {false, IncomingWindow0, Margin0}
    end,
    State = State0#state{incoming_window = IncomingWindow,
                         cfg = Cfg#cfg{resource_alarms = Alarms,
                                       incoming_window_margin = Margin}},
    case SendFlow of
        true ->
            Flow = session_flow_fields(#'v1_0.flow'{}, State),
            rabbit_amqp_writer:send_command(WriterPid, Ch, Flow);
        false ->
            ok
    end,
    noreply(State);
handle_cast(refresh_config, #state{cfg = #cfg{vhost = Vhost} = Cfg} = State0) ->
    State = State0#state{cfg = Cfg#cfg{trace_state = rabbit_trace:init(Vhost)}},
    noreply(State).

log_error_and_close_session(
  Error, State = #state{cfg = #cfg{reader_pid = ReaderPid,
                                   writer_pid = WriterPid,
                                   channel_num = Ch}}) ->
    End = #'v1_0.end'{error = Error},
    rabbit_log:warning("Closing session for connection ~p: ~tp",
                       [ReaderPid, Error]),
    ok = rabbit_amqp_writer:send_command_sync(WriterPid, Ch, End),
    {stop, {shutdown, Error}, State}.

%% Batch confirms / rejects to publishers.
noreply_coalesce(#state{stashed_rejected = [],
                        stashed_settled = [],
                        stashed_down = [],
                        stashed_eol = []} = State) ->
    noreply(State);
noreply_coalesce(State) ->
    Timeout = 0,
    {noreply, State, Timeout}.

noreply(State0) ->
    State = send_buffered(State0),
    {noreply, State}.

reply(Reply, State0) ->
    State = send_buffered(State0),
    {reply, Reply, State}.

send_buffered(State0) ->
    State = send_delivery_state_changes(State0),
    send_pending(State).

%% Send confirms / rejects to publishers.
send_delivery_state_changes(#state{stashed_rejected = [],
                                   stashed_settled = [],
                                   stashed_down = [],
                                   stashed_eol = []} = State) ->
    State;
send_delivery_state_changes(State0 = #state{cfg = #cfg{writer_pid = Writer,
                                                       channel_num = ChannelNum}}) ->
    %% Order is important:
    %% 1. Process queue rejections.
    {RejectedIds, GrantCredits0, State1} = handle_stashed_rejected(State0),
    send_dispositions(RejectedIds, #'v1_0.rejected'{}, Writer, ChannelNum),
    %% 2. Process queue confirmations.
    {AcceptedIds0, GrantCredits1, State2} = handle_stashed_settled(GrantCredits0, State1),
    %% 3. Process unavailable classic queues.
    {DetachFrames0, State3} = handle_stashed_down(State2),
    %% 4. Process queue deletions.
    {ReleasedIds, AcceptedIds1, DetachFrames, GrantCredits, State} = handle_stashed_eol(DetachFrames0, GrantCredits1, State3),
    send_dispositions(ReleasedIds, #'v1_0.released'{}, Writer, ChannelNum),
    AcceptedIds = AcceptedIds1 ++ AcceptedIds0,
    send_dispositions(AcceptedIds, #'v1_0.accepted'{}, Writer, ChannelNum),
    rabbit_global_counters:messages_confirmed(?PROTOCOL, length(AcceptedIds)),
    %% Send DETACH frames after DISPOSITION frames such that
    %% clients can handle DISPOSITIONs before closing their links.
    lists:foreach(fun(Frame) ->
                          rabbit_amqp_writer:send_command(Writer, ChannelNum, Frame)
                  end, DetachFrames),
    maps:foreach(fun(HandleInt, DeliveryCount) ->
                         F0 = flow(?UINT(HandleInt), DeliveryCount),
                         F = session_flow_fields(F0, State),
                         rabbit_amqp_writer:send_command(Writer, ChannelNum, F)
                 end, GrantCredits),
    State.

handle_stashed_rejected(#state{stashed_rejected = []} = State) ->
    {[], #{}, State};
handle_stashed_rejected(#state{stashed_rejected = Actions,
                               incoming_links = Links} = State0) ->
    {Ids, GrantCredits, Ls} =
    lists:foldl(
      fun({rejected, _QName, Correlations}, Accum) ->
              lists:foldl(
                fun({HandleInt, DeliveryId}, {Ids0, GrantCreds0, Links0} = Acc) ->
                        case Links0 of
                            #{HandleInt := Link0 = #incoming_link{incoming_unconfirmed_map = U0}} ->
                                case maps:take(DeliveryId, U0) of
                                    {{_, Settled, _}, U} ->
                                        Ids1 = case Settled of
                                                   true -> Ids0;
                                                   false -> [DeliveryId | Ids0]
                                               end,
                                        Link1 = Link0#incoming_link{incoming_unconfirmed_map = U},
                                        {Link, GrantCreds} = maybe_grant_link_credit(
                                                               HandleInt, Link1, GrantCreds0),
                                        {Ids1, GrantCreds, maps:update(HandleInt, Link, Links0)};
                                    error ->
                                        Acc
                                end;
                            _ ->
                                Acc
                        end
                end, Accum, Correlations)
      end, {[], #{}, Links}, Actions),

    State = State0#state{stashed_rejected = [],
                         incoming_links = Ls},
    {Ids, GrantCredits, State}.

handle_stashed_settled(GrantCredits, #state{stashed_settled = []} = State) ->
    {[], GrantCredits, State};
handle_stashed_settled(GrantCredits0, #state{stashed_settled = Actions,
                                             incoming_links = Links} = State0) ->
    {Ids, GrantCredits, Ls} =
    lists:foldl(
      fun({settled, QName, Correlations}, Accum) ->
              lists:foldl(
                fun({HandleInt, DeliveryId}, {Ids0, GrantCreds0, Links0} = Acc) ->
                        case Links0 of
                            #{HandleInt := Link0 = #incoming_link{incoming_unconfirmed_map = U0}} ->
                                case maps:take(DeliveryId, U0) of
                                    {{#{QName := _} = Qs, Settled, _}, U1} ->
                                        UnconfirmedQs = map_size(Qs),
                                        {Ids2, U} =
                                        if UnconfirmedQs =:= 1 ->
                                               %% last queue confirmed
                                               Ids1 = case Settled of
                                                          true -> Ids0;
                                                          false -> [DeliveryId | Ids0]
                                                      end,
                                               {Ids1, U1};
                                           UnconfirmedQs > 1 ->
                                               U2 = maps:update(
                                                      DeliveryId,
                                                      {maps:remove(QName, Qs), Settled, true},
                                                      U0),
                                               {Ids0, U2}
                                        end,
                                        Link1 = Link0#incoming_link{incoming_unconfirmed_map = U},
                                        {Link, GrantCreds} = maybe_grant_link_credit(
                                                               HandleInt, Link1, GrantCreds0),
                                        {Ids2, GrantCreds, maps:update(HandleInt, Link, Links0)};
                                    _ ->
                                        Acc
                                end;
                            _ ->
                                Acc
                        end
                end, Accum, Correlations)
      end, {[], GrantCredits0, Links}, Actions),

    State = State0#state{stashed_settled = [],
                         incoming_links = Ls},
    {Ids, GrantCredits, State}.

handle_stashed_down(#state{stashed_down = []} = State) ->
    {[], State};
handle_stashed_down(#state{stashed_down = QNames,
                           outgoing_links = OutgoingLinks0} = State0) ->
    %% We already processed queue actions settled and rejected for classic queues that are down.
    %% Here, we destroy any outgoing links that consume from unavailable classic queues.
    %% (This roughly corresponds to consumer_cancel_notify sent from server to client in AMQP 0.9.1.)
    {DetachFrames, OutgoingLinks} =
    lists:foldl(fun(#resource{name = QNameBinDown}, Acc = {_, OutgoingLinks1}) ->
                        maps:fold(fun(Handle, Link = #outgoing_link{queue_name = #resource{name = QNameBin}}, {Frames0, Links0})
                                        when QNameBin =:= QNameBinDown ->
                                          Detach = detach(Handle, Link, ?V_1_0_AMQP_ERROR_ILLEGAL_STATE),
                                          Frames = [Detach | Frames0],
                                          Links = maps:remove(Handle, Links0),
                                          {Frames, Links};
                                     (_, _, Accum) ->
                                          Accum
                                  end, Acc, OutgoingLinks1)
                end, {[], OutgoingLinks0}, QNames),
    State = State0#state{stashed_down = [],
                         outgoing_links = OutgoingLinks},
    {DetachFrames, State}.

handle_stashed_eol(DetachFrames, GrantCredits, #state{stashed_eol = []} = State) ->
    {[], [], DetachFrames, GrantCredits, State};
handle_stashed_eol(DetachFrames0, GrantCredits0, #state{stashed_eol = Eols} = State0) ->
    {ReleasedIs, AcceptedIds, DetachFrames, GrantCredits, State1} =
    lists:foldl(fun(QName, {RIds0, AIds0, DetachFrames1, GrantCreds0, S0 = #state{incoming_links = Links0,
                                                                                  queue_states = QStates0}}) ->
                        {RIds, AIds, GrantCreds1, Links} = settle_eol(QName, {RIds0, AIds0, GrantCreds0, Links0}),
                        QStates = rabbit_queue_type:remove(QName, QStates0),
                        S1 = S0#state{incoming_links = Links,
                                      queue_states = QStates},
                        {DetachFrames2, GrantCreds, S} = destroy_links(QName, DetachFrames1, GrantCreds1, S1),
                        {RIds, AIds, DetachFrames2, GrantCreds, S}
                end, {[], [], DetachFrames0, GrantCredits0, State0}, Eols),

    State = State1#state{stashed_eol = []},
    {ReleasedIs, AcceptedIds, DetachFrames, GrantCredits, State}.

settle_eol(QName, {_ReleasedIds, _AcceptedIds, _GrantCredits, Links} = Acc) ->
    maps:fold(fun(HandleInt,
                  #incoming_link{incoming_unconfirmed_map = U0} = Link0,
                  {RelIds0, AcceptIds0, GrantCreds0, Links0}) ->
                      {RelIds, AcceptIds, U} = settle_eol0(QName, {RelIds0, AcceptIds0, U0}),
                      Link1 = Link0#incoming_link{incoming_unconfirmed_map = U},
                      {Link, GrantCreds} = maybe_grant_link_credit(
                                             HandleInt, Link1, GrantCreds0),
                      Links1 = maps:update(HandleInt,
                                           Link,
                                           Links0),
                      {RelIds, AcceptIds, GrantCreds, Links1}
              end, Acc, Links).

settle_eol0(QName, {_ReleasedIds, _AcceptedIds, UnconfirmedMap} = Acc) ->
    maps:fold(
      fun(DeliveryId,
          {#{QName := _} = Qs, Settled, AtLeastOneQueueConfirmed},
          {RelIds, AcceptIds, U0}) ->
              UnconfirmedQs = map_size(Qs),
              if UnconfirmedQs =:= 1 ->
                     %% The last queue that this delivery ID was waiting a confirm for got deleted.
                     U = maps:remove(DeliveryId, U0),
                     case Settled of
                         true ->
                             {RelIds, AcceptIds, U};
                         false ->
                             case AtLeastOneQueueConfirmed of
                                 true ->
                                     %% Since at least one queue confirmed this message, we reply to
                                     %% the client with ACCEPTED. This allows e.g. for large fanout
                                     %% scenarios where temporary target queues are deleted
                                     %% (think about an MQTT subscriber disconnects).
                                     {RelIds, [DeliveryId | AcceptIds], U};
                                 false ->
                                     %% Since no queue confirmed this message, we reply to the client
                                     %% with RELEASED. (The client can then re-publish this message.)
                                     {[DeliveryId | RelIds], AcceptIds, U}
                             end
                     end;
                 UnconfirmedQs > 1 ->
                     U = maps:update(DeliveryId,
                                     {maps:remove(QName, Qs), Settled, AtLeastOneQueueConfirmed},
                                     U0),
                     {RelIds, AcceptIds, U}
              end;
         (_, _, A) ->
              A
      end, Acc, UnconfirmedMap).

destroy_links(#resource{kind = queue,
                        name = QNameBin},
              Frames0,
              GrantCredits0,
              #state{incoming_links = IncomingLinks0,
                     outgoing_links = OutgoingLinks0,
                     outgoing_unsettled_map = Unsettled0,
                     outgoing_pending = Pending0} = State0) ->
    {Frames1,
     GrantCredits,
     IncomingLinks} = maps:fold(fun(Handle, Link, Acc) ->
                                        destroy_incoming_link(Handle, Link, QNameBin, Acc)
                                end, {Frames0, GrantCredits0, IncomingLinks0}, IncomingLinks0),
    {Frames,
     Unsettled,
     Pending,
     OutgoingLinks} = maps:fold(fun(Handle, Link, Acc) ->
                                        destroy_outgoing_link(Handle, Link, QNameBin, Acc)
                                end, {Frames1, Unsettled0, Pending0, OutgoingLinks0}, OutgoingLinks0),
    State = State0#state{incoming_links = IncomingLinks,
                         outgoing_links = OutgoingLinks,
                         outgoing_unsettled_map = Unsettled,
                         outgoing_pending = Pending},
    {Frames, GrantCredits, State}.

destroy_incoming_link(Handle,
                      Link = #incoming_link{queue_name_bin = QNameBin},
                      QNameBin,
                      {Frames, GrantCreds, Links}) ->
    {[detach(Handle, Link, ?V_1_0_AMQP_ERROR_RESOURCE_DELETED) | Frames],
     %% Don't grant credits for a link that we destroy.
     maps:remove(Handle, GrantCreds),
     maps:remove(Handle, Links)};
destroy_incoming_link(_, _, _, Acc) ->
    Acc.

destroy_outgoing_link(Handle,
                      Link = #outgoing_link{queue_name = #resource{name = QNameBin}},
                      QNameBin,
                      {Frames, Unsettled0, Pending0, Links}) ->
    {Unsettled, Pending} = remove_outgoing_link(Handle, Unsettled0, Pending0),
    {[detach(Handle, Link, ?V_1_0_AMQP_ERROR_RESOURCE_DELETED) | Frames],
     Unsettled,
     Pending,
     maps:remove(Handle, Links)};
destroy_outgoing_link(_, _, _, Acc) ->
    Acc.

detach(Handle, Link, Error = #'v1_0.error'{}) ->
    rabbit_log:warning("Detaching link handle ~b due to error: ~tp",
                       [Handle, Error]),
    publisher_or_consumer_deleted(Link),
    #'v1_0.detach'{handle = ?UINT(Handle),
                   closed = true,
                   error = Error};
detach(Handle, Link, ErrorCondition) ->
    detach(Handle, Link, #'v1_0.error'{condition = ErrorCondition}).

send_dispositions(Ids, DeliveryState, Writer, ChannelNum) ->
    Ranges = serial_number:ranges(Ids),
    lists:foreach(fun({First, Last}) ->
                          Disposition = disposition(DeliveryState, First, Last),
                          rabbit_amqp_writer:send_command(Writer, ChannelNum, Disposition)
                  end, Ranges).

disposition(DeliveryState, First, Last) ->
    Last1 = case First of
                Last ->
                    %% "If not set, this is taken to be the same as first." [2.7.6]
                    %% Save a few bytes.
                    undefined;
                _ ->
                    ?UINT(Last)
            end,
    #'v1_0.disposition'{
       role = ?AMQP_ROLE_RECEIVER,
       settled = true,
       state = DeliveryState,
       first = ?UINT(First),
       last = Last1}.

handle_control(#'v1_0.attach'{
                  role = ?AMQP_ROLE_SENDER,
                  snd_settle_mode = ?V_1_0_SENDER_SETTLE_MODE_SETTLED,
                  name = Name = {utf8, LinkName},
                  handle = Handle = ?UINT(HandleInt),
                  source = Source = #'v1_0.source'{address = ClientTerminusAddress},
                  target = Target = #'v1_0.target'{address = {utf8, ?MANAGEMENT_NODE_ADDRESS}},
                  initial_delivery_count = DeliveryCount = ?UINT(DeliveryCountInt),
                  properties = Properties
                 } = Attach,
               #state{management_link_pairs = Pairs0,
                      incoming_management_links = Links
                     } = State0) ->
    ok = validate_attach(Attach),
    ok = check_paired(Properties),
    Pairs = case Pairs0 of
                #{LinkName := #management_link_pair{
                                 client_terminus_address = ClientTerminusAddress,
                                 incoming_half = unattached,
                                 outgoing_half = H} = Pair}
                  when is_integer(H) ->
                    maps:update(LinkName,
                                Pair#management_link_pair{incoming_half = HandleInt},
                                Pairs0);
                #{LinkName := Other} ->
                    protocol_error(?V_1_0_AMQP_ERROR_PRECONDITION_FAILED,
                                   "received invalid attach ~p for management link pair ~p",
                                   [Attach, Other]);
                _ ->
                    maps:put(LinkName,
                             #management_link_pair{client_terminus_address = ClientTerminusAddress,
                                                   incoming_half = HandleInt,
                                                   outgoing_half = unattached},
                             Pairs0)
            end,
    MaxMessageSize = persistent_term:get(max_message_size),
    Link = #management_link{name = LinkName,
                            delivery_count = DeliveryCountInt,
                            credit = ?MAX_MANAGEMENT_LINK_CREDIT,
                            max_message_size = MaxMessageSize},
    State = State0#state{management_link_pairs = Pairs,
                         incoming_management_links = maps:put(HandleInt, Link, Links)},
    Reply = #'v1_0.attach'{
               name = Name,
               handle = Handle,
               %% We are the receiver.
               role = ?AMQP_ROLE_RECEIVER,
               snd_settle_mode = ?V_1_0_SENDER_SETTLE_MODE_SETTLED,
               rcv_settle_mode = ?V_1_0_RECEIVER_SETTLE_MODE_FIRST,
               source = Source,
               target = Target,
               max_message_size = {ulong, MaxMessageSize},
               properties = Properties},
    Flow = #'v1_0.flow'{handle = Handle,
                        delivery_count = DeliveryCount,
                        link_credit = ?UINT(?MAX_MANAGEMENT_LINK_CREDIT)},
    reply0([Reply, Flow], State);

handle_control(#'v1_0.attach'{
                  role = ?AMQP_ROLE_RECEIVER,
                  name = Name = {utf8, LinkName},
                  handle = Handle = ?UINT(HandleInt),
                  source = Source = #'v1_0.source'{address = {utf8, ?MANAGEMENT_NODE_ADDRESS}},
                  target = Target = #'v1_0.target'{address = ClientTerminusAddress},
                  rcv_settle_mode = RcvSettleMode,
                  max_message_size = MaybeMaxMessageSize,
                  properties = Properties
                 } = Attach,
               #state{management_link_pairs = Pairs0,
                      outgoing_management_links = Links
                     } = State0) ->
    ok = validate_attach(Attach),
    ok = check_paired(Properties),
    Pairs = case Pairs0 of
                #{LinkName := #management_link_pair{
                                 client_terminus_address = ClientTerminusAddress,
                                 incoming_half = H,
                                 outgoing_half = unattached} = Pair}
                  when is_integer(H) ->
                    maps:update(LinkName,
                                Pair#management_link_pair{outgoing_half = HandleInt},
                                Pairs0);
                #{LinkName := Other} ->
                    protocol_error(?V_1_0_AMQP_ERROR_PRECONDITION_FAILED,
                                   "received invalid attach ~p for management link pair ~p",
                                   [Attach, Other]);
                _ ->
                    maps:put(LinkName,
                             #management_link_pair{client_terminus_address = ClientTerminusAddress,
                                                   incoming_half = unattached,
                                                   outgoing_half = HandleInt},
                             Pairs0)
            end,
    MaxMessageSize = max_message_size(MaybeMaxMessageSize),
    Link = #management_link{name = LinkName,
                            delivery_count = ?INITIAL_DELIVERY_COUNT,
                            credit = 0,
                            max_message_size = MaxMessageSize},
    State = State0#state{management_link_pairs = Pairs,
                         outgoing_management_links = maps:put(HandleInt, Link, Links)},
    Reply = #'v1_0.attach'{
               name = Name,
               handle = Handle,
               role = ?AMQP_ROLE_SENDER,
               snd_settle_mode = ?V_1_0_SENDER_SETTLE_MODE_SETTLED,
               rcv_settle_mode = RcvSettleMode,
               source = Source,
               target = Target,
               initial_delivery_count = ?UINT(?INITIAL_DELIVERY_COUNT),
               %% Echo back that we will respect the client's requested max-message-size.
               max_message_size = MaybeMaxMessageSize,
               properties = Properties},
    reply0(Reply, State);

handle_control(#'v1_0.attach'{role = ?AMQP_ROLE_SENDER,
                              name = LinkName,
                              handle = Handle = ?UINT(HandleInt),
                              source = Source,
                              snd_settle_mode = SndSettleMode,
                              target = Target,
                              initial_delivery_count = DeliveryCount = ?UINT(DeliveryCountInt)
                             } = Attach,
               State0 = #state{incoming_links = IncomingLinks0,
                               permission_cache = PermCache0,
                               cfg = #cfg{vhost = Vhost,
                                          user = User}}) ->
    ok = validate_attach(Attach),
    case ensure_target(Target, Vhost, User, PermCache0) of
        {ok, Exchange, RoutingKey, QNameBin, PermCache} ->
            IncomingLink = #incoming_link{
                              exchange = Exchange,
                              routing_key = RoutingKey,
                              queue_name_bin = QNameBin,
                              delivery_count = DeliveryCountInt,
                              credit = ?MAX_LINK_CREDIT},
            _Outcomes = outcomes(Source),
            Reply = #'v1_0.attach'{
                       name = LinkName,
                       handle = Handle,
                       source = Source,
                       snd_settle_mode = SndSettleMode,
                       rcv_settle_mode = ?V_1_0_RECEIVER_SETTLE_MODE_FIRST,
                       target = Target,
                       %% We are the receiver.
                       role = ?AMQP_ROLE_RECEIVER,
                       max_message_size = {ulong, persistent_term:get(max_message_size)}},
            Flow = #'v1_0.flow'{handle = Handle,
                                delivery_count = DeliveryCount,
                                link_credit = ?UINT(?MAX_LINK_CREDIT)},
            %%TODO check that handle is not in use for any other open links.
            %%"The handle MUST NOT be used for other open links. An attempt to attach
            %% using a handle which is already associated with a link MUST be responded to
            %% with an immediate close carrying a handle-in-use session-error."
            IncomingLinks = IncomingLinks0#{HandleInt => IncomingLink},
            State = State0#state{incoming_links = IncomingLinks,
                                 permission_cache = PermCache},
            rabbit_global_counters:publisher_created(?PROTOCOL),
            reply0([Reply, Flow], State);
        {error, Reason} ->
            protocol_error(?V_1_0_AMQP_ERROR_INVALID_FIELD,
                           "Attach rejected: ~tp",
                           [Reason])
    end;

handle_control(#'v1_0.attach'{role = ?AMQP_ROLE_RECEIVER,
                              name = LinkName,
                              handle = Handle = ?UINT(HandleInt),
                              source = Source,
                              snd_settle_mode = SndSettleMode,
                              rcv_settle_mode = RcvSettleMode,
                              max_message_size = MaybeMaxMessageSize} = Attach,
               State0 = #state{queue_states = QStates0,
                               outgoing_links = OutgoingLinks0,
                               permission_cache = PermCache0,
                               topic_permission_cache = TopicPermCache0,
                               cfg = #cfg{vhost = Vhost,
                                          user = User = #user{username = Username},
                                          reader_pid = ReaderPid}}) ->
    ok = validate_attach(Attach),
    {SndSettled, EffectiveSndSettleMode} =
    case SndSettleMode of
        ?V_1_0_SENDER_SETTLE_MODE_SETTLED ->
            {true, SndSettleMode};
        _ ->
            %% In the future, we might want to support sender settle
            %% mode mixed where we would expect a settlement from the
            %% client only for durable messages.
            {false, ?V_1_0_SENDER_SETTLE_MODE_UNSETTLED}
    end,
    case ensure_source(Source, Vhost, User, PermCache0, TopicPermCache0) of
        {error, Reason} ->
            protocol_error(?V_1_0_AMQP_ERROR_INVALID_FIELD, "Attach rejected: ~tp", [Reason]);
        {ok, QName = #resource{name = QNameBin}, PermCache1, TopicPermCache} ->
            PermCache = check_resource_access(QName, read, User, PermCache1),
            case rabbit_amqqueue:with(
                   QName,
                   fun(Q) ->
                           try rabbit_amqqueue:check_exclusive_access(Q, ReaderPid)
                           catch exit:#amqp_error{name = resource_locked} ->
                                     %% An exclusive queue can only be consumed from by its declaring connection.
                                     protocol_error(
                                       ?V_1_0_AMQP_ERROR_RESOURCE_LOCKED,
                                       "cannot obtain exclusive access to locked ~s",
                                       [rabbit_misc:rs(QName)])
                           end,
                           QType = amqqueue:get_type(Q),
                           %% Whether credit API v1 or v2 is used is decided only here at link attachment time.
                           %% This decision applies to the whole life time of the link.
                           %% This means even when feature flag credit_api_v2 will be enabled later, this consumer will
                           %% continue to use credit API v1. This is the safest and easiest solution avoiding
                           %% transferring link flow control state (the delivery-count) at runtime from this session
                           %% process to the queue process.
                           %% Eventually, after feature flag credit_api_v2 gets enabled and a subsequent rolling upgrade,
                           %% all consumers will use credit API v2.
                           %% Streams always use credit API v2 since the stream client (rabbit_stream_queue) holds the link
                           %% flow control state. Hence, credit API mixed version isn't an issue for streams.
                           {CreditApiVsn, Mode, DeliveryCount, ClientFlowCtl,
                            QueueFlowCtl, CreditReqInFlight, StashedCreditReq} =
                           case rabbit_feature_flags:is_enabled(credit_api_v2) orelse
                                QType =:= rabbit_stream_queue of
                               true ->
                                   {2,
                                    {credited, ?INITIAL_DELIVERY_COUNT},
                                    credit_api_v2,
                                    #client_flow_ctl{delivery_count = ?INITIAL_DELIVERY_COUNT,
                                                     credit = 0,
                                                     echo = false},
                                    #queue_flow_ctl{delivery_count = ?INITIAL_DELIVERY_COUNT,
                                                    credit = 0,
                                                    drain = false},
                                    false,
                                    none};
                               false ->
                                   {1,
                                    {credited, credit_api_v1},
                                    ?INITIAL_DELIVERY_COUNT,
                                    credit_api_v1,
                                    credit_api_v1,
                                    credit_api_v1,
                                    credit_api_v1}
                           end,
                           Spec = #{no_ack => SndSettled,
                                    channel_pid => self(),
                                    limiter_pid => none,
                                    limiter_active => false,
                                    mode => Mode,
                                    consumer_tag => handle_to_ctag(HandleInt),
                                    exclusive_consume => false,
                                    args => consumer_arguments(Attach),
                                    ok_msg => undefined,
                                    acting_user => Username},
                           case rabbit_queue_type:consume(Q, Spec, QStates0) of
                               {ok, QStates} ->
                                   A = #'v1_0.attach'{
                                          name = LinkName,
                                          handle = Handle,
                                          initial_delivery_count = ?UINT(?INITIAL_DELIVERY_COUNT),
                                          snd_settle_mode = EffectiveSndSettleMode,
                                          rcv_settle_mode = RcvSettleMode,
                                          %% The queue process monitors our session process. When our session process
                                          %% terminates (abnormally) any messages checked out to our session process
                                          %% will be requeued. That's why the we only support RELEASED as the default outcome.
                                          source = Source#'v1_0.source'{
                                                            default_outcome = #'v1_0.released'{},
                                                            outcomes = outcomes(Source)},
                                          role = ?AMQP_ROLE_SENDER,
                                          %% Echo back that we will respect the client's requested max-message-size.
                                          max_message_size = MaybeMaxMessageSize},
                                   MaxMessageSize = max_message_size(MaybeMaxMessageSize),
                                   Link = #outgoing_link{
                                             queue_name = queue_resource(Vhost, QNameBin),
                                             queue_type = QType,
                                             send_settled = SndSettled,
                                             max_message_size = MaxMessageSize,
                                             credit_api_version = CreditApiVsn,
                                             delivery_count = DeliveryCount,
                                             client_flow_ctl = ClientFlowCtl,
                                             queue_flow_ctl = QueueFlowCtl,
                                             at_least_one_credit_req_in_flight = CreditReqInFlight,
                                             stashed_credit_req = StashedCreditReq},
                                   OutgoingLinks = OutgoingLinks0#{HandleInt => Link},
                                   State1 = State0#state{queue_states = QStates,
                                                         outgoing_links = OutgoingLinks,
                                                         permission_cache = PermCache,
                                                         topic_permission_cache = TopicPermCache},
                                   rabbit_global_counters:consumer_created(?PROTOCOL),
                                   {ok, [A], State1};
                               {error, Reason} ->
                                   protocol_error(
                                     ?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                                     "Consuming from ~s failed: ~tp",
                                     [rabbit_misc:rs(QName), Reason]);
                               {protocol_error, _Type, Reason, Args} ->
                                   protocol_error(
                                     ?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                                     Reason, Args)
                           end
                   end) of
                {ok, Reply, State} ->
                    reply0(Reply, State);
                {error, Reason} ->
                    protocol_error(
                      ?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                      "Could not operate on ~s: ~tp",
                      [rabbit_misc:rs(QName), Reason])
            end
    end;

handle_control({Performative = #'v1_0.transfer'{handle = ?UINT(Handle)}, Paylaod},
               State0 = #state{incoming_links = IncomingLinks}) ->
    {Flows, State1} = session_flow_control_received_transfer(State0),

    {Reply, State} =
    case IncomingLinks of
        #{Handle := Link0} ->
            case incoming_link_transfer(Performative, Paylaod, Link0, State1) of
                {ok, Reply0, Link, State2} ->
                    {Reply0, State2#state{incoming_links = IncomingLinks#{Handle := Link}}};
                {error, Reply0} ->
                    %% "When an error occurs at a link endpoint, the endpoint MUST be detached
                    %% with appropriate error information supplied in the error field of the
                    %% detach frame. The link endpoint MUST then be destroyed." [2.6.5]
                    {Reply0, State1#state{incoming_links = maps:remove(Handle, IncomingLinks)}}
            end;
        _ ->
            incoming_mgmt_link_transfer(Performative, Paylaod, State1)
    end,
    reply0(Reply ++ Flows, State);


%% Although the AMQP message format [3.2] requires a body, it is valid to send a transfer frame without payload.
%% For example, when a large multi transfer message is streamed using the ProtonJ2 client, the client could send
%% a final #'v1_0.transfer'{more=false} frame without a payload.
handle_control(Performative = #'v1_0.transfer'{}, State) ->
    handle_control({Performative, <<>>}, State);

%% Flow control. These frames come with two pieces of information:
%% the session window, and optionally, credit for a particular link.
%% We'll deal with each of them separately.
handle_control(#'v1_0.flow'{handle = Handle} = Flow,
               #state{incoming_links = IncomingLinks,
                      outgoing_links = OutgoingLinks,
                      incoming_management_links = IncomingMgmtLinks,
                      outgoing_management_links = OutgoingMgmtLinks
                     } = State0) ->
    State = session_flow_control_received_flow(Flow, State0),
    S = case Handle of
            undefined ->
                %% "If not set, the flow frame is carrying only information
                %% pertaining to the session endpoint." [2.7.4]
                State;
            ?UINT(HandleInt) ->
                %% "If set, indicates that the flow frame carries flow state information
                %% for the local link endpoint associated with the given handle." [2.7.4]
                case OutgoingLinks of
                    #{HandleInt := OutgoingLink} ->
                        handle_outgoing_link_flow_control(OutgoingLink, Flow, State);
                    _ ->
                        case OutgoingMgmtLinks of
                            #{HandleInt := OutgoingMgmtLink} ->
                                handle_outgoing_mgmt_link_flow_control(OutgoingMgmtLink, Flow, State);
                            _ when is_map_key(HandleInt, IncomingLinks) orelse
                                   is_map_key(HandleInt, IncomingMgmtLinks) ->
                                %% We're being told about available messages at the sender.
                                State;
                            _ ->
                                %% "If set to a handle that is not currently associated with
                                %% an attached link, the recipient MUST respond by ending the
                                %% session with an unattached-handle session error." [2.7.4]
                                rabbit_log:warning(
                                  "Received Flow frame for unknown link handle: ~tp", [Flow]),
                                protocol_error(
                                  ?V_1_0_SESSION_ERROR_UNATTACHED_HANDLE,
                                  "Unattached link handle: ~b", [HandleInt])
                        end
                end
        end,
    {noreply, S};

handle_control(Detach = #'v1_0.detach'{handle = ?UINT(HandleInt)},
               State0 = #state{incoming_links = IncomingLinks,
                               outgoing_links = OutgoingLinks0,
                               outgoing_unsettled_map = Unsettled0,
                               outgoing_pending = Pending0,
                               queue_states = QStates0,
                               cfg = #cfg{user = #user{username = Username}}}) ->
    {OutgoingLinks, Unsettled, Pending, QStates} =
    case maps:take(HandleInt, OutgoingLinks0) of
        {#outgoing_link{queue_name = QName}, OutgoingLinks1} ->
            Ctag = handle_to_ctag(HandleInt),
            {Unsettled1, Pending1} = remove_outgoing_link(Ctag, Unsettled0, Pending0),
            case rabbit_amqqueue:lookup(QName) of
                {ok, Q} ->
                    Spec = #{consumer_tag => Ctag,
                             reason => remove,
                             user => Username},
                    case rabbit_queue_type:cancel(Q, Spec, QStates0) of
                        {ok, QStates1} ->
                            {OutgoingLinks1, Unsettled1, Pending1, QStates1};
                        {error, Reason} ->
                            protocol_error(
                              ?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                              "Failed to remove consumer from ~s: ~tp",
                              [rabbit_misc:rs(amqqueue:get_name(Q)), Reason])
                    end;
                {error, not_found} ->
                    {OutgoingLinks1, Unsettled1, Pending1, QStates0}
            end;
        error ->
            {OutgoingLinks0, Unsettled0, Pending0, QStates0}
    end,

    State1 = State0#state{incoming_links = maps:remove(HandleInt, IncomingLinks),
                          outgoing_links = OutgoingLinks,
                          outgoing_unsettled_map = Unsettled,
                          outgoing_pending = Pending,
                          queue_states = QStates},
    State = maybe_detach_mgmt_link(HandleInt, State1),
    maybe_detach_reply(Detach, State, State0),
    publisher_or_consumer_deleted(State, State0),
    {noreply, State};

handle_control(#'v1_0.end'{},
               State0 = #state{cfg = #cfg{writer_pid = WriterPid,
                                          channel_num = Ch}}) ->
    State = send_delivery_state_changes(State0),
    ok = try rabbit_amqp_writer:send_command_sync(WriterPid, Ch, #'v1_0.end'{})
         catch exit:{Reason, {gen_server, call, _ArgList}}
                 when Reason =:= shutdown orelse
                      Reason =:= noproc ->
                   %% AMQP connection and therefore the writer process got already terminated
                   %% before we had the chance to synchronously end the session.
                   ok
         end,
    {stop, normal, State};

handle_control(#'v1_0.disposition'{role = ?AMQP_ROLE_RECEIVER,
                                   first = ?UINT(First),
                                   last = Last0,
                                   state = Outcome,
                                   settled = DispositionSettled} = Disposition,
               #state{outgoing_unsettled_map = UnsettledMap0,
                      queue_states = QStates0} = State0) ->
    Last = case Last0 of
               ?UINT(L) ->
                   L;
               undefined ->
                   %% "If not set, this is taken to be the same as first." [2.7.6]
                   First
           end,
    UnsettledMapSize = map_size(UnsettledMap0),
    case UnsettledMapSize of
        0 ->
            {noreply, State0};
        _ ->
            DispositionRangeSize = diff(Last, First) + 1,
            {Settled, UnsettledMap} =
            case DispositionRangeSize =< UnsettledMapSize of
                true ->
                    %% It is cheaper to iterate over the range of settled delivery IDs.
                    serial_number:foldl(fun settle_delivery_id/2,
                                        {#{}, UnsettledMap0},
                                        First, Last);
                false ->
                    %% It is cheaper to iterate over the outgoing unsettled map.
                    Iter = maps:iterator(UnsettledMap0,
                                         fun(D1, D2) -> compare(D1, D2) =/= greater end),
                    {Settled0, UnsettledList} =
                    maps:fold(
                      fun (DeliveryId,
                           #outgoing_unsettled{queue_name = QName,
                                               consumer_tag = Ctag,
                                               msg_id = MsgId} = Unsettled,
                           {SettledAcc, UnsettledAcc}) ->
                              case serial_number:in_range(DeliveryId, First, Last) of
                                  true ->
                                      SettledAcc1 = maps_update_with(
                                                      {QName, Ctag},
                                                      fun(MsgIds) -> [MsgId | MsgIds] end,
                                                      [MsgId],
                                                      SettledAcc),
                                      {SettledAcc1, UnsettledAcc};
                                  false ->
                                      {SettledAcc, [{DeliveryId, Unsettled} | UnsettledAcc]}
                              end
                      end,
                      {#{}, []}, Iter),
                    {Settled0, maps:from_list(UnsettledList)}
            end,

            SettleOp = settle_op_from_outcome(Outcome),
            {QStates, Actions} =
            maps:fold(
              fun({QName, Ctag}, MsgIdsRev, {QS0, ActionsAcc}) ->
                      MsgIds = lists:reverse(MsgIdsRev),
                      case rabbit_queue_type:settle(QName, SettleOp, Ctag, MsgIds, QS0) of
                          {ok, QS, Actions0} ->
                              messages_acknowledged(SettleOp, QName, QS, MsgIds),
                              {QS, ActionsAcc ++ Actions0};
                          {protocol_error, _ErrorType, Reason, ReasonArgs} ->
                              protocol_error(?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                                             Reason, ReasonArgs)
                      end
              end, {QStates0, []}, Settled),

            State1 = State0#state{outgoing_unsettled_map = UnsettledMap,
                                  queue_states = QStates},
            Reply = case DispositionSettled of
                        true  -> [];
                        false -> [Disposition#'v1_0.disposition'{settled = true,
                                                                 role = ?AMQP_ROLE_SENDER}]
                    end,
            State = handle_queue_actions(Actions, State1),
            reply0(Reply, State)
    end;

handle_control(Frame, _State) ->
    protocol_error(?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                   "Unexpected frame ~tp",
                   [amqp10_framing:pprint(Frame)]).

send_pending(#state{remote_incoming_window = RemoteIncomingWindow,
                    outgoing_pending = Buf0
                   } = State) ->
    case queue:out(Buf0) of
        {empty, _} ->
            State;
        {{value, CreditReply}, Buf}
          when element(1, CreditReply) =:= credit_reply ->
            State1 = State#state{outgoing_pending = Buf},
            State2 = handle_credit_reply(CreditReply, State1),
            send_pending(State2);
        {{value, #'v1_0.flow'{} = Flow0}, Buf} ->
            #cfg{writer_pid = WriterPid,
                 channel_num = Ch} = State#state.cfg,
            State1 = State#state{outgoing_pending = Buf},
            Flow = session_flow_fields(Flow0, State1),
            rabbit_amqp_writer:send_command(WriterPid, Ch, Flow),
            send_pending(State1);
        {{value, Delivery}, Buf} ->
            case RemoteIncomingWindow =:= 0 orelse
                 credit_flow:blocked() of
                true ->
                    State;
                false ->
                    {NewRemoteIncomingWindow, State1} =
                    send_pending_delivery(Delivery, Buf, State),
                    NumTransfersSent = RemoteIncomingWindow - NewRemoteIncomingWindow,
                    State2 = session_flow_control_sent_transfers(NumTransfersSent, State1),
                    %% Recurse to possibly send FLOW frames.
                    send_pending(State2)
            end
    end.

handle_credit_reply(Action = {credit_reply, Ctag, _DeliveryCount, _Credit, _Available, _Drain},
                    State = #state{outgoing_links = OutgoingLinks}) ->
    Handle = ctag_to_handle(Ctag),
    case OutgoingLinks of
        #{Handle := Link} ->
            handle_credit_reply0(Action, Handle, Link, State);
        _ ->
            %% Ignore credit reply for a detached link.
            State
    end.

handle_credit_reply0(
  {credit_reply, Ctag, DeliveryCount, Credit, Available, _Drain = false},
  Handle,
  #outgoing_link{
     client_flow_ctl = #client_flow_ctl{
                          delivery_count = CDeliveryCount,
                          credit = CCredit,
                          echo = CEcho
                         },
     queue_flow_ctl = #queue_flow_ctl{
                         delivery_count = QDeliveryCount
                        } = QFC0,
     stashed_credit_req = StashedCreditReq
    } = Link0,
  #state{outgoing_links = OutgoingLinks,
         queue_states = QStates0
        } = S0) ->

    %% Assertion: Our (receiver) delivery-count should be always
    %% in sync with the delivery-count of the sending queue.
    QDeliveryCount = DeliveryCount,

    case StashedCreditReq of
        #credit_req{} ->
            %% We prioritise the stashed client request over finishing the current
            %% top-up rounds because the latest link state from the client applies.
            S = pop_credit_req(Handle, Ctag, Link0, S0),
            echo(CEcho, Handle, CDeliveryCount, CCredit, Available, S),
            S;
        none when Credit =:= 0 andalso
                  CCredit > 0 ->
            QName = Link0#outgoing_link.queue_name,
            %% Provide queue next batch of credits.
            CappedCredit = cap_credit(CCredit),
            {ok, QStates, Actions} =
            rabbit_queue_type:credit(
              QName, Ctag, DeliveryCount, CappedCredit, false, QStates0),
            Link = Link0#outgoing_link{
                     queue_flow_ctl = QFC0#queue_flow_ctl{credit = CappedCredit},
                     at_least_one_credit_req_in_flight = true},
            S = S0#state{queue_states = QStates,
                         outgoing_links = OutgoingLinks#{Handle := Link}},
            handle_queue_actions(Actions, S);
        none ->
            %% Although we (the receiver) usually determine link credit, we set here
            %% our link credit to what the queue says our link credit is (which is safer
            %% in case credit requests got applied out of order in quorum queues).
            %% This should be fine given that we asserted earlier that our delivery-count is
            %% in sync with the delivery-count of the sending queue.
            QFC = QFC0#queue_flow_ctl{credit = Credit},
            Link = Link0#outgoing_link{
                     queue_flow_ctl = QFC,
                     at_least_one_credit_req_in_flight = false},
            S = S0#state{outgoing_links = OutgoingLinks#{Handle := Link}},
            echo(CEcho, Handle, CDeliveryCount, CCredit, Available, S),
            S
    end;
handle_credit_reply0(
  {credit_reply, Ctag, DeliveryCount, Credit, Available, _Drain = true},
  Handle,
  Link0 = #outgoing_link{
             queue_name = QName,
             client_flow_ctl = #client_flow_ctl{
                                  delivery_count = CDeliveryCount0,
                                  credit = CCredit
                                 } = CFC,
             queue_flow_ctl = #queue_flow_ctl{
                                 delivery_count = QDeliveryCount0
                                } = QFC,
             stashed_credit_req = StashedCreditReq},
  S0 = #state{cfg = #cfg{writer_pid = Writer,
                         channel_num = ChanNum},
              outgoing_links = OutgoingLinks,
              queue_states = QStates0}) ->
    %% If the queue sent us a drain credit_reply,
    %% the queue must have consumed all our granted credit.
    0 = Credit,

    case DeliveryCount =:= QDeliveryCount0 andalso
         CCredit > 0 of
        true ->
            %% We're in drain mode. The queue did not advance its delivery-count which means
            %% it might still have messages available for us. The client also desires more messages.
            %% Therefore, we do the next round of credit top-up. We prioritise finishing
            %% the current drain credit top-up rounds over a stashed credit request because
            %% this is easier to reason about and the queue will reply promptly meaning
            %% the stashed request will be processed soon enough.
            CappedCredit = cap_credit(CCredit),
            {ok, QStates, Actions} = rabbit_queue_type:credit(
                                       QName, Ctag, DeliveryCount,
                                       CappedCredit, true, QStates0),
            Link = Link0#outgoing_link{
                     queue_flow_ctl = QFC#queue_flow_ctl{credit = CappedCredit},
                     at_least_one_credit_req_in_flight = true},
            S = S0#state{queue_states = QStates,
                         outgoing_links = OutgoingLinks#{Handle := Link}},
            handle_queue_actions(Actions, S);
        false ->
            case compare(DeliveryCount, QDeliveryCount0) of
                equal -> ok;
                greater -> ok; %% the sending queue advanced its delivery-count
                less -> error({unexpected_delivery_count, DeliveryCount, QDeliveryCount0})
            end,

            %% We're in drain mode.
            %% The queue either advanced its delivery-count which means it has
            %% no more messages available for us, or the client does not desire more messages.
            %% Therefore, we're done with draining and we "the sender will (after sending
            %% all available messages) advance the delivery-count as much as possible,
            %% consuming all link-credit, and send the flow state to the receiver."
            CDeliveryCount = add(CDeliveryCount0, CCredit),
            Flow0 = #'v1_0.flow'{handle = ?UINT(Handle),
                                 delivery_count = ?UINT(CDeliveryCount),
                                 link_credit = ?UINT(0),
                                 drain = true,
                                 available = ?UINT(Available)},
            Flow = session_flow_fields(Flow0, S0),
            rabbit_amqp_writer:send_command(Writer, ChanNum, Flow),
            Link = Link0#outgoing_link{
                     client_flow_ctl = CFC#client_flow_ctl{
                                         delivery_count = CDeliveryCount,
                                         credit = 0},
                     queue_flow_ctl = QFC#queue_flow_ctl{
                                        delivery_count = DeliveryCount,
                                        credit = 0,
                                        drain = false},
                     at_least_one_credit_req_in_flight = false
                    },
            S = S0#state{outgoing_links = OutgoingLinks#{Handle := Link}},
            case StashedCreditReq of
                none ->
                    S;
                #credit_req{} ->
                    pop_credit_req(Handle, Ctag, Link, S)
            end
    end.

pop_credit_req(
  Handle, Ctag,
  Link0 = #outgoing_link{
             queue_name = QName,
             client_flow_ctl = #client_flow_ctl{
                                  delivery_count = CDeliveryCount
                                 } = CFC,
             queue_flow_ctl = #queue_flow_ctl{
                                 delivery_count = QDeliveryCount
                                } = QFC,
             stashed_credit_req = #credit_req{
                                     delivery_count = DeliveryCountRcv,
                                     credit = LinkCreditRcv,
                                     drain = Drain,
                                     echo = Echo
                                    }},
  S0 = #state{outgoing_links = OutgoingLinks,
              queue_states = QStates0}) ->
    LinkCreditSnd = amqp10_util:link_credit_snd(
                      DeliveryCountRcv, LinkCreditRcv, CDeliveryCount),
    CappedCredit = cap_credit(LinkCreditSnd),
    {ok, QStates, Actions} = rabbit_queue_type:credit(
                               QName, Ctag, QDeliveryCount,
                               CappedCredit, Drain, QStates0),
    Link = Link0#outgoing_link{
             client_flow_ctl = CFC#client_flow_ctl{
                                 credit = LinkCreditSnd,
                                 echo = Echo},
             queue_flow_ctl = QFC#queue_flow_ctl{
                                credit = CappedCredit,
                                drain = Drain},
             at_least_one_credit_req_in_flight = true,
             stashed_credit_req = none
            },
    S = S0#state{queue_states = QStates,
                 outgoing_links = OutgoingLinks#{Handle := Link}},
    handle_queue_actions(Actions, S).

echo(Echo, HandleInt, DeliveryCount, LinkCredit, Available, State) ->
    case Echo of
        true ->
            Flow0 = #'v1_0.flow'{handle = ?UINT(HandleInt),
                                 delivery_count = ?UINT(DeliveryCount),
                                 link_credit = ?UINT(LinkCredit),
                                 available = ?UINT(Available)},
            Flow = session_flow_fields(Flow0, State),
            #cfg{writer_pid = Writer,
                 channel_num = Channel} = State#state.cfg,
            rabbit_amqp_writer:send_command(Writer, Channel, Flow);
        false ->
            ok
    end.

send_pending_delivery(#pending_delivery{
                         frames = Frames,
                         queue_pid = QPid,
                         outgoing_unsettled = #outgoing_unsettled{consumer_tag = Ctag,
                                                                  queue_name = QName}
                        } = Pending,
                      Buf0,
                      #state{remote_incoming_window = Space,
                             outgoing_links = OutgoingLinks,
                             queue_states = QStates,
                             cfg = #cfg{writer_pid = WriterPid,
                                        channel_num = Ch}
                            } = State0) ->
    Handle = ctag_to_handle(Ctag),
    case is_map_key(Handle, OutgoingLinks) of
        true ->
            SendFun = case QPid of
                          credit_api_v2 ->
                              send_fun(WriterPid, Ch);
                          _ ->
                              case rabbit_queue_type:module(QName, QStates) of
                                  {ok, rabbit_classic_queue} ->
                                      %% Classic queue client and classic queue process that
                                      %% communicate via credit API v1 use RabbitMQ internal
                                      %% credit flow control.
                                      fun(Transfer, Sections) ->
                                              rabbit_amqp_writer:send_command_and_notify(
                                                WriterPid, QPid, Ch, Transfer, Sections)
                                      end;
                                  {ok, _QType} ->
                                      send_fun(WriterPid, Ch)
                              end
                      end,
            case send_frames(SendFun, Frames, Space) of
                {sent_all, SpaceLeft} ->
                    State1 = State0#state{outgoing_pending = Buf0},
                    State = sent_pending_delivery(Pending, Handle, State1),
                    {SpaceLeft, State};
                {sent_some, SpaceLeft, Rest} ->
                    Buf = queue:in_r(Pending#pending_delivery{frames = Rest}, Buf0),
                    State = State0#state{outgoing_pending = Buf},
                    {SpaceLeft, State}
            end;
        false ->
            %% Link got detached. Either the client closed the link in which case the queue
            %% already requeued all checked out messages or the queue doesn't exist anymore
            %% in which case there is no point in requeuing this message.
            %% Therefore, ignore (drop) this delivery.
            State = State0#state{outgoing_pending = Buf0},
            {Space, State}
    end;
send_pending_delivery(#pending_management_delivery{frames = Frames} = Pending,
                      Buf0,
                      #state{remote_incoming_window = Space,
                             cfg = #cfg{writer_pid = WriterPid,
                                        channel_num = Ch}
                            } = State0) ->
    SendFun = send_fun(WriterPid, Ch),
    case send_frames(SendFun, Frames, Space) of
        {sent_all, SpaceLeft} ->
            State = State0#state{outgoing_pending = Buf0},
            {SpaceLeft, State};
        {sent_some, SpaceLeft, Rest} ->
            Buf = queue:in_r(Pending#pending_management_delivery{frames = Rest}, Buf0),
            State = State0#state{outgoing_pending = Buf},
            {SpaceLeft, State}
    end.

send_frames(_, [], SpaceLeft) ->
    {sent_all, SpaceLeft};
send_frames(_, Rest, SpaceLeft = 0) ->
    {sent_some, SpaceLeft, Rest};
send_frames(SendFun, [[Transfer, Sections] | Rest] = Frames, SpaceLeft) ->
    case SendFun(Transfer, Sections) of
        ok ->
            send_frames(SendFun, Rest, SpaceLeft - 1);
        {error, blocked} ->
            {sent_some, SpaceLeft, Frames}
    end.

send_fun(WriterPid, Ch) ->
    fun(Transfer, Sections) ->
            rabbit_amqp_writer:send_command(WriterPid, Ch, Transfer, Sections)
    end.

sent_pending_delivery(
  Pending = #pending_delivery{
               outgoing_unsettled = #outgoing_unsettled{
                                       consumer_tag = Ctag,
                                       queue_name = QName}},
  Handle,
  S0 = #state{outgoing_links = OutgoingLinks0,
              queue_states = QStates0}) ->

    #outgoing_link{
       credit_api_version = CreditApiVsn,
       client_flow_ctl = CFC0,
       queue_flow_ctl = QFC0,
       at_least_one_credit_req_in_flight = CreditReqInFlight0
      } = Link0 = maps:get(Handle, OutgoingLinks0),

    S = case CreditApiVsn of
            1 ->
                S0;
            2 ->
                #client_flow_ctl{
                   delivery_count = CDeliveryCount0,
                   credit = CCredit0
                  } = CFC0,
                #queue_flow_ctl{
                   delivery_count = QDeliveryCount0,
                   credit = QCredit0
                  } = QFC0,

                CDeliveryCount = add(CDeliveryCount0, 1),
                %% Even though the spec mandates
                %% "If the link-credit is less than or equal to zero, i.e.,
                %% the delivery-count is the same as or greater than the
                %% delivery-limit, a sender MUST NOT send more messages."
                %% we forced the message through to be sent to the client.
                %% Due to our dual link approach, we don't want to buffer any
                %% messages in the session if the receiving client dynamically
                %% decreased link credit. The alternative is to requeue messages.
                %% "the receiver MAY either handle the excess messages normally
                %% or detach the link with a transfer-limit-exceeded error code."
                CCredit = max(0, CCredit0 - 1),

                QDeliveryCount = add(QDeliveryCount0, 1),
                QCredit1 = max(0, QCredit0 - 1),

                {QCredit, CreditReqInFlight, QStates, Actions} =
                case QCredit1 =:= 0 andalso
                     CCredit > 0 andalso
                     not CreditReqInFlight0 of
                    true ->
                        %% assertion
                        none = Link0#outgoing_link.stashed_credit_req,
                        %% Provide queue next batch of credits.
                        CappedCredit = cap_credit(CCredit),
                        {ok, QStates1, Actions0} =
                        rabbit_queue_type:credit(
                          QName, Ctag, QDeliveryCount, CappedCredit,
                          QFC0#queue_flow_ctl.drain, QStates0),
                        {CappedCredit, true, QStates1, Actions0};
                    false ->
                        {QCredit1, CreditReqInFlight0, QStates0, []}
                end,

                CFC = CFC0#client_flow_ctl{
                        delivery_count = CDeliveryCount,
                        credit = CCredit},
                QFC = QFC0#queue_flow_ctl{
                        delivery_count = QDeliveryCount,
                        credit = QCredit},
                Link = Link0#outgoing_link{
                         client_flow_ctl = CFC,
                         queue_flow_ctl = QFC,
                         at_least_one_credit_req_in_flight = CreditReqInFlight},
                OutgoingLinks = OutgoingLinks0#{Handle := Link},
                S1 = S0#state{outgoing_links = OutgoingLinks,
                              queue_states = QStates},
                handle_queue_actions(Actions, S1)
        end,
    record_outgoing_unsettled(Pending, S).

record_outgoing_unsettled(#pending_delivery{queue_ack_required = true,
                                            delivery_id = DeliveryId,
                                            outgoing_unsettled = Unsettled},
                          #state{outgoing_unsettled_map = Map0} = State) ->
    %% Record by DeliveryId such that we will ack this message to the queue
    %% once we receive the DISPOSITION from the AMQP client.
    Map = Map0#{DeliveryId => Unsettled},
    State#state{outgoing_unsettled_map = Map};
record_outgoing_unsettled(#pending_delivery{queue_ack_required = false}, State) ->
    %% => 'snd-settle-mode' at attachment must have been 'settled'.
    %% => 'settled' field in TRANSFER must have been 'true'.
    %% => AMQP client won't ack this message.
    %% Also, queue client already acked to queue on behalf of us.
    State.

reply0([], State) ->
    {noreply, State};
reply0(Reply, State) ->
    {reply, session_flow_fields(Reply, State), State}.

%% Implements section "receiving a transfer" in 2.5.6
session_flow_control_received_transfer(
  #state{next_incoming_id = NextIncomingId,
         incoming_window = InWindow0,
         remote_outgoing_window = RemoteOutgoingWindow,
         cfg = #cfg{incoming_window_margin = Margin,
                    resource_alarms = Alarms,
                    max_incoming_window = MaxIncomingWindow}
        } = State) ->
    InWindow1 = InWindow0 - 1,
    case InWindow1 < -Margin of
        true ->
            protocol_error(
              ?V_1_0_SESSION_ERROR_WINDOW_VIOLATION,
              "incoming window violation (tolerated excess tranfers: ~b)",
              [Margin]);
        false ->
            ok
    end,
    {Flows, InWindow} = case InWindow1 =< (MaxIncomingWindow div 2) andalso
                             sets:is_empty(Alarms) of
                            true ->
                                %% We've reached halfway and there are no
                                %% disk or memory alarm, open the window.
                                {[#'v1_0.flow'{}], MaxIncomingWindow};
                            false ->
                                {[], InWindow1}
                        end,
    {Flows, State#state{incoming_window = InWindow,
                        next_incoming_id = add(NextIncomingId, 1),
                        remote_outgoing_window = RemoteOutgoingWindow - 1}}.

%% Implements section "sending a transfer" in 2.5.6
session_flow_control_sent_transfers(
  NumTransfers,
  #state{remote_incoming_window = RemoteIncomingWindow,
         next_outgoing_id = NextOutgoingId} = State) ->
    State#state{remote_incoming_window = RemoteIncomingWindow - NumTransfers,
                next_outgoing_id = add(NextOutgoingId, NumTransfers)}.

settle_delivery_id(Current, {Settled, Unsettled} = Acc) ->
    case maps:take(Current, Unsettled) of
        {#outgoing_unsettled{queue_name = QName,
                             consumer_tag = Ctag,
                             msg_id = MsgId}, Unsettled1} ->
            Settled1 = maps_update_with(
                         {QName, Ctag},
                         fun(MsgIds) -> [MsgId | MsgIds] end,
                         [MsgId],
                         Settled),
            {Settled1, Unsettled1};
        error ->
            Acc
    end.

settle_op_from_outcome(#'v1_0.accepted'{}) ->
    complete;
settle_op_from_outcome(#'v1_0.rejected'{}) ->
    discard;
settle_op_from_outcome(#'v1_0.released'{}) ->
    requeue;
%% Keep the same Modified behaviour as in RabbitMQ 3.x
settle_op_from_outcome(#'v1_0.modified'{delivery_failed = true,
                                        undeliverable_here = UndelHere})
  when UndelHere =/= true ->
    requeue;
settle_op_from_outcome(#'v1_0.modified'{}) ->
    %% If delivery_failed is not true, we can't increment its delivery_count.
    %% So, we will have to reject without requeue.
    %%
    %% If undeliverable_here is true, this is not quite correct because
    %% undeliverable_here refers to the link, and not the message in general.
    %% However, we cannot filter messages from being assigned to individual consumers.
    %% That's why we will have to reject it without requeue.
    discard;
settle_op_from_outcome(Outcome) ->
    protocol_error(
      ?V_1_0_AMQP_ERROR_INVALID_FIELD,
      "Unrecognised state: ~tp in DISPOSITION",
      [Outcome]).

-spec flow({uint, link_handle()}, sequence_no()) ->
    #'v1_0.flow'{}.
flow(Handle, DeliveryCount) ->
    flow(Handle, DeliveryCount, ?MAX_LINK_CREDIT).

-spec flow({uint, link_handle()}, sequence_no(), rabbit_queue_type:credit()) ->
    #'v1_0.flow'{}.
flow(Handle, DeliveryCount, LinkCredit) ->
    #'v1_0.flow'{handle = Handle,
                 delivery_count = ?UINT(DeliveryCount),
                 link_credit = ?UINT(LinkCredit)}.

session_flow_fields(Frames, State)
  when is_list(Frames) ->
    [session_flow_fields(F, State) || F <- Frames];
session_flow_fields(Flow = #'v1_0.flow'{},
                    #state{next_outgoing_id = NextOutgoingId,
                           next_incoming_id = NextIncomingId,
                           incoming_window = IncomingWindow}) ->
    Flow#'v1_0.flow'{
           next_outgoing_id = ?UINT(NextOutgoingId),
           outgoing_window = ?UINT_OUTGOING_WINDOW,
           next_incoming_id = ?UINT(NextIncomingId),
           incoming_window = ?UINT(IncomingWindow)};
session_flow_fields(Frame, _State) ->
    Frame.

%% Implements section "receiving a flow" in 2.5.6
session_flow_control_received_flow(
  #'v1_0.flow'{next_incoming_id = FlowNextIncomingId,
               incoming_window = ?UINT(FlowIncomingWindow),
               next_outgoing_id = ?UINT(FlowNextOutgoingId),
               outgoing_window = ?UINT(FlowOutgoingWindow)},
  #state{next_outgoing_id = NextOutgoingId} = State) ->

    Seq = case FlowNextIncomingId of
              ?UINT(Id) ->
                  case compare(Id, NextOutgoingId) of
                      greater ->
                          protocol_error(
                            ?V_1_0_SESSION_ERROR_WINDOW_VIOLATION,
                            "next-incoming-id from FLOW (~b) leads next-outgoing-id (~b)",
                            [Id, NextOutgoingId]);
                      _ ->
                          Id
                  end;
              undefined ->
                  %% The AMQP client might not have yet received our #begin.next_outgoing_id
                  ?INITIAL_OUTGOING_TRANSFER_ID
          end,

    RemoteIncomingWindow0 = diff(add(Seq, FlowIncomingWindow), NextOutgoingId),
    %% RemoteIncomingWindow0 can be negative, for example if we sent a TRANSFER to the
    %% client between the point in time the client sent us a FLOW with updated
    %% incoming_window=0 and we received that FLOW. Whether 0 or negative doesn't matter:
    %% In both cases we're blocked sending more TRANSFERs to the client until it sends us
    %% a new FLOW with a positive incoming_window. For better understandibility
    %% across the code base, we ensure a floor of 0 here.
    RemoteIncomingWindow = max(0, RemoteIncomingWindow0),

    State#state{next_incoming_id = FlowNextOutgoingId,
                remote_outgoing_window = FlowOutgoingWindow,
                remote_incoming_window = RemoteIncomingWindow}.

% TODO: validate effective settle modes against
%       those declared during attach

% TODO: handle aborted transfers

handle_queue_event({queue_event, QRef, Evt},
                   #state{queue_states = QStates0} = S0) ->
    case rabbit_queue_type:handle_event(QRef, Evt, QStates0) of
        {ok, QStates1, Actions} ->
            S = S0#state{queue_states = QStates1},
            handle_queue_actions(Actions, S);
        {eol, Actions} ->
            S = handle_queue_actions(Actions, S0),
            S#state{stashed_eol = [QRef | S#state.stashed_eol]};
        {protocol_error, _Type, Reason, ReasonArgs} ->
            protocol_error(?V_1_0_AMQP_ERROR_INTERNAL_ERROR, Reason, ReasonArgs)
    end.

handle_queue_actions(Actions, State) ->
    lists:foldl(
      fun ({settled, _QName, _DelIds} = Action, S = #state{stashed_settled = As}) ->
              S#state{stashed_settled = [Action | As]};
          ({rejected, _QName, _DelIds} = Action, S = #state{stashed_rejected = As}) ->
              S#state{stashed_rejected = [Action | As]};
          ({deliver, CTag, AckRequired, Msgs}, S0) ->
              lists:foldl(fun(Msg, S) ->
                                  handle_deliver(CTag, AckRequired, Msg, S)
                          end, S0, Msgs);
          ({credit_reply, _Ctag, _DeliveryCount, _Credit, _Available, _Drain} = Action,
           S = #state{outgoing_pending = Pending}) ->
              %% credit API v2
              S#state{outgoing_pending = queue:in(Action, Pending)};
          ({credit_reply_v1, Ctag, Credit0, Available, Drain},
           S0 = #state{outgoing_links = OutgoingLinks0,
                       outgoing_pending = Pending}) ->
              %% credit API v1
              %% Delete this branch when feature flag credit_api_v2 becomes required.
              Handle = ctag_to_handle(Ctag),
              Link = #outgoing_link{delivery_count = Count0} = maps:get(Handle, OutgoingLinks0),
              {Count, Credit, S} = case Drain of
                                       true ->
                                           Count1 = add(Count0, Credit0),
                                           OutgoingLinks = maps:update(
                                                             Handle,
                                                             Link#outgoing_link{delivery_count = Count1},
                                                             OutgoingLinks0),
                                           S1 = S0#state{outgoing_links = OutgoingLinks},
                                           {Count1, 0, S1};
                                       false ->
                                           {Count0, Credit0, S0}
                                   end,
              Flow = #'v1_0.flow'{
                        handle = ?UINT(Handle),
                        delivery_count = ?UINT(Count),
                        link_credit = ?UINT(Credit),
                        available = ?UINT(Available),
                        drain = Drain},
              S#state{outgoing_pending = queue:in(Flow, Pending)};
          ({queue_down, QName}, S = #state{stashed_down = L}) ->
              S#state{stashed_down = [QName | L]};
          ({Action, _QName}, S)
            when Action =:= block orelse
                 Action =:= unblock ->
              %% Ignore since we rely on our own mechanism to detect if a client sends to fast
              %% into a link: If the number of outstanding queue confirmations grows,
              %% we won't grant new credits to publishers.
              S
      end, State, Actions).

handle_deliver(ConsumerTag, AckRequired,
               Msg = {QName, QPid0, MsgId, Redelivered, Mc0},
               State = #state{outgoing_pending = Pending,
                              outgoing_delivery_id = DeliveryId,
                              outgoing_links = OutgoingLinks0,
                              cfg = #cfg{outgoing_max_frame_size = MaxFrameSize,
                                         conn_name = ConnName,
                                         channel_num = ChannelNum,
                                         user = #user{username = Username},
                                         trace_state = Trace}}) ->
    Handle = ctag_to_handle(ConsumerTag),
    case OutgoingLinks0 of
        #{Handle := #outgoing_link{queue_type = QType,
                                   send_settled = SendSettled,
                                   max_message_size = MaxMessageSize,
                                   credit_api_version = CreditApiVsn,
                                   delivery_count = DelCount} = Link0} ->
            Dtag = delivery_tag(MsgId, SendSettled),
            Transfer = #'v1_0.transfer'{
                          handle = ?UINT(Handle),
                          delivery_id = ?UINT(DeliveryId),
                          delivery_tag = {binary, Dtag},
                          message_format = ?UINT(?MESSAGE_FORMAT),
                          settled = SendSettled},
            Mc1 = mc:convert(mc_amqp, Mc0),
            Mc = mc:set_annotation(redelivered, Redelivered, Mc1),
            Sections = mc:protocol_state(Mc),
            validate_message_size(Sections, MaxMessageSize),
            Frames = transfer_frames(Transfer, Sections, MaxFrameSize),
            messages_delivered(Redelivered, QType),
            rabbit_trace:tap_out(Msg, ConnName, ChannelNum, Username, Trace),
            {OutgoingLinks, QPid
            } = case CreditApiVsn of
                    2 ->
                        {OutgoingLinks0, credit_api_v2};
                    1 ->
                        DelCount = Link0#outgoing_link.delivery_count,
                        Link = Link0#outgoing_link{delivery_count = add(DelCount, 1)},
                        OutgoingLinks1 = maps:update(Handle, Link, OutgoingLinks0),
                        {OutgoingLinks1, QPid0}
                end,
            Del = #outgoing_unsettled{
                     msg_id = MsgId,
                     consumer_tag = ConsumerTag,
                     queue_name = QName},
            PendingDelivery = #pending_delivery{
                                 frames = Frames,
                                 queue_ack_required = AckRequired,
                                 queue_pid = QPid,
                                 delivery_id = DeliveryId,
                                 outgoing_unsettled = Del},
            State#state{outgoing_pending = queue:in(PendingDelivery, Pending),
                        outgoing_delivery_id = add(DeliveryId, 1),
                        outgoing_links = OutgoingLinks};
        _ ->
            %% TODO handle missing link -- why does the queue think it's there?
            rabbit_log:warning(
              "No link handle ~b exists for delivery with consumer tag ~p from queue ~tp",
              [Handle, ConsumerTag, QName]),
            State
    end.

%% "The delivery-tag MUST be unique amongst all deliveries that could be
%% considered unsettled by either end of the link." [2.6.12]
delivery_tag(MsgId, _)
  when is_integer(MsgId) ->
    %% We use MsgId (the consumer scoped sequence number from the queue) as
    %% delivery-tag since delivery-tag must be unique only per link (not per session).
    %% "A delivery-tag can be up to 32 octets of binary data." [2.8.7]
    case MsgId =< ?UINT_MAX of
        true -> <<MsgId:32>>;
        false -> <<MsgId:64>>
    end;
delivery_tag(undefined, true) ->
    %% Both ends of the link will always consider this message settled because
    %% "the sender will send all deliveries settled to the receiver" [3.8.2].
    %% Hence, the delivery tag does not have to be unique on this link.
    %% However, the spec still mandates to send a delivery tag.
    <<>>;
%% Message comes from a (classic) priority queue.
delivery_tag({Priority, undefined}, true)
  when is_integer(Priority) ->
    <<>>;
delivery_tag(MsgId = {Priority, Seq}, _)
  when is_integer(Priority) andalso
       is_integer(Seq) ->
    term_to_binary(MsgId).

%%%%%%%%%%%%%%%%%%%%%
%%% Incoming Link %%%
%%%%%%%%%%%%%%%%%%%%%

incoming_mgmt_link_transfer(
  #'v1_0.transfer'{
     settled = Settled,
     more = More,
     handle = IncomingHandle = ?UINT(IncomingHandleInt)},
  Request,
  #state{management_link_pairs = LinkPairs,
         incoming_management_links = IncomingLinks,
         outgoing_management_links = OutgoingLinks,
         outgoing_pending = Pending,
         outgoing_delivery_id = OutgoingDeliveryId,
         permission_cache = PermCache0,
         topic_permission_cache = TopicPermCache0,
         cfg = #cfg{outgoing_max_frame_size = MaxFrameSize,
                    vhost = Vhost,
                    user = User,
                    reader_pid = ReaderPid}
        } = State0) ->
    IncomingLink0 = case maps:find(IncomingHandleInt, IncomingLinks) of
                        {ok, Link} ->
                            Link;
                        error ->
                            protocol_error(
                              ?V_1_0_SESSION_ERROR_UNATTACHED_HANDLE,
                              "Unknown link handle: ~p", [IncomingHandleInt])
                    end,
    %% We only allow settled management requests
    %% given that we are going to send a response anyway.
    true = Settled,
    %% In the current implementation, we disallow large incoming management request messages.
    false = default(More, false),
    #management_link{name = Name,
                     delivery_count = IncomingDeliveryCount0,
                     credit = IncomingCredit0,
                     max_message_size = IncomingMaxMessageSize
                    } = IncomingLink0,
    case IncomingCredit0 > 0 of
        true ->
            ok;
        false ->
            protocol_error(
              ?V_1_0_LINK_ERROR_TRANSFER_LIMIT_EXCEEDED,
              "insufficient credit (~b) for management link from client to RabbitMQ",
              [IncomingCredit0])
    end,
    #management_link_pair{
       incoming_half = IncomingHandleInt,
       outgoing_half = OutgoingHandleInt
      } = maps:get(Name, LinkPairs),
    OutgoingLink0 = case OutgoingHandleInt of
                        unattached ->
                            protocol_error(
                              ?V_1_0_AMQP_ERROR_PRECONDITION_FAILED,
                              "received transfer on half open management link pair", []);
                        _ ->
                            maps:get(OutgoingHandleInt, OutgoingLinks)
                    end,
    #management_link{name = Name,
                     delivery_count = OutgoingDeliveryCount,
                     credit = OutgoingCredit,
                     max_message_size = OutgoingMaxMessageSize} = OutgoingLink0,
    case OutgoingCredit > 0 of
        true ->
            ok;
        false ->
            protocol_error(
              ?V_1_0_AMQP_ERROR_PRECONDITION_FAILED,
              "insufficient credit (~b) for management link from RabbitMQ to client",
              [OutgoingCredit])
    end,
    validate_message_size(Request, IncomingMaxMessageSize),
    {Response,
     {PermCache, TopicPermCache}} = rabbit_amqp_management:handle_request(
                                      Request, Vhost, User, ReaderPid, {PermCache0, TopicPermCache0}),

    Transfer = #'v1_0.transfer'{
                  handle = ?UINT(OutgoingHandleInt),
                  delivery_id = ?UINT(OutgoingDeliveryId),
                  delivery_tag = {binary, <<>>},
                  message_format = ?UINT(?MESSAGE_FORMAT),
                  settled = true},
    validate_message_size(Response, OutgoingMaxMessageSize),
    Frames = transfer_frames(Transfer, Response, MaxFrameSize),
    PendingDelivery = #pending_management_delivery{frames = Frames},
    IncomingDeliveryCount = add(IncomingDeliveryCount0, 1),
    IncomingCredit1 = IncomingCredit0 - 1,
    {IncomingCredit, Reply} = maybe_grant_mgmt_link_credit(
                                IncomingCredit1, IncomingDeliveryCount, IncomingHandle),
    IncomingLink = IncomingLink0#management_link{delivery_count = IncomingDeliveryCount,
                                                 credit = IncomingCredit},
    OutgoingLink = OutgoingLink0#management_link{delivery_count = add(OutgoingDeliveryCount, 1),
                                                 credit = OutgoingCredit - 1},
    State = State0#state{
              outgoing_delivery_id = add(OutgoingDeliveryId, 1),
              outgoing_pending = queue:in(PendingDelivery, Pending),
              incoming_management_links = maps:update(IncomingHandleInt, IncomingLink, IncomingLinks),
              outgoing_management_links = maps:update(OutgoingHandleInt, OutgoingLink, OutgoingLinks),
              permission_cache = PermCache,
              topic_permission_cache = TopicPermCache},
    {Reply, State}.

incoming_link_transfer(
  #'v1_0.transfer'{more = true,
                   %% "The delivery-id MUST be supplied on the first transfer of a multi-transfer delivery."
                   delivery_id = ?UINT(DeliveryId),
                   settled = Settled},
  MsgPart,
  Link0 = #incoming_link{multi_transfer_msg = undefined},
  State) ->
    %% This is the first transfer of a multi-transfer message.
    Link = Link0#incoming_link{
             multi_transfer_msg = #multi_transfer_msg{
                                     payload_fragments_rev = [MsgPart],
                                     delivery_id = DeliveryId,
                                     %% "If not set on the first (or only) transfer for a (multi-transfer)
                                     %% delivery, then the settled flag MUST be interpreted as being false."
                                     settled = default(Settled, false)}},
    {ok, [], Link, State};
incoming_link_transfer(
  #'v1_0.transfer'{more = true,
                   delivery_id = DeliveryId,
                   settled = Settled},
  MsgPart,
  Link0 = #incoming_link{
             multi_transfer_msg = Multi = #multi_transfer_msg{
                                             payload_fragments_rev = PFR0,
                                             delivery_id = FirstDeliveryId,
                                             settled = FirstSettled}},
  State) ->
    %% This is a continuation transfer with even more transfers to come.
    validate_multi_transfer_delivery_id(DeliveryId, FirstDeliveryId),
    validate_multi_transfer_settled(Settled, FirstSettled),
    PFR = [MsgPart | PFR0],
    validate_incoming_message_size(PFR),
    Link = Link0#incoming_link{multi_transfer_msg = Multi#multi_transfer_msg{payload_fragments_rev = PFR}},
    {ok, [], Link, State};
incoming_link_transfer(
  #'v1_0.transfer'{handle = ?UINT(HandleInt)},
  _,
  #incoming_link{credit = Credit} = Link,
  _)
  when Credit =< 0 ->
    Detach = detach(HandleInt, Link, ?V_1_0_LINK_ERROR_TRANSFER_LIMIT_EXCEEDED),
    {error, [Detach]};
incoming_link_transfer(
  #'v1_0.transfer'{delivery_id = MaybeDeliveryId,
                   delivery_tag = DeliveryTag,
                   settled = MaybeSettled,
                   rcv_settle_mode = RcvSettleMode,
                   handle = Handle = ?UINT(HandleInt)},
  MsgPart,
  #incoming_link{exchange = LinkExchange,
                 routing_key = LinkRKey,
                 delivery_count = DeliveryCount0,
                 incoming_unconfirmed_map = U0,
                 credit = Credit0,
                 multi_transfer_msg = MultiTransfer
                } = Link0,
  State0 = #state{queue_states = QStates0,
                  permission_cache = PermCache0,
                  topic_permission_cache = TopicPermCache0,
                  cfg = #cfg{user = User = #user{username = Username},
                             vhost = Vhost,
                             trace_state = Trace,
                             conn_name = ConnName,
                             channel_num = ChannelNum}}) ->

    {PayloadBin, DeliveryId, Settled} =
    case MultiTransfer of
        undefined ->
            ?UINT(DeliveryId0) = MaybeDeliveryId,
            {MsgPart, DeliveryId0, default(MaybeSettled, false)};
        #multi_transfer_msg{payload_fragments_rev = PFR,
                            delivery_id = FirstDeliveryId,
                            settled = FirstSettled} ->
            MsgBin0 = list_to_binary(lists:reverse([MsgPart | PFR])),
            ok = validate_multi_transfer_delivery_id(MaybeDeliveryId, FirstDeliveryId),
            ok = validate_multi_transfer_settled(MaybeSettled, FirstSettled),
            {MsgBin0, FirstDeliveryId, FirstSettled}
    end,
    validate_transfer_rcv_settle_mode(RcvSettleMode, Settled),
    validate_incoming_message_size(PayloadBin),

    Mc0 = mc:init(mc_amqp, PayloadBin, #{}),
    case lookup_target(LinkExchange, LinkRKey, Mc0, Vhost, User, PermCache0) of
        {ok, X, RoutingKey, Mc1, PermCache} ->
            Mc2 = rabbit_message_interceptor:intercept(Mc1),
            check_user_id(Mc2, User),
            TopicPermCache = check_write_permitted_on_topic(
                               X, User, RoutingKey, TopicPermCache0),
            messages_received(Settled),
            QNames = rabbit_exchange:route(X, Mc2, #{return_binding_keys => true}),
            rabbit_trace:tap_in(Mc2, QNames, ConnName, ChannelNum, Username, Trace),
            Opts = #{correlation => {HandleInt, DeliveryId}},
            Qs0 = rabbit_amqqueue:lookup_many(QNames),
            Qs = rabbit_amqqueue:prepend_extra_bcc(Qs0),
            Mc = ensure_mc_cluster_compat(Mc2),
            case rabbit_queue_type:deliver(Qs, Mc, Opts, QStates0) of
                {ok, QStates, Actions} ->
                    State1 = State0#state{queue_states = QStates,
                                          permission_cache = PermCache,
                                          topic_permission_cache = TopicPermCache},
                    %% Confirms must be registered before processing actions
                    %% because actions may contain rejections of publishes.
                    {U, Reply0} = process_routing_confirm(
                                    Qs, Settled, DeliveryId, U0),
                    State = handle_queue_actions(Actions, State1),
                    DeliveryCount = add(DeliveryCount0, 1),
                    Credit1 = Credit0 - 1,
                    {Credit, Reply1} = maybe_grant_link_credit(
                                         Credit1, DeliveryCount, map_size(U), Handle),
                    Reply = Reply0 ++ Reply1,
                    Link = Link0#incoming_link{
                             delivery_count = DeliveryCount,
                             credit = Credit,
                             incoming_unconfirmed_map = U,
                             multi_transfer_msg = undefined},
                    {ok, Reply, Link, State};
                {error, Reason} ->
                    protocol_error(?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                                   "Failed to deliver message to queues, "
                                   "delivery_tag=~p, delivery_id=~p, reason=~p",
                                   [DeliveryTag, DeliveryId, Reason])
            end;
        {error, #'v1_0.error'{} = Err} ->
            Disposition = released(DeliveryId),
            Detach = detach(HandleInt, Link0, Err),
            {error, [Disposition, Detach]}
    end.

lookup_target(#exchange{} = X, LinkRKey, Mc, _, _, PermCache) ->
    lookup_routing_key(X, LinkRKey, Mc, PermCache);
lookup_target(#resource{} = XName, LinkRKey, Mc, _, _, PermCache) ->
    case rabbit_exchange:lookup(XName) of
        {ok, X} ->
            lookup_routing_key(X, LinkRKey, Mc, PermCache);
        {error, not_found} ->
            {error, error_not_found(XName)}
    end;
lookup_target(to, to, Mc, Vhost, User, PermCache0) ->
    case mc:property(to, Mc) of
        {utf8, String} ->
            case parse_target_v2_string(String) of
                {ok, XNameBin, RKey, _} ->
                    XName = exchange_resource(Vhost, XNameBin),
                    PermCache = check_resource_access(XName, write, User, PermCache0),
                    case rabbit_exchange:lookup(XName) of
                        {ok, X} ->
                            check_internal_exchange(X),
                            lookup_routing_key(X, RKey, Mc, PermCache);
                        {error, not_found} ->
                            {error, error_not_found(XName)}
                    end;
                {error, bad_address} ->
                    {error,
                     #'v1_0.error'{
                        condition = ?V_1_0_AMQP_ERROR_PRECONDITION_FAILED,
                        description = {utf8, <<"bad 'to' address string: ", String/binary>>}}}
            end;
        undefined ->
            {error,
             #'v1_0.error'{
                condition = ?V_1_0_AMQP_ERROR_PRECONDITION_FAILED,
                description = {utf8, <<"anonymous terminus requires 'to' address to be set">>}}}
    end.

lookup_routing_key(X = #exchange{name = #resource{name = XNameBin}},
                   RKey0, Mc0, PermCache) ->
    RKey = case RKey0 of
               subject ->
                   case mc:property(subject, Mc0) of
                       {utf8, Subject} ->
                           Subject;
                       undefined ->
                           <<>>
                   end;
               _ when is_binary(RKey0) ->
                   RKey0
           end,
    Mc1 = mc:set_annotation(?ANN_EXCHANGE, XNameBin, Mc0),
    Mc = mc:set_annotation(?ANN_ROUTING_KEYS, [RKey], Mc1),
    {ok, X, RKey, Mc, PermCache}.

process_routing_confirm([], _SenderSettles = true, _, U) ->
    rabbit_global_counters:messages_unroutable_dropped(?PROTOCOL, 1),
    {U, []};
process_routing_confirm([], _SenderSettles = false, DeliveryId, U) ->
    rabbit_global_counters:messages_unroutable_returned(?PROTOCOL, 1),
    Disposition = released(DeliveryId),
    {U, [Disposition]};
process_routing_confirm([_|_] = Qs, SenderSettles, DeliveryId, U0) ->
    QNames = rabbit_amqqueue:queue_names(Qs),
    false = maps:is_key(DeliveryId, U0),
    Map = maps:from_keys(QNames, ok),
    U = U0#{DeliveryId => {Map, SenderSettles, false}},
    rabbit_global_counters:messages_routed(?PROTOCOL, map_size(Map)),
    {U, []}.

released(DeliveryId) ->
    #'v1_0.disposition'{role = ?AMQP_ROLE_RECEIVER,
                        first = ?UINT(DeliveryId),
                        settled = true,
                        state = #'v1_0.released'{}}.

maybe_grant_link_credit(Credit, DeliveryCount, NumUnconfirmed, Handle) ->
    case grant_link_credit(Credit, NumUnconfirmed) of
        true ->
            {?MAX_LINK_CREDIT, [flow(Handle, DeliveryCount)]};
        false ->
            {Credit, []}
    end.

maybe_grant_link_credit(
  HandleInt,
  Link = #incoming_link{credit = Credit,
                        incoming_unconfirmed_map = U,
                        delivery_count = DeliveryCount},
  AccMap) ->
    case grant_link_credit(Credit, map_size(U)) of
        true ->
            {Link#incoming_link{credit = ?MAX_LINK_CREDIT},
             AccMap#{HandleInt => DeliveryCount}};
        false ->
            {Link, AccMap}
    end.

grant_link_credit(Credit, NumUnconfirmed) ->
    MaxLinkCredit = ?MAX_LINK_CREDIT,
    Credit =< MaxLinkCredit div 2 andalso
    NumUnconfirmed < MaxLinkCredit.

maybe_grant_mgmt_link_credit(Credit, DeliveryCount, Handle)
  when Credit =< ?MAX_MANAGEMENT_LINK_CREDIT div 2 ->
    {?MAX_MANAGEMENT_LINK_CREDIT,
     [flow(Handle, DeliveryCount, ?MAX_MANAGEMENT_LINK_CREDIT)]};
maybe_grant_mgmt_link_credit(Credit, _, _) ->
    {Credit, []}.

-spec ensure_source(#'v1_0.source'{},
                    rabbit_types:vhost(),
                    rabbit_types:user(),
                    permission_cache(),
                    topic_permission_cache()) ->
    {ok, rabbit_amqqueue:name(), permission_cache(), topic_permission_cache()} |
    {error, term()}.
ensure_source(#'v1_0.source'{dynamic = true}, _, _, _, _) ->
    exit_not_implemented("Dynamic sources not supported");
ensure_source(#'v1_0.source'{address = Address,
                             durable = Durable},
              Vhost, User, PermCache, TopicPermCache) ->
    case Address of
        {utf8, <<"/queues/", QNameBinQuoted/binary>>} ->
            %% The only possible v2 source address format is:
            %%  /queues/:queue
            try rabbit_uri:urldecode(QNameBinQuoted) of
                QNameBin ->
                    QName = queue_resource(Vhost, QNameBin),
                    ok = exit_if_absent(QName),
                    {ok, QName, PermCache, TopicPermCache}
            catch error:_ ->
                      {error, {bad_address, Address}}
            end;
        {utf8, SourceAddr} ->
            case address_v1_permitted() of
                true ->
                    ensure_source_v1(SourceAddr, Vhost, User, Durable,
                                     PermCache, TopicPermCache);
                false ->
                    {error, {amqp_address_v1_not_permitted, Address}}
            end;
        _ ->
            {error, {bad_address, Address}}
    end.

ensure_source_v1(Address,
                 Vhost,
                 User = #user{username = Username},
                 Durable,
                 PermCache0,
                 TopicPermCache0) ->
    case rabbit_routing_parser:parse_endpoint(Address, false) of
        {ok, Src} ->
            {QNameBin, PermCache1} = ensure_terminus(source, Src, Vhost, User, Durable, PermCache0),
            case rabbit_routing_parser:parse_routing(Src) of
                {"", QNameList} ->
                    true = string:equal(QNameList, QNameBin),
                    QName = queue_resource(Vhost, QNameBin),
                    {ok, QName, PermCache1, TopicPermCache0};
                {XNameList, RoutingKeyList} ->
                    RoutingKey = unicode:characters_to_binary(RoutingKeyList),
                    XNameBin = unicode:characters_to_binary(XNameList),
                    XName = exchange_resource(Vhost, XNameBin),
                    QName = queue_resource(Vhost, QNameBin),
                    Binding = #binding{source = XName,
                                       destination = QName,
                                       key = RoutingKey},
                    PermCache2 = check_resource_access(QName, write, User, PermCache1),
                    PermCache = check_resource_access(XName, read, User, PermCache2),
                    {ok, X} = rabbit_exchange:lookup(XName),
                    TopicPermCache = check_read_permitted_on_topic(
                                       X, User, RoutingKey, TopicPermCache0),
                    case rabbit_binding:add(Binding, Username) of
                        ok ->
                            {ok, QName, PermCache, TopicPermCache};
                        {error, _} = Err ->
                            Err
                    end
            end;
        {error, _} = Err ->
            Err
    end.

-spec ensure_target(#'v1_0.target'{},
                    rabbit_types:vhost(),
                    rabbit_types:user(),
                    permission_cache()) ->
    {ok,
     rabbit_types:exchange() | rabbit_exchange:name() | to,
     rabbit_types:routing_key() | to | subject,
     rabbit_misc:resource_name() | undefined,
     permission_cache()} |
    {error, term()}.
ensure_target(#'v1_0.target'{dynamic = true}, _, _, _) ->
    exit_not_implemented("Dynamic targets not supported");
ensure_target(#'v1_0.target'{address = Address,
                             durable = Durable},
              Vhost, User, PermCache) ->
    case target_address_version(Address) of
        2 ->
            case ensure_target_v2(Address, Vhost) of
                {ok, to, RKey, QNameBin} ->
                    {ok, to, RKey, QNameBin, PermCache};
                {ok, XNameBin, RKey, QNameBin} ->
                    check_exchange(XNameBin, RKey, QNameBin, User, Vhost, PermCache);
                {error, _} = Err ->
                    Err
            end;
        1 ->
            case address_v1_permitted() of
                true ->
                    case ensure_target_v1(Address, Vhost, User, Durable, PermCache) of
                        {ok, XNameBin, RKey, QNameBin, PermCache1} ->
                            check_exchange(XNameBin, RKey, QNameBin, User, Vhost, PermCache1);
                        {error, _} = Err ->
                            Err
                    end;
                false ->
                    {error, {amqp_address_v1_not_permitted, Address}}
            end
    end.

check_exchange(XNameBin, RKey, QNameBin, User, Vhost, PermCache0) ->
    XName = exchange_resource(Vhost, XNameBin),
    PermCache = check_resource_access(XName, write, User, PermCache0),
    case rabbit_exchange:lookup(XName) of
        {ok, X} ->
            check_internal_exchange(X),
            %% Pre-declared exchanges are protected against deletion and modification.
            %% Let's cache the whole #exchange{} record to save a
            %% rabbit_exchange:lookup(XName) call each time we receive a message.
            Exchange = case XNameBin of
                           ?DEFAULT_EXCHANGE_NAME -> X;
                           <<"amq.", _/binary>> -> X;
                           _ -> XName
                       end,
            {ok, Exchange, RKey, QNameBin, PermCache};
        {error, not_found} ->
            exit_not_found(XName)
    end.

address_v1_permitted() ->
    rabbit_deprecated_features:is_permitted(amqp_address_v1).

target_address_version({utf8, <<"/exchanges/", _/binary>>}) ->
    2;
target_address_version({utf8, <<"/queues/", _/binary>>}) ->
    2;
target_address_version(undefined) ->
    %% anonymous terminus
    %% https://docs.oasis-open.org/amqp/anonterm/v1.0/cs01/anonterm-v1.0-cs01.html#doc-anonymous-relay
    2;
target_address_version(_Address) ->
    1.

%% The possible v2 target address formats are:
%%  /exchanges/:exchange/:routing-key
%%  /exchanges/:exchange
%%  /queues/:queue
%%  <null>
ensure_target_v2({utf8, String}, Vhost) ->
    case parse_target_v2_string(String) of
        {ok, _XNameBin, _RKey, undefined} = Ok ->
            Ok;
        {ok, _XNameBin, _RKey, QNameBin} = Ok ->
            ok = exit_if_absent(queue, Vhost, QNameBin),
            Ok;
        {error, bad_address} ->
            {error, {bad_address_string, String}}
    end;
ensure_target_v2(undefined, _) ->
    %% anonymous terminus
    %% https://docs.oasis-open.org/amqp/anonterm/v1.0/cs01/anonterm-v1.0-cs01.html#doc-anonymous-relay
    {ok, to, to, undefined}.

parse_target_v2_string(String) ->
    try parse_target_v2_string0(String)
    catch error:_ ->
              {error, bad_address}
    end.

parse_target_v2_string0(<<"/exchanges/", Rest/binary>>) ->
    Key = cp_slash,
    Pattern = try persistent_term:get(Key)
              catch error:badarg ->
                        Cp = binary:compile_pattern(<<"/">>),
                        ok = persistent_term:put(Key, Cp),
                        Cp
              end,
    case binary:split(Rest, Pattern, [global]) of
        [?DEFAULT_EXCHANGE_NAME | _] ->
            {error, bad_address};
        [<<"amq.default">> | _] ->
            {error, bad_address};
        [XNameBinQuoted] ->
            XNameBin = rabbit_uri:urldecode(XNameBinQuoted),
            {ok, XNameBin, <<>>, undefined};
        [XNameBinQuoted, RKeyQuoted] ->
            XNameBin = rabbit_uri:urldecode(XNameBinQuoted),
            RKey = rabbit_uri:urldecode(RKeyQuoted),
            {ok, XNameBin, RKey, undefined};
        _ ->
            {error, bad_address}
    end;
parse_target_v2_string0(<<"/queues/">>) ->
    %% empty queue name is invalid
    {error, bad_address};
parse_target_v2_string0(<<"/queues/", QNameBinQuoted/binary>>) ->
    QNameBin = rabbit_uri:urldecode(QNameBinQuoted),
    {ok, ?DEFAULT_EXCHANGE_NAME, QNameBin, QNameBin};
parse_target_v2_string0(_) ->
    {error, bad_address}.

ensure_target_v1({utf8, Address}, Vhost, User, Durable, PermCache0) ->
    case rabbit_routing_parser:parse_endpoint(Address, true) of
        {ok, Dest} ->
            {QNameBin, PermCache} = ensure_terminus(
                                      target, Dest, Vhost, User, Durable, PermCache0),
            {XNameList1, RK} = rabbit_routing_parser:parse_routing(Dest),
            XNameBin = unicode:characters_to_binary(XNameList1),
            RoutingKey = case RK of
                             undefined -> subject;
                             []        -> subject;
                             _         -> unicode:characters_to_binary(RK)
                         end,
            {ok, XNameBin, RoutingKey, QNameBin, PermCache};
        {error, _} = Err ->
            Err
    end;
ensure_target_v1(Address, _, _, _, _) ->
    {error, {bad_address, Address}}.

handle_outgoing_mgmt_link_flow_control(
  #management_link{delivery_count = DeliveryCountSnd} = Link0,
  #'v1_0.flow'{handle = Handle = ?UINT(HandleInt),
               delivery_count = MaybeDeliveryCountRcv,
               link_credit = ?UINT(LinkCreditRcv),
               drain = Drain0,
               echo = Echo0},
  #state{outgoing_management_links = Links0,
         outgoing_pending = Pending
        } = State0) ->
    Drain = default(Drain0, false),
    Echo = default(Echo0, false),
    DeliveryCountRcv = delivery_count_rcv(MaybeDeliveryCountRcv),
    LinkCreditSnd = amqp10_util:link_credit_snd(DeliveryCountRcv, LinkCreditRcv, DeliveryCountSnd),
    {Count, Credit} = case Drain of
                          true -> {add(DeliveryCountSnd, LinkCreditSnd), 0};
                          false -> {DeliveryCountSnd, LinkCreditSnd}
                      end,
    State = case Echo orelse Drain of
                true ->
                    Flow = #'v1_0.flow'{
                              handle = Handle,
                              delivery_count = ?UINT(Count),
                              link_credit = ?UINT(Credit),
                              available = ?UINT(0),
                              drain = Drain},
                    State0#state{outgoing_pending = queue:in(Flow, Pending)};
                false ->
                    State0
            end,
    Link = Link0#management_link{delivery_count = Count,
                                 credit = Credit},
    Links = maps:update(HandleInt, Link, Links0),
    State#state{outgoing_management_links = Links}.

handle_outgoing_link_flow_control(
  #outgoing_link{queue_name = QName,
                 credit_api_version = CreditApiVsn,
                 client_flow_ctl = CFC,
                 queue_flow_ctl = QFC,
                 at_least_one_credit_req_in_flight = CreditReqInFlight
                } = Link0,
  #'v1_0.flow'{handle = ?UINT(HandleInt),
               delivery_count = MaybeDeliveryCountRcv,
               link_credit = ?UINT(LinkCreditRcv),
               drain = Drain0,
               echo = Echo0},
  #state{outgoing_links = OutgoingLinks,
         queue_states = QStates0
        } = State0) ->
    Ctag = handle_to_ctag(HandleInt),
    DeliveryCountRcv = delivery_count_rcv(MaybeDeliveryCountRcv),
    Drain = default(Drain0, false),
    Echo = default(Echo0, false),
    case CreditApiVsn of
        2 ->
            case CreditReqInFlight of
                false ->
                    LinkCreditSnd = amqp10_util:link_credit_snd(
                                      DeliveryCountRcv,
                                      LinkCreditRcv,
                                      CFC#client_flow_ctl.delivery_count),
                    CappedCredit = cap_credit(LinkCreditSnd),
                    Link = Link0#outgoing_link{
                             client_flow_ctl = CFC#client_flow_ctl{
                                                 credit = LinkCreditSnd,
                                                 echo = Echo},
                             queue_flow_ctl = QFC#queue_flow_ctl{
                                                credit = CappedCredit,
                                                drain = Drain},
                             at_least_one_credit_req_in_flight = true},
                    {ok, QStates, Actions} = rabbit_queue_type:credit(
                                               QName, Ctag,
                                               QFC#queue_flow_ctl.delivery_count,
                                               CappedCredit, Drain, QStates0),
                    State = State0#state{
                              queue_states = QStates,
                              outgoing_links = OutgoingLinks#{HandleInt := Link}},
                    handle_queue_actions(Actions, State);
                true ->
                    %% A credit request is currently in-flight. Let's first process its reply
                    %% before sending the next request. This ensures our outgoing_pending
                    %% queue won't contain lots of credit replies for the same consumer
                    %% when the client floods us with credit requests, but closed its incoming-window.
                    %% Processing one credit top up at a time between us and the queue is also easier
                    %% to reason about. Therefore, we stash the new request. If there is already a
                    %% stashed request, we replace it because the latest flow control state from the
                    %% client applies.
                    Link = Link0#outgoing_link{
                             stashed_credit_req = #credit_req{
                                                     delivery_count = DeliveryCountRcv,
                                                     credit = LinkCreditRcv,
                                                     drain = Drain,
                                                     echo = Echo}},
                    State0#state{outgoing_links = OutgoingLinks#{HandleInt := Link}}
            end;
        1 ->
            DeliveryCountSnd = Link0#outgoing_link.delivery_count,
            LinkCreditSnd = amqp10_util:link_credit_snd(
                              DeliveryCountRcv, LinkCreditRcv, DeliveryCountSnd),
            {ok, QStates, Actions} = rabbit_queue_type:credit_v1(
                                       QName, Ctag, LinkCreditSnd, Drain, QStates0),
            State1 = State0#state{queue_states = QStates},
            State = handle_queue_actions(Actions, State1),
            process_credit_reply_sync(Ctag, QName, LinkCreditSnd, State)
    end.

delivery_count_rcv(?UINT(DeliveryCount)) ->
    DeliveryCount;
delivery_count_rcv(undefined) ->
    %% "In the event that the receiver does not yet know the delivery-count,
    %% i.e., delivery-countrcv is unspecified, the sender MUST assume that the
    %% delivery-countrcv is the first delivery-countsnd sent from sender to
    %% receiver, i.e., the delivery-countsnd specified in the flow state carried
    %% by the initial attach frame from the sender to the receiver." [2.6.7]
    ?INITIAL_DELIVERY_COUNT.

%% The AMQP 0.9.1 credit extension was poorly designed because a consumer granting
%% credits to a queue has to synchronously wait for a credit reply from the queue:
%% https://github.com/rabbitmq/rabbitmq-server/blob/b9566f4d02f7ceddd2f267a92d46affd30fb16c8/deps/rabbitmq_codegen/credit_extension.json#L43
%% This blocks our entire AMQP 1.0 session process. Since the credit reply from the
%% queue did not contain the consumr tag prior to feature flag credit_api_v2, we
%% must behave here the same way as non-native AMQP 1.0: We wait until the queue
%% sends us a credit reply sucht that we can correlate that reply with our consumer tag.
process_credit_reply_sync(
  Ctag, QName, Credit, State = #state{queue_states = QStates}) ->
    case rabbit_queue_type:module(QName, QStates) of
        {ok, rabbit_classic_queue} ->
            receive {'$gen_cast',
                     {queue_event,
                      QName,
                      {send_credit_reply, Avail}}} ->
                        Action = {credit_reply_v1, Ctag, Credit, Avail, false},
                        handle_queue_actions([Action], State)
            after ?CREDIT_REPLY_TIMEOUT ->
                      credit_reply_timeout(classic, QName)
            end;
        {ok, rabbit_quorum_queue} ->
            process_credit_reply_sync_quorum_queue(Ctag, QName, Credit, State);
        {error, not_found} ->
            State
    end.

process_credit_reply_sync_quorum_queue(Ctag, QName, Credit, State0) ->
    receive {'$gen_cast',
             {queue_event,
              QName,
              {QuorumQueue,
               {applied,
                Applied0}}}} ->

                {Applied, ReceivedCreditReply}
                = lists:mapfoldl(
                    %% Convert v1 send_credit_reply to credit_reply_v1 action.
                    %% Available refers to *after* and Credit refers to *before*
                    %% quorum queue sends messages.
                    %% We therefore keep the same wrong behaviour of RabbitMQ 3.x.
                    fun({RaIdx, {send_credit_reply, Available}}, _) ->
                            Action = {credit_reply_v1, Ctag, Credit, Available, false},
                            {{RaIdx, Action}, true};
                       ({RaIdx, {multi, [{send_credit_reply, Available},
                                         {send_drained, _} = SendDrained]}}, _) ->
                            Action = {credit_reply_v1, Ctag, Credit, Available, false},
                            {{RaIdx, {multi, [Action, SendDrained]}}, true};
                       (E, Acc) ->
                            {E, Acc}
                    end, false, Applied0),

                Evt = {queue_event, QName, {QuorumQueue, {applied, Applied}}},
                %% send_drained action must be processed by
                %% rabbit_fifo_client to advance the delivery count.
                State = handle_queue_event(Evt, State0),
                case ReceivedCreditReply of
                    true ->
                        State;
                    false ->
                        process_credit_reply_sync_quorum_queue(Ctag, QName, Credit, State)
                end
    after ?CREDIT_REPLY_TIMEOUT ->
              credit_reply_timeout(quorum, QName)
    end.

-spec credit_reply_timeout(atom(), rabbit_types:rabbit_amqqueue_name()) ->
    no_return().
credit_reply_timeout(QType, QName) ->
    Fmt = "Timed out waiting for credit reply from ~s ~s. "
    "Hint: Enable feature flag credit_api_v2",
    Args = [QType, rabbit_misc:rs(QName)],
    rabbit_log:error(Fmt, Args),
    protocol_error(?V_1_0_AMQP_ERROR_INTERNAL_ERROR, Fmt, Args).

default(undefined, Default) -> Default;
default(Thing,    _Default) -> Thing.

transfer_frames(Transfer, Sections, unlimited) ->
    [[Transfer, Sections]];
transfer_frames(Transfer, Sections, MaxFrameSize) ->
    PerformativeSize = iolist_size(amqp10_framing:encode_bin(Transfer)),
    encode_frames(Transfer, Sections, MaxFrameSize - PerformativeSize, []).

encode_frames(_T, _Msg, MaxPayloadSize, _Transfers) when MaxPayloadSize =< 0 ->
    protocol_error(?V_1_0_AMQP_ERROR_FRAME_SIZE_TOO_SMALL,
                   "Frame size is too small by ~b bytes",
                   [-MaxPayloadSize]);
encode_frames(T, Msg, MaxPayloadSize, Transfers) ->
    case iolist_size(Msg) > MaxPayloadSize of
        true ->
            MsgBin = iolist_to_binary(Msg),
            {Chunk, Rest} = split_binary(MsgBin, MaxPayloadSize),
            T1 = T#'v1_0.transfer'{more = true},
            encode_frames(T, Rest, MaxPayloadSize, [[T1, Chunk] | Transfers]);
        false ->
            lists:reverse([[T, Msg] | Transfers])
    end.

consumer_arguments(#'v1_0.attach'{
                      source = #'v1_0.source'{filter = Filter},
                      properties = Properties}) ->
    properties_to_consumer_args(Properties) ++
    filter_to_consumer_args(Filter).

properties_to_consumer_args({map, KVList}) ->
    Key = {symbol, <<"rabbitmq:priority">>},
    case proplists:lookup(Key, KVList) of
        {Key, Val = {int, _Prio}} ->
            [mc_amqpl:to_091(<<"x-priority">>, Val)];
        _ ->
            []
    end;
properties_to_consumer_args(_) ->
    [].

filter_to_consumer_args({map, KVList}) ->
    filter_to_consumer_args(
      [<<"rabbitmq:stream-offset-spec">>,
       <<"rabbitmq:stream-filter">>,
       <<"rabbitmq:stream-match-unfiltered">>],
      KVList,
      []);
filter_to_consumer_args(_) ->
    [].

filter_to_consumer_args([], _KVList, Acc) ->
    Acc;
filter_to_consumer_args([<<"rabbitmq:stream-offset-spec">> = H | T], KVList, Acc) ->
    Key = {symbol, H},
    Arg = case keyfind_unpack_described(Key, KVList) of
              {_, {timestamp, Ts}} ->
                  [{<<"x-stream-offset">>, timestamp, Ts div 1000}]; %% 0.9.1 uses second based timestamps
              {_, {utf8, Spec}} ->
                  [{<<"x-stream-offset">>, longstr, Spec}]; %% next, last, first and "10m" etc
              {_, {_, Offset}} when is_integer(Offset) ->
                  [{<<"x-stream-offset">>, long, Offset}]; %% integer offset
              _ ->
                  []
          end,
    filter_to_consumer_args(T, KVList, Arg ++ Acc);
filter_to_consumer_args([<<"rabbitmq:stream-filter">> = H | T], KVList, Acc) ->
    Key = {symbol, H},
    Arg = case keyfind_unpack_described(Key, KVList) of
              {_, {list, Filters0}} when is_list(Filters0) ->
                  Filters = lists:foldl(fun({utf8, Filter}, L) ->
                                                [{longstr, Filter} | L];
                                           (_, L) ->
                                                L
                                        end, [], Filters0),
                  [{<<"x-stream-filter">>, array, Filters}];
              {_, {utf8, Filter}} ->
                  [{<<"x-stream-filter">>, longstr, Filter}];
              _ ->
                  []
          end,
    filter_to_consumer_args(T, KVList, Arg ++ Acc);
filter_to_consumer_args([<<"rabbitmq:stream-match-unfiltered">> = H | T], KVList, Acc) ->
    Key = {symbol, H},
    Arg = case keyfind_unpack_described(Key, KVList) of
              {_, MU} when is_boolean(MU) ->
                  [{<<"x-stream-match-unfiltered">>, bool, MU}];
              _ ->
                  []
          end,
    filter_to_consumer_args(T, KVList, Arg ++ Acc);
filter_to_consumer_args([_ | T], KVList, Acc) ->
    filter_to_consumer_args(T, KVList, Acc).

keyfind_unpack_described(Key, KvList) ->
    %% filterset values _should_ be described values
    %% they aren't always however for historical reasons so we need this bit of
    %% code to return a plain value for the given filter key
    case lists:keyfind(Key, 1, KvList) of
        {Key, {described, Key, Value}} ->
            {Key, Value};
        {Key, _} = Kv ->
            Kv;
        false ->
            false
    end.

validate_attach(#'v1_0.attach'{target = #'v1_0.coordinator'{}}) ->
    exit_not_implemented("Transactions not supported");
validate_attach(#'v1_0.attach'{unsettled = {map, [_|_]}}) ->
    exit_not_implemented("Link recovery not supported");
validate_attach(#'v1_0.attach'{incomplete_unsettled = true}) ->
    exit_not_implemented("Link recovery not supported");
validate_attach(
  #'v1_0.attach'{snd_settle_mode = SndSettleMode,
                 rcv_settle_mode = ?V_1_0_RECEIVER_SETTLE_MODE_SECOND})
  when SndSettleMode =/= ?V_1_0_SENDER_SETTLE_MODE_SETTLED ->
    exit_not_implemented("rcv-settle-mode second not supported");
validate_attach(#'v1_0.attach'{}) ->
    ok.

validate_multi_transfer_delivery_id(?UINT(Id), Id) ->
    ok;
validate_multi_transfer_delivery_id(undefined, _FirstDeliveryId) ->
    %% "On continuation transfers the delivery-id MAY be omitted."
    ok;
validate_multi_transfer_delivery_id(OtherId, FirstDeliveryId) ->
    %% "It is an error if the delivery-id on a continuation transfer
    %% differs from the delivery-id on the first transfer of a delivery."
    protocol_error(
      ?V_1_0_CONNECTION_ERROR_FRAMING_ERROR,
      "delivery-id of continuation transfer (~p) differs from delivery-id on first transfer (~p)",
      [OtherId, FirstDeliveryId]).

validate_multi_transfer_settled(Settled, Settled)
  when is_boolean(Settled) ->
    ok;
validate_multi_transfer_settled(undefined, Settled)
  when is_boolean(Settled) ->
    ok;
validate_multi_transfer_settled(Other, First)
  when is_boolean(First) ->
    protocol_error(
      ?V_1_0_CONNECTION_ERROR_FRAMING_ERROR,
      "field 'settled' of continuation transfer (~p) differs from "
      "(interpreted) field 'settled' on first transfer (~p)",
      [Other, First]).

%% "If the message is being sent settled by the sender,
%% the value of this field [rcv-settle-mode] is ignored." [2.7.5]
validate_transfer_rcv_settle_mode(?V_1_0_RECEIVER_SETTLE_MODE_SECOND, _Settled = false) ->
    exit_not_implemented("rcv-settle-mode second not supported");
validate_transfer_rcv_settle_mode(_, _) ->
    ok.

validate_incoming_message_size(Message) ->
    validate_message_size(Message, persistent_term:get(max_message_size)).

validate_message_size(_, unlimited) ->
    ok;
validate_message_size(Message, MaxMsgSize)
  when is_integer(MaxMsgSize) ->
    MsgSize = iolist_size(Message),
    case MsgSize =< MaxMsgSize of
        true ->
            ok;
        false ->
            %% "Any attempt to deliver a message larger than this results in a message-size-exceeded link-error." [2.7.3]
            %% We apply that sentence to both incoming messages that are too large for us and outgoing messages that are
            %% too large for the client.
            %% This is an interesting protocol difference to MQTT where we instead discard outgoing messages that are too
            %% large to send then behave as if we had completed sending that message [MQTT 5.0, MQTT-3.1.2-25].
            protocol_error(
              ?V_1_0_LINK_ERROR_MESSAGE_SIZE_EXCEEDED,
              "message size (~b bytes) > maximum message size (~b bytes)",
              [MsgSize, MaxMsgSize])
    end.

-spec ensure_terminus(source | target,
                      term(),
                      rabbit_types:vhost(),
                      rabbit_types:user(),
                      {uint, 0..2},
                      permission_cache()) ->
    {undefined | rabbit_misc:resource_name(),
     permission_cache()}.
ensure_terminus(Type, {exchange, {XNameList, _RoutingKey}}, Vhost, User, Durability, PermCache) ->
    ok = exit_if_absent(exchange, Vhost, XNameList),
    case Type of
        target -> {undefined, PermCache};
        source -> declare_queue(generate_queue_name(), Vhost, User, Durability, PermCache)
    end;
ensure_terminus(target, {topic, _bindingkey}, _, _, _, PermCache) ->
    %% exchange amq.topic exists
    {undefined, PermCache};
ensure_terminus(source, {topic, _BindingKey}, Vhost, User, Durability, PermCache) ->
    %% exchange amq.topic exists
    declare_queue(generate_queue_name(), Vhost, User, Durability, PermCache);
ensure_terminus(target, {queue, undefined}, _, _, _, PermCache) ->
    %% Target "/queue" means publish to default exchange with message subject as routing key.
    %% Default exchange exists.
    {undefined, PermCache};
ensure_terminus(_, {queue, QNameList}, Vhost, User, Durability, PermCache) ->
    declare_queue(unicode:characters_to_binary(QNameList), Vhost, User, Durability, PermCache);
ensure_terminus(_, {amqqueue, QNameList}, Vhost, _, _, PermCache) ->
    %% Target "/amq/queue/" is handled specially due to AMQP legacy:
    %% "Queue names starting with "amq." are reserved for pre-declared and
    %% standardised queues. The client MAY declare a queue starting with "amq."
    %% if the passive option is set, or the queue already exists."
    QNameBin = unicode:characters_to_binary(QNameList),
    ok = exit_if_absent(queue, Vhost, QNameBin),
    {QNameBin, PermCache}.

exit_if_absent(Kind, Vhost, Name) when is_list(Name) ->
    exit_if_absent(Kind, Vhost, unicode:characters_to_binary(Name));
exit_if_absent(Kind, Vhost, Name) when is_binary(Name) ->
    exit_if_absent(rabbit_misc:r(Vhost, Kind, Name)).

exit_if_absent(ResourceName = #resource{kind = Kind}) ->
    Mod = case Kind of
              exchange -> rabbit_exchange;
              queue -> rabbit_amqqueue
          end,
    case Mod:exists(ResourceName) of
        true -> ok;
        false -> exit_not_found(ResourceName)
    end.

generate_queue_name() ->
    rabbit_guid:binary(rabbit_guid:gen_secure(), "amq.gen").

declare_queue(QNameBin,
              Vhost,
              User = #user{username = Username},
              TerminusDurability,
              PermCache0) ->
    QName = queue_resource(Vhost, QNameBin),
    PermCache = check_resource_access(QName, configure, User, PermCache0),
    rabbit_core_metrics:queue_declared(QName),
    Q0 = amqqueue:new(QName,
                      _Pid = none,
                      queue_is_durable(TerminusDurability),
                      _AutoDelete = false,
                      _QOwner = none,
                      _QArgs = [],
                      Vhost,
                      #{user => Username},
                      rabbit_classic_queue),
    case rabbit_queue_type:declare(Q0, node()) of
        {new, _Q}  ->
            rabbit_core_metrics:queue_created(QName);
        {existing, _Q} ->
            ok;
        {error, queue_limit_exceeded, Reason, ReasonArgs} ->
            protocol_error(
              ?V_1_0_AMQP_ERROR_RESOURCE_LIMIT_EXCEEDED,
              Reason,
              ReasonArgs);
        Other ->
            protocol_error(?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                           "Failed to declare ~s: ~p",
                           [rabbit_misc:rs(QName), Other])
    end,
    {QNameBin, PermCache}.

outcomes(#'v1_0.source'{outcomes = undefined}) ->
    {array, symbol, ?OUTCOMES};
outcomes(#'v1_0.source'{outcomes = {array, symbol, Syms} = Outcomes}) ->
    case lists:filter(fun(O) -> not lists:member(O, ?OUTCOMES) end, Syms) of
        [] ->
            Outcomes;
        Unsupported ->
            exit_not_implemented("Outcomes not supported: ~tp", [Unsupported])
    end;
outcomes(#'v1_0.source'{outcomes = Unsupported}) ->
    exit_not_implemented("Outcomes not supported: ~tp", [Unsupported]);
outcomes(_) ->
    {array, symbol, ?OUTCOMES}.

-spec handle_to_ctag(link_handle()) -> rabbit_types:ctag().
handle_to_ctag(Handle) ->
    integer_to_binary(Handle).

-spec ctag_to_handle(rabbit_types:ctag()) -> link_handle().
ctag_to_handle(Ctag) ->
    binary_to_integer(Ctag).

queue_is_durable(?V_1_0_TERMINUS_DURABILITY_NONE) ->
    false;
queue_is_durable(?V_1_0_TERMINUS_DURABILITY_CONFIGURATION) ->
    true;
queue_is_durable(?V_1_0_TERMINUS_DURABILITY_UNSETTLED_STATE) ->
    true;
queue_is_durable(undefined) ->
    %% <field name="durable" type="terminus-durability" default="none"/>
    %% [3.5.3]
    queue_is_durable(?V_1_0_TERMINUS_DURABILITY_NONE).

-spec remove_outgoing_link(link_handle() | rabbit_types:ctag(), Map, queue:queue()) ->
    {Map, queue:queue()}
      when Map :: #{delivery_number() => #outgoing_unsettled{}}.
remove_outgoing_link(Handle, Map, Queue)
  when is_integer(Handle) ->
    Ctag = handle_to_ctag(Handle),
    remove_outgoing_link(Ctag, Map, Queue);
remove_outgoing_link(Ctag, OutgoingUnsettledMap0, OutgoingPending0)
  when is_binary(Ctag) ->
    OutgoingUnsettledMap = maps:filter(
                             fun(_DeliveryId, #outgoing_unsettled{consumer_tag = Tag}) ->
                                     Tag =/= Ctag
                             end, OutgoingUnsettledMap0),
    OutgoingPending = queue:filter(
                        fun(#pending_delivery{outgoing_unsettled = #outgoing_unsettled{consumer_tag = Tag}}) ->
                                Tag =/= Ctag;
                           ({credit_reply, Tag, _DeliveryCount, _Credit, _Available, _Drain}) ->
                                Tag =/= Ctag;
                           (#pending_management_delivery{}) ->
                                true;
                           (#'v1_0.flow'{}) ->
                                true
                        end, OutgoingPending0),
    {OutgoingUnsettledMap, OutgoingPending}.

messages_received(Settled) ->
    rabbit_global_counters:messages_received(?PROTOCOL, 1),
    case Settled of
        true -> ok;
        false -> rabbit_global_counters:messages_received_confirm(?PROTOCOL, 1)
    end.

messages_delivered(Redelivered, QueueType) ->
    rabbit_global_counters:messages_delivered(?PROTOCOL, QueueType, 1),
    case Redelivered of
        true -> rabbit_global_counters:messages_redelivered(?PROTOCOL, QueueType, 1);
        false -> ok
    end.

messages_acknowledged(complete, QName, QS, MsgIds) ->
    case rabbit_queue_type:module(QName, QS) of
        {ok, QType} ->
            rabbit_global_counters:messages_acknowledged(?PROTOCOL, QType, length(MsgIds));
        _ ->
            ok
    end;
messages_acknowledged(_, _, _, _) ->
    ok.

publisher_or_consumer_deleted(#incoming_link{}) ->
    rabbit_global_counters:publisher_deleted(?PROTOCOL);
publisher_or_consumer_deleted(#outgoing_link{}) ->
    rabbit_global_counters:consumer_deleted(?PROTOCOL).

publisher_or_consumer_deleted(
  #state{incoming_links = NewIncomingLinks,
         outgoing_links = NewOutgoingLinks},
  #state{incoming_links = OldIncomingLinks,
         outgoing_links = OldOutgoingLinks}) ->
    if map_size(NewIncomingLinks) < map_size(OldIncomingLinks) ->
           rabbit_global_counters:publisher_deleted(?PROTOCOL);
       map_size(NewOutgoingLinks) < map_size(OldOutgoingLinks) ->
           rabbit_global_counters:consumer_deleted(?PROTOCOL);
       true ->
           ok
    end.

%% If we previously already sent a detach with an error condition, and the Detach we
%% receive here is therefore the client's reply, do not reply again with a 3rd detach.
maybe_detach_reply(
  Detach,
  #state{incoming_links = NewIncomingLinks,
         outgoing_links = NewOutgoingLinks,
         incoming_management_links = NewIncomingMgmtLinks,
         outgoing_management_links = NewOutgoingMgmtLinks,
         cfg = #cfg{writer_pid = WriterPid,
                    channel_num = Ch}},
  #state{incoming_links = OldIncomingLinks,
         outgoing_links = OldOutgoingLinks,
         incoming_management_links = OldIncomingMgmtLinks,
         outgoing_management_links = OldOutgoingMgmtLinks})
  when map_size(NewIncomingLinks) < map_size(OldIncomingLinks) orelse
       map_size(NewOutgoingLinks) < map_size(OldOutgoingLinks) orelse
       map_size(NewIncomingMgmtLinks) < map_size(OldIncomingMgmtLinks) orelse
       map_size(NewOutgoingMgmtLinks) < map_size(OldOutgoingMgmtLinks) ->
    Reply = Detach#'v1_0.detach'{error = undefined},
    rabbit_amqp_writer:send_command(WriterPid, Ch, Reply);
maybe_detach_reply(_, _, _) ->
    ok.

-spec maybe_detach_mgmt_link(link_handle(), state()) -> state().
maybe_detach_mgmt_link(
  HandleInt,
  State = #state{management_link_pairs = LinkPairs0,
                 incoming_management_links = IncomingLinks0,
                 outgoing_management_links = OutgoingLinks0}) ->
    case maps:take(HandleInt, IncomingLinks0) of
        {#management_link{name = Name}, IncomingLinks} ->
            Pair = #management_link_pair{outgoing_half = OutgoingHalf} = maps:get(Name, LinkPairs0),
            LinkPairs = case OutgoingHalf of
                            unattached ->
                                maps:remove(Name, LinkPairs0);
                            _ ->
                                maps:update(Name,
                                            Pair#management_link_pair{incoming_half = unattached},
                                            LinkPairs0)
                        end,
            State#state{incoming_management_links = IncomingLinks,
                        management_link_pairs = LinkPairs};
        error ->
            case maps:take(HandleInt, OutgoingLinks0) of
                {#management_link{name = Name}, OutgoingLinks} ->
                    Pair = #management_link_pair{incoming_half = IncomingHalf} = maps:get(Name, LinkPairs0),
                    LinkPairs = case IncomingHalf of
                                    unattached ->
                                        maps:remove(Name, LinkPairs0);
                                    _ ->
                                        maps:update(Name,
                                                    Pair#management_link_pair{outgoing_half = unattached},
                                                    LinkPairs0)
                                end,
                    State#state{outgoing_management_links = OutgoingLinks,
                                management_link_pairs = LinkPairs};
                error ->
                    State
            end
    end.

check_internal_exchange(#exchange{internal = true,
                                  name = XName}) ->
    protocol_error(?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS,
                   "forbidden to publish to internal ~ts",
                   [rabbit_misc:rs(XName)]);
check_internal_exchange(_) ->
    ok.

-spec check_resource_access(rabbit_types:r(exchange | queue),
                            rabbit_types:permission_atom(),
                            rabbit_types:user(),
                            permission_cache()) ->
    permission_cache().
check_resource_access(Resource, Perm, User, Cache) ->
    CacheElem = {Resource, Perm},
    case lists:member(CacheElem, Cache) of
        true ->
            Cache;
        false ->
            Context = #{},
            try rabbit_access_control:check_resource_access(User, Resource, Perm, Context) of
                ok ->
                    CacheTail = lists:sublist(Cache, ?MAX_PERMISSION_CACHE_SIZE - 1),
                    [CacheElem | CacheTail]
            catch
                exit:#amqp_error{name = access_refused,
                                 explanation = Msg} ->
                    protocol_error(?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS, Msg, [])
            end
    end.

-spec check_write_permitted_on_topic(
        rabbit_types:exchange(),
        rabbit_types:user(),
        rabbit_types:routing_key(),
        topic_permission_cache()) ->
    topic_permission_cache().
check_write_permitted_on_topic(Resource, User, RoutingKey, TopicPermCache) ->
    check_topic_authorisation(Resource, User, RoutingKey, write, TopicPermCache).

-spec check_read_permitted_on_topic(
        rabbit_types:exchange(),
        rabbit_types:user(),
        rabbit_types:routing_key(),
        topic_permission_cache()) ->
    topic_permission_cache().
check_read_permitted_on_topic(Resource, User, RoutingKey, TopicPermCache) ->
    check_topic_authorisation(Resource, User, RoutingKey, read, TopicPermCache).

check_topic_authorisation(#exchange{type = topic,
                                    name = XName = #resource{virtual_host = VHost}},
                          User = #user{username = Username},
                          RoutingKey,
                          Permission,
                          Cache) ->
    Resource = XName#resource{kind = topic},
    CacheElem = {Resource, RoutingKey, Permission},
    case lists:member(CacheElem, Cache) of
        true ->
            Cache;
        false ->
            VariableMap = #{<<"vhost">> => VHost,
                            <<"username">> => Username},
            Context = #{routing_key => RoutingKey,
                        variable_map => VariableMap},
            try rabbit_access_control:check_topic_access(User, Resource, Permission, Context) of
                ok ->
                    CacheTail = lists:sublist(Cache, ?MAX_PERMISSION_CACHE_SIZE - 1),
                    [CacheElem | CacheTail]
            catch
                exit:#amqp_error{name = access_refused,
                                 explanation = Msg} ->
                    protocol_error(?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS, Msg, [])
            end
    end;
check_topic_authorisation(_, _, _, _, Cache) ->
    Cache.

check_user_id(Mc, User) ->
    case rabbit_access_control:check_user_id(Mc, User) of
        ok ->
            ok;
        {refused, Reason, Args} ->
            protocol_error(?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS, Reason, Args)
    end.

maps_update_with(Key, Fun, Init, Map) ->
    case Map of
        #{Key := Value} ->
            Map#{Key := Fun(Value)};
        _ ->
            Map#{Key => Init}
    end.

max_message_size({ulong, Size})
  when Size > 0 ->
    Size;
max_message_size(_) ->
    %% "If this field is zero or unset, there is no
    %% maximum size imposed by the link endpoint."
    unlimited.

check_paired({map, Properties}) ->
    case lists:any(fun({{symbol, <<"paired">>}, true}) ->
                           true;
                      (_) ->
                           false
                   end, Properties) of
        true ->
            ok;
        false ->
            exit_property_paired_not_set()
    end;
check_paired(_) ->
    exit_property_paired_not_set().

-spec exit_property_paired_not_set() -> no_return().
exit_property_paired_not_set() ->
    protocol_error(?V_1_0_AMQP_ERROR_INVALID_FIELD,
                   "Link property 'paired' is not set to boolean value 'true'", []).

-spec exit_not_implemented(io:format()) -> no_return().
exit_not_implemented(Format) ->
    exit_not_implemented(Format, []).

-spec exit_not_implemented(io:format(), [term()]) -> no_return().
exit_not_implemented(Format, Args) ->
    protocol_error(?V_1_0_AMQP_ERROR_NOT_IMPLEMENTED, Format, Args).

-spec exit_not_found(rabbit_types:r(exchange | queue)) -> no_return().
exit_not_found(Resource) ->
    protocol_error(?V_1_0_AMQP_ERROR_NOT_FOUND,
                   "no ~ts",
                   [rabbit_misc:rs(Resource)]).

-spec error_not_found(rabbit_types:r(exchange | queue)) -> #'v1_0.error'{}.
error_not_found(Resource) ->
    Description = unicode:characters_to_binary("no " ++ rabbit_misc:rs(Resource)),
    #'v1_0.error'{
       condition = ?V_1_0_AMQP_ERROR_NOT_FOUND,
       description = {utf8, Description}}.

is_valid_max(Val) ->
    is_integer(Val) andalso
    Val > 0 andalso
    Val =< ?UINT_MAX.

-spec cap_credit(rabbit_queue_type:credit()) ->
    rabbit_queue_type:credit().
cap_credit(DesiredCredit) ->
    MaxCredit = persistent_term:get(max_queue_credit),
    min(DesiredCredit, MaxCredit).

ensure_mc_cluster_compat(Mc) ->
    IsEnabled = rabbit_feature_flags:is_enabled(message_containers_store_amqp_v1),
    case IsEnabled of
        true ->
            Mc;
        false ->
            McEnv = #{message_containers_store_amqp_v1 => IsEnabled},
            %% other nodes in the cluster may not understand the new internal
            %% amqp mc format - in this case we convert to AMQP legacy format
            %% for compatibility
            mc:convert(mc_amqpl, Mc, McEnv)
    end.

format_status(
  #{state := #state{cfg = Cfg,
                    outgoing_pending = OutgoingPending,
                    remote_incoming_window = RemoteIncomingWindow,
                    remote_outgoing_window = RemoteOutgoingWindow,
                    next_incoming_id = NextIncomingId,
                    incoming_window = IncomingWindow,
                    next_outgoing_id = NextOutgoingId,
                    outgoing_delivery_id = OutgoingDeliveryId,
                    incoming_links = IncomingLinks,
                    outgoing_links = OutgoingLinks,
                    management_link_pairs = ManagementLinks,
                    incoming_management_links = IncomingManagementLinks,
                    outgoing_management_links = OutgoingManagementLinks,
                    outgoing_unsettled_map = OutgoingUnsettledMap,
                    stashed_rejected = StashedRejected,
                    stashed_settled = StashedSettled,
                    stashed_down = StashedDown,
                    stashed_eol = StashedEol,
                    queue_states = QueueStates,
                    permission_cache = PermissionCache,
                    topic_permission_cache = TopicPermissionCache}} = Status) ->
    State = #{cfg => Cfg,
              outgoing_pending => queue:len(OutgoingPending),
              remote_incoming_window => RemoteIncomingWindow,
              remote_outgoing_window => RemoteOutgoingWindow,
              next_incoming_id => NextIncomingId,
              incoming_window => IncomingWindow,
              next_outgoing_id => NextOutgoingId,
              outgoing_delivery_id => OutgoingDeliveryId,
              incoming_links => IncomingLinks,
              outgoing_links => OutgoingLinks,
              management_link_pairs => ManagementLinks,
              incoming_management_links => IncomingManagementLinks,
              outgoing_management_links => OutgoingManagementLinks,
              outgoing_unsettled_map => OutgoingUnsettledMap,
              stashed_rejected => StashedRejected,
              stashed_settled => StashedSettled,
              stashed_down => StashedDown,
              stashed_eol => StashedEol,
              queue_states => rabbit_queue_type:format_status(QueueStates),
              permission_cache => PermissionCache,
              topic_permission_cache => TopicPermissionCache},
    maps:update(state, State, Status).
