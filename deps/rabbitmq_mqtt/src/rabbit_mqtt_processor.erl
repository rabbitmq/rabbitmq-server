%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(rabbit_mqtt_processor).
-feature(maybe_expr, enable).

-export([info/2, init/4, process_packet/2,
         terminate/3, handle_pre_hibernate/0,
         handle_down/2, handle_queue_event/2,
         proto_version_tuple/1, throttle/2, format_status/1,
         remove_duplicate_client_id_connections/2,
         remove_duplicate_client_id_connections/3,
         update_trace/2, send_disconnect/2]).

-ifdef(TEST).
-export([get_vhost_username/1,
         get_vhost/3,
         get_vhost_from_user_mapping/2]).
-endif.

-export_type([state/0,
              send_fun/0]).

-import(rabbit_mqtt_util, [mqtt_to_amqp/1,
                           amqp_to_mqtt/1,
                           ip_address_to_binary/1]).
-import(rabbit_misc, [maps_put_truthy/3]).

-include_lib("kernel/include/logger.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit/include/amqqueue.hrl").
-include_lib("rabbit/include/mc.hrl").
-include("rabbit_mqtt.hrl").
-include("rabbit_mqtt_packet.hrl").

-define(MAX_PERMISSION_CACHE_SIZE, 12).
-define(CONSUMER_TAG, <<"mqtt">>).
-define(QUEUE_TTL_KEY, <<"x-expires">>).
-define(DEFAULT_EXCHANGE_NAME, <<>>).

-ifdef(TEST).
-define(SILENT_CLOSE_DELAY, 10).
-else.
-define(SILENT_CLOSE_DELAY, 3_000).
-endif.

-type send_fun() :: fun((iodata()) -> ok).
-type session_expiry_interval() :: non_neg_integer() | infinity.
-type subscriptions() :: #{topic_filter() => #mqtt_subscription_opts{}}.
-type topic_aliases() :: {Inbound :: #{pos_integer() => topic()},
                          Outbound :: #{topic() => pos_integer()}}.

-record(auth_state,
        {user :: #user{},
         authz_ctx :: #{binary() := binary()}
        }).

-record(cfg,
        {socket :: rabbit_net:socket(),
         proto_ver :: protocol_version_atom(),
         clean_start :: boolean(),
         session_expiry_interval_secs :: session_expiry_interval(),
         will_msg :: option(mqtt_msg()),
         exchange :: rabbit_exchange:name(),
         %% Set if client has at least one subscription with QoS 1.
         queue_qos1 :: option(rabbit_amqqueue:name()),
         %% Did the client ever sent us a PUBLISH packet?
         published = false :: boolean(),
         ssl_login_name :: none | binary(),
         retainer_pid :: pid(),
         delivery_flow :: flow | noflow,
         trace_state :: rabbit_trace:state(),
         prefetch :: non_neg_integer(),
         vhost :: rabbit_types:vhost(),
         client_id :: client_id(),
         %% User Property set in the CONNECT packet.
         user_prop :: user_property(),
         conn_name :: option(binary()),
         ip_addr :: inet:ip_address(),
         port :: inet:port_number(),
         peer_ip_addr :: inet:ip_address(),
         peer_port :: inet:port_number(),
         connected_at = os:system_time(millisecond) :: pos_integer(),
         send_fun :: send_fun(),
         %% Maximum MQTT packet size in bytes for packets sent from server to client.
         max_packet_size_outbound :: max_packet_size(),
         topic_alias_maximum_outbound :: non_neg_integer(),
         %% https://github.com/rabbitmq/rabbitmq-server/issues/13040
         %% The database stores the MQTT subscription options in the binding arguments for:
         %% * v1 as Erlang record #mqtt_subscription_opts{}
         %% * v2 as AMQP 0.9.1 table
         binding_args_v2 :: boolean(),
         msg_interceptor_ctx :: rabbit_msg_interceptor:context()
        }).

-record(state,
        {cfg :: #cfg{},
         queue_states = rabbit_queue_type:init() :: rabbit_queue_type:state(),
         %% Packet IDs published to queues but not yet confirmed.
         unacked_client_pubs = rabbit_mqtt_confirms:init() :: rabbit_mqtt_confirms:state(),
         %% Packet IDs published to MQTT subscribers but not yet acknowledged.
         unacked_server_pubs = #{} :: #{packet_id() => QueueMsgId :: non_neg_integer()},
         %% Packet ID of next PUBLISH packet (with QoS > 0) sent from server to client.
         %% (Not to be confused with packet IDs sent from client to server which can be the
         %% same IDs because client and server assign IDs independently of each other.)
         packet_id = 1 :: packet_id(),
         %% "A Session cannot have more than one Non‑shared Subscription with the same Topic Filter,
         %% so the Topic Filter can be used as a key to identify the subscription within that Session."
         %% [v5 4.8.1]
         subscriptions = #{} :: subscriptions(),
         auth_state = #auth_state{},
         %% quorum queues and streams whose soft limit has been exceeded
         queues_soft_limit_exceeded = sets:new([{version, 2}]) :: sets:set(),
         qos0_messages_dropped = 0 :: non_neg_integer(),
         topic_aliases = {#{}, #{}} :: topic_aliases()
        }).

-opaque state() :: #state{}.

%% NB: If init/4 returns an error, it must clean up itself because terminate/3 will not be called.
-spec init(ConnectPacket :: mqtt_packet(),
           RawSocket :: rabbit_net:socket(),
           ConnectionName :: binary(),
           SendFun :: send_fun()) ->
    {ok, state()} | {error, {socket_ends, any()} | reason_code()}.
init(#mqtt_packet{fixed = #mqtt_packet_fixed{type = ?CONNECT},
                  variable = ConnectPacket},
     Socket, ConnName, SendFun) ->
    %% Check whether peer closed the connection.
    %% For example, this can happen when connection was blocked because of resource
    %% alarm and client therefore disconnected due to client side CONNACK timeout.
    case rabbit_net:socket_ends(Socket, inbound) of
        {ok, SocketEnds} ->
            process_connect(ConnectPacket, Socket, ConnName, SendFun, SocketEnds);
        {error, Reason} ->
            {error, {socket_ends, Reason}}
    end.

process_connect(
  #mqtt_packet_connect{
     username   = Username0,
     password   = Password0,
     proto_ver  = ProtoVer,
     clean_start = CleanStart,
     client_id  = ClientId0,
     keep_alive = KeepaliveSecs,
     props = ConnectProps,
     will_props = WillProps} = Packet,
  Socket, ConnName0, SendFun, {PeerIp, PeerPort, Ip, Port}) ->
    ?LOG_DEBUG("Received a CONNECT, client ID: ~s, username: ~s, clean start: ~s, "
               "protocol version: ~p, keepalive: ~p, property names: ~p",
               [ClientId0, Username0, CleanStart, ProtoVer, KeepaliveSecs, maps:keys(ConnectProps)]),
    SslLoginName = ssl_login_name(Socket),
    Flow = case rabbit_misc:get_env(rabbit, classic_queue_flow_control, true) of
             true   -> flow;
             false  -> noflow
           end,
    MaxPacketSize = maps:get('Maximum-Packet-Size', ConnectProps, ?MAX_PACKET_SIZE),
    TopicAliasMax = persistent_term:get(?PERSISTENT_TERM_TOPIC_ALIAS_MAXIMUM),
    TopicAliasMaxOutbound = min(maps:get('Topic-Alias-Maximum', ConnectProps, 0), TopicAliasMax),
    {ok, MaxSessionExpiry} = application:get_env(?APP_NAME, max_session_expiry_interval_seconds),
    SessionExpiry =
    case {ProtoVer, CleanStart} of
        {5, _} ->
            %% "If the Session Expiry Interval is absent the value 0 is used."
            case maps:get('Session-Expiry-Interval', ConnectProps, 0) of
                ?UINT_MAX ->
                    %% "If the Session Expiry Interval is 0xFFFFFFFF (UINT_MAX),
                    %% the Session does not expire."
                    MaxSessionExpiry;
                Seconds ->
                    min(Seconds, MaxSessionExpiry)
            end;
        {_, _CleanSession = true} ->
            %% "Setting Clean Start to 1 and a Session Expiry Interval of 0, is equivalent
            %% to setting CleanSession to 1 in the MQTT Specification Version 3.1.1."
            0;
        {_, _CleanSession = false} ->
            %% The following sentence of the MQTT 5 spec 3.1.2.11.2 is wrong:
            %% "Setting Clean Start to 0 and no Session Expiry Interval, is equivalent to
            %% setting CleanSession to 0 in the MQTT Specification Version 3.1.1."
            %% Correct is:
            %% "CleanStart=0 and SessionExpiry=0xFFFFFFFF (UINT_MAX) for MQTT 5.0 would
            %% provide the same as CleanSession=0 for 3.1.1."
            %% see https://issues.oasis-open.org/projects/MQTT/issues/MQTT-538
            %% Therefore, we use the maximum allowed session expiry interval.
            MaxSessionExpiry
    end,
    Result0 =
    maybe
        ok ?= check_extended_auth(ConnectProps),
        {ok, ClientId1} ?= extract_client_id_from_certificate(ClientId0, Socket),
        {ok, ClientId} ?= ensure_client_id(ClientId1, CleanStart, ProtoVer),
        {ok, Username1, Password} ?= check_credentials(Username0, Password0, SslLoginName, PeerIp),
        {VHostPickedUsing, {VHost, Username2}} = get_vhost(Username1, SslLoginName, Port),
        ?LOG_DEBUG("MQTT connection ~s picked vhost using ~s", [ConnName0, VHostPickedUsing]),
        ok ?= check_vhost_exists(VHost, Username2, PeerIp),
        ok ?= check_vhost_alive(VHost),
        ok ?= check_vhost_connection_limit(VHost),
        {ok, User = #user{username = Username}} ?= check_user_login(VHost, Username2, Password,
                                                                    ClientId, PeerIp, ConnName0),
        ok ?= check_user_connection_limit(Username),
        {ok, AuthzCtx} ?= check_vhost_access(VHost, User, ClientId, PeerIp),
        ok ?= check_user_loopback(Username, PeerIp),
        ok ?= ensure_credential_expiry_timer(User, PeerIp),
        rabbit_core_metrics:auth_attempt_succeeded(PeerIp, Username, mqtt),
        ok = register_client_id(VHost, ClientId, CleanStart, WillProps),
        {ok, WillMsg} ?= make_will_msg(Packet),
        {TraceState, ConnName} = init_trace(VHost, ConnName0),
        ok = rabbit_mqtt_keepalive:start(KeepaliveSecs, Socket),
        Exchange = rabbit_misc:r(VHost, exchange, persistent_term:get(?PERSISTENT_TERM_EXCHANGE)),
        %% To simplify logic, we decide at connection establishment time to stick
        %% with either binding args v1 or v2 for the lifetime of the connection.
        BindingArgsV2 = rabbit_feature_flags:is_enabled('rabbitmq_4.1.0'),
        ProtoVerAtom = proto_integer_to_atom(ProtoVer),
        MsgIcptCtx = #{protocol => ProtoVerAtom,
                       vhost => VHost,
                       username => Username,
                       connection_name => ConnName,
                       client_id => ClientId},
        S = #state{
               cfg = #cfg{socket = Socket,
                          proto_ver = ProtoVerAtom,
                          clean_start = CleanStart,
                          session_expiry_interval_secs = SessionExpiry,
                          ssl_login_name = SslLoginName,
                          delivery_flow = Flow,
                          trace_state = TraceState,
                          prefetch = prefetch(ConnectProps),
                          conn_name = ConnName,
                          ip_addr = Ip,
                          port = Port,
                          peer_ip_addr = PeerIp,
                          peer_port = PeerPort,
                          send_fun = SendFun,
                          exchange = Exchange,
                          retainer_pid = rabbit_mqtt_retainer_sup:start_child_for_vhost(VHost),
                          vhost = VHost,
                          client_id = ClientId,
                          user_prop = maps:get('User-Property', ConnectProps, []),
                          will_msg = WillMsg,
                          max_packet_size_outbound = MaxPacketSize,
                          topic_alias_maximum_outbound = TopicAliasMaxOutbound,
                          binding_args_v2 = BindingArgsV2,
                          msg_interceptor_ctx = MsgIcptCtx},
               auth_state = #auth_state{
                               user = User,
                               authz_ctx = AuthzCtx}},
        ok ?= clear_will_msg(S),
        {ok, S}
    end,
    Result = case Result0 of
                 {ok, State0 = #state{}} ->
                     process_connect(State0);
                 {error, _} = Err0 ->
                     Err0
             end,
    case Result of
        {ok, SessPresent, State = #state{}} ->
            Props0 = #{'Maximum-QoS' => ?QOS_1,
                       'Topic-Alias-Maximum' => TopicAliasMax,
                       'Maximum-Packet-Size' => persistent_term:get(
                                                  ?PERSISTENT_TERM_MAX_PACKET_SIZE_AUTHENTICATED),
                       'Shared-Subscription-Available' => 0,
                       'Session-Expiry-Interval' => case SessionExpiry of
                                                        infinity -> ?UINT_MAX;
                                                        Secs -> Secs
                                                    end},
            Props = case {ClientId0, ProtoVer} of
                        {<<>>, 5} ->
                            %% "If the Client connects using a zero length Client Identifier, the Server
                            %% MUST respond with a CONNACK containing an Assigned Client Identifier."
                            maps:put('Assigned-Client-Identifier', State#state.cfg#cfg.client_id, Props0);
                        _ ->
                            Props0
                    end,
            send_conn_ack(?RC_SUCCESS, SessPresent, ProtoVer, SendFun, MaxPacketSize, Props),
            {ok, State};
        {error, ConnectReasonCode} = Err
          when is_integer(ConnectReasonCode) ->
            %% If a server sends a CONNACK packet containing a non-zero return
            %% code it MUST set Session Present to 0 [MQTT-3.2.2-4].
            SessPresent = false,
            send_conn_ack(ConnectReasonCode, SessPresent, ProtoVer, SendFun, MaxPacketSize, #{}),
            Err
    end.

-spec prefetch(ConnectProperties :: properties()) -> pos_integer().
prefetch(Props) ->
    %% "If the Receive Maximum value is absent then its value defaults to 65,535" [v5 3.1.2.11.3]
    ReceiveMax = maps:get('Receive-Maximum', Props, ?TWO_BYTE_INTEGER_MAX),
    %% "The Server might choose to send fewer than Receive Maximum messages to the Client
    %% without receiving acknowledgement, even if it has more than this number of messages
    %% available to send." [v5 3.3.4]
    min(rabbit_mqtt_util:env(prefetch), ReceiveMax).

-spec send_conn_ack(reason_code(), boolean(), protocol_version(), send_fun(),
                    max_packet_size(), properties()) -> ok.
send_conn_ack(ConnectReasonCode, SessPresent, ProtoVer, SendFun, MaxPacketSize, Props) ->
    Code = case ProtoVer of
               5 -> ConnectReasonCode;
               _ -> connect_reason_code_to_return_code(ConnectReasonCode)
           end,
    Packet = #mqtt_packet{fixed = #mqtt_packet_fixed{type = ?CONNACK},
                          variable = #mqtt_packet_connack{
                                        session_present = SessPresent,
                                        code = Code,
                                        props = Props}},
    _ = send(Packet, ProtoVer, SendFun, MaxPacketSize),
    ok.

%% "Connect Reason Code" used in v5:
%% https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901079
%% "Connect Return Code" used in v3 and v4:
%% http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc385349257
-spec connect_reason_code_to_return_code(reason_code()) ->
    connect_return_code().
connect_reason_code_to_return_code(?RC_SUCCESS) ->
    ?CONNACK_ACCEPT;
connect_reason_code_to_return_code(?RC_UNSUPPORTED_PROTOCOL_VERSION) ->
    ?CONNACK_UNACCEPTABLE_PROTO_VER;
connect_reason_code_to_return_code(?RC_CLIENT_IDENTIFIER_NOT_VALID) ->
    ?CONNACK_ID_REJECTED;
connect_reason_code_to_return_code(?RC_BAD_USER_NAME_OR_PASSWORD) ->
    ?CONNACK_BAD_CREDENTIALS;
connect_reason_code_to_return_code(RC) when RC =:= ?RC_NOT_AUTHORIZED orelse
                                            RC =:= ?RC_QUOTA_EXCEEDED ->
    ?CONNACK_NOT_AUTHORIZED;
connect_reason_code_to_return_code(_) ->
    %% Everything else gets mapped to the most generic Connect Return Code.
    ?CONNACK_SERVER_UNAVAILABLE.

process_connect(State0) ->
    maybe
        {ok, QoS0SessPresent, State1} ?= handle_clean_start_qos0(State0),
        {ok, SessPresent, State2} ?= handle_clean_start_qos1(QoS0SessPresent, State1),
        {ok, State} ?= init_subscriptions(SessPresent, State2),
        rabbit_networking:register_non_amqp_connection(self()),
        self() ! connection_created,
        {ok, SessPresent, State}
    else
        {error, _} = Error ->
            Error
    end.

-spec process_packet(mqtt_packet(), state()) ->
    {ok, state()} |
    {stop, {disconnect, {client_initiated, SendWill :: boolean()} | server_initiated}, state()} |
    {error, Reason :: term(), state()}.
process_packet(Packet = #mqtt_packet{fixed = #mqtt_packet_fixed{type = Type}},
               State = #state{auth_state = #auth_state{}})
  when Type =/= ?CONNECT ->
    process_request(Type, Packet, State).

-spec process_request(packet_type(), mqtt_packet(), state()) ->
    {ok, state()} |
    {stop, {disconnect, {client_initiated, SendWill :: boolean()} | server_initiated}, state()} |
    {error, Reason :: term(), state()}.
process_request(?PUBACK,
                #mqtt_packet{variable = #mqtt_packet_puback{packet_id = PacketId,
                                                            reason_code = ReasonCode}},
                #state{unacked_server_pubs = U0,
                       queue_states = QStates0,
                       cfg = #cfg{queue_qos1 = QName}} = State) ->
    case maps:take(PacketId, U0) of
        {QMsgId, U} ->
            SettleOp = case is_success(ReasonCode) of
                           true ->
                               complete;
                           false ->
                               %% 'discard' instead of 'requeue' due to v5 spec:
                               %% "If PUBACK or PUBREC is received containing a Reason Code of 0x80
                               %% or greater the corresponding PUBLISH packet is treated as
                               %% acknowledged, and MUST NOT be retransmitted [MQTT-4.4.0-2]."
                               discard
                       end,
            case rabbit_queue_type:settle(QName, SettleOp, ?CONSUMER_TAG, [QMsgId], QStates0) of
                {ok, QStates, Actions} ->
                    message_acknowledged(QName, State),
                    {ok, handle_queue_actions(Actions, State#state{unacked_server_pubs = U,
                                                                   queue_states = QStates})};
                {protocol_error, _ErrorType, _Reason, _ReasonArgs} = Err ->
                    {error, Err, State}
            end;
        error ->
            {ok, State}
    end;

process_request(?PUBLISH,
                #mqtt_packet{fixed = #mqtt_packet_fixed{qos = ?QOS_2}},
                State = #state{cfg = #cfg{proto_ver = ?MQTT_PROTO_V5,
                                          client_id = ClientId}}) ->
    %% MQTT 5 spec 3.3.1.2 QoS
    %% "If the Server included a Maximum QoS in its CONNACK response
    %% to a Client and it receives a PUBLISH packet with a QoS greater than this
    %% then it uses DISCONNECT with Reason Code 0x9B (QoS not supported)."
    ?LOG_WARNING("Received a PUBLISH with QoS2. Disconnecting MQTT client ~ts", [ClientId]),
    send_disconnect(?RC_QOS_NOT_SUPPORTED, State),
    {stop, {disconnect, server_initiated}, State};
process_request(?PUBLISH,
                #mqtt_packet{
                   fixed = #mqtt_packet_fixed{qos = Qos,
                                              retain = Retain,
                                              dup = Dup},
                   variable = Variable = #mqtt_packet_publish{packet_id = PacketId},
                   payload = Payload},
                State0 = #state{unacked_client_pubs = U,
                                cfg = #cfg{proto_ver = ProtoVer}}) ->
    case process_topic_alias_inbound(Variable, State0) of
        {ok, Topic, Props, State1} ->
            EffectiveQos = maybe_downgrade_qos(Qos),
            rabbit_global_counters:messages_received(ProtoVer, 1),
            rabbit_msg_size_metrics:observe(ProtoVer, iolist_size(Payload)),
            State = maybe_increment_publisher(State1),
            Msg = #mqtt_msg{retain = Retain,
                            qos = EffectiveQos,
                            topic = Topic,
                            dup = Dup,
                            packet_id  = PacketId,
                            payload = Payload,
                            props = Props},
            case EffectiveQos of
                ?QOS_0 ->
                    publish_to_queues_with_checks(Msg, State);
                ?QOS_1 ->
                    rabbit_global_counters:messages_received_confirm(ProtoVer, 1),
                    case rabbit_mqtt_confirms:contains(PacketId, U) of
                        false ->
                            publish_to_queues_with_checks(Msg, State);
                        true ->
                            %% Client re-sent this PUBLISH packet.
                            %% We already sent this message to target queues awaiting confirmations.
                            %% Hence, we ignore this re-send.
                            {ok, State}
                    end
            end;
        {error, ReasonCode} ->
            send_disconnect(ReasonCode, State0),
            {stop, {disconnect, server_initiated}, State0}
    end;

process_request(?SUBSCRIBE,
                #mqtt_packet{
                   variable = #mqtt_packet_subscribe{
                                 packet_id  = SubscribePktId,
                                 subscriptions = Subscriptions},
                   payload = undefined},
                State0 = #state{cfg = #cfg{proto_ver = ProtoVer,
                                           binding_args_v2 = BindingArgsV2}}) ->
    ?LOG_DEBUG("Received a SUBSCRIBE with subscription(s) ~p", [Subscriptions]),
    {ResultRev, RetainedRev, State1} =
    lists:foldl(
      fun(_Subscription, {[{error, _} = E | _] = L, R, S}) ->
              %% Once a subscription failed, mark all following subscriptions
              %% as failed instead of creating bindings because we are going
              %% to close the client connection anyway.
              {[E | L], R, S};
         (#mqtt_subscription{topic_filter = TopicFilter,
                             options = Opts0 = #mqtt_subscription_opts{
                                                  qos = Qos0,
                                                  retain_handling = Rh}},
          {L0, R0, S0}) ->
              QoS = maybe_downgrade_qos(Qos0),
              Opts = Opts0#mqtt_subscription_opts{qos = QoS},
              L = [QoS | L0],
              R1 = [{TopicFilter, QoS} | R0],
              case S0#state.subscriptions of
                  #{TopicFilter := Opts} ->
                      R = if Rh =:= 0 -> R1;
                             Rh > 0 -> R0
                          end,
                      {L, R, S0};
                  _ ->
                      maybe
                          {ok, Q} ?= ensure_queue(QoS, S0),
                          QName = amqqueue:get_name(Q),
                          BindingArgs = binding_args_for_proto_ver(ProtoVer, TopicFilter, Opts, BindingArgsV2),
                          ok ?= add_subscription(TopicFilter, BindingArgs, QName, S0),
                          ok ?= maybe_delete_old_subscription(TopicFilter, Opts, S0),
                          Subs = maps:put(TopicFilter, Opts, S0#state.subscriptions),
                          S1 = S0#state{subscriptions = Subs},
                          maybe_increment_consumer(S0, S1),
                          R = if Rh < 2 -> R1;
                                 Rh =:= 2 -> R0
                              end,
                          case self_consumes(Q) of
                              false ->
                                  case consume(Q, QoS, S1) of
                                      {ok, S2} ->
                                          {L, R, S2};
                                      {error, _} = E1 ->
                                          {[E1 | L0], R, S1}
                                  end;
                              true ->
                                  {L, R, S1}
                          end
                      else
                          {error, _} = E2 -> {[E2 | L0], R0, S0}
                      end
              end
      end, {[], [], State0}, Subscriptions),
    ReasonCodesRev = subscribe_result_to_reason_codes(ResultRev, ProtoVer),
    Reply = #mqtt_packet{fixed    = #mqtt_packet_fixed{type = ?SUBACK},
                         variable = #mqtt_packet_suback{
                                       packet_id = SubscribePktId,
                                       reason_codes = lists:reverse(ReasonCodesRev)}},
    _ = send(Reply, State1),
    case hd(ResultRev) of
        {error, _} ->
            {error, subscribe_error, State1};
        _ ->
            State = send_retained_messages(lists:reverse(RetainedRev), State1),
            {ok, State}
    end;

process_request(?UNSUBSCRIBE,
                #mqtt_packet{variable = #mqtt_packet_unsubscribe{packet_id  = PacketId,
                                                                 topic_filters = TopicFilters},
                             payload = undefined},
                State0) ->
    ?LOG_DEBUG("Received an UNSUBSCRIBE for topic filter(s) ~p", [TopicFilters]),
    {ReasonCodes, State} =
    lists:foldl(
      fun(TopicFilter, {L, #state{subscriptions = Subs0,
                                  cfg = #cfg{proto_ver = ProtoVer,
                                             binding_args_v2 = BindingArgsV2}} = S0}) ->
              case maps:take(TopicFilter, Subs0) of
                  {Opts, Subs} ->
                      BindingArgs = binding_args_for_proto_ver(ProtoVer, TopicFilter, Opts, BindingArgsV2),
                      case delete_subscription(
                             TopicFilter, BindingArgs, Opts#mqtt_subscription_opts.qos, S0) of
                          ok ->
                              S = S0#state{subscriptions = Subs},
                              maybe_decrement_consumer(S0, S),
                              {[?RC_SUCCESS | L], S};
                          {error, access_refused} ->
                              {[?RC_NOT_AUTHORIZED | L], S0};
                          {error, _} ->
                              {[?RC_UNSPECIFIED_ERROR | L], S0}
                      end;
                  error ->
                      {[?RC_NO_SUBSCRIPTION_EXISTED | L], S0}
              end
      end, {[], State0}, TopicFilters),
    Reply = #mqtt_packet{fixed = #mqtt_packet_fixed{type = ?UNSUBACK},
                         variable = #mqtt_packet_unsuback{
                                       packet_id = PacketId,
                                       reason_codes = lists:reverse(ReasonCodes)}},
    _ = send(Reply, State),
    {ok, State};

process_request(?PINGREQ, #mqtt_packet{}, State = #state{cfg = #cfg{client_id = ClientId}}) ->
    ?LOG_DEBUG("Received a PINGREQ from client ID ~s", [ClientId]),
    Reply = #mqtt_packet{fixed = #mqtt_packet_fixed{type = ?PINGRESP}},
    _ = send(Reply, State),
    ?LOG_DEBUG("Sent a PINGRESP to client ID ~s", [ClientId]),
    {ok, State};

process_request(?DISCONNECT,
                #mqtt_packet{variable = #mqtt_packet_disconnect{reason_code = Rc,
                                                                props = Props}},
                #state{cfg = #cfg{session_expiry_interval_secs = CurrentSEI} = Cfg} = State0) ->
    ?LOG_DEBUG("Received a DISCONNECT with reason code ~b and properties ~p", [Rc, Props]),
    RequestedSEI = case maps:find('Session-Expiry-Interval', Props) of
                       {ok, ?UINT_MAX} ->
                           %% "If the Session Expiry Interval is 0xFFFFFFFF (UINT_MAX),
                           %% the Session does not expire."
                           infinity;
                       {ok, Secs} ->
                           Secs;
                       error ->
                           %% "If the Session Expiry Interval is absent, the Session
                           %% Expiry Interval in the CONNECT packet is used."
                           CurrentSEI
                   end,
    State =
    case CurrentSEI of
        RequestedSEI ->
            State0;
        0 when RequestedSEI > 0 ->
            %% "If the Session Expiry Interval in the CONNECT packet was zero, then it is a Protocol
            %% Error to set a non-zero Session Expiry Interval in the DISCONNECT packet sent by the
            %% Client. If such a non-zero Session Expiry Interval is received by the Server, it does
            %% not treat it as a valid DISCONNECT packet. The Server uses DISCONNECT with Reason
            %% Code 0x82 (Protocol Error) as described in section 4.13."
            %% The last sentence does not make sense because the client already closed the network
            %% connection after it sent us the DISCONNECT. Hence, we do not reply with another
            %% DISCONNECT.
            ?LOG_WARNING("MQTT protocol error: Ignoring requested Session Expiry "
                         "Interval ~p in DISCONNECT because it was 0 in CONNECT.",
                         [RequestedSEI]),
            State0;
        _ ->
            %% "The session expiry interval can be modified at disconnect."
            {ok, MaxSEI} = application:get_env(?APP_NAME, max_session_expiry_interval_seconds),
            NewSEI = min(RequestedSEI, MaxSEI),
            lists:foreach(fun(QName) ->
                                  update_session_expiry_interval(QName, NewSEI)
                          end, existing_queue_names(State0)),
            State0#state{cfg = Cfg#cfg{session_expiry_interval_secs = NewSEI}}
    end,
    %% "If the Network Connection is closed without the Client first sending a DISCONNECT packet with Reason
    %% Code 0x00 (Normal disconnection) [...] the Will Message is published." [v5 3.14]
    SendWill = Rc > ?RC_NORMAL_DISCONNECTION,
    {stop, {disconnect, {client_initiated, SendWill}}, State}.

-spec maybe_update_session_expiry_interval(amqqueue:amqqueue(), session_expiry_interval()) -> ok.
maybe_update_session_expiry_interval(Queue, Expiry) ->
    OldExpiry = case rabbit_misc:table_lookup(amqqueue:get_arguments(Queue), ?QUEUE_TTL_KEY) of
                    undefined ->
                        infinity;
                    {long, Millis} ->
                        Millis div 1000
                end,
    case OldExpiry of
        Expiry ->
            ok;
        _ ->
            update_session_expiry_interval(amqqueue:get_name(Queue), Expiry)
    end.

-spec update_session_expiry_interval(rabbit_amqqueue:name(), session_expiry_interval()) -> ok.
update_session_expiry_interval(QName, Expiry) ->
    Fun = fun(Q) ->
                  Args0 = amqqueue:get_arguments(Q),
                  Args = if Expiry =:= infinity ->
                                proplists:delete(?QUEUE_TTL_KEY, Args0);
                            true ->
                                rabbit_misc:set_table_value(
                                  Args0, ?QUEUE_TTL_KEY, long, timer:seconds(Expiry))
                         end,
                  amqqueue:set_arguments(Q, Args)
          end,
    case rabbit_amqqueue:update(QName, Fun) of
        not_found ->
            ok;
        Q ->
            ok = rabbit_queue_type:policy_changed(Q) % respects queue args
    end.

check_extended_auth(#{'Authentication-Method' := Method}) ->
    %% In future, we could support SASL via rabbit_auth_mechanism
    %% as done by rabbit_reader and rabbit_stream_reader.
    ?LOG_ERROR("Extended authentication (method ~p) is not supported", [Method]),
    {error, ?RC_BAD_AUTHENTICATION_METHOD};
check_extended_auth(_) ->
    ok.

check_credentials(Username, Password, SslLoginName, PeerIp) ->
    case creds(Username, Password, SslLoginName) of
        {ok, _, _} = Ok ->
            Ok;
        nocreds ->
            ?LOG_ERROR("MQTT login failed: no credentials provided"),
            auth_attempt_failed(PeerIp, <<>>),
            {error, ?RC_BAD_USER_NAME_OR_PASSWORD};
        {invalid_creds, {undefined, Pass}} when is_binary(Pass) ->
            ?LOG_ERROR("MQTT login failed: no username is provided"),
            auth_attempt_failed(PeerIp, <<>>),
            {error, ?RC_BAD_USER_NAME_OR_PASSWORD};
        {invalid_creds, {User, _Pass}} when is_binary(User) ->
            ?LOG_ERROR("MQTT login failed for user '~s': no password provided", [User]),
            auth_attempt_failed(PeerIp, User),
            {error, ?RC_BAD_USER_NAME_OR_PASSWORD}
    end.

%% Extract client_id from the certificate provided it was configured to do so and
%% it is possible to extract it else returns the client_id passed as parameter
-spec extract_client_id_from_certificate(client_id(), rabbit_net:socket()) -> {ok, client_id()} | {error, reason_code()}.
extract_client_id_from_certificate(Client0, Socket) ->
    case extract_ssl_cert_client_id_settings() of
        none -> {ok, Client0};
        SslClientIdSettings ->
            case ssl_client_id(Socket, SslClientIdSettings) of
                none ->
                    {ok, Client0};
                Client0 ->
                    {ok, Client0};
                Other ->
                    ?LOG_ERROR(
                        "MQTT login failed: client_id in the certificate (~tp) does not match the client-provided ID (~p)",
                        [Other, Client0]),
                    {error, ?RC_CLIENT_IDENTIFIER_NOT_VALID}
            end
    end.

-spec ensure_client_id(client_id(), boolean(), protocol_version()) ->
    {ok, client_id()} | {error, reason_code()}.
ensure_client_id(<<>>, _CleanStart = false, ProtoVer)
  when ProtoVer < 5 ->
    ?LOG_ERROR("MQTT client ID must be provided for non-clean session in MQTT v~b", [ProtoVer]),
    {error, ?RC_CLIENT_IDENTIFIER_NOT_VALID};
ensure_client_id(<<>>, _, _) ->
    {ok, rabbit_data_coercion:to_binary(
           rabbit_misc:base64url(
             rabbit_guid:gen_secure()))};
ensure_client_id(ClientId, _, _)
  when is_binary(ClientId) ->
    {ok, ClientId}.

-spec register_client_id(rabbit_types:vhost(), client_id(), boolean(), properties()) -> ok.
register_client_id(VHost, ClientId, CleanStart, WillProps)
  when is_binary(VHost), is_binary(ClientId) ->
    PgGroup = {VHost, ClientId},
    ok = pg:join(persistent_term:get(?PG_SCOPE), PgGroup, self()),
    %% "If a Network Connection uses a Client Identifier of an existing Network Connection to
    %% the Server, the Will Message for the exiting connection is sent unless the new
    %% connection specifies Clean Start of 0 and the Will Delay is greater than zero."
    %% [v5 3.1.3.2.2]
    SendWill = case {CleanStart, WillProps} of
                   {false, #{'Will-Delay-Interval' := I}} when I > 0 ->
                       false;
                   _ ->
                       true
               end,
    ok = erpc:multicast([node() | nodes()],
                        ?MODULE,
                        remove_duplicate_client_id_connections,
                        [PgGroup, self(), SendWill]).

%% remove_duplicate_client_id_connections/2 is only called from 3.13 nodes.
%% Hence, this function can be deleted when mixed version clusters between
%% this version and 3.13 are disallowed.
-spec remove_duplicate_client_id_connections(
        {rabbit_types:vhost(), client_id()}, pid()) -> ok.
remove_duplicate_client_id_connections(PgGroup, PidToKeep) ->
    remove_duplicate_client_id_connections(PgGroup, PidToKeep, true).

-spec remove_duplicate_client_id_connections(
        {rabbit_types:vhost(), client_id()}, pid(), boolean()) -> ok.
remove_duplicate_client_id_connections(PgGroup, PidToKeep, SendWill) ->
    try persistent_term:get(?PG_SCOPE) of
        PgScope ->
            Pids = pg:get_local_members(PgScope, PgGroup),
            lists:foreach(fun(Pid) ->
                                  gen_server:cast(Pid, {duplicate_id, SendWill})
                          end, Pids -- [PidToKeep])
    catch _:badarg ->
              %% MQTT supervision tree on this node not fully started
              ok
    end.

-spec init_trace(rabbit_types:vhost(), binary()) ->
    {rabbit_trace:state(), undefined | binary()}.
init_trace(VHost, ConnName0) ->
    TraceState = rabbit_trace:init(VHost),
    ConnName = case rabbit_trace:enabled(TraceState) of
                   true ->
                       ConnName0;
                   false ->
                       %% Tracing does not need connection name.
                       %% Use less memmory by setting to undefined.
                       undefined
               end,
    {TraceState, ConnName}.

-spec update_trace(binary(), state()) -> state().
update_trace(ConnName0, State = #state{cfg = Cfg0 = #cfg{vhost = VHost}}) ->
    {TraceState, ConnName} = init_trace(VHost, ConnName0),
    Cfg = Cfg0#cfg{trace_state = TraceState,
                   conn_name = ConnName},
    State#state{cfg = Cfg}.

-spec self_consumes(amqqueue:amqqueue()) -> boolean().
self_consumes(Queue) ->
    case amqqueue:get_type(Queue) of
        ?QUEUE_TYPE_QOS_0 ->
            false;
        _ ->
            lists:any(fun(Consumer) ->
                              element(1, Consumer) =:= self()
                      end, rabbit_amqqueue:consumers(Queue))
    end.

handle_clean_start_qos0(State) ->
    handle_clean_start(false, ?QOS_0, State).

handle_clean_start_qos1(QoS0SessPresent, State) ->
    handle_clean_start(QoS0SessPresent, ?QOS_1, State).

handle_clean_start(_, QoS, State = #state{cfg = #cfg{clean_start = true}}) ->
    %% "If the Server accepts a connection with CleanSession set to 1, the Server
    %% MUST set Session Present to 0 in the CONNACK packet [MQTT-3.2.2-1].
    SessPresent = false,
    case get_queue(QoS, State) of
        {error, _} ->
            {ok, SessPresent, State};
        {ok, Q0} ->
            QName = amqqueue:get_name(Q0),
            case delete_queue(QName, State) of
                ok ->
                    {ok, SessPresent, State};
                {error, access_refused} ->
                    {error, ?RC_NOT_AUTHORIZED};
                {error, _Reason} ->
                    {error, ?RC_IMPLEMENTATION_SPECIFIC_ERROR}
            end
    end;
handle_clean_start(SessPresent, QoS,
                   State0 = #state{cfg = #cfg{clean_start = false,
                                              session_expiry_interval_secs = Expiry}}) ->
    case get_queue(QoS, State0) of
        {error, _} ->
            %% Queue will be created later when client subscribes.
            {ok, SessPresent, State0};
        {ok, Q} ->
            case consume(Q, QoS, State0) of
                {ok, State} ->
                    maybe_update_session_expiry_interval(Q, Expiry),
                    {ok, _SessionPresent = true, State};
                {error, access_refused} ->
                    {error, ?RC_NOT_AUTHORIZED};
                {error, _Reason} ->
                    {error, ?RC_IMPLEMENTATION_SPECIFIC_ERROR}
            end
    end.

-spec get_queue(qos(), state()) ->
    {ok, amqqueue:amqqueue()} |
    {error, not_found | {resource_locked, amqqueue:amqqueue()}}.
get_queue(QoS, State) ->
    QName = queue_name(QoS, State),
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} = Ok ->
            try rabbit_amqqueue:check_exclusive_access(Q, self()) of
                ok ->
                    Ok
            catch
                exit:#amqp_error{name = resource_locked} ->
                    %% This can happen when same client ID re-connects
                    %% while its old connection is not yet closed.
                    {error, {resource_locked, Q}}
            end;
        {error, not_found} = Err ->
            Err
    end.

-spec subscribe_result_to_reason_codes(nonempty_list(qos() | {error, term()}),
                                       protocol_version_atom()) ->
    nonempty_list(reason_code()).
subscribe_result_to_reason_codes(SubscribeResult, ProtoVer) ->
    lists:map(fun(QoS) when is_integer(QoS) ->
                      QoS;
                 ({error, Reason}) when ProtoVer =:= ?MQTT_PROTO_V5 ->
                      case Reason of
                          access_refused -> ?RC_NOT_AUTHORIZED;
                          queue_limit_exceeded -> ?RC_QUOTA_EXCEEDED;
                          _ -> ?RC_UNSPECIFIED_ERROR
                      end;
                 ({error, _}) ->
                      ?RC_UNSPECIFIED_ERROR
              end, SubscribeResult).

-spec queue_name(qos(), state()) -> rabbit_amqqueue:name().
queue_name(?QOS_1, #state{cfg = #cfg{queue_qos1 = #resource{kind = queue} = Name}}) ->
    Name;
queue_name(QoS, #state{cfg = #cfg{client_id = ClientId,
                                  vhost = VHost}}) ->
    QNameBin = rabbit_mqtt_util:queue_name_bin(ClientId, QoS),
    rabbit_misc:r(VHost, queue, QNameBin).

%% Returns names of queues that exist in the database.
-spec existing_queue_names(state()) -> [rabbit_amqqueue:name()].
existing_queue_names(State) ->
    QNames = [queue_name(QoS, State) || QoS <- [?QOS_0, ?QOS_1]],
    lists:filter(fun rabbit_amqqueue:exists/1, QNames).

%% To save memory, we only store the queue_qos1 value in process state if there is a QoS 1 subscription.
%% We store it in the process state such that we don't have to build the binary on every PUBACK we receive.
maybe_set_queue_qos1(?QOS_1, State = #state{cfg = Cfg = #cfg{queue_qos1 = undefined}}) ->
    State#state{cfg = Cfg#cfg{queue_qos1 = queue_name(?QOS_1, State)}};
maybe_set_queue_qos1(_, State) ->
    State.

-spec init_subscriptions(boolean(), state()) ->
    {ok, state()} | {error, reason_code()}.
init_subscriptions(_SessionPresent = _SubscriptionsPresent = true,
                   State = #state{cfg = #cfg{proto_ver = ProtoVer}}) ->
    maybe
        {ok, SubsQos0} ?= init_subscriptions0(?QOS_0, State),
        {ok, SubsQos1} ?= init_subscriptions0(?QOS_1, State),
        Subs = maps:merge(SubsQos0, SubsQos1),
        rabbit_global_counters:consumer_created(ProtoVer),
        %% Cache subscriptions in process state to avoid future mnesia:match_object/3 queries.
        {ok, State#state{subscriptions = Subs}}
    end;
init_subscriptions(_, State) ->
    {ok, State}.

%% We suppress a warning because rabbit_misc:table_lookup/2 declares the correct spec and
%% we must handle binding args v1 where binding arguments are not a valid AMQP 0.9.1 table.
-dialyzer({no_match, init_subscriptions0/2}).

-spec init_subscriptions0(qos(), state()) ->
    {ok, subscriptions()} | {error, reason_code()}.
init_subscriptions0(QoS, State = #state{cfg = #cfg{proto_ver = ProtoVer,
                                                   exchange = Exchange,
                                                   binding_args_v2 = BindingArgsV2}}) ->
    Bindings =
    rabbit_binding:list_for_source_and_destination(
      Exchange,
      queue_name(QoS, State),
      %% Querying table rabbit_route is catastrophic for CPU usage.
      %% Querying table rabbit_reverse_route is acceptable because
      %% the source exchange is always the same in the MQTT plugin whereas
      %% the destination queue is different for each MQTT client and
      %% rabbit_reverse_route is sorted by destination queue.
      _Reverse = true),
    try
        Subs = lists:map(
                 fun(#binding{key = Key,
                              args = Args = []}) ->
                         Opts = #mqtt_subscription_opts{qos = QoS},
                         TopicFilter = amqp_to_mqtt(Key),
                         case ProtoVer of
                             ?MQTT_PROTO_V5 ->
                                 %% session upgrade
                                 NewBindingArgs = binding_args_for_proto_ver(ProtoVer, TopicFilter, Opts, BindingArgsV2),
                                 ok = recreate_subscription(TopicFilter, Args, NewBindingArgs, QoS, State);
                             _ ->
                                 ok
                         end,
                         {TopicFilter, Opts};
                    (#binding{key = Key,
                              args = Args}) ->
                         TopicFilter = amqp_to_mqtt(Key),
                         Opts = case ProtoVer of
                                    ?MQTT_PROTO_V5 ->
                                        case rabbit_misc:table_lookup(Args, <<"x-mqtt-subscription-opts">>) of
                                            {table, Table} ->
                                                %% binding args v2
                                                subscription_opts_from_table(Table);
                                            undefined ->
                                                %% binding args v1
                                                Opts0 = #mqtt_subscription_opts{} = lists:keyfind(
                                                                                      mqtt_subscription_opts, 1, Args),
                                                case BindingArgsV2 of
                                                    true ->
                                                        %% Migrate v1 to v2.
                                                        %% Note that this migration must be in place even for some versions
                                                        %% (jump upgrade) after feature flag 'rabbitmq_4.1.0' has become
                                                        %% required since enabling the feature flag doesn't migrate binding
                                                        %% args for existing connections.
                                                        NewArgs = binding_args_for_proto_ver(
                                                                    ProtoVer, TopicFilter, Opts0, BindingArgsV2),
                                                        ok = recreate_subscription(TopicFilter, Args, NewArgs, QoS, State);
                                                    false ->
                                                        ok
                                                end,
                                                Opts0
                                        end;
                                    _ ->
                                        %% session downgrade
                                        ok = recreate_subscription(TopicFilter, Args, [], QoS, State),
                                        #mqtt_subscription_opts{qos = QoS}
                                end,
                         {TopicFilter, Opts}
                 end, Bindings),
        {ok, maps:from_list(Subs)}
    catch throw:{error, Reason} ->
              Rc = case Reason of
                       access_refused -> ?RC_NOT_AUTHORIZED;
                       _Other -> ?RC_IMPLEMENTATION_SPECIFIC_ERROR
                   end,
              {error, Rc}
    end.

recreate_subscription(TopicFilter, OldBindingArgs, NewBindingArgs, Qos, State) ->
    case add_subscription(TopicFilter, NewBindingArgs, Qos, State) of
        ok ->
            case delete_subscription(TopicFilter, OldBindingArgs, Qos, State) of
                ok ->
                    ok;
                {error, _} = Err ->
                    throw(Err)
            end;
        {error, _} = Err ->
            throw(Err)
    end.

-spec hand_off_to_retainer(pid(), topic(), mqtt_msg()) -> ok.
hand_off_to_retainer(RetainerPid, Topic0, Msg = #mqtt_msg{payload = Payload}) ->
    Topic = amqp_to_mqtt(Topic0),
    if Payload =:= <<>> ->
           rabbit_mqtt_retainer:clear(RetainerPid, Topic);
       true ->
           rabbit_mqtt_retainer:retain(RetainerPid, Topic, Msg)
    end.

-spec send_retained_messages([{topic_filter(), qos()}], state()) -> state().
send_retained_messages(Subscriptions, State) ->
    lists:foldl(fun({TopicFilter, Qos}, S) ->
                        send_retained_message(TopicFilter, Qos, S)
                end, State, Subscriptions).

-spec send_retained_message(topic_filter(), qos(), state()) -> state().
send_retained_message(TopicFilter0, SubscribeQos,
                      State0 = #state{packet_id = PacketId0,
                                      cfg = #cfg{retainer_pid = RPid}}) ->
    TopicFilter = amqp_to_mqtt(TopicFilter0),
    case rabbit_mqtt_retainer:fetch(RPid, TopicFilter) of
        undefined ->
            State0;
        #mqtt_msg{qos = MsgQos,
                  retain = Retain,
                  payload = Payload,
                  props = Props0} ->
            Qos = effective_qos(MsgQos, SubscribeQos),
            %% Wildcards are currently not supported when fetching retained
            %% messages. Therefore, TopicFilter must must be a topic name.
            {Topic, Props, State1} = process_topic_alias_outbound(TopicFilter, Props0, State0),
            {PacketId, State} = case Qos of
                                    ?QOS_0 ->
                                        {undefined, State1};
                                    ?QOS_1 ->
                                        {PacketId0,
                                         State1#state{packet_id = increment_packet_id(PacketId0)}}
                                end,
            Packet = #mqtt_packet{
                        fixed = #mqtt_packet_fixed{
                                   type = ?PUBLISH,
                                   qos  = Qos,
                                   dup  = false,
                                   retain = Retain
                                  },
                        variable = #mqtt_packet_publish{
                                      packet_id = PacketId,
                                      topic_name = Topic,
                                      props = Props
                                     },
                        payload = Payload},
            _ = send(Packet, State),
            State
    end.

clear_will_msg(#state{cfg = #cfg{vhost = Vhost,
                                 client_id = ClientId}} = State) ->
    QNameBin = rabbit_mqtt_util:queue_name_bin(ClientId, will),
    QName = #resource{virtual_host = Vhost, kind = queue, name = QNameBin},
    case delete_queue(QName, State) of
        ok -> ok;
        {error, access_refused} -> {error, ?RC_NOT_AUTHORIZED};
        {error, _Reason} -> {error, ?RC_IMPLEMENTATION_SPECIFIC_ERROR}
    end.

make_will_msg(#mqtt_packet_connect{will_flag = false}) ->
    {ok, undefined};
make_will_msg(#mqtt_packet_connect{will_flag = true,
                                   will_qos = ?QOS_2,
                                   proto_ver = 5}) ->
    {error, ?RC_QOS_NOT_SUPPORTED};
make_will_msg(#mqtt_packet_connect{will_flag = true,
                                   will_retain = Retain,
                                   will_qos = Qos,
                                   will_topic = Topic,
                                   will_props = Props,
                                   will_payload = Payload}) ->
    EffectiveQos = maybe_downgrade_qos(Qos),
    Correlation = case EffectiveQos of
                      ?QOS_0 -> undefined;
                      ?QOS_1 -> ?WILL_MSG_QOS_1_CORRELATION
                  end,
    {ok, #mqtt_msg{retain = Retain,
                   qos = EffectiveQos,
                   packet_id = Correlation,
                   topic = Topic,
                   dup = false,
                   props = Props,
                   payload = Payload}}.

check_vhost_exists(VHost, Username, PeerIp) ->
    case rabbit_vhost:exists(VHost) of
        true  ->
            ok;
        false ->
            ?LOG_ERROR("MQTT connection failed: virtual host '~s' does not exist", [VHost]),
            auth_attempt_failed(PeerIp, Username),
            {error, ?RC_BAD_USER_NAME_OR_PASSWORD}
    end.

check_vhost_connection_limit(VHost) ->
    case rabbit_vhost_limit:is_over_connection_limit(VHost) of
        false ->
            ok;
        {true, Limit} ->
            ?LOG_ERROR("MQTT connection failed: connection limit ~p is reached for vhost '~s'",
                       [Limit, VHost]),
            {error, ?RC_QUOTA_EXCEEDED}
    end.

check_vhost_alive(VHost) ->
    case rabbit_vhost_sup_sup:is_vhost_alive(VHost) of
        true ->
            ok;
        false ->
            ?LOG_ERROR("MQTT connection failed: vhost '~s' is down", [VHost]),
            {error, ?RC_NOT_AUTHORIZED}
    end.

check_user_login(VHost, Username, Password, ClientId, PeerIp, ConnName) ->
    AuthProps = [{vhost, VHost},
                  {client_id, ClientId},
                  {password, Password}],
    case rabbit_access_control:check_user_login(Username, AuthProps) of
        {ok, User = #user{username = Username1}} ->
            notify_auth_result(user_authentication_success, Username1, ConnName),
            {ok, User};
        {refused, Username, Msg, Args} ->
            ?LOG_ERROR("MQTT connection failed: access refused for user '~s':" ++ Msg,
                       [Username | Args]),
            notify_auth_result(user_authentication_failure, Username, ConnName),
            auth_attempt_failed(PeerIp, Username),
            {error, ?RC_BAD_USER_NAME_OR_PASSWORD}
    end.

notify_auth_result(AuthResult, Username, ConnName) ->
    rabbit_event:notify(AuthResult,
                        [{name, Username},
                         {connection_name, ConnName},
                         {connection_type, network}]).

check_user_connection_limit(Username) ->
    case rabbit_auth_backend_internal:is_over_connection_limit(Username) of
        false ->
            ok;
        {true, Limit} ->
            ?LOG_ERROR(
               "MQTT connection failed: connection limit ~p is reached for user ~s",
               [Limit, Username]),
            {error, ?RC_QUOTA_EXCEEDED}
    end.


check_vhost_access(VHost, User = #user{username = Username}, ClientId, PeerIp) ->
    AuthzCtx = #{<<"client_id">> => ClientId},
    try rabbit_access_control:check_vhost_access(
          User, VHost, {ip, PeerIp}, AuthzCtx) of
        ok ->
            {ok, AuthzCtx}
    catch exit:#amqp_error{name = not_allowed} ->
              ?LOG_ERROR("MQTT connection failed: access refused for user '~s' to vhost '~s'",
                         [Username, VHost]),
              auth_attempt_failed(PeerIp, Username),
              {error, ?RC_NOT_AUTHORIZED}
    end.

check_user_loopback(Username, PeerIp) ->
    case rabbit_access_control:check_user_loopback(Username, PeerIp) of
        ok ->
            ok;
        not_allowed ->
            ?LOG_WARNING("MQTT login failed: user '~s' can only connect via localhost",
                         [Username]),
            auth_attempt_failed(PeerIp, Username),
            {error, ?RC_NOT_AUTHORIZED}
    end.


ensure_credential_expiry_timer(User = #user{username = Username}, PeerIp) ->
    case rabbit_access_control:expiry_timestamp(User) of
        never ->
            ok;
        Ts when is_integer(Ts) ->
            Time = (Ts - os:system_time(second)) * 1000,
            ?LOG_DEBUG("Credential expires in ~b ms frow now "
                       "(absolute timestamp = ~b seconds since epoch)",
                       [Time, Ts]),
            case Time > 0 of
                true ->
                    _TimerRef = erlang:send_after(Time, self(), credential_expired),
                    ok;
                false ->
                    ?LOG_WARNING("Credential expired ~b ms ago", [abs(Time)]),
                    auth_attempt_failed(PeerIp, Username),
                    {error, ?RC_NOT_AUTHORIZED}
            end
    end.

get_vhost(UserBin, none, Port) ->
    get_vhost_no_ssl(UserBin, Port);
get_vhost(UserBin, SslLogin, Port) ->
    get_vhost_ssl(UserBin, SslLogin, Port).

get_vhost_no_ssl(UserBin, Port) ->
    case get_vhost_username(UserBin) of
        undefined ->
            case get_vhost_from_port_mapping(Port) of
                undefined ->
                    VhostFromConfig = rabbit_mqtt_util:env(vhost),
                    {plugin_configuration_or_default_vhost, {VhostFromConfig, UserBin}};
                VHostFromPortMapping ->
                    {port_to_vhost_mapping, {VHostFromPortMapping, UserBin}}
            end;
        VHostUser ->
            {vhost_in_username, VHostUser}
    end.

get_vhost_ssl(UserBin, SslLoginName, Port) ->
    case get_vhost_from_user_mapping(SslLoginName) of
        undefined ->
            case get_vhost_from_port_mapping(Port) of
                undefined ->
                    case get_vhost_username(UserBin) of
                        undefined ->
                            VhostFromConfig = rabbit_mqtt_util:env(vhost),
                            {plugin_configuration_or_default_vhost, {VhostFromConfig, UserBin}};
                        VHostUser ->
                            {vhost_in_username, VHostUser}
                    end;
                VHostFromPortMapping ->
                    {port_to_vhost_mapping, {VHostFromPortMapping, UserBin}}
            end;
        VHostFromCertMapping ->
            {client_cert_to_vhost_mapping, {VHostFromCertMapping, UserBin}}
    end.

get_vhost_username(UserBin) ->
    case application:get_env(?APP_NAME, ignore_colons_in_username) of
        {ok, true} -> undefined;
        _ ->
            %% split at the last colon, disallowing colons in username
            case re:split(UserBin, ":(?!.*?:)") of
                [Vhost, UserName] -> {Vhost,  UserName};
                [UserBin]         -> undefined;
                []                -> undefined
            end
    end.

get_vhost_from_user_mapping(User) ->
    UserVirtualHostMapping = rabbit_runtime_parameters:value_global(
                               mqtt_default_vhosts
                              ),
    get_vhost_from_user_mapping(User, UserVirtualHostMapping).

get_vhost_from_user_mapping(_User, not_found) ->
    undefined;
get_vhost_from_user_mapping(User, Mapping) ->
    M = rabbit_data_coercion:to_proplist(Mapping),
    case rabbit_misc:pget(User, M) of
        undefined ->
            undefined;
        VHost ->
            VHost
    end.

get_vhost_from_port_mapping(Port) ->
    PortVirtualHostMapping = rabbit_runtime_parameters:value_global(
                               mqtt_port_to_vhost_mapping
                              ),
    get_vhost_from_port_mapping(Port, PortVirtualHostMapping).

get_vhost_from_port_mapping(_Port, not_found) ->
    undefined;
get_vhost_from_port_mapping(Port, Mapping) ->
    M = rabbit_data_coercion:to_proplist(Mapping),
    Res = case rabbit_misc:pget(rabbit_data_coercion:to_binary(Port), M) of
              undefined ->
                  undefined;
              VHost ->
                  VHost
          end,
    Res.

creds(User, Pass, SSLLoginName) ->
    CredentialsProvided = User =/= undefined orelse Pass =/= undefined,
    ValidCredentials = is_binary(User) andalso is_binary(Pass) andalso Pass =/= <<>>,
    {ok, TLSAuth} = application:get_env(?APP_NAME, ssl_cert_login),
    SSLLoginProvided = TLSAuth =:= true andalso SSLLoginName =/= none,

    case {CredentialsProvided, ValidCredentials, SSLLoginProvided} of
        {true, true, _} ->
            %% Username and password take priority
            {ok, User, Pass};
        {true, false, _} ->
            %% Either username or password is provided
            {invalid_creds, {User, Pass}};
        {false, false, true} ->
            %% rabbitmq_mqtt.ssl_cert_login is true. SSL user name provided.
            %% Authenticating using username only.
            {ok, SSLLoginName, none};
        {false, false, false} ->
            {ok, AllowAnon} = application:get_env(?APP_NAME, allow_anonymous),
            case AllowAnon of
                true ->
                    case rabbit_auth_mechanism_anonymous:credentials() of
                        {ok, _, _} = Ok ->
                            Ok;
                        error ->
                            nocreds
                    end;
                false ->
                    nocreds
            end;
        _ ->
            nocreds
    end.

-spec auth_attempt_failed(inet:ip_address(), binary()) -> ok.
auth_attempt_failed(PeerIp, Username) ->
    rabbit_core_metrics:auth_attempt_failed(PeerIp, Username, mqtt),
    timer:sleep(?SILENT_CLOSE_DELAY).

maybe_downgrade_qos(?QOS_0) -> ?QOS_0;
maybe_downgrade_qos(?QOS_1) -> ?QOS_1;
maybe_downgrade_qos(?QOS_2) -> ?QOS_1.

process_topic_alias_inbound(#mqtt_packet_publish{topic_name = Topic,
                                                 props = Props0 = #{'Topic-Alias' := Alias}},
                            State = #state{topic_aliases = As = {Aliases, _},
                                           cfg = #cfg{client_id = ClientId}}) ->
    AliasMax = persistent_term:get(?PERSISTENT_TERM_TOPIC_ALIAS_MAXIMUM),
    case Alias > 0 andalso Alias =< AliasMax of
        true ->
            Props = maps:remove('Topic-Alias', Props0),
            if Topic =:= <<>> ->
                   case maps:find(Alias, Aliases) of
                       {ok, TopicName} ->
                           {ok, TopicName, Props, State};
                       error ->
                           ?LOG_WARNING("Unknown Topic Alias: ~b. Disconnecting MQTT client ~ts",
                                        [Alias, ClientId]),
                           {error, ?RC_PROTOCOL_ERROR}
                   end;
               is_binary(Topic) ->
                   Aliases1 = Aliases#{Alias => Topic},
                   State1 = State#state{topic_aliases = setelement(1, As, Aliases1)},
                   {ok, Topic, Props, State1}
            end;
        false ->
            ?LOG_WARNING("Invalid Topic Alias: ~b. Disconnecting MQTT client ~ts",
                         [Alias, ClientId]),
            {error, ?RC_TOPIC_ALIAS_INVALID}
    end;
process_topic_alias_inbound(#mqtt_packet_publish{topic_name = Topic, props = Props}, State) ->
    {ok, Topic, Props, State}.

process_topic_alias_outbound(Topic, Props, State = #state{cfg = #cfg{topic_alias_maximum_outbound = 0}}) ->
    {Topic, Props, State};
process_topic_alias_outbound(Topic, Props, State = #state{topic_aliases = As = {_, Aliases},
                                                          cfg = #cfg{topic_alias_maximum_outbound = Max}}) ->
    case Aliases of
        #{Topic := Alias} ->
            {<<>>, Props#{'Topic-Alias' => Alias}, State};
        _ ->
            MapSize = maps:size(Aliases),
            case MapSize < Max andalso
                 %% There's no point in sending a Topic Alias if the Topic Name has a length of only 1 byte
                 %% because sending a Topic Alias requires (at least) 3 bytes
                 %% (1 byte for the Property Identifier and 2 bytes for the Topic Alias value)
                 %% and sending the Topic Name directly also requires 3 bytes
                 %% (2 bytes String prefix length and 1 byte for the Topic Name).
                 byte_size(Topic) > 1 of
                true ->
                    Alias = MapSize + 1,
                    Aliases1 = Aliases#{Topic => Alias},
                    State1 = State#state{topic_aliases = setelement(2, As, Aliases1)},
                    {Topic, Props#{'Topic-Alias' => Alias}, State1};
                false ->
                    {Topic, Props, State}
            end
    end.

ensure_queue(QoS, State) ->
    case get_queue(QoS, State) of
        {ok, _Q} = Ok ->
            Ok;
        {error, {resource_locked, Q}} ->
            QName = amqqueue:get_name(Q),
            ?LOG_DEBUG("MQTT deleting exclusive ~s owned by ~p",
                       [rabbit_misc:rs(QName), ?amqqueue_v2_field_exclusive_owner(Q)]),
            case delete_queue(QName, State) of
                ok ->
                    create_queue(QoS, State);
                {error, _} = Err ->
                    Err;
                {protocol_error, _, _, _} = Err ->
                    {error, Err}
            end;
        {error, not_found} ->
            create_queue(QoS, State)
    end.

create_queue(Qos, State = #state{cfg = #cfg{session_expiry_interval_secs = SessionExpiry}}) ->
    Owner = queue_owner(SessionExpiry),
    Args = queue_args(Qos, SessionExpiry),
    Type = queue_type(Qos, SessionExpiry, Args),
    create_queue(Qos, Owner, Args, Type, State).

create_queue(QNamePart, QOwner, QArgs, QType,
             #state{cfg = #cfg{
                             vhost = VHost,
                             client_id = ClientId},
                    auth_state = #auth_state{
                                    user = User = #user{username = Username},
                                    authz_ctx = AuthzCtx}
                   }) ->
    QNameBin = rabbit_mqtt_util:queue_name_bin(ClientId, QNamePart),
    QName = rabbit_misc:r(VHost, queue, QNameBin),
    maybe
        %% configure access to queue required for queue.declare
        ok ?= check_resource_access(User, QName, configure, AuthzCtx),
        ok ?= case rabbit_misc:table_lookup(QArgs, <<"x-dead-letter-exchange">>) of
                  undefined ->
                      ok;
                  {longstr, XNameBin} ->
                      %% with DLX requires additionally read access to queue
                      %% and write access to DLX exchange
                      case check_resource_access(User, QName, read, AuthzCtx) of
                          ok ->
                              XName = #resource{virtual_host = VHost,
                                                kind = exchange,
                                                name = XNameBin},
                              check_resource_access(User, XName, write, AuthzCtx);
                          Err0 -> Err0
                      end
              end,
        rabbit_core_metrics:queue_declared(QName),
        Q0 = amqqueue:new(QName,
                          none,
                          _Durable = true,
                          _AutoDelete = false,
                          QOwner,
                          QArgs,
                          VHost,
                          #{user => Username},
                          QType),
        case rabbit_queue_type:declare(Q0, node()) of
            {new, Q} when ?is_amqqueue(Q) ->
                rabbit_core_metrics:queue_created(QName),
                {ok, Q};
            {error, queue_limit_exceeded, Reason, ReasonArgs} ->
                ?LOG_ERROR(Reason, ReasonArgs),
                {error, queue_limit_exceeded};
            Other ->
                ?LOG_ERROR("Failed to declare ~s: ~p",
                           [rabbit_misc:rs(QName), Other]),
                {error, queue_declare}
        end
    else
        {error, access_refused} = Err ->
            Err
    end.

-spec queue_owner(SessionExpiryInterval :: non_neg_integer()) ->
    pid() | none.
queue_owner(0) ->
    %% Session Expiry Interval set to 0 means that the Session ends when the Network
    %% Connection is closed. Therefore we want the queue to be auto deleted.
    %% Exclusive queues are auto deleted after node restart while auto-delete queues are not.
    %% Therefore make the durable queue exclusive.
    self();
queue_owner(_) ->
    none.

queue_args(_, 0) ->
    [];
queue_args(QoS, SessionExpiry) ->
    Args = queue_ttl_args(SessionExpiry),
    case {QoS, rabbit_mqtt_util:env(durable_queue_type)} of
        {?QOS_1, quorum} ->
            [{<<"x-queue-type">>, longstr, <<"quorum">>} | Args];
        _ ->
            Args
    end.

queue_ttl_args(infinity) ->
    [];
queue_ttl_args(SessionExpirySecs)
  when is_integer(SessionExpirySecs) andalso SessionExpirySecs > 0 ->
    [{?QUEUE_TTL_KEY, long, timer:seconds(SessionExpirySecs)}].

queue_type(?QOS_0, 0, _QArgs) ->
    ?QUEUE_TYPE_QOS_0;
queue_type(_, _, QArgs) ->
    rabbit_amqqueue:get_queue_type(QArgs).

consume(Q, QoS, #state{
                   queue_states = QStates0,
                   cfg = #cfg{prefetch = Prefetch},
                   auth_state = #auth_state{
                                   authz_ctx = AuthzCtx,
                                   user = User = #user{username = Username}}
                  } = State0) ->
    QName = amqqueue:get_name(Q),
    %% read access to queue required for basic.consume
    case check_resource_access(User, QName, read, AuthzCtx) of
        ok ->
            case amqqueue:get_type(Q) of
                ?QUEUE_TYPE_QOS_0 ->
                    %% Messages get delivered directly to our process without
                    %% explicitly calling rabbit_queue_type:consume/3.
                    {ok, State0};
                _ ->
                    Spec = #{no_ack => QoS =:= ?QOS_0,
                             channel_pid => self(),
                             limiter_pid => none,
                             limiter_active => false,
                             mode => {simple_prefetch, Prefetch},
                             consumer_tag => ?CONSUMER_TAG,
                             exclusive_consume => false,
                             args => [],
                             ok_msg => undefined,
                             acting_user => Username},
                    rabbit_amqqueue:with(
                      QName,
                      fun(Q1) ->
                              case rabbit_queue_type:consume(Q1, Spec, QStates0) of
                                  {ok, QStates} ->
                                      State1 = State0#state{queue_states = QStates},
                                      State = maybe_set_queue_qos1(QoS, State1),
                                      {ok, State};
                                  {error, Type, Fmt, Args} ->
                                      ?LOG_ERROR(Fmt, Args),
                                      {error, Type}
                              end
                      end)
            end;
        {error, access_refused} = Err ->
            Err
    end.

binding_args_for_proto_ver(?MQTT_PROTO_V3, _, _, _) ->
    [];
binding_args_for_proto_ver(?MQTT_PROTO_V4, _, _, _) ->
    [];
binding_args_for_proto_ver(?MQTT_PROTO_V5, TopicFilter, SubOpts0, V2) ->
    SubOpts = case V2 of
                  true ->
                      Table = subscription_opts_to_table(SubOpts0),
                      {<<"x-mqtt-subscription-opts">>, table, Table};
                  false ->
                      SubOpts0
              end,
    BindingKey = mqtt_to_amqp(TopicFilter),
    [SubOpts, {<<"x-binding-key">>, longstr, BindingKey}].

subscription_opts_to_table(#mqtt_subscription_opts{
                              qos = Qos,
                              no_local = NoLocal,
                              retain_as_published = RetainAsPublished,
                              retain_handling = RetainHandling,
                              id = Id}) ->
    Table0 = [{<<"qos">>, unsignedbyte, Qos},
              {<<"no-local">>, bool, NoLocal},
              {<<"retain-as-published">>, bool, RetainAsPublished},
              {<<"retain-handling">>, unsignedbyte, RetainHandling}],
    Table = case Id of
                undefined ->
                    Table0;
                _ ->
                    [{<<"id">>, unsignedint, Id} | Table0]
            end,
    rabbit_misc:sort_field_table(Table).

subscription_opts_from_table(Table) ->
    #{<<"qos">> := Qos,
      <<"no-local">> := NoLocal,
      <<"retain-as-published">> := RetainAsPublished,
      <<"retain-handling">> := RetainHandling
     } = Map = rabbit_misc:amqp_table(Table),
    #mqtt_subscription_opts{
       qos = Qos,
       no_local = NoLocal,
       retain_as_published = RetainAsPublished,
       retain_handling = RetainHandling,
       id = maps:get(<<"id">>, Map, undefined)}.

add_subscription(TopicFilter, BindingArgs, Qos, State)
  when is_integer(Qos) ->
    add_subscription(TopicFilter, BindingArgs, queue_name(Qos, State), State);
add_subscription(TopicFilter, BindingArgs, QName, State) ->
    binding_action_with_checks(QName, TopicFilter, BindingArgs, add, State).

delete_subscription(TopicFilter, BindingArgs, Qos, State) ->
    binding_action_with_checks(
      queue_name(Qos, State), TopicFilter, BindingArgs, remove, State).

%% "If a Server receives a SUBSCRIBE packet containing a Topic Filter that is identical to a
%% Non‑shared Subscription’s Topic Filter for the current Session, then it MUST replace that
%% existing Subscription with a new Subscription [MQTT-3.8.4-3]. The Topic Filter in the new
%% Subscription will be identical to that in the previous Subscription, although its
%% Subscription Options could be different." [v5 3.8.4]
maybe_delete_old_subscription(TopicFilter, Opts, State = #state{subscriptions = Subs,
                                                                cfg = #cfg{proto_ver = ProtoVer,
                                                                           binding_args_v2 = BindingArgsV2}}) ->
    case Subs of
        #{TopicFilter := OldOpts}
          when OldOpts =/= Opts ->
            delete_subscription(TopicFilter,
                                binding_args_for_proto_ver(ProtoVer, TopicFilter, OldOpts, BindingArgsV2),
                                OldOpts#mqtt_subscription_opts.qos,
                                State);
        _ ->
            ok
    end.

binding_action_with_checks(QName, TopicFilter, BindingArgs, Action,
                           State = #state{cfg = #cfg{exchange = ExchangeName},
                                          auth_state = AuthState}) ->
    %% Same permissions required for binding or unbinding queue to/from topic exchange.
    maybe
        ok ?= check_queue_write_access(QName, AuthState),
        ok ?= check_exchange_read_access(ExchangeName, AuthState),
        ok ?= check_topic_access(TopicFilter, read, State),
        ok ?= binding_action(ExchangeName, TopicFilter, QName, BindingArgs,
                             fun rabbit_binding:Action/2, AuthState)
    else
        {error, Reason} = Err ->
            ?LOG_ERROR("Failed to ~s binding between ~s and ~s for topic filter ~s: ~p",
                       [Action, rabbit_misc:rs(ExchangeName), rabbit_misc:rs(QName), TopicFilter, Reason]),
            Err
    end.

check_queue_write_access(QName, #auth_state{user = User,
                                            authz_ctx = AuthzCtx}) ->
    %% write access to queue required for queue.(un)bind
    check_resource_access(User, QName, write, AuthzCtx).

check_exchange_read_access(ExchangeName, #auth_state{user = User,
                                                     authz_ctx = AuthzCtx}) ->
    %% read access to exchange required for queue.(un)bind
    check_resource_access(User, ExchangeName, read, AuthzCtx).

binding_action(ExchangeName, TopicFilter, QName, BindingArgs,
               BindingFun, #auth_state{user = #user{username = Username}}) ->
    RoutingKey = mqtt_to_amqp(TopicFilter),
    Binding = #binding{source = ExchangeName,
                       destination = QName,
                       key = RoutingKey,
                       args = BindingArgs},
    BindingFun(Binding, Username).

publish_to_queues(
  #mqtt_msg{topic = Topic,
            packet_id = PacketId} = MqttMsg,
  #state{cfg = #cfg{exchange = ExchangeName = #resource{name = ExchangeNameBin},
                    delivery_flow = Flow,
                    conn_name = ConnName,
                    trace_state = TraceState,
                    msg_interceptor_ctx = MsgIcptCtx},
         auth_state = #auth_state{user = #user{username = Username}}} = State) ->
    Anns = #{?ANN_EXCHANGE => ExchangeNameBin,
             ?ANN_ROUTING_KEYS => [mqtt_to_amqp(Topic)]},
    Msg0 = mc:init(mc_mqtt, MqttMsg, Anns, mc_env()),
    case rabbit_exchange:lookup(ExchangeName) of
        {ok, Exchange} ->
            Msg = rabbit_msg_interceptor:intercept_incoming(Msg0, MsgIcptCtx),
            QNames0 = rabbit_exchange:route(Exchange, Msg, #{return_binding_keys => true}),
            QNames = drop_local(QNames0, State),
            rabbit_trace:tap_in(Msg, QNames, ConnName, Username, TraceState),
            Opts = maps_put_truthy(flow, Flow, maps_put_truthy(correlation, PacketId, #{})),
            deliver_to_queues(Msg, Opts, QNames, State);
        {error, not_found} ->
            ?LOG_ERROR("~s not found", [rabbit_misc:rs(ExchangeName)]),
            {error, exchange_not_found, State}
    end.

%% "Bit 2 of the Subscription Options represents the No Local option.
%% If the value is 1, Application Messages MUST NOT be forwarded to a connection with a ClientID
%% equal to the ClientID of the publishing connection [MQTT-3.8.3-3]." [v5 3.8.3.1]
drop_local(QNames, #state{subscriptions = Subs,
                          cfg = #cfg{proto_ver = ?MQTT_PROTO_V5,
                                     vhost = Vhost,
                                     client_id = ClientId}}) ->
    ClientIdSize = byte_size(ClientId),
    lists:filter(
      fun({#resource{virtual_host = Vhost0,
                     name = <<"mqtt-subscription-",
                              ClientId0:ClientIdSize/binary,
                              "qos", _:1/binary >>},
           #{binding_keys := BindingKeys}})
            when Vhost0 =:= Vhost andalso
                 ClientId0 =:= ClientId andalso
                 map_size(BindingKeys) > 0 ->
              rabbit_misc:maps_any(
                fun(BKey, true) ->
                        TopicFilter = amqp_to_mqtt(BKey),
                        case Subs of
                            #{TopicFilter := #mqtt_subscription_opts{
                                                no_local = NoLocal}} ->
                                not NoLocal;
                            _ ->
                                true
                        end
                end, BindingKeys);
         (_) ->
              true
      end, QNames);
drop_local(QNames, _) ->
    QNames.

deliver_to_queues(Message,
                  Options,
                  RoutedToQNames,
                  State0 = #state{queue_states = QStates0,
                                  cfg = #cfg{proto_ver = ProtoVer}}) ->
    Qs0 = rabbit_amqqueue:lookup_many(RoutedToQNames),
    Qs = rabbit_amqqueue:prepend_extra_bcc(Qs0),
    case rabbit_queue_type:deliver(Qs, Message, Options, QStates0) of
        {ok, QStates, Actions} ->
            rabbit_global_counters:messages_routed(ProtoVer, length(Qs)),
            State = process_routing_confirm(Options, Qs,
                                            State0#state{queue_states = QStates}),
            %% Actions must be processed after registering confirms as actions may
            %% contain rejections of publishes.
            {ok, handle_queue_actions(Actions, State)};
        {error, Reason} ->
            Corr = maps:get(correlation, Options, undefined),
            ?LOG_ERROR("Failed to deliver message with packet_id=~p to queues: ~p",
                       [Corr, Reason]),
            {error, Reason, State0}
    end.

process_routing_confirm(Options,
                        [], State = #state{cfg = #cfg{proto_ver = ProtoVer}})
  when not is_map_key(correlation, Options) ->
    rabbit_global_counters:messages_unroutable_dropped(ProtoVer, 1),
    State;
process_routing_confirm(#{correlation := ?WILL_MSG_QOS_1_CORRELATION},
                        [], State = #state{cfg = #cfg{proto_ver = ProtoVer}}) ->
    %% unroutable will message with QoS 1
    rabbit_global_counters:messages_unroutable_dropped(ProtoVer, 1),
    State;
process_routing_confirm(#{correlation := PktId},
                        [], State = #state{cfg = #cfg{proto_ver = ProtoVer}}) ->
    rabbit_global_counters:messages_unroutable_returned(ProtoVer, 1),
    send_puback(PktId, ?RC_NO_MATCHING_SUBSCRIBERS, State),
    State;
process_routing_confirm(#{correlation := ?WILL_MSG_QOS_1_CORRELATION},
                        [_|_], State) ->
    %% routable will message with QoS 1
    State;
process_routing_confirm(#{correlation := PktId},
                        Qs, State = #state{unacked_client_pubs = U0}) ->
    QNames = rabbit_amqqueue:queue_names(Qs),
    U = rabbit_mqtt_confirms:insert(PktId, QNames, U0),
    State#state{unacked_client_pubs = U};
process_routing_confirm(#{}, _, State) ->
    State.

-spec send_puback(packet_id() | list(packet_id()), reason_code(), state()) -> ok.
send_puback(PktIds0, ReasonCode, State)
  when is_list(PktIds0) ->
    case rabbit_node_monitor:pause_partition_guard() of
        ok ->
            %% Classic queues confirm messages unordered.
            %% Let's sort them here assuming most MQTT clients send with an increasing packet identifier.
            PktIds = lists:usort(PktIds0),
            lists:foreach(fun(Id) ->
                                  send_puback(Id, ReasonCode, State)
                          end, PktIds);
        pausing ->
            ok
    end;
send_puback(PktId, ReasonCode, State = #state{cfg = #cfg{proto_ver = ProtoVer}}) ->
    rabbit_global_counters:messages_confirmed(ProtoVer, 1),
    Packet = #mqtt_packet{fixed = #mqtt_packet_fixed{type = ?PUBACK},
                          variable = #mqtt_packet_puback{packet_id = PktId,
                                                         reason_code = ReasonCode}},
    _ = send(Packet, State),
    ok.

-spec send(mqtt_packet(), state()) ->
    ok | {error, packet_too_large}.
send(Packet, #state{cfg = #cfg{proto_ver = ProtoVer,
                               send_fun = SendFun,
                               max_packet_size_outbound = MaxPacketSize}}) ->
    send(Packet, proto_atom_to_integer(ProtoVer), SendFun, MaxPacketSize).

-spec send(mqtt_packet(), protocol_version(), send_fun(), max_packet_size()) ->
    ok | {error, packet_too_large}.
send(Packet, ProtoVer, SendFun, MaxPacketSize) ->
    Data = rabbit_mqtt_packet:serialise(Packet, ProtoVer),
    PacketSize = iolist_size(Data),
    if PacketSize =< MaxPacketSize ->
           ok = SendFun(Data);
       true ->
           %% "Where a Packet is too large to send, the Server MUST discard it without sending it
           %% and then behave as if it had completed sending that Application Message [MQTT-3.1.2-25]."
           case Packet#mqtt_packet.fixed#mqtt_packet_fixed.type of
               T when T =/= ?PUBLISH andalso
                      T =/= ?PUBACK ->
                   ?LOG_DEBUG("Dropping MQTT packet (type ~b). Packet size "
                              "(~b bytes) exceeds maximum packet size (~b bytes)",
                              [T, PacketSize, MaxPacketSize]);
               _ ->
                   ok
           end,
           {error, packet_too_large}
    end.

-spec send_disconnect(reason_code(), state()) -> ok.
send_disconnect(ReasonCode, #state{cfg = #cfg{proto_ver = ?MQTT_PROTO_V5}} = State) ->
    Packet = #mqtt_packet{fixed = #mqtt_packet_fixed{type = ?DISCONNECT},
                          variable = #mqtt_packet_disconnect{reason_code = ReasonCode}},
    _ = send(Packet, State),
    ok;
send_disconnect(_, _) ->
    ok.

-spec terminate(boolean(), rabbit_event:event_props(), state()) -> ok.
terminate(SendWill, Infos, State = #state{queue_states = QStates}) ->
    rabbit_queue_type:close(QStates),
    rabbit_core_metrics:connection_closed(self()),
    rabbit_event:notify(connection_closed, Infos),
    rabbit_networking:unregister_non_amqp_connection(self()),
    maybe_decrement_consumer(State),
    maybe_decrement_publisher(State),
    _ = maybe_delete_mqtt_qos0_queue(State),
    maybe_send_will(SendWill, State).

-spec maybe_send_will(boolean(), state()) -> ok.
maybe_send_will(
  true,
  #state{cfg = #cfg{will_msg = #mqtt_msg{
                                  props = Props = #{'Will-Delay-Interval' := Delay},
                                  topic = Topic} = MqttMsg,
                    session_expiry_interval_secs = SessionExpiry,
                    exchange = #resource{name = XName},
                    client_id = ClientId,
                    vhost = Vhost
                   }} = State)
  when is_integer(Delay) andalso Delay > 0 andalso SessionExpiry > 0 ->
    QArgs0 = queue_ttl_args(SessionExpiry),
    QArgs =  QArgs0 ++ [{<<"x-dead-letter-exchange">>, longstr, XName},
                        {<<"x-dead-letter-routing-key">>, longstr,  mqtt_to_amqp(Topic)}],
    T = erlang:monotonic_time(millisecond),
    case create_queue(will, none, QArgs, rabbit_queue_type:default(), State) of
        {ok, Q} ->
            %% "The Server delays publishing the Client’s Will Message until the Will Delay
            %% Interval has passed or the Session ends, whichever happens first." [v5 3.1.3.2.2]
            Ttl0 = timer:seconds(min(Delay, SessionExpiry)),
            Ttl = if SessionExpiry =:= infinity ->
                         Ttl0;
                     is_integer(SessionExpiry) ->
                         %% Queue creation could have taken several milliseconds.
                         Elapsed = erlang:monotonic_time(millisecond) - T,
                         SessionExpiryFromNow = timer:seconds(SessionExpiry) - Elapsed,
                         %% Ensure the Will Message is dead lettered BEFORE the queue expires.
                         %% 5 ms should be enough time to send out the Will Message.
                         %% The important bit is that, in the queue implementation, the
                         %% message expiry timer fires before the queue expiry timer.
                         %% From MQTT client perspective, the granularity of defined intervals
                         %% is seconds. So sending the Will Message a few milliseconds earlier
                         %% doesn't matter from the client's point of view.
                         %% However, we shouldn't send the Will Message too early because
                         %% "The Client can arrange for the Will Message to notify that Session
                         %% Expiry has occurred" [v5 3.1.2.5]
                         %% So, we don't want to send out a false positive session expiry
                         %% notification in case the client reconnects shortly after.
                         Interval0 = SessionExpiryFromNow - 5,
                         Interval = max(0, Interval0),
                         min(Ttl0, Interval)
                  end,
            DefaultX = #resource{virtual_host = Vhost,
                                 kind = exchange,
                                 name = ?DEFAULT_EXCHANGE_NAME},
            #resource{name = QNameBin} = amqqueue:get_name(Q),
            Anns0 = #{?ANN_EXCHANGE => ?DEFAULT_EXCHANGE_NAME,
                      ?ANN_ROUTING_KEYS => [QNameBin],
                      ttl => Ttl,
                      %% Persist message regardless of Will QoS since there is no noticable
                      %% performance benefit if that single message is transient. This ensures that
                      %% delayed Will Messages are not lost after a broker restart.
                      ?ANN_DURABLE => true},
            Anns = case Props of
                       #{'Message-Expiry-Interval' := MEI} ->
                           Anns0#{dead_letter_ttl => timer:seconds(MEI)};
                       _ ->
                           Anns0
                   end,
            Msg = mc:init(mc_mqtt, MqttMsg, Anns, mc_env()),
            case check_publish_permitted(DefaultX, Topic, State) of
                ok ->
                    ok = rabbit_queue_type:publish_at_most_once(DefaultX, Msg),
                    ?LOG_DEBUG("scheduled delayed Will Message to topic ~s "
                               "for MQTT client ID ~s to be sent in ~b ms",
                               [Topic, ClientId, Ttl]);
                {error, access_refused = Reason} ->
                    log_delayed_will_failure(Topic, ClientId, Reason)
            end;
        {error, Reason} ->
            log_delayed_will_failure(Topic, ClientId, Reason)
    end;
maybe_send_will(true, State = #state{cfg = #cfg{will_msg = WillMsg = #mqtt_msg{topic = Topic},
                                                client_id = ClientId}}) ->
    case publish_to_queues_with_checks(WillMsg, State) of
        {ok, _} ->
            ?LOG_DEBUG("sent Will Message to topic ~s for MQTT client ID ~s",
                       [Topic, ClientId]);
        {error, Reason, _} ->
            ?LOG_DEBUG("failed to send Will Message to topic ~s for MQTT client ID ~s: ~p",
                       [Topic, ClientId, Reason])
    end;
maybe_send_will(_, _) ->
    ok.

log_delayed_will_failure(Topic, ClientId, Reason) ->
    ?LOG_DEBUG("failed to schedule delayed Will Message to topic ~s for MQTT client ID ~s: ~p",
               [Topic, ClientId, Reason]).

maybe_delete_mqtt_qos0_queue(
  State = #state{cfg = #cfg{session_expiry_interval_secs = 0},
                 auth_state = #auth_state{user = #user{username = Username}}}) ->
    case get_queue(?QOS_0, State) of
        {ok, Q} ->
            %% double check we delete the right queue
            case {amqqueue:get_type(Q), amqqueue:get_pid(Q)} of
                {?QUEUE_TYPE_QOS_0, Pid}
                  when Pid =:= self() ->
                    rabbit_queue_type:delete(Q, false, false, Username);
                _ ->
                    ok
            end;
        _ ->
            ok
    end;
maybe_delete_mqtt_qos0_queue(_) ->
    ok.

-spec delete_queue(rabbit_amqqueue:name(), state()) ->
    ok |
    {error, access_refused} |
    {error, timeout} |
    {protocol_error, Type :: atom(), Reason :: string(), Args :: term()}.
delete_queue(QName,
             #state{auth_state = #auth_state{
                                    user = User = #user{username = Username},
                                    authz_ctx = AuthzCtx}}) ->
    %% configure access to queue required for queue.delete
    %% We only check access if the queue actually exists.
    rabbit_amqqueue:with(
      QName,
      fun (Q) ->
              case check_resource_access(User, QName, configure, AuthzCtx) of
                  ok ->
                      case rabbit_queue_type:delete(Q, false, false, Username) of
                          {ok, _} ->
                              ok;
                          Err ->
                              Err
                      end;
                  Err ->
                      Err
              end
      end,
      fun (not_found) ->
              ok;
          ({absent, Q, State})
            when State =:= crashed orelse
                 State =:= stopped ->
              case check_resource_access(User, QName, configure, AuthzCtx) of
                  ok ->
                      rabbit_classic_queue:delete_crashed(Q, Username);
                  Err ->
                      Err
              end;
          ({absent, _Q, _State}) ->
              ok
      end).

-spec handle_pre_hibernate() -> ok.
handle_pre_hibernate() ->
    erase(permission_cache),
    erase(topic_permission_cache),
    ok.

-spec handle_down(term(), state()) ->
    {ok, state()} | {error, Reason :: any()}.
handle_down({{'DOWN', QName}, _MRef, process, QPid, Reason},
            State0 = #state{queue_states = QStates0,
                            unacked_client_pubs = U0}) ->
    credit_flow:peer_down(QPid),
    case rabbit_queue_type:handle_down(QPid, QName, Reason, QStates0) of
        {ok, QStates1, Actions} ->
            State1 = State0#state{queue_states = QStates1},
            try handle_queue_actions(Actions, State1) of
                State ->
                    {ok, State}
            catch throw:consuming_queue_down ->
                    {error, consuming_queue_down}
            end;
        {eol, QStates1, QRef} ->
            {ConfirmPktIds, U} = rabbit_mqtt_confirms:remove_queue(QRef, U0),
            QStates = rabbit_queue_type:remove(QRef, QStates1),
            State = State0#state{queue_states = QStates,
                                 unacked_client_pubs = U},
            send_puback(ConfirmPktIds, ?RC_SUCCESS, State),
            try handle_queue_down(QName, State) of
                State ->
                    {ok, State}
            catch throw:consuming_queue_down ->
                    {error, consuming_queue_down}
            end
    end.

-spec handle_queue_event(
        {queue_event, rabbit_amqqueue:name() | ?QUEUE_TYPE_QOS_0, term()}, state()) ->
    {ok, state()} | {error, Reason :: any(), state()}.
handle_queue_event({queue_event, ?QUEUE_TYPE_QOS_0, {eol, QName}},
                   State0) ->
    try handle_queue_down(QName, State0) of
        State ->
            {ok, State}
    catch throw:consuming_queue_down ->
            {error, consuming_queue_down, State0}
    end;
handle_queue_event({queue_event, ?QUEUE_TYPE_QOS_0, Msg},
                   State0 = #state{qos0_messages_dropped = N}) ->
    State = case drop_qos0_message(State0) of
                false ->
                    deliver_one_to_client(Msg, false, State0);
                true ->
                    rabbit_global_counters:messages_dead_lettered(
                      maxlen, ?QUEUE_TYPE_QOS_0, disabled, 1),
                    State0#state{qos0_messages_dropped = N + 1}
            end,
    {ok, State};
handle_queue_event({queue_event, QName, Evt},
                   State0 = #state{queue_states = QStates0,
                                   unacked_client_pubs = U0}) ->
    case rabbit_queue_type:handle_event(QName, Evt, QStates0) of
        {ok, QStates, Actions} ->
            State1 = State0#state{queue_states = QStates},
            State = handle_queue_actions(Actions, State1),
            {ok, State};
        {eol, Actions} ->
            State1 = handle_queue_actions(Actions, State0),
            {ConfirmPktIds, U} = rabbit_mqtt_confirms:remove_queue(QName, U0),
            QStates = rabbit_queue_type:remove(QName, QStates0),
            State = State1#state{queue_states = QStates,
                                 unacked_client_pubs = U},
            send_puback(ConfirmPktIds, ?RC_SUCCESS, State),
            try handle_queue_down(QName, State) of
                State2 ->
                    {ok, State2}
            catch throw:consuming_queue_down ->
                    {error, consuming_queue_down, State}
            end;
        {protocol_error, _Type, _Reason, _ReasonArgs} = Error ->
            {error, Error, State0}
    end.

handle_queue_actions(Actions, #state{} = State0) ->
    lists:foldl(
      fun ({deliver, ?CONSUMER_TAG, Ack, Msgs}, S) ->
              deliver_to_client(Msgs, Ack, S);
          ({settled, QName, PktIds}, S = #state{unacked_client_pubs = U0}) ->
              {ConfirmPktIds, U} = rabbit_mqtt_confirms:confirm(PktIds, QName, U0),
              send_puback(ConfirmPktIds, ?RC_SUCCESS, S),
              S#state{unacked_client_pubs = U};
          ({rejected, _QName, PktIds}, S0 = #state{unacked_client_pubs = U0,
                                                   cfg = #cfg{proto_ver = ProtoVer}}) ->
              {RejectPktIds, U} = rabbit_mqtt_confirms:reject(PktIds, U0),
              S = S0#state{unacked_client_pubs = U},
              %% Negative acks are supported only in MQTT v5. In MQTT v3 and v4 we ignore
              %% rejected messages since we can only (but must not) send a positive ack.
              case ProtoVer of
                  ?MQTT_PROTO_V5 ->
                      send_puback(RejectPktIds, ?RC_IMPLEMENTATION_SPECIFIC_ERROR, S);
                  _ ->
                      ok
              end,
              S;
          ({block, QName}, S = #state{queues_soft_limit_exceeded = QSLE}) ->
              S#state{queues_soft_limit_exceeded = sets:add_element(QName, QSLE)};
          ({unblock, QName}, S = #state{queues_soft_limit_exceeded = QSLE}) ->
              S#state{queues_soft_limit_exceeded = sets:del_element(QName, QSLE)};
          ({queue_down, QName}, S) ->
              handle_queue_down(QName, S)
      end, State0, Actions).

handle_queue_down(QName, State0 = #state{cfg = #cfg{client_id = ClientId}}) ->
    %% Classic queue is down.
    case rabbit_mqtt_util:qos_from_queue_name(QName, ClientId) of
        no_consuming_queue ->
            State0;
        _QoS ->
            %% Consuming classic queue is down.
            ?LOG_INFO("Terminating MQTT connection because consuming ~s is down.",
                      [rabbit_misc:rs(QName)]),
            throw(consuming_queue_down)
    end.

deliver_to_client(Msgs, Ack, State) ->
    lists:foldl(fun(Msg, S) ->
                        deliver_one_to_client(Msg, Ack, S)
                end, State, Msgs).

deliver_one_to_client({QNameOrType, QPid, QMsgId, _Redelivered, Mc0} = Delivery,
                      AckRequired,
                      #state{cfg = #cfg{msg_interceptor_ctx = MsgIcptCtx}} = State0) ->
    SubscriberQoS = case AckRequired of
                        true -> ?QOS_1;
                        false -> ?QOS_0
                    end,
    Mc = rabbit_msg_interceptor:intercept_outgoing(Mc0, MsgIcptCtx),
    McMqtt = mc:convert(mc_mqtt, Mc, mc_env()),
    MqttMsg = #mqtt_msg{qos = PublisherQos} = mc:protocol_state(McMqtt),
    QoS = effective_qos(PublisherQos, SubscriberQoS),
    {SettleOp, State1} = maybe_publish_to_client(MqttMsg, Delivery, QoS, State0),
    State = maybe_auto_settle(AckRequired, SettleOp, QoS, QNameOrType, QMsgId, State1),
    ok = maybe_notify_sent(QNameOrType, QPid, State),
    State.

-spec effective_qos(qos(), qos()) -> qos().
effective_qos(PublisherQoS, SubscriberQoS) ->
    %% "The QoS of Application Messages sent in response to a Subscription MUST be the minimum
    %% of the QoS of the originally published message and the Maximum QoS granted by the Server
    %% [MQTT-3.8.4-8]."
    erlang:min(PublisherQoS, SubscriberQoS).

maybe_publish_to_client(_, {_, _, _, _Redelivered = true, _}, ?QOS_0, State) ->
    %% Do not redeliver to MQTT subscriber who gets message at most once.
    {complete, State};
maybe_publish_to_client(
  #mqtt_msg{retain = Retain,
            topic = Topic0,
            payload = Payload,
            props = Props0},
  {QNameOrType, _QPid, QMsgId, Redelivered, Mc} = Delivery,
  QoS, State0) ->
    MatchedTopicFilters = matched_topic_filters_v5(Mc, State0),
    Props1 = maybe_add_subscription_ids(MatchedTopicFilters, Props0, State0),
    {Topic, Props, State1} = process_topic_alias_outbound(Topic0, Props1, State0),
    {PacketId, State} = msg_id_to_packet_id(QMsgId, QoS, State1),
    Packet = #mqtt_packet{
                fixed = #mqtt_packet_fixed{
                           type = ?PUBLISH,
                           qos = QoS,
                           dup = Redelivered,
                           retain = retain(Retain, MatchedTopicFilters, State)},
                variable = #mqtt_packet_publish{
                              packet_id = PacketId,
                              topic_name = Topic,
                              props = Props},
                payload = Payload},
    SettleOp = case send(Packet, State) of
                   ok ->
                       trace_tap_out(Delivery, State),
                       message_delivered(QNameOrType, Redelivered, QoS, State),
                       complete;
                   {error, packet_too_large} ->
                       discard
               end,
    {SettleOp, State}.

matched_topic_filters_v5(Msg, #state{cfg = #cfg{proto_ver = ?MQTT_PROTO_V5}}) ->
    case mc:get_annotation(binding_keys, Msg) of
        undefined -> [];
        BKeys -> lists:map(fun rabbit_mqtt_util:amqp_to_mqtt/1, BKeys)
    end;
matched_topic_filters_v5(_, _) ->
    [].

maybe_add_subscription_ids(TopicFilters, Props, #state{subscriptions = Subs}) ->
    Ids = lists:filtermap(fun(T) -> case maps:get(T, Subs, undefined) of
                                        #mqtt_subscription_opts{id = Id}
                                          when is_integer(Id) ->
                                            {true, Id};
                                        _ ->
                                            false
                                    end
                          end, TopicFilters),
    case Ids of
        [] -> Props;
        _ -> maps:put('Subscription-Identifier', Ids, Props)
    end.

%% "Bit 3 of the Subscription Options represents the Retain As Published option.
%% If 1, Application Messages forwarded using this subscription keep the RETAIN
%% flag they were published with. If 0, Application Messages forwarded using
%% this subscription have the RETAIN flag set to 0." [v5 3.8.3.1]
retain(false, _, _) ->
    false;
retain(true, TopicFilters, #state{subscriptions = Subs}) ->
    lists:any(fun(T) -> case maps:get(T, Subs, undefined) of
                            #mqtt_subscription_opts{retain_as_published = Rap} -> Rap;
                            undefined -> false
                        end
              end, TopicFilters).

msg_id_to_packet_id(_, ?QOS_0, State) ->
    %% "A PUBLISH packet MUST NOT contain a Packet Identifier if its QoS value is set to 0 [MQTT-2.2.1-2]."
    {undefined, State};
msg_id_to_packet_id(QMsgId, ?QOS_1, #state{packet_id = PktId,
                                           unacked_server_pubs = U} = State) ->
    {PktId, State#state{packet_id = increment_packet_id(PktId),
                        unacked_server_pubs = maps:put(PktId, QMsgId, U)}}.

-spec increment_packet_id(packet_id()) -> packet_id().
increment_packet_id(Id)
  when Id >= ?MAX_PACKET_ID ->
    1;
increment_packet_id(Id) ->
    Id + 1.

maybe_auto_settle(_AckRequired = true, SettleOp, QoS, QName, QMsgId,
                  State = #state{queue_states = QStates0})
%% We have to auto-settle if the client is not going to ack the message. This happens
  when
      QoS =:= ?QOS_0 %% QoS 0 messages are never acked,
      orelse
      SettleOp =:= discard %% message was never sent to the client because it was too large
      ->
    {ok, QStates, Actions} = rabbit_queue_type:settle(QName, SettleOp, ?CONSUMER_TAG, [QMsgId], QStates0),
    handle_queue_actions(Actions, State#state{queue_states = QStates});
maybe_auto_settle(_, _, _, _, _, State) ->
    State.

maybe_notify_sent(?QUEUE_TYPE_QOS_0, _, _) ->
    ok;
maybe_notify_sent(QName, QPid, #state{queue_states = QStates}) ->
    case rabbit_queue_type:module(QName, QStates) of
        {ok, rabbit_classic_queue} ->
            rabbit_amqqueue:notify_sent(QPid, self());
        _ ->
            ok
    end.

trace_tap_out(Msg = {#resource{}, _, _, _, _},
              #state{auth_state = #auth_state{user = #user{username = Username}},
                     cfg = #cfg{conn_name = ConnName,
                                trace_state = TraceState}}) ->
    rabbit_trace:tap_out(Msg, ConnName, Username, TraceState);
trace_tap_out(Msg0 = {?QUEUE_TYPE_QOS_0, _, _, _, _},
              State = #state{cfg = #cfg{trace_state = TraceState}}) ->
    case rabbit_trace:enabled(TraceState) of
        false ->
            ok;
        true ->
            %% Pay penalty of creating queue name only if tracing is enabled.
            QName = queue_name(?QOS_0, State),
            Msg = setelement(1, Msg0, QName),
            trace_tap_out(Msg, State)
    end.

-spec publish_to_queues_with_checks(mqtt_msg(), state()) ->
    {ok, state()} | {error, any(), state()}.
publish_to_queues_with_checks(
  Msg = #mqtt_msg{topic = Topic,
                  retain = Retain},
  State = #state{cfg = #cfg{exchange = Exchange,
                            retainer_pid = RPid}}) ->
    case check_publish_permitted(Exchange, Topic, State) of
        ok ->
            case publish_to_queues(Msg, State) of
                {ok, _} = Ok ->
                    case Retain of
                        false ->
                            ok;
                        true ->
                            hand_off_to_retainer(RPid, Topic, Msg)
                    end,
                    Ok;
                Error ->
                    Error
            end;
        {error, access_refused} ->
            {error, access_refused, State}
    end.

-spec check_publish_permitted(rabbit_exchange:name(), topic(), state()) ->
    ok | {error, access_refused}.
check_publish_permitted(Exchange, Topic,
                         State = #state{auth_state = #auth_state{
                                                        user = User,
                                                        authz_ctx = AuthzCtx}}) ->
    case check_resource_access(User, Exchange, write, AuthzCtx) of
        ok -> check_topic_access(Topic, write, State);
        {error, access_refused} = E -> E
    end.

check_resource_access(User, Resource, Perm, Context) ->
    V = {Resource, Context, Perm},
    Cache = case get(permission_cache) of
                undefined -> [];
                Other     -> Other
            end,
    case lists:member(V, Cache) of
        true ->
            ok;
        false ->
            try rabbit_access_control:check_resource_access(User, Resource, Perm, Context) of
                ok ->
                    CacheTail = lists:sublist(Cache, ?MAX_PERMISSION_CACHE_SIZE-1),
                    put(permission_cache, [V | CacheTail]),
                    ok
            catch
                exit:#amqp_error{name = access_refused,
                                 explanation = Msg} ->
                    ?LOG_ERROR("MQTT resource access refused: ~s", [Msg]),
                    {error, access_refused}
            end
    end.

check_topic_access(
  Topic, Access,
  #state{auth_state = #auth_state{user = User = #user{username = Username}},
         cfg = #cfg{client_id = ClientId,
                    vhost = VHost,
                    exchange = XName = #resource{name = XNameBin}}}) ->
    Cache = case get(topic_permission_cache) of
                undefined -> [];
                Other     -> Other
            end,
    Key = {Topic, Username, ClientId, VHost, XNameBin, Access},
    case lists:member(Key, Cache) of
        true ->
            ok;
        false ->
            Resource = XName#resource{kind = topic},
            RoutingKey = mqtt_to_amqp(Topic),
            Context = #{routing_key  => RoutingKey,
                        variable_map => #{<<"username">> => Username,
                                          <<"vhost">> => VHost,
                                          <<"client_id">> => ClientId}},
            try rabbit_access_control:check_topic_access(User, Resource, Access, Context) of
                ok ->
                    CacheTail = lists:sublist(Cache, ?MAX_PERMISSION_CACHE_SIZE - 1),
                    put(topic_permission_cache, [Key | CacheTail]),
                    ok
            catch
                exit:#amqp_error{name = access_refused,
                                 explanation = Msg} ->
                    ?LOG_ERROR("MQTT topic access refused: ~s", [Msg]),
                    {error, access_refused}
            end
    end.

-spec drop_qos0_message(state()) ->
    boolean().
drop_qos0_message(State) ->
    mailbox_soft_limit_exceeded() andalso
    is_socket_busy(State#state.cfg#cfg.socket).

-spec mailbox_soft_limit_exceeded() ->
    boolean().
mailbox_soft_limit_exceeded() ->
    case persistent_term:get(?PERSISTENT_TERM_MAILBOX_SOFT_LIMIT) of
        Limit when Limit > 0 ->
            case erlang:process_info(self(), message_queue_len) of
                {message_queue_len, Len} when Len > Limit ->
                    true;
                _ ->
                    false
            end;
        _ ->
            false
    end.

is_socket_busy(Socket) ->
    case rabbit_net:getstat(Socket, [send_pend]) of
        {ok, [{send_pend, NumBytes}]}
          when is_integer(NumBytes) andalso NumBytes > 0 ->
            true;
        _ ->
            false
    end.

-spec throttle(boolean(), state()) -> boolean().
throttle(Conserve, #state{queues_soft_limit_exceeded = QSLE,
                          cfg = #cfg{published = Published}}) ->
    Conserve andalso Published orelse
    not sets:is_empty(QSLE) orelse
    credit_flow:blocked().

-spec info(rabbit_types:info_key(), state()) -> any().
info(host, #state{cfg = #cfg{ip_addr = Val}}) -> Val;
info(port, #state{cfg = #cfg{port = Val}}) -> Val;
info(peer_host, #state{cfg = #cfg{peer_ip_addr = Val}}) -> Val;
info(peer_port, #state{cfg = #cfg{peer_port = Val}}) -> Val;
info(connected_at, #state{cfg = #cfg{connected_at = Val}}) -> Val;
info(ssl_login_name, #state{cfg = #cfg{ssl_login_name = Val}}) -> Val;
info(user_who_performed_action, S) ->
    info(user, S);
info(user, #state{auth_state = #auth_state{user = #user{username = Val}}}) -> Val;
info(clean_sess, #state{cfg = #cfg{clean_start = CleanStart,
                                   session_expiry_interval_secs = SEI}}) ->
    %% "Setting Clean Start to 1 and a Session Expiry Interval of 0, is equivalent
    %% to setting CleanSession to 1 in the MQTT Specification Version 3.1.1."
    CleanStart andalso SEI =:= 0;
info(will_msg, #state{cfg = #cfg{will_msg = Val}}) -> Val;
info(retainer_pid, #state{cfg = #cfg{retainer_pid = Val}}) -> Val;
info(exchange, #state{cfg = #cfg{exchange = #resource{name = Val}}}) -> Val;
info(prefetch, #state{cfg = #cfg{prefetch = Val}}) -> Val;
info(messages_unconfirmed, #state{unacked_client_pubs = Val}) ->
    rabbit_mqtt_confirms:size(Val);
info(messages_unacknowledged, #state{unacked_server_pubs = Val}) ->
    maps:size(Val);
info(node, _) -> node();
info(client_id, #state{cfg = #cfg{client_id = Val}}) -> Val;
info(user_property, #state{cfg = #cfg{user_prop = Val}}) -> Val;
info(vhost, #state{cfg = #cfg{vhost = Val}}) -> Val;
%% for rabbitmq_management/priv/www/js/tmpl/connection.ejs
info(client_properties, #state{cfg = #cfg{client_id = ClientId,
                                          user_prop = Prop}}) ->
    L = [{client_id, longstr, ClientId}],
    if Prop =:= [] ->
           L;
       Prop =/= [] ->
           Tab = rabbit_misc:to_amqp_table(maps:from_list(Prop)),
           [{user_property, table, Tab} | L]
    end;
info(channel_max, _) -> 0;
%% Maximum packet size supported only in MQTT 5.0.
info(frame_max, _) -> 0;
%% SASL supported only in MQTT 5.0.
info(auth_mechanism, _) -> none;
info(Other, _) -> throw({bad_argument, Other}).

-spec ssl_login_name(rabbit_net:socket()) ->
    none | binary().
ssl_login_name(Sock) ->
    case rabbit_net:peercert(Sock) of
        {ok, C}              -> case rabbit_ssl:peer_cert_auth_name(C) of
                                    unsafe    -> none;
                                    not_found -> none;
                                    Name      -> Name
                                end;
        {error, no_peercert} -> none;
        nossl                -> none
    end.

-spec extract_ssl_cert_client_id_settings() -> none | rabbit_ssl:ssl_cert_login_type().
extract_ssl_cert_client_id_settings() ->
    case application:get_env(?APP_NAME, ssl_cert_client_id_from) of
        {ok, Mode} ->
            case Mode of
                subject_alternative_name -> extract_client_id_san_type(Mode);
                _ -> {Mode, undefined, undefined}
            end;
        undefined -> none
    end.

extract_client_id_san_type(Mode) ->
    {Mode,
        application:get_env(?APP_NAME, ssl_cert_login_san_type, dns),
        application:get_env(?APP_NAME, ssl_cert_login_san_index, 0)
    }.


-spec ssl_client_id(rabbit_net:socket(), rabbit_ssl:ssl_cert_login_type()) ->
    none | binary().
ssl_client_id(Sock, SslClientIdSettings) ->
    case rabbit_net:peercert(Sock) of
        {ok, C}              -> case rabbit_ssl:peer_cert_auth_name(SslClientIdSettings, C) of
                                    unsafe    -> none;
                                    not_found -> none;
                                    Name      -> Name
                                end;
        {error, no_peercert} -> none;
        nossl                -> none
    end.

-spec proto_integer_to_atom(protocol_version()) -> protocol_version_atom().
proto_integer_to_atom(3) ->
    ?MQTT_PROTO_V3;
proto_integer_to_atom(4) ->
    ?MQTT_PROTO_V4;
proto_integer_to_atom(5) ->
    ?MQTT_PROTO_V5.

-spec proto_atom_to_integer(protocol_version_atom()) -> protocol_version().
proto_atom_to_integer(?MQTT_PROTO_V3) ->
    3;
proto_atom_to_integer(?MQTT_PROTO_V4) ->
    4;
proto_atom_to_integer(?MQTT_PROTO_V5) ->
    5.

-spec proto_version_tuple(state()) -> tuple().
proto_version_tuple(#state{cfg = #cfg{proto_ver = ?MQTT_PROTO_V3}}) ->
    {3, 1, 0};
proto_version_tuple(#state{cfg = #cfg{proto_ver = ?MQTT_PROTO_V4}}) ->
    {3, 1, 1};
proto_version_tuple(#state{cfg = #cfg{proto_ver = ?MQTT_PROTO_V5}}) ->
    {5, 0}.

maybe_increment_publisher(State = #state{cfg = Cfg = #cfg{published = false,
                                                          proto_ver = ProtoVer}}) ->
    rabbit_global_counters:publisher_created(ProtoVer),
    State#state{cfg = Cfg#cfg{published = true}};
maybe_increment_publisher(State) ->
    State.

maybe_decrement_publisher(#state{cfg = #cfg{published = true,
                                            proto_ver = ProtoVer}}) ->
    rabbit_global_counters:publisher_deleted(ProtoVer);
maybe_decrement_publisher(_) ->
    ok.

%% Multiple subscriptions from the same connection count as one consumer.
maybe_increment_consumer(#state{subscriptions = OldSubs},
                         #state{subscriptions = NewSubs,
                                cfg = #cfg{proto_ver = ProtoVer}})
  when map_size(OldSubs) =:= 0 andalso
       map_size(NewSubs) > 0 ->
    rabbit_global_counters:consumer_created(ProtoVer);
maybe_increment_consumer(_, _) ->
    ok.

maybe_decrement_consumer(#state{subscriptions = Subs,
                                cfg = #cfg{proto_ver = ProtoVer}})
  when map_size(Subs) > 0 ->
    rabbit_global_counters:consumer_deleted(ProtoVer);
maybe_decrement_consumer(_) ->
    ok.

maybe_decrement_consumer(#state{subscriptions = OldSubs},
                         #state{subscriptions = NewSubs,
                                cfg = #cfg{proto_ver = ProtoVer}})
  when map_size(OldSubs) > 0 andalso
       map_size(NewSubs) =:= 0 ->
    rabbit_global_counters:consumer_deleted(ProtoVer);
maybe_decrement_consumer(_, _) ->
    ok.

message_acknowledged(QName, #state{queue_states = QStates,
                                   cfg = #cfg{proto_ver = ProtoVer}}) ->
    case rabbit_queue_type:module(QName, QStates) of
        {ok, QType} ->
            rabbit_global_counters:messages_acknowledged(ProtoVer, QType, 1);
        _ ->
            ok
    end.

message_delivered(?QUEUE_TYPE_QOS_0, false, ?QOS_0,
                  #state{cfg = #cfg{proto_ver = ProtoVer}}) ->
    rabbit_global_counters:messages_delivered(ProtoVer, ?QUEUE_TYPE_QOS_0, 1),
    %% Technically, the message is not acked to a queue at all.
    %% However, from a user perspective it is still auto acked because:
    %% "In automatic acknowledgement mode, a message is considered to be successfully
    %% delivered immediately after it is sent."
    rabbit_global_counters:messages_delivered_consume_auto_ack(ProtoVer, ?QUEUE_TYPE_QOS_0, 1);
message_delivered(QName, Redelivered, QoS,
                  #state{queue_states = QStates,
                         cfg = #cfg{proto_ver = ProtoVer}}) ->
    case rabbit_queue_type:module(QName, QStates) of
        {ok, QType} ->
            rabbit_global_counters:messages_delivered(ProtoVer, QType, 1),
            message_delivered_ack(QoS, ProtoVer, QType),
            message_redelivered(Redelivered, ProtoVer, QType);
        _ ->
            ok
    end.

message_delivered_ack(?QOS_0, ProtoVer, QType) ->
    rabbit_global_counters:messages_delivered_consume_auto_ack(ProtoVer, QType, 1);
message_delivered_ack(?QOS_1, ProtoVer, QType) ->
    rabbit_global_counters:messages_delivered_consume_manual_ack(ProtoVer, QType, 1).

message_redelivered(true, ProtoVer, QType) ->
    rabbit_global_counters:messages_redelivered(ProtoVer, QType, 1);
message_redelivered(_, _, _) ->
    ok.

%% "Reason Codes less than 0x80 indicate successful completion of an operation.
%% Reason Code values of 0x80 or greater indicate failure."
-spec is_success(reason_code()) -> boolean().
is_success(ReasonCode) ->
    ReasonCode < ?RC_UNSPECIFIED_ERROR.

-spec format_status(state()) -> map().
format_status(
  #state{queue_states = QState,
         unacked_client_pubs = UnackClientPubs,
         unacked_server_pubs = UnackSerPubs,
         packet_id = PackID,
         subscriptions = Subscriptions,
         auth_state = AuthState,
         queues_soft_limit_exceeded = QSLE,
         qos0_messages_dropped = Qos0MsgsDropped,
         cfg = #cfg{
                  socket = Socket,
                  proto_ver = ProtoVersion,
                  clean_start = CleanStart,
                  session_expiry_interval_secs = SessionExpiryInterval,
                  will_msg = WillMsg,
                  exchange = Exchange,
                  queue_qos1 = _,
                  published = Published,
                  ssl_login_name = SSLLoginName,
                  retainer_pid = RetainerPid,
                  delivery_flow = DeliveryFlow,
                  trace_state = TraceState,
                  prefetch = Prefetch,
                  client_id = ClientID,
                  conn_name = ConnName,
                  ip_addr = IpAddr,
                  port = Port,
                  peer_ip_addr = PeerIpAddr,
                  peer_port = PeerPort,
                  connected_at = ConnectedAt,
                  send_fun = _
                 }}) ->
    Cfg = #{socket => Socket,
            proto_ver => ProtoVersion,
            clean_start => CleanStart,
            session_expiry_interval_secs => SessionExpiryInterval,
            will_msg_defined => WillMsg =/= undefined,
            exchange => Exchange,
            published => Published,
            ssl_login_name => SSLLoginName,
            retainer_pid => RetainerPid,

            delivery_flow => DeliveryFlow,
            trace_state => TraceState,
            prefetch => Prefetch,
            client_id => ClientID,
            conn_name => ConnName,
            ip_addr => IpAddr,
            port => Port,
            peer_ip_addr => PeerIpAddr,
            peer_port => PeerPort,
            connected_at => ConnectedAt},
    #{cfg => Cfg,
      queue_states => rabbit_queue_type:format_status(QState),
      unacked_client_pubs => UnackClientPubs,
      unacked_server_pubs => UnackSerPubs,
      packet_id => PackID,
      subscriptions => Subscriptions,
      auth_state => AuthState,
      queues_soft_limit_exceeded => QSLE,
      qos0_messages_dropped => Qos0MsgsDropped}.

mc_env() ->
    case persistent_term:get(?PERSISTENT_TERM_EXCHANGE) of
        ?DEFAULT_MQTT_EXCHANGE ->
            #{};
        MqttX ->
            #{mqtt_x => MqttX}
    end.
