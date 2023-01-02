%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% This module contains code that is common to MQTT and Web MQTT connections.
-module(rabbit_mqtt_processor).

-export([info/2, initial_state/2, initial_state/4,
         process_packet/2, serialise/2,
         terminate/4, handle_pre_hibernate/0,
         handle_ra_event/2, handle_down/2, handle_queue_event/2,
         soft_limit_exceeded/1, proto_version_tuple/1,
         format_status/1]).

%% for testing purposes
-export([get_vhost_username/1, get_vhost/3, get_vhost_from_user_mapping/2]).

-export_type([state/0]).

-import(rabbit_mqtt_util, [mqtt_to_amqp/1,
                           amqp_to_mqtt/1]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("rabbit/include/amqqueue.hrl").
-include("rabbit_mqtt.hrl").
-include("rabbit_mqtt_packet.hrl").

-define(MAX_PERMISSION_CACHE_SIZE, 12).
-define(CONSUMER_TAG, <<"mqtt">>).

-record(auth_state, {username :: binary(),
                     user :: #user{},
                     vhost :: rabbit_types:vhost(),
                     authz_ctx :: #{binary() := binary()}
                    }).

-record(info, {prefetch :: non_neg_integer(),
               host :: inet:ip_address(),
               port :: inet:port_number(),
               peer_host :: inet:ip_address(),
               peer_port :: inet:port_number(),
               connected_at :: pos_integer()}).

-record(state,
        {socket,
         proto_ver :: option(mqtt310 | mqtt311),
         queue_states = rabbit_queue_type:init() :: rabbit_queue_type:state(),
         %% Packet IDs published to queues but not yet confirmed.
         unacked_client_pubs = rabbit_mqtt_confirms:init() :: rabbit_mqtt_confirms:state(),
         %% Packet IDs published to MQTT subscribers but not yet acknowledged.
         unacked_server_pubs = #{} :: #{packet_id() => QueueMsgId :: non_neg_integer()},
         %% Packet ID of next PUBLISH packet (with QoS > 0) sent from server to client.
         %% (Not to be confused with packet IDs sent from client to server which can be the
         %% same IDs because client and server assign IDs independently of each other.)
         packet_id = 1 :: packet_id(),
         client_id :: option(binary()),
         clean_sess :: option(boolean()),
         will_msg :: option(mqtt_msg()),
         exchange :: option(rabbit_exchange:name()),
         %% Set if client has at least one subscription with QoS 1.
         queue_qos1 :: option(rabbit_amqqueue:name()),
         has_published = false :: boolean(),
         ssl_login_name :: none | binary(),
         retainer_pid :: option(pid()),
         auth_state :: option(#auth_state{}),
         peer_addr :: inet:ip_address(),
         send_fun :: fun((Packet :: tuple(), state()) -> term()),
         register_state,
         conn_name :: option(binary()),
         info :: option(#info{}),
         delivery_flow :: flow | noflow,
         %% quorum queues and streams whose soft limit has been exceeded
         soft_limit_exceeded = sets:new([{version, 2}]) :: sets:set(),
         qos0_messages_dropped = 0 :: non_neg_integer()
        }).

-opaque state() :: #state{}.

-spec initial_state(Socket :: any(), ConnectionName :: binary()) ->
    state().
initial_state(Socket, ConnectionName) ->
    {ok, {PeerAddr, _PeerPort}} = rabbit_net:peername(Socket),
    initial_state(Socket,
                  ConnectionName,
                  fun serialise_and_send_to_client/2,
                  PeerAddr).

-spec initial_state(Socket :: any(),
                    ConnectionName :: binary(),
                    SendFun :: fun((mqtt_packet(), state()) -> any()),
                    PeerAddr :: inet:ip_address()) ->
    state().
initial_state(Socket, ConnectionName, SendFun, PeerAddr) ->
    Flow = case rabbit_misc:get_env(rabbit, mirroring_flow_control, true) of
               true   -> flow;
               false  -> noflow
           end,
    #state{socket         = Socket,
           conn_name      = ConnectionName,
           ssl_login_name = ssl_login_name(Socket),
           peer_addr      = PeerAddr,
           send_fun       = SendFun,
           delivery_flow  = Flow}.

-spec process_packet(mqtt_packet(), state()) ->
    {ok, state()} |
    {stop, disconnect, state()} |
    {error, Reason :: term(), state()}.
process_packet(#mqtt_packet{fixed = #mqtt_packet_fixed{type = Type}},
               State = #state{auth_state = undefined})
  when Type =/= ?CONNECT ->
    {error, connect_expected, State};
process_packet(Packet = #mqtt_packet{fixed = #mqtt_packet_fixed{type = Type}}, State) ->
    process_request(Type, Packet, State).

-spec process_request(packet_type(), mqtt_packet(), state()) ->
    {ok, state()} |
    {stop, disconnect, state()} |
    {error, Reason :: term(), state()}.
process_request(?CONNECT, Packet, State = #state{socket = Socket}) ->
    %% Check whether peer closed the connection.
    %% For example, this can happen when connection was blocked because of resource
    %% alarm and client therefore disconnected due to client side CONNACK timeout.
    case rabbit_net:socket_ends(Socket, inbound) of
        {error, Reason} ->
            {error, {socket_ends, Reason}, State};
        _ ->
            process_connect(Packet, State)
    end;

process_request(?PUBACK,
                #mqtt_packet{
                   variable = #mqtt_packet_publish{packet_id = PacketId}},
                #state{unacked_server_pubs = U0,
                       queue_states = QStates0,
                       queue_qos1 = QName} = State) ->
    case maps:take(PacketId, U0) of
        {QMsgId, U} ->
            case rabbit_queue_type:settle(QName, complete, ?CONSUMER_TAG, [QMsgId], QStates0) of
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
                Packet = #mqtt_packet{
                            fixed = Fixed = #mqtt_packet_fixed{qos = ?QOS_2}},
                State) ->
    % Downgrade QOS_2 to QOS_1
    process_request(?PUBLISH,
                    Packet#mqtt_packet{
                      fixed = Fixed#mqtt_packet_fixed{qos = ?QOS_1}},
                    State);
process_request(?PUBLISH,
                #mqtt_packet{
                   fixed = #mqtt_packet_fixed{qos = Qos,
                                              retain = Retain,
                                              dup = Dup },
                   variable = #mqtt_packet_publish{topic_name = Topic,
                                                   packet_id = PacketId },
                   payload = Payload},
                State0 = #state{retainer_pid = RPid,
                                unacked_client_pubs = U,
                                proto_ver = ProtoVer}) ->
    rabbit_global_counters:messages_received(ProtoVer, 1),
    State = maybe_increment_publisher(State0),
    Publish = fun() ->
                      Msg = #mqtt_msg{retain     = Retain,
                                      qos        = Qos,
                                      topic      = Topic,
                                      dup        = Dup,
                                      packet_id  = PacketId,
                                      payload    = Payload},
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
                      end
              end,
    case Qos of
        N when N > ?QOS_0 ->
            rabbit_global_counters:messages_received_confirm(ProtoVer, 1),
            case rabbit_mqtt_confirms:contains(PacketId, U) of
                false ->
                    publish_to_queues_with_checks(Topic, Publish, State);
                true ->
                    %% Client re-sent this PUBLISH packet.
                    %% We already sent this message to target queues awaiting confirmations.
                    %% Hence, we ignore this re-send.
                    {ok, State}
            end;
        _ ->
            publish_to_queues_with_checks(Topic, Publish, State)
    end;

process_request(?SUBSCRIBE,
                #mqtt_packet{
                   variable = #mqtt_packet_subscribe{
                                 packet_id  = SubscribePktId,
                                 topic_table = Topics},
                   payload = undefined},
                #state{send_fun = SendFun,
                       retainer_pid = RPid} = State0) ->
    rabbit_log_connection:debug("Received a SUBSCRIBE for topic(s) ~p", [Topics]),
    TopicNamesQos0 = topic_names(?QOS_0, State0),
    TopicNamesQos1 = topic_names(?QOS_1, State0),
    HasSubsBefore = TopicNamesQos0 =/= [] orelse TopicNamesQos1 =/= [],

    {QosResponse, State1} =
    lists:foldl(
      fun(_Topic, {[?SUBACK_FAILURE | _] = L, S}) ->
              %% Once a subscription failed, mark all following subscriptions
              %% as failed instead of creating bindings because we are going
              %% to close the client connection anyway.
              {[?SUBACK_FAILURE | L], S};
         (#mqtt_topic{name = TopicName,
                      qos = Qos0} = Topic,
          {L, S0}) ->
              QoS = supported_sub_qos(Qos0),
              case maybe_replace_old_sub(Topic, TopicNamesQos0, TopicNamesQos1, S0) of
                  {ok, S1} ->
                      case ensure_queue(QoS, S1) of
                          {ok, Q} ->
                              QName = amqqueue:get_name(Q),
                              case bind(QName, TopicName, S1) of
                                  {ok, _Output, S2} ->
                                      case self_consumes(Q) of
                                          false ->
                                              case consume(Q, QoS, S2) of
                                                  {ok, S3} ->
                                                      {[QoS | L], S3};
                                                  {error, _Reason} ->
                                                      {[?SUBACK_FAILURE | L], S2}
                                              end;
                                          true ->
                                              {[QoS | L], S2}
                                      end;
                                  {error, Reason, S2} ->
                                      rabbit_log:error("Failed to bind ~s with topic ~s: ~p",
                                                       [rabbit_misc:rs(QName), TopicName, Reason]),
                                      {[?SUBACK_FAILURE | L], S2}
                              end;
                          {error, _} ->
                              {[?SUBACK_FAILURE | L], S1}
                      end;
                  {error, _Reason, S1} ->
                      {[?SUBACK_FAILURE | L], S1}
              end
      end, {[], State0}, Topics),

    maybe_increment_consumer(HasSubsBefore, State1),
    SendFun(
      #mqtt_packet{fixed    = #mqtt_packet_fixed{type = ?SUBACK},
                   variable = #mqtt_packet_suback{
                                 packet_id = SubscribePktId,
                                 qos_table  = QosResponse}},
      State1),
    case QosResponse of
        [?SUBACK_FAILURE | _] ->
            {error, subscribe_error, State1};
        _ ->
            State = lists:foldl(fun(Topic, S) ->
                                        maybe_send_retained_message(RPid, Topic, S)
                                end, State1, Topics),
            {ok, State}
    end;

process_request(?UNSUBSCRIBE,
                #mqtt_packet{variable = #mqtt_packet_subscribe{packet_id  = PacketId,
                                                               topic_table = Topics},
                             payload = undefined},
                State0 = #state{send_fun = SendFun}) ->
    rabbit_log_connection:debug("Received an UNSUBSCRIBE for topic(s) ~p", [Topics]),
    HasSubsBefore = has_subs(State0),
    State = lists:foldl(
              fun(#mqtt_topic{name = TopicName}, #state{} = S0) ->
                      case find_queue_name(TopicName, S0) of
                          {ok, QName} ->
                              case unbind(QName, TopicName, S0) of
                                  {ok, _, S} ->
                                      S;
                                  {error, Reason, S} ->
                                      rabbit_log:error("Failed to unbind ~s with topic ~s: ~p",
                                                       [rabbit_misc:rs(QName), TopicName, Reason]),
                                      S
                              end;
                          {not_found, _} ->
                              S0
                      end
              end, State0, Topics),
    SendFun(
      #mqtt_packet{fixed    = #mqtt_packet_fixed {type       = ?UNSUBACK},
                   variable = #mqtt_packet_suback{packet_id = PacketId}},
      State),

    maybe_decrement_consumer(HasSubsBefore, State),
    {ok, State};

process_request(?PINGREQ, #mqtt_packet{}, State = #state{send_fun = SendFun}) ->
    rabbit_log_connection:debug("Received a PINGREQ"),
    SendFun(
      #mqtt_packet{fixed = #mqtt_packet_fixed{type = ?PINGRESP}},
      State),
    rabbit_log_connection:debug("Sent a PINGRESP"),
    {ok, State};

process_request(?DISCONNECT, #mqtt_packet{}, State) ->
    rabbit_log_connection:debug("Received a DISCONNECT"),
    {stop, disconnect, State}.

process_connect(#mqtt_packet{
                   variable = #mqtt_packet_connect{
                                 username   = Username,
                                 proto_ver  = ProtoVersion,
                                 clean_sess = CleanSess,
                                 client_id  = ClientId,
                                 keep_alive = Keepalive} = PacketConnect},
                State0 = #state{send_fun = SendFun}) ->
    rabbit_log_connection:debug("Received a CONNECT, client ID: ~s, username: ~s, "
                                "clean session: ~s, protocol version: ~p, keepalive: ~p",
                                [ClientId, Username, CleanSess, ProtoVersion, Keepalive]),
    {ReturnCode, SessionPresent, State} =
    case rabbit_misc:pipeline([fun check_protocol_version/1,
                               fun check_client_id/1,
                               fun check_credentials/2,
                               fun login/2,
                               fun register_client/2,
                               fun start_keepalive/2,
                               fun notify_connection_created/1,
                               fun handle_clean_session/2],
                              PacketConnect, State0) of
        {ok, SessionPresent0, State1} ->
            {?CONNACK_ACCEPT, SessionPresent0, State1};
        {error, ConnectionRefusedReturnCode, State1} ->
            {ConnectionRefusedReturnCode, false, State1}
    end,
    Response = #mqtt_packet{fixed = #mqtt_packet_fixed{type = ?CONNACK},
                            variable = #mqtt_packet_connack{
                                          session_present = SessionPresent,
                                          return_code = ReturnCode}},
    SendFun(Response, State),
    return_connack(ReturnCode, State).

check_protocol_version(#mqtt_packet_connect{proto_ver = ProtoVersion}) ->
    case lists:member(ProtoVersion, proplists:get_keys(?PROTOCOL_NAMES)) of
        true ->
            ok;
        false ->
            {error, ?CONNACK_UNACCEPTABLE_PROTO_VER}
    end.

check_client_id(#mqtt_packet_connect{clean_sess = false,
                                     client_id = <<>>}) ->
    {error, ?CONNACK_ID_REJECTED};
check_client_id(_) ->
    ok.

check_credentials(Packet = #mqtt_packet_connect{username = Username,
                                                password = Password},
                  State = #state{ssl_login_name = SslLoginName,
                                 peer_addr = PeerAddr}) ->
    Ip = list_to_binary(inet:ntoa(PeerAddr)),
    case creds(Username, Password, SslLoginName) of
        nocreds ->
            rabbit_core_metrics:auth_attempt_failed(Ip, <<>>, mqtt),
            rabbit_log_connection:error("MQTT login failed: no credentials provided"),
            {error, ?CONNACK_BAD_CREDENTIALS};
        {invalid_creds, {undefined, Pass}} when is_binary(Pass) ->
            rabbit_core_metrics:auth_attempt_failed(Ip, <<>>, mqtt),
            rabbit_log_connection:error("MQTT login failed: no username is provided"),
            {error, ?CONNACK_BAD_CREDENTIALS};
        {invalid_creds, {User, undefined}} when is_binary(User) ->
            rabbit_core_metrics:auth_attempt_failed(Ip, User, mqtt),
            rabbit_log_connection:error("MQTT login failed for user '~p': no password provided", [User]),
            {error, ?CONNACK_BAD_CREDENTIALS};
        {UserBin, PassBin} ->
            {ok, {UserBin, PassBin, Packet}, State}
    end.

login({UserBin, PassBin,
       Packet = #mqtt_packet_connect{client_id = ClientId0,
                                     clean_sess = CleanSess}},
      State0) ->
    ClientId = ensure_client_id(ClientId0),
    case process_login(UserBin, PassBin, ClientId, State0) of
        {ok, State} ->
            {ok, Packet, State#state{clean_sess = CleanSess,
                                     client_id = ClientId}};
        {error, _ConnectionRefusedReturnCode, _State} = Err ->
            Err
    end.

-spec ensure_client_id(binary()) -> binary().
ensure_client_id(<<>>) ->
    rabbit_data_coercion:to_binary(
      rabbit_misc:base64url(
        rabbit_guid:gen_secure()));
ensure_client_id(ClientId)
  when is_binary(ClientId) ->
    ClientId.

register_client(Packet = #mqtt_packet_connect{proto_ver = ProtoVersion},
                State = #state{client_id = ClientId,
                               socket = Socket,
                               auth_state = #auth_state{vhost = VHost}}) ->
    NewProcState =
    fun(RegisterState) ->
            rabbit_mqtt_util:register_clientid(VHost, ClientId),
            RetainerPid = rabbit_mqtt_retainer_sup:child_for_vhost(VHost),
            Prefetch = rabbit_mqtt_util:env(prefetch),
            {ok, {PeerHost, PeerPort, Host, Port}} = rabbit_net:socket_ends(Socket, inbound),
            ExchangeBin = rabbit_mqtt_util:env(exchange),
            ExchangeName = rabbit_misc:r(VHost, exchange, ExchangeBin),
            State#state{
              exchange = ExchangeName,
              will_msg = make_will_msg(Packet),
              retainer_pid = RetainerPid,
              register_state = RegisterState,
              proto_ver = proto_integer_to_atom(ProtoVersion),
              info = #info{prefetch = Prefetch,
                           peer_host = PeerHost,
                           peer_port = PeerPort,
                           host = Host,
                           port = Port,
                           connected_at = os:system_time(milli_seconds)
                          }}
    end,
    case rabbit_mqtt_ff:track_client_id_in_ra() of
        true ->
            case rabbit_mqtt_collector:register(ClientId, self()) of
                {ok, Corr} ->
                    {ok, NewProcState({pending, Corr})};
                {error, _} = Err ->
                    %% e.g. this node was removed from the MQTT cluster members
                    rabbit_log_connection:error("MQTT cannot accept a connection: "
                                                "client ID tracker is unavailable: ~p", [Err]),
                    {error, ?CONNACK_SERVER_UNAVAILABLE}
            end;
        false ->
            {ok, NewProcState(undefined)}
    end.

notify_connection_created(#mqtt_packet_connect{}) ->
    rabbit_networking:register_non_amqp_connection(self()),
    self() ! connection_created,
    ok.

return_connack(?CONNACK_ACCEPT, S) ->
    {ok, S};
return_connack(?CONNACK_BAD_CREDENTIALS, S) ->
    {error, unauthenticated, S};
return_connack(?CONNACK_NOT_AUTHORIZED, S) ->
    {error, unauthorized, S};
return_connack(?CONNACK_SERVER_UNAVAILABLE, S) ->
    {error, unavailable, S};
return_connack(?CONNACK_ID_REJECTED, S) ->
    {error, invalid_client_id, S};
return_connack(?CONNACK_UNACCEPTABLE_PROTO_VER, S) ->
    {error, unsupported_protocol_version, S}.

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

start_keepalive(#mqtt_packet_connect{keep_alive = Seconds},
                #state{socket = Socket}) ->
    ok = rabbit_mqtt_keepalive:start(Seconds, Socket).

handle_clean_session(_, State0 = #state{clean_sess = false,
                                        proto_ver = ProtoVer}) ->
    case get_queue(?QOS_1, State0) of
        {error, _} ->
            %% Queue will be created later when client subscribes.
            {ok, _SessionPresent = false, State0};
        {ok, Q} ->
            case consume(Q, ?QOS_1, State0) of
                {ok, State} ->
                    rabbit_global_counters:consumer_created(ProtoVer),
                    {ok, _SessionPresent = true, State};
                {error, access_refused} ->
                    {error, ?CONNACK_NOT_AUTHORIZED};
                {error, _Reason} ->
                    %% Let's use most generic error return code.
                    {error, ?CONNACK_SERVER_UNAVAILABLE}
            end
    end;
handle_clean_session(_, State = #state{clean_sess = true,
                                       auth_state = #auth_state{user = User,
                                                                username = Username,
                                                                authz_ctx = AuthzCtx}}) ->
    case get_queue(?QOS_1, State) of
        {error, _} ->
            {ok, _SessionPresent = false, State};
        {ok, Q0} ->
            QName = amqqueue:get_name(Q0),
            %% configure access to queue required for queue.delete
            case check_resource_access(User, QName, configure, AuthzCtx) of
                ok ->
                    delete_queue(QName, Username),
                    {ok, _SessionPresent = false, State};
                {error, access_refused} ->
                    {error, ?CONNACK_NOT_AUTHORIZED}
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

queue_name(QoS, #state{client_id = ClientId,
                       auth_state = #auth_state{vhost = VHost}}) ->
    QNameBin = rabbit_mqtt_util:queue_name_bin(ClientId, QoS),
    rabbit_misc:r(VHost, queue, QNameBin).

find_queue_name(TopicName, #state{exchange = Exchange} = State) ->
    RoutingKey = mqtt_to_amqp(TopicName),
    QNameQoS0 = queue_name(?QOS_0, State),
    case lookup_binding(Exchange, QNameQoS0, RoutingKey) of
        true ->
            {ok, QNameQoS0};
        false ->
            QNameQoS1 = queue_name(?QOS_1, State),
            case lookup_binding(Exchange, QNameQoS1, RoutingKey) of
                true ->
                    {ok, QNameQoS1};
                false ->
                    {not_found, []}
            end
    end.

lookup_binding(Exchange, QueueName, RoutingKey) ->
    B = #binding{source = Exchange,
                 destination = QueueName,
                 key = RoutingKey},
    lists:member(B, rabbit_binding:list_for_source_and_destination(Exchange, QueueName)).

has_subs(State) ->
    topic_names(?QOS_0, State) =/= [] orelse
    topic_names(?QOS_1, State) =/= [].

topic_names(QoS, #state{exchange = Exchange} = State) ->
    Bindings = rabbit_binding:list_for_source_and_destination(Exchange, queue_name(QoS, State)),
    lists:map(fun(B) -> amqp_to_mqtt(B#binding.key) end, Bindings).

%% "If a Server receives a SUBSCRIBE Packet containing a Topic Filter that is identical
%% to an existing Subscription’s Topic Filter then it MUST completely replace that
%% existing Subscription with a new Subscription. The Topic Filter in the new Subscription
%% will be identical to that in the previous Subscription, although its maximum QoS value
%% could be different. [...]" [MQTT-3.8.4-3].
%%
%% Therefore, if we receive a QoS 0 subscription for a topic that already has QoS 1,
%% we unbind QoS 1 (and vice versa).
maybe_replace_old_sub(#mqtt_topic{name = TopicName, qos = ?QOS_0},
                      _, OldTopicNamesQos1, State) ->
    QName = queue_name(?QOS_1, State),
    maybe_unbind(TopicName, OldTopicNamesQos1, QName, State);
maybe_replace_old_sub(#mqtt_topic{name = TopicName, qos = QoS},
                      OldTopicNamesQos0, _, State)
  when QoS =:= ?QOS_1 orelse QoS =:= ?QOS_2 ->
    QName = queue_name(?QOS_0, State),
    maybe_unbind(TopicName, OldTopicNamesQos0, QName, State).

maybe_unbind(TopicName, TopicNames, QName, State0) ->
    case lists:member(TopicName, TopicNames) of
        false ->
            {ok, State0};
        true ->
            case unbind(QName, TopicName, State0) of
                {ok, _Output, State} ->
                    {ok, State};
                {error, Reason, _State} = Err ->
                    rabbit_log:error("Failed to unbind ~s with topic '~s': ~p",
                                     [rabbit_misc:rs(QName), TopicName, Reason]),
                    Err
            end
    end.

-spec hand_off_to_retainer(pid(), binary(), mqtt_msg()) -> ok.
hand_off_to_retainer(RetainerPid, Topic0, #mqtt_msg{payload = <<"">>}) ->
    Topic1 = amqp_to_mqtt(Topic0),
    rabbit_mqtt_retainer:clear(RetainerPid, Topic1),
    ok;
hand_off_to_retainer(RetainerPid, Topic0, Msg) ->
    Topic1 = amqp_to_mqtt(Topic0),
    rabbit_mqtt_retainer:retain(RetainerPid, Topic1, Msg),
    ok.

maybe_send_retained_message(RPid, #mqtt_topic{name = Topic0, qos = SubscribeQos},
                            #state{packet_id = PacketId0,
                                   send_fun = SendFun} = State0) ->
    Topic1 = amqp_to_mqtt(Topic0),
    case rabbit_mqtt_retainer:fetch(RPid, Topic1) of
        undefined ->
            State0;
        Msg ->
            Qos = effective_qos(Msg#mqtt_msg.qos, SubscribeQos),
            {PacketId, State} = case Qos of
                                    ?QOS_0 ->
                                        {undefined, State0};
                                    ?QOS_1 ->
                                        {PacketId0, State0#state{packet_id = increment_packet_id(PacketId0)}}
                                end,
            SendFun(
              #mqtt_packet{fixed = #mqtt_packet_fixed{
                                      type = ?PUBLISH,
                                      qos  = Qos,
                                      dup  = false,
                                      retain = Msg#mqtt_msg.retain
                                     }, variable = #mqtt_packet_publish{
                                                      packet_id = PacketId,
                                                      topic_name = Topic1
                                                     },
                           payload = Msg#mqtt_msg.payload},
              State),
            State
    end.

make_will_msg(#mqtt_packet_connect{will_flag   = false}) ->
    undefined;
make_will_msg(#mqtt_packet_connect{will_retain = Retain,
                                   will_qos    = Qos,
                                   will_topic  = Topic,
                                   will_msg    = Msg}) ->
    #mqtt_msg{retain  = Retain,
              qos     = Qos,
              topic   = Topic,
              dup     = false,
              payload = Msg}.

process_login(_UserBin, _PassBin, ClientId,
              #state{peer_addr  = Addr,
                     auth_state = #auth_state{username = Username,
                                              user = User,
                                              vhost = VHost
                                             }} = State)
  when Username =/= undefined, User =/= undefined, VHost =/= underfined ->
    rabbit_core_metrics:auth_attempt_failed(list_to_binary(inet:ntoa(Addr)), Username, mqtt),
    rabbit_log_connection:error(
      "MQTT detected duplicate connect attempt for client ID '~ts', user '~ts', vhost '~ts'",
      [ClientId, Username, VHost]),
    {error, ?CONNACK_ID_REJECTED, State};
process_login(UserBin, PassBin, ClientId,
              #state{socket = Sock,
                     ssl_login_name = SslLoginName,
                     peer_addr = Addr,
                     auth_state = undefined} = State0) ->
    {ok, {_PeerHost, _PeerPort, _Host, Port}} = rabbit_net:socket_ends(Sock, inbound),
    {VHostPickedUsing, {VHost, UsernameBin}} = get_vhost(UserBin, SslLoginName, Port),
    rabbit_log_connection:debug(
      "MQTT vhost picked using ~s",
      [human_readable_vhost_lookup_strategy(VHostPickedUsing)]),
    RemoteIpAddressBin = list_to_binary(inet:ntoa(Addr)),
    Input = #{vhost => VHost,
              username_bin => UsernameBin,
              pass_bin => PassBin,
              client_id => ClientId},
    case rabbit_misc:pipeline(
           [fun check_vhost_exists/1,
            fun check_vhost_connection_limit/1,
            fun check_vhost_alive/1,
            fun check_user_login/2,
            fun check_user_connection_limit/1,
            fun check_vhost_access/2,
            fun check_user_loopback/2
           ],
           Input, State0) of
        {ok, _Output, State} ->
            rabbit_core_metrics:auth_attempt_succeeded(RemoteIpAddressBin, UsernameBin, mqtt),
            {ok, State};
        {error, _ConnectionRefusedReturnCode, _State} = Err ->
            rabbit_core_metrics:auth_attempt_failed(RemoteIpAddressBin, UsernameBin, mqtt),
            Err
    end.

check_vhost_exists(#{vhost := VHost,
                     username_bin := UsernameBin}) ->
    case rabbit_vhost:exists(VHost) of
        true  ->
            ok;
        false ->
            rabbit_log_connection:error("MQTT login failed for user '~s': virtual host '~s' does not exist",
                                        [UsernameBin, VHost]),
            {error, ?CONNACK_BAD_CREDENTIALS}
    end.

check_vhost_connection_limit(#{vhost := VHost,
                               client_id := ClientId,
                               username_bin := Username}) ->
    case rabbit_vhost_limit:is_over_connection_limit(VHost) of
        false ->
            ok;
        {true, Limit} ->
            rabbit_log_connection:error(
              "Failed to create MQTT connection because vhost connection limit is reached; "
              "vhost: '~s'; connection limit: ~p; user: '~s'; client ID '~s'",
              [VHost, Limit, Username, ClientId]),
            {error, ?CONNACK_NOT_AUTHORIZED}
    end.

check_vhost_alive(#{vhost := VHost,
                    client_id := ClientId,
                    username_bin := UsernameBin}) ->
    case rabbit_vhost_sup_sup:is_vhost_alive(VHost) of
        true  ->
            ok;
        false ->
            rabbit_log_connection:error(
              "Failed to create MQTT connection because vhost is down; "
              "vhost: ~s; user: ~s; client ID: ~s",
              [VHost, UsernameBin, ClientId]),
            {error, ?CONNACK_NOT_AUTHORIZED}
    end.

check_user_login(#{vhost := VHost,
                   username_bin := UsernameBin,
                   pass_bin := PassBin,
                   client_id := ClientId
                  } = In, State0) ->
    AuthProps = case PassBin of
                    none ->
                        %% SSL user name provided.
                        %% Authenticating using username only.
                        [];
                    _ ->
                        [{password, PassBin},
                         {vhost, VHost},
                         {client_id, ClientId}]
                end,
    case rabbit_access_control:check_user_login(
           UsernameBin, AuthProps) of
        {ok, User = #user{username = Username}} ->
            notify_auth_result(user_authentication_success, Username, State0),
            State = State0#state{
                      %% We won't need conn_name anymore.
                      %% Use less memmory by setting to undefined.
                      conn_name = undefined},
            {ok, maps:put(user, User, In), State};
        {refused, Username, Msg, Args} ->
            rabbit_log_connection:error(
              "Error on MQTT connection ~p~n"
              "access refused for user '~s' in vhost '~s' "
              ++ Msg,
              [self(), Username, VHost] ++ Args),
            notify_auth_result(user_authentication_failure, Username, State0),
            {error, ?CONNACK_BAD_CREDENTIALS}
    end.

notify_auth_result(AuthResult, Username, #state{conn_name = ConnName}) ->
    rabbit_event:notify(
      AuthResult,
      [{name, Username},
       {connection_name, ConnName},
       {connection_type, network}]).

check_user_connection_limit(#{user := #user{username = Username},
                              client_id := ClientId}) ->
    case rabbit_auth_backend_internal:is_over_connection_limit(Username) of
        false ->
            ok;
        {true, Limit} ->
            rabbit_log_connection:error(
              "Failed to create MQTT connection because user connection limit is reached; "
              "user: '~s'; connection limit: ~p; client ID '~s'",
              [Username, Limit, ClientId]),
            {error, ?CONNACK_NOT_AUTHORIZED}
    end.


check_vhost_access(#{vhost := VHost,
                     client_id := ClientId,
                     user := User = #user{username = Username}
                    } = In,
                   #state{peer_addr = PeerAddr} = State) ->
    AuthzCtx = #{<<"client_id">> => ClientId},
    try rabbit_access_control:check_vhost_access(
          User,
          VHost,
          {ip, PeerAddr},
          AuthzCtx) of
        ok ->
            {ok, maps:put(authz_ctx, AuthzCtx, In), State}
    catch exit:#amqp_error{name = not_allowed} ->
              rabbit_log_connection:error(
                "Error on MQTT connection ~p~n"
                "access refused for user '~s'",
                [self(), Username]),
              {error, ?CONNACK_NOT_AUTHORIZED}
    end.

check_user_loopback(#{vhost := VHost,
                      username_bin := UsernameBin,
                      user := User,
                      authz_ctx := AuthzCtx
                     },
                    #state{peer_addr = PeerAddr} = State) ->
    case rabbit_access_control:check_user_loopback(UsernameBin, PeerAddr) of
        ok ->
            AuthState = #auth_state{user = User,
                                    username = UsernameBin,
                                    vhost = VHost,
                                    authz_ctx = AuthzCtx},
            {ok, State#state{auth_state = AuthState}};
        not_allowed ->
            rabbit_log_connection:warning(
              "MQTT login failed: user '~s' can only connect via localhost",
              [UsernameBin]),
            {error, ?CONNACK_NOT_AUTHORIZED}
    end.

get_vhost(UserBin, none, Port) ->
    get_vhost_no_ssl(UserBin, Port);
get_vhost(UserBin, undefined, Port) ->
    get_vhost_no_ssl(UserBin, Port);
get_vhost(UserBin, SslLogin, Port) ->
    get_vhost_ssl(UserBin, SslLogin, Port).

get_vhost_no_ssl(UserBin, Port) ->
    case vhost_in_username(UserBin) of
        true  ->
            {vhost_in_username_or_default, get_vhost_username(UserBin)};
        false ->
            PortVirtualHostMapping = rabbit_runtime_parameters:value_global(
                                       mqtt_port_to_vhost_mapping
                                      ),
            case get_vhost_from_port_mapping(Port, PortVirtualHostMapping) of
                undefined ->
                    {default_vhost, {rabbit_mqtt_util:env(vhost), UserBin}};
                VHost ->
                    {port_to_vhost_mapping, {VHost, UserBin}}
            end
    end.

get_vhost_ssl(UserBin, SslLoginName, Port) ->
    UserVirtualHostMapping = rabbit_runtime_parameters:value_global(
                               mqtt_default_vhosts
                              ),
    case get_vhost_from_user_mapping(SslLoginName, UserVirtualHostMapping) of
        undefined ->
            PortVirtualHostMapping = rabbit_runtime_parameters:value_global(
                                       mqtt_port_to_vhost_mapping
                                      ),
            case get_vhost_from_port_mapping(Port, PortVirtualHostMapping) of
                undefined ->
                    {vhost_in_username_or_default, get_vhost_username(UserBin)};
                VHostFromPortMapping ->
                    {port_to_vhost_mapping, {VHostFromPortMapping, UserBin}}
            end;
        VHostFromCertMapping ->
            {cert_to_vhost_mapping, {VHostFromCertMapping, UserBin}}
    end.

vhost_in_username(UserBin) ->
    case application:get_env(?APP_NAME, ignore_colons_in_username) of
        {ok, true} -> false;
        _ ->
            %% split at the last colon, disallowing colons in username
            case re:split(UserBin, ":(?!.*?:)") of
                [_, _]      -> true;
                [UserBin]   -> false
            end
    end.

get_vhost_username(UserBin) ->
    Default = {rabbit_mqtt_util:env(vhost), UserBin},
    case application:get_env(?APP_NAME, ignore_colons_in_username) of
        {ok, true} -> Default;
        _ ->
            %% split at the last colon, disallowing colons in username
            case re:split(UserBin, ":(?!.*?:)") of
                [Vhost, UserName] -> {Vhost,  UserName};
                [UserBin]         -> Default
            end
    end.

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

human_readable_vhost_lookup_strategy(vhost_in_username_or_default) ->
    "vhost in username or default";
human_readable_vhost_lookup_strategy(port_to_vhost_mapping) ->
    "MQTT port to vhost mapping";
human_readable_vhost_lookup_strategy(cert_to_vhost_mapping) ->
    "client certificate to vhost mapping";
human_readable_vhost_lookup_strategy(default_vhost) ->
    "plugin configuration or default".

creds(User, Pass, SSLLoginName) ->
    DefaultUser   = rabbit_mqtt_util:env(default_user),
    DefaultPass   = rabbit_mqtt_util:env(default_pass),
    {ok, Anon}    = application:get_env(?APP_NAME, allow_anonymous),
    {ok, TLSAuth} = application:get_env(?APP_NAME, ssl_cert_login),
    HaveDefaultCreds = Anon =:= true andalso
        is_binary(DefaultUser) andalso
        is_binary(DefaultPass),

    CredentialsProvided = User =/= undefined orelse Pass =/= undefined,
    CorrectCredentials = is_binary(User) andalso is_binary(Pass),
    SSLLoginProvided = TLSAuth =:= true andalso SSLLoginName =/= none,

    case {CredentialsProvided, CorrectCredentials, SSLLoginProvided, HaveDefaultCreds} of
        %% Username and password take priority
        {true, true, _, _}          -> {User, Pass};
        %% Either username or password is provided
        {true, false, _, _}         -> {invalid_creds, {User, Pass}};
        %% rabbitmq_mqtt.ssl_cert_login is true. SSL user name provided.
        %% Authenticating using username only.
        {false, false, true, _}     -> {SSLLoginName, none};
        %% Anonymous connection uses default credentials
        {false, false, false, true} -> {DefaultUser, DefaultPass};
        _                           -> nocreds
    end.

supported_sub_qos(?QOS_0) -> ?QOS_0;
supported_sub_qos(?QOS_1) -> ?QOS_1;
supported_sub_qos(?QOS_2) -> ?QOS_1.

delivery_mode(?QOS_0) -> 1;
delivery_mode(?QOS_1) -> 2;
delivery_mode(?QOS_2) -> 2.

ensure_queue(QoS, State = #state{auth_state = #auth_state{user = #user{username = Username}}}) ->
    case get_queue(QoS, State) of
        {ok, Q} ->
            {ok, Q};
        {error, {resource_locked, Q}} ->
            QName = amqqueue:get_name(Q),
            rabbit_log:debug("MQTT deleting exclusive ~s owned by ~p",
                             [rabbit_misc:rs(QName),
                              ?amqqueue_v2_field_exclusive_owner(Q)]),
            delete_queue(QName, Username),
            create_queue(QoS, State);
        {error, not_found} ->
            create_queue(QoS, State)
    end.

create_queue(QoS, #state{
                     client_id = ClientId,
                     clean_sess = CleanSess,
                     auth_state = #auth_state{
                                     vhost = VHost,
                                     user = User = #user{username = Username},
                                     authz_ctx = AuthzCtx}
                    }) ->
    QNameBin = rabbit_mqtt_util:queue_name_bin(ClientId, QoS),
    QName = rabbit_misc:r(VHost, queue, QNameBin),
    %% configure access to queue required for queue.declare
    case check_resource_access(User, QName, configure, AuthzCtx) of
        ok ->
            case rabbit_vhost_limit:is_over_queue_limit(VHost) of
                false ->
                    rabbit_core_metrics:queue_declared(QName),
                    QArgs = queue_args(QoS, CleanSess),
                    Q0 = amqqueue:new(QName,
                                      self(),
                                      _Durable = true,
                                      _AutoDelete = false,
                                      queue_owner(QoS, CleanSess),
                                      QArgs,
                                      VHost,
                                      #{user => Username},
                                      queue_type(QoS, CleanSess, QArgs)
                                     ),
                    case rabbit_queue_type:declare(Q0, node()) of
                        {new, Q} when ?is_amqqueue(Q) ->
                            rabbit_core_metrics:queue_created(QName),
                            {ok, Q};
                        Other ->
                            rabbit_log:error("Failed to declare ~s: ~p",
                                             [rabbit_misc:rs(QName), Other]),
                            {error, queue_declare}
                    end;
                {true, Limit} ->
                    rabbit_log:error("cannot declare ~s because "
                                     "queue limit ~p in vhost '~s' is reached",
                                     [rabbit_misc:rs(QName), Limit, VHost]),
                    {error, access_refused}
            end;
        {error, access_refused} = E ->
            E
    end.

queue_owner(QoS, CleanSess)
  when QoS =:= ?QOS_0 orelse CleanSess ->
    %% Exclusive queues are auto-deleted after node restart while auto-delete queues are not.
    %% Therefore make durable queue exclusive.
    self();
queue_owner(_, _) ->
    none.

queue_args(QoS, CleanSess)
  when QoS =:= ?QOS_0 orelse CleanSess ->
    [];
queue_args(_, _) ->
    Args = case rabbit_mqtt_util:env(subscription_ttl) of
               Ms when is_integer(Ms) ->
                   [{<<"x-expires">>, long, Ms}];
               _ ->
                   []
           end,
    case rabbit_mqtt_util:env(durable_queue_type) of
        quorum ->
            [{<<"x-queue-type">>, longstr, <<"quorum">>} | Args];
        _ ->
            Args
    end.

queue_type(?QOS_0, true, QArgs) ->
    case rabbit_feature_flags:is_enabled(?QUEUE_TYPE_QOS_0) of
        true ->
            ?QUEUE_TYPE_QOS_0;
        false ->
            rabbit_amqqueue:get_queue_type(QArgs)
    end;
queue_type(_, _, QArgs) ->
    rabbit_amqqueue:get_queue_type(QArgs).

consume(Q, QoS, #state{
                   queue_states = QStates0,
                   auth_state = #auth_state{
                                   user = User = #user{username = Username},
                                   authz_ctx = AuthzCtx},
                   info = #info{prefetch = Prefetch}
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
                             prefetch_count => Prefetch,
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
                                  {error, Reason} = Err ->
                                      rabbit_log:error("Failed to consume from ~s: ~p",
                                                       [rabbit_misc:rs(QName), Reason]),
                                      Err
                              end
                      end)
            end;
        {error, access_refused} = Err ->
            Err
    end.

%% To save memory, we only store the queue_qos1 value in process state if there is a QoS 1 subscription.
%% We store it in the process state such that we don't have to build the binary on every PUBACK we receive.
maybe_set_queue_qos1(?QOS_1, State = #state{queue_qos1 = undefined}) ->
    State#state{queue_qos1 = queue_name(?QOS_1, State)};
maybe_set_queue_qos1(_, State) ->
    State.

bind(QueueName, TopicName, State) ->
    binding_action_with_checks({QueueName, TopicName, fun rabbit_binding:add/2}, State).
unbind(QueueName, TopicName, State) ->
    binding_action_with_checks({QueueName, TopicName, fun rabbit_binding:remove/2}, State).

binding_action_with_checks(Input, State) ->
    %% Same permission checks required for both binding and unbinding
    %% queue to / from topic exchange.
    rabbit_misc:pipeline(
      [fun check_queue_write_access/2,
       fun check_exchange_read_access/2,
       fun check_topic_access/2,
       fun binding_action/2],
      Input, State).

check_queue_write_access(
  {QueueName, _, _},
  #state{auth_state = #auth_state{
                         user = User,
                         authz_ctx = AuthzCtx}}) ->
    %% write access to queue required for queue.(un)bind
    check_resource_access(User, QueueName, write, AuthzCtx).

check_exchange_read_access(
  _, #state{exchange = ExchangeName,
            auth_state = #auth_state{
                            user = User,
                            authz_ctx = AuthzCtx}}) ->
    %% read access to exchange required for queue.(un)bind
    check_resource_access(User, ExchangeName, read, AuthzCtx).

check_topic_access({_, TopicName, _}, State) ->
    check_topic_access(TopicName, read, State).

binding_action(
  {QueueName, TopicName, BindingFun},
  #state{exchange = ExchangeName,
         auth_state = #auth_state{
                         user = #user{username = Username}}
        }) ->
    RoutingKey = mqtt_to_amqp(TopicName),
    Binding = #binding{source = ExchangeName,
                       destination = QueueName,
                       key = RoutingKey},
    BindingFun(Binding, Username).

publish_to_queues(
  #mqtt_msg{qos        = Qos,
            topic      = Topic,
            dup        = Dup,
            packet_id  = PacketId,
            payload    = Payload},
  #state{exchange       = ExchangeName,
         delivery_flow  = Flow} = State) ->
    RoutingKey = mqtt_to_amqp(Topic),
    Confirm = Qos > ?QOS_0,
    Headers = [{<<"x-mqtt-publish-qos">>, byte, Qos},
               {<<"x-mqtt-dup">>, bool, Dup}],
    Props = #'P_basic'{
               headers = Headers,
               delivery_mode = delivery_mode(Qos)},
    {ClassId, _MethodId} = rabbit_framing_amqp_0_9_1:method_id('basic.publish'),
    Content = #content{
                 class_id = ClassId,
                 properties = Props,
                 properties_bin = none,
                 protocol = none,
                 payload_fragments_rev = [Payload]
                },
    BasicMessage = #basic_message{
                      exchange_name = ExchangeName,
                      routing_keys = [RoutingKey],
                      content = Content,
                      id = <<>>, %% GUID set in rabbit_classic_queue
                      is_persistent = Confirm
                     },
    Delivery = #delivery{
                  mandatory = false,
                  confirm = Confirm,
                  sender = self(),
                  message = BasicMessage,
                  msg_seq_no = PacketId,
                  flow = Flow
                 },
    case rabbit_exchange:lookup(ExchangeName) of
        {ok, Exchange} ->
            QNames = rabbit_exchange:route(Exchange, Delivery),
            deliver_to_queues(Delivery, QNames, State);
        {error, not_found} ->
            rabbit_log:error("~s not found", [rabbit_misc:rs(ExchangeName)]),
            {error, exchange_not_found, State}
    end.

deliver_to_queues(Delivery,
                  RoutedToQNames,
                  State0 = #state{queue_states = QStates0,
                                  proto_ver = ProtoVer}) ->
    Qs0 = rabbit_amqqueue:lookup(RoutedToQNames),
    Qs = rabbit_amqqueue:prepend_extra_bcc(Qs0),
    case rabbit_queue_type:deliver(Qs, Delivery, QStates0) of
        {ok, QStates, Actions} ->
            rabbit_global_counters:messages_routed(ProtoVer, length(Qs)),
            State = process_routing_confirm(Delivery, Qs,
                                            State0#state{queue_states = QStates}),
            %% Actions must be processed after registering confirms as actions may
            %% contain rejections of publishes.
            {ok, handle_queue_actions(Actions, State)};
        {error, Reason} ->
            rabbit_log:error("Failed to deliver message with packet_id=~p to queues: ~p",
                             [Delivery#delivery.msg_seq_no, Reason]),
            {error, Reason, State0}
    end.

process_routing_confirm(#delivery{confirm = false},
                        [], State = #state{proto_ver = ProtoVer}) ->
    rabbit_global_counters:messages_unroutable_dropped(ProtoVer, 1),
    State;
process_routing_confirm(#delivery{confirm = true,
                                  msg_seq_no = undefined},
                        [], State = #state{proto_ver = ProtoVer}) ->
    %% unroutable will message with QoS > 0
    rabbit_global_counters:messages_unroutable_dropped(ProtoVer, 1),
    State;
process_routing_confirm(#delivery{confirm = true,
                                  msg_seq_no = PktId},
                        [], State = #state{proto_ver = ProtoVer}) ->
    rabbit_global_counters:messages_unroutable_returned(ProtoVer, 1),
    %% MQTT 5 spec:
    %% If the Server knows that there are no matching subscribers, it MAY use
    %% Reason Code 0x10 (No matching subscribers) instead of 0x00 (Success).
    send_puback(PktId, State),
    State;
process_routing_confirm(#delivery{confirm = false}, _, State) ->
    State;
process_routing_confirm(#delivery{confirm = true,
                                  msg_seq_no = undefined}, [_|_], State) ->
    %% routable will message with QoS > 0
    State;
process_routing_confirm(#delivery{confirm = true,
                                  msg_seq_no = PktId},
                        Qs, State = #state{unacked_client_pubs = U0}) ->
    QNames = lists:map(fun amqqueue:get_name/1, Qs),
    U = rabbit_mqtt_confirms:insert(PktId, QNames, U0),
    State#state{unacked_client_pubs = U}.

send_puback(PktIds0, State)
  when is_list(PktIds0) ->
    %% Classic queues confirm messages unordered.
    %% Let's sort them here assuming most MQTT clients send with an increasing packet identifier.
    PktIds = lists:usort(PktIds0),
    lists:foreach(fun(Id) ->
                          send_puback(Id, State)
                  end, PktIds);
send_puback(PktId, State = #state{send_fun = SendFun,
                                  proto_ver = ProtoVer}) ->
    rabbit_global_counters:messages_confirmed(ProtoVer, 1),
    SendFun(
      #mqtt_packet{fixed = #mqtt_packet_fixed{type = ?PUBACK},
                   variable = #mqtt_packet_publish{packet_id = PktId}},
      State).

serialise_and_send_to_client(Packet, #state{proto_ver = ProtoVer, socket = Sock}) ->
    Data = rabbit_mqtt_packet:serialise(Packet, ProtoVer),
    try rabbit_net:port_command(Sock, Data)
    catch error:Error ->
              rabbit_log_connection:error(
                "MQTT: a socket write failed: ~p", [Error]),
              rabbit_log_connection:debug(
                "Failed to write to socket ~p, error: ~p, packet: ~p", [Sock, Error, Packet])
    end.

serialise(Packet, #state{proto_ver = ProtoVer}) ->
    rabbit_mqtt_packet:serialise(Packet, ProtoVer).

terminate(SendWill, ConnName, ProtoFamily, State) ->
    maybe_send_will(SendWill, ConnName, State),
    Infos = [{name, ConnName},
             {node, node()},
             {pid, self()},
             {disconnected_at, os:system_time(milli_seconds)}
            ] ++ additional_connection_closed_info(ProtoFamily, State),
    rabbit_core_metrics:connection_closed(self()),
    rabbit_event:notify(connection_closed, Infos),
    rabbit_networking:unregister_non_amqp_connection(self()),
    maybe_unregister_client(State),
    maybe_decrement_consumer(State),
    maybe_decrement_publisher(State),
    maybe_delete_mqtt_qos0_queue(State).

maybe_send_will(true, ConnStr,
                #state{will_msg = WillMsg = #mqtt_msg{retain = Retain,
                                                      topic = Topic},
                       retainer_pid = RPid
                      } = State) ->
    rabbit_log_connection:debug("sending MQTT will message to topic ~s on connection ~s",
                                [Topic, ConnStr]),
    case check_topic_access(Topic, write, State) of
        ok ->
            publish_to_queues(WillMsg, State),
            case Retain of
                false ->
                    ok;
                true ->
                    hand_off_to_retainer(RPid, Topic, WillMsg)
            end;
        {error, access_refused = Reason}  ->
            rabbit_log:error("failed to send will message: ~p", [Reason])
    end;
maybe_send_will(_, _, _) ->
    ok.

additional_connection_closed_info(
  ProtoFamily,
  State = #state{auth_state = #auth_state{vhost = VHost,
                                          username = Username}}) ->
    [{protocol, {ProtoFamily, proto_version_tuple(State)}},
     {vhost, VHost},
     {user, Username}];
additional_connection_closed_info(_, _) ->
    [].

maybe_unregister_client(#state{client_id = ClientId})
  when ClientId =/= undefined ->
    case rabbit_mqtt_ff:track_client_id_in_ra() of
        true ->
            %% ignore any errors as we are shutting down
            rabbit_mqtt_collector:unregister(ClientId, self());
        false ->
            ok
    end;
maybe_unregister_client(_) ->
    ok.

maybe_delete_mqtt_qos0_queue(State = #state{clean_sess = true,
                                            auth_state = #auth_state{username = Username}}) ->
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

delete_queue(QName, Username) ->
    rabbit_amqqueue:with(
      QName,
      fun (Q) ->
              rabbit_queue_type:delete(Q, false, false, Username)
      end,
      fun (not_found) ->
              ok;
          ({absent, Q, crashed}) ->
              rabbit_classic_queue:delete_crashed(Q, Username);
          ({absent, Q, stopped}) ->
              rabbit_classic_queue:delete_crashed(Q, Username);
          ({absent, _Q, _Reason}) ->
              ok
      end).

handle_pre_hibernate() ->
    erase(permission_cache),
    erase(topic_permission_cache),
    ok.

handle_ra_event({applied, [{Corr, ok}]},
                State = #state{register_state = {pending, Corr}}) ->
    %% success case - command was applied transition into registered state
    State#state{register_state = registered};
handle_ra_event({not_leader, Leader, Corr},
                State = #state{register_state = {pending, Corr},
                               client_id = ClientId}) ->
    case rabbit_mqtt_ff:track_client_id_in_ra() of
        true ->
            %% retry command against actual leader
            {ok, NewCorr} = rabbit_mqtt_collector:register(Leader, ClientId, self()),
            State#state{register_state = {pending, NewCorr}};
        false ->
            State
    end;
handle_ra_event(register_timeout,
                State = #state{register_state = {pending, _Corr},
                               client_id = ClientId}) ->
    case rabbit_mqtt_ff:track_client_id_in_ra() of
        true ->
            {ok, NewCorr} = rabbit_mqtt_collector:register(ClientId, self()),
            State#state{register_state = {pending, NewCorr}};
        false ->
            State
    end;
handle_ra_event(register_timeout, State) ->
    State;
handle_ra_event(Evt, State) ->
    %% log these?
    rabbit_log:debug("unhandled ra_event: ~w ", [Evt]),
    State.

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
            send_puback(ConfirmPktIds, State),
            {ok, State}
    end.

-spec handle_queue_event(
        {queue_event, rabbit_amqqueue:name() | ?QUEUE_TYPE_QOS_0, term()}, state()) ->
    {ok, state()} | {error, Reason :: any(), state()}.
handle_queue_event({queue_event, ?QUEUE_TYPE_QOS_0, Msg},
                   State0 = #state{qos0_messages_dropped = N}) ->
    State = case drop_qos0_message(State0) of
                false ->
                    deliver_one_to_client(Msg, false, State0);
                true ->
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
            send_puback(ConfirmPktIds, State),
            {ok, State};
        {protocol_error, _Type, _Reason, _ReasonArgs} = Error ->
            {error, Error, State0}
    end.

handle_queue_actions(Actions, #state{} = State0) ->
    lists:foldl(
      fun ({deliver, ?CONSUMER_TAG, Ack, Msgs}, S) ->
              deliver_to_client(Msgs, Ack, S);
          ({settled, QName, PktIds}, S = #state{unacked_client_pubs = U0}) ->
              {ConfirmPktIds, U} = rabbit_mqtt_confirms:confirm(PktIds, QName, U0),
              send_puback(ConfirmPktIds, S),
              S#state{unacked_client_pubs = U};
          ({rejected, _QName, PktIds}, S = #state{unacked_client_pubs = U0}) ->
              %% Negative acks are supported in MQTT v5 only.
              %% Therefore, in MQTT v3 and v4 we ignore rejected messages.
              U = lists:foldl(
                    fun(PktId, Acc0) ->
                            case rabbit_mqtt_confirms:reject(PktId, Acc0) of
                                {ok, Acc} -> Acc;
                                {error, not_found} -> Acc0
                            end
                    end, U0, PktIds),
              S#state{unacked_client_pubs = U};
          ({block, QName}, S = #state{soft_limit_exceeded = SLE}) ->
              S#state{soft_limit_exceeded = sets:add_element(QName, SLE)};
          ({unblock, QName}, S = #state{soft_limit_exceeded = SLE}) ->
              S#state{soft_limit_exceeded = sets:del_element(QName, SLE)};
          ({queue_down, QName}, S) ->
              handle_queue_down(QName, S)
      end, State0, Actions).

handle_queue_down(QName, State0 = #state{client_id = ClientId}) ->
    %% Classic queue is down.
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            case rabbit_mqtt_util:qos_from_queue_name(QName, ClientId) of
                no_consuming_queue ->
                    State0;
                QoS ->
                    %% Consuming classic queue is down.
                    %% Let's try to re-consume: HA failover for classic mirrored queues.
                    case consume(Q, QoS, State0) of
                        {ok, State} ->
                            State;
                        {error, _Reason} ->
                            rabbit_log:info("Terminating MQTT connection because consuming ~s is down.",
                                            [rabbit_misc:rs(QName)]),
                            throw(consuming_queue_down)
                    end
            end;
        {error, not_found} ->
            State0
    end.

deliver_to_client(Msgs, Ack, State) ->
    lists:foldl(fun(Msg, S) ->
                        deliver_one_to_client(Msg, Ack, S)
                end, State, Msgs).

deliver_one_to_client(Msg = {QNameOrType, QPid, QMsgId, _Redelivered,
                             #basic_message{content = #content{properties = #'P_basic'{headers = Headers}}}},
                      AckRequired, State0) ->
    PublisherQoS = case rabbit_mqtt_util:table_lookup(Headers, <<"x-mqtt-publish-qos">>) of
                       {byte, QoS0} ->
                           QoS0;
                       undefined ->
                           %% non-MQTT publishes are assumed to be QoS 1 regardless of delivery_mode
                           ?QOS_1
                   end,
    SubscriberQoS = case AckRequired of
                        true ->
                            ?QOS_1;
                        false ->
                            ?QOS_0
                    end,
    QoS = effective_qos(PublisherQoS, SubscriberQoS),
    State1 = maybe_publish_to_client(Msg, QoS, State0),
    State = maybe_auto_ack(AckRequired, QoS, QNameOrType, QMsgId, State1),
    ok = maybe_notify_sent(QNameOrType, QPid, State),
    State.

-spec effective_qos(qos(), qos()) -> qos().
effective_qos(PublisherQoS, SubscriberQoS) ->
    %% "The QoS of Application Messages sent in response to a Subscription MUST be the minimum
    %% of the QoS of the originally published message and the Maximum QoS granted by the Server
    %% [MQTT-3.8.4-8]."
    erlang:min(PublisherQoS, SubscriberQoS).

maybe_publish_to_client({_, _, _, _Redelivered = true, _}, ?QOS_0, State) ->
    %% Do not redeliver to MQTT subscriber who gets message at most once.
    State;
maybe_publish_to_client(
  {QNameOrType, _QPid, QMsgId, Redelivered,
   #basic_message{
      routing_keys = [RoutingKey | _CcRoutes],
      content = #content{payload_fragments_rev = FragmentsRev}}},
  QoS, State0 = #state{send_fun = SendFun}) ->
    {PacketId, State} = msg_id_to_packet_id(QMsgId, QoS, State0),
    Packet =
    #mqtt_packet{
       fixed = #mqtt_packet_fixed{
                  type = ?PUBLISH,
                  qos = QoS,
                  %% "The value of the DUP flag from an incoming PUBLISH packet is not
                  %% propagated when the PUBLISH Packet is sent to subscribers by the Server.
                  %% The DUP flag in the outgoing PUBLISH packet is set independently to the
                  %% incoming PUBLISH packet, its value MUST be determined solely by whether
                  %% the outgoing PUBLISH packet is a retransmission [MQTT-3.3.1-3]."
                  %% Therefore, we do not consider header value <<"x-mqtt-dup">> here.
                  dup = Redelivered},
       variable = #mqtt_packet_publish{
                     packet_id = PacketId,
                     topic_name = amqp_to_mqtt(RoutingKey)},
       payload = lists:reverse(FragmentsRev)},
    SendFun(Packet, State),
    message_delivered(QNameOrType, Redelivered, QoS, State),
    State.

msg_id_to_packet_id(_, ?QOS_0, State) ->
    %% "A PUBLISH packet MUST NOT contain a Packet Identifier if its QoS value is set to 0 [MQTT-2.2.1-2]."
    {undefined, State};
msg_id_to_packet_id(QMsgId, ?QOS_1, #state{packet_id = PktId,
                                           unacked_server_pubs = U} = State) ->
    {PktId, State#state{packet_id = increment_packet_id(PktId),
                        unacked_server_pubs = maps:put(PktId, QMsgId, U)}}.

-spec increment_packet_id(packet_id()) -> packet_id().
increment_packet_id(Id)
  when Id >= 16#ffff ->
    1;
increment_packet_id(Id) ->
    Id + 1.

maybe_auto_ack(_AckRequired = true, ?QOS_0, QName, QMsgId,
               State = #state{queue_states = QStates0}) ->
    {ok, QStates, Actions} = rabbit_queue_type:settle(QName, complete, ?CONSUMER_TAG, [QMsgId], QStates0),
    handle_queue_actions(Actions, State#state{queue_states = QStates});
maybe_auto_ack(_, _, _, _, State) ->
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

publish_to_queues_with_checks(
  TopicName, PublishFun,
  #state{exchange = Exchange,
         auth_state = #auth_state{user = User,
                                  authz_ctx = AuthzCtx}} = State) ->
    case check_resource_access(User, Exchange, write, AuthzCtx) of
        ok ->
            case check_topic_access(TopicName, write, State) of
                ok ->
                    PublishFun();
                {error, access_refused} ->
                    {error, unauthorized, State}
            end;
        {error, access_refused} ->
            {error, unauthorized, State}
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
                    rabbit_log:error("MQTT resource access refused: ~s", [Msg]),
                    {error, access_refused}
            end
    end.

check_topic_access(TopicName, Access,
                   #state{
                      auth_state = #auth_state{user = User = #user{username = Username},
                                               vhost = VHost,
                                               authz_ctx = AuthzCtx},
                      exchange = #resource{name = ExchangeBin},
                      client_id = ClientId
                     }) ->
    Cache = case get(topic_permission_cache) of
                undefined -> [];
                Other     -> Other
            end,
    Key = {TopicName, Username, ClientId, VHost, ExchangeBin, Access},
    case lists:member(Key, Cache) of
        true ->
            ok;
        false ->
            Resource = #resource{virtual_host = VHost,
                                 kind = topic,
                                 name = ExchangeBin},
            RoutingKey = mqtt_to_amqp(TopicName),
            Context = #{routing_key  => RoutingKey,
                        variable_map => AuthzCtx},
            try rabbit_access_control:check_topic_access(User, Resource, Access, Context) of
                ok ->
                    CacheTail = lists:sublist(Cache, ?MAX_PERMISSION_CACHE_SIZE - 1),
                    put(topic_permission_cache, [Key | CacheTail]),
                    ok
            catch
                exit:#amqp_error{name = access_refused,
                                 explanation = Msg} ->
                    rabbit_log:error("MQTT topic access refused: ~s", [Msg]),
                    {error, access_refused}
            end
    end.

-spec drop_qos0_message(state()) ->
    boolean().
drop_qos0_message(State) ->
    mailbox_soft_limit_exceeded() andalso
    is_socket_busy(State#state.socket).

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
          when is_number(NumBytes) andalso NumBytes > 0 ->
            true;
        _ ->
            false
    end.

info(host, #state{info = #info{host = Val}}) -> Val;
info(port, #state{info = #info{port = Val}}) -> Val;
info(peer_host, #state{info = #info{peer_host = Val}}) -> Val;
info(peer_port, #state{info = #info{peer_port = Val}}) -> Val;
info(connected_at, #state{info = #info{connected_at = Val}}) -> Val;
info(ssl_login_name, #state{ssl_login_name = Val}) -> Val;
info(vhost, #state{auth_state = #auth_state{vhost = Val}}) -> Val;
info(user_who_performed_action, S) ->
    info(user, S);
info(user, #state{auth_state = #auth_state{username = Val}}) -> Val;
info(clean_sess, #state{clean_sess = Val}) -> Val;
info(will_msg, #state{will_msg = Val}) -> Val;
info(retainer_pid, #state{retainer_pid = Val}) -> Val;
info(exchange, #state{exchange = #resource{name = Val}}) -> Val;
info(prefetch, #state{info = #info{prefetch = Val}}) -> Val;
info(messages_unconfirmed, #state{unacked_client_pubs = Val}) ->
    rabbit_mqtt_confirms:size(Val);
info(messages_unacknowledged, #state{unacked_server_pubs = Val}) ->
    maps:size(Val);
info(node, _) -> node();
info(client_id, #state{client_id = Val}) -> Val;
%% for rabbitmq_management/priv/www/js/tmpl/connection.ejs
info(client_properties, #state{client_id = Val}) ->
    [{client_id, longstr, Val}];
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

-spec format_status(state()) -> map().
format_status(#state{queue_states = QState,
                     proto_ver = ProtoVersion,
                     unacked_client_pubs = UnackClientPubs,
                     unacked_server_pubs = UnackSerPubs,
                     packet_id = PackID,
                     client_id = ClientID,
                     clean_sess = CleanSess,
                     will_msg = WillMsg,
                     exchange = Exchange,
                     ssl_login_name = SSLLoginName,
                     retainer_pid = RetainerPid,
                     auth_state = AuthState,
                     peer_addr = PeerAddr,
                     register_state = RegisterState,
                     conn_name = ConnName,
                     info = Info,
                     qos0_messages_dropped = Qos0MsgsDropped
                    } = State) ->
    #{queue_states => rabbit_queue_type:format_status(QState),
      proto_ver => ProtoVersion,
      unacked_client_pubs => UnackClientPubs,
      unacked_server_pubs => UnackSerPubs,
      packet_id => PackID,
      client_id => ClientID,
      clean_sess => CleanSess,
      will_msg_defined => WillMsg =/= undefined,
      exchange => Exchange,
      ssl_login_name => SSLLoginName,
      retainer_pid => RetainerPid,
      auth_state => AuthState,
      peer_addr => PeerAddr,
      register_state => RegisterState,
      conn_name => ConnName,
      info => Info,
      soft_limit_exceeded => soft_limit_exceeded(State),
      qos0_messages_dropped => Qos0MsgsDropped}.

-spec soft_limit_exceeded(state()) -> boolean().
soft_limit_exceeded(#state{soft_limit_exceeded = SLE}) ->
    not sets:is_empty(SLE).

proto_integer_to_atom(3) ->
    ?MQTT_PROTO_V3;
proto_integer_to_atom(4) ->
    ?MQTT_PROTO_V4.

-spec proto_version_tuple(state()) -> tuple().
proto_version_tuple(#state{proto_ver = ?MQTT_PROTO_V3}) ->
    {3, 1, 0};
proto_version_tuple(#state{proto_ver = ?MQTT_PROTO_V4}) ->
    {3, 1, 1}.

maybe_increment_publisher(State = #state{has_published = false,
                                         proto_ver = ProtoVer}) ->
    rabbit_global_counters:publisher_created(ProtoVer),
    State#state{has_published = true};
maybe_increment_publisher(State) ->
    State.

maybe_decrement_publisher(#state{has_published = true,
                                 proto_ver = ProtoVer}) ->
    rabbit_global_counters:publisher_deleted(ProtoVer);
maybe_decrement_publisher(_) ->
    ok.

%% multiple subscriptions from the same connection count as one consumer
maybe_increment_consumer(_WasConsumer = false,
                         #state{proto_ver = ProtoVer} = State) ->
    case has_subs(State) of
        true ->
            rabbit_global_counters:consumer_created(ProtoVer);
        false ->
            ok
    end;
maybe_increment_consumer(_, _) ->
    ok.

maybe_decrement_consumer(_WasConsumer = true,
                         #state{proto_ver = ProtoVer} = State) ->
    case has_subs(State) of
        false ->
            rabbit_global_counters:consumer_deleted(ProtoVer);
        true ->
            ok
    end;
maybe_decrement_consumer(_, _) ->
    ok.

maybe_decrement_consumer(#state{proto_ver = ProtoVer,
                                auth_state = #auth_state{}} = State) ->
    case has_subs(State) of
        true ->
            rabbit_global_counters:consumer_deleted(ProtoVer);
        false ->
            ok
    end;
maybe_decrement_consumer(_) ->
    ok.

message_acknowledged(QName, #state{proto_ver = ProtoVer,
                                   queue_states = QStates}) ->
    case rabbit_queue_type:module(QName, QStates) of
        {ok, QType} ->
            rabbit_global_counters:messages_acknowledged(ProtoVer, QType, 1);
        _ ->
            ok
    end.

message_delivered(?QUEUE_TYPE_QOS_0, false, ?QOS_0, #state{proto_ver = ProtoVer}) ->
    rabbit_global_counters:messages_delivered(ProtoVer, ?QUEUE_TYPE_QOS_0, 1),
    %% Technically, the message is not acked to a queue at all.
    %% However, from a user perspective it is still auto acked because:
    %% "In automatic acknowledgement mode, a message is considered to be successfully
    %% delivered immediately after it is sent."
    rabbit_global_counters:messages_delivered_consume_auto_ack(ProtoVer, ?QUEUE_TYPE_QOS_0, 1);
message_delivered(QName, Redelivered, QoS, #state{proto_ver = ProtoVer,
                                                  queue_states = QStates
                                                 }) ->
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
