%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mqtt_processor).

-export([info/2, initial_state/2, initial_state/4,
         process_frame/2, serialise/2, send_will/1,
         terminate/2, handle_pre_hibernate/0,
         handle_ra_event/2, handle_down/2, handle_queue_event/2,
         soft_limit_exceeded/1, format_status/1]).

%%TODO Use single queue per MQTT subscriber connection?
%% * when publishing we store in x-mqtt-publish-qos header the publishing QoS
%% * routing key is present in the delivered message
%% * therefore, we can map routing key -> topic -> subscription -> subscription max QoS
%% Advantages:
%% * better scaling when single client creates subscriptions with different QoS levels
%% * no duplicates when single client creates overlapping subscriptions with different QoS levels

%% for testing purposes
-export([get_vhost_username/1, get_vhost/3, get_vhost_from_user_mapping/2]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("rabbit/include/amqqueue.hrl").
-include("rabbit_mqtt.hrl").
-include("rabbit_mqtt_frame.hrl").

-define(APP, rabbitmq_mqtt).
-define(MAX_PERMISSION_CACHE_SIZE, 12).
-define(CONSUMER_TAG, mqtt).

initial_state(Socket, ConnectionName) ->
    {ok, {PeerAddr, _PeerPort}} = rabbit_net:peername(Socket),
    initial_state(Socket,
                  ConnectionName,
                  fun serialise_and_send_to_client/2,
                  PeerAddr).

initial_state(Socket, ConnectionName, SendFun, PeerAddr) ->
    {ok, {mqtt2amqp_fun, M2A}, {amqp2mqtt_fun, A2M}} = rabbit_mqtt_util:get_topic_translation_funs(),
    Flow = case rabbit_misc:get_env(rabbit, mirroring_flow_control, true) of
             true   -> flow;
             false  -> noflow
           end,
    #proc_state{socket         = Socket,
                conn_name      = ConnectionName,
                ssl_login_name = ssl_login_name(Socket),
                peer_addr      = PeerAddr,
                send_fun       = SendFun,
                mqtt2amqp_fun  = M2A,
                amqp2mqtt_fun  = A2M,
                delivery_flow  = Flow}.

process_frame(#mqtt_frame{fixed = #mqtt_frame_fixed{type = Type}},
              PState = #proc_state{auth_state = undefined})
  when Type =/= ?CONNECT ->
    {error, connect_expected, PState};
process_frame(Frame = #mqtt_frame{fixed = #mqtt_frame_fixed{type = Type}}, PState) ->
    process_request(Type, Frame, PState).

process_request(?CONNECT, Frame, PState = #proc_state{socket = Socket}) ->
    %% Check whether peer closed the connection.
    %% For example, this can happen when connection was blocked because of resource
    %% alarm and client therefore disconnected due to client side CONNACK timeout.
    case rabbit_net:socket_ends(Socket, inbound) of
        {error, Reason} ->
            {error, {socket_ends, Reason}, PState};
        _ ->
            process_connect(Frame, PState)
    end;

process_request(?PUBACK,
                #mqtt_frame{
                   variable = #mqtt_frame_publish{message_id = PacketId}},
                #proc_state{unacked_server_pubs = U0,
                            queue_states = QStates0} = PState) ->
    case maps:take(PacketId, U0) of
        {QMsgId, U} ->
            %% TODO creating binary is expensive
            QName = queue_name(?QOS_1, PState),
            case rabbit_queue_type:settle(QName, complete, ?CONSUMER_TAG, [QMsgId], QStates0) of
                {ok, QStates, [] = _Actions} ->
                    % incr_queue_stats(QRef, MsgIds, State),
                    %%TODO handle actions
                    {ok, PState#proc_state{unacked_server_pubs = U,
                                           queue_states = QStates }};
                {protocol_error, _ErrorType, _Reason, _ReasonArgs} = Err ->
                    {error, Err, PState}
            end;
        error ->
            {ok, PState}
    end;

process_request(?PUBLISH,
                Frame = #mqtt_frame{
                           fixed = Fixed = #mqtt_frame_fixed{qos = ?QOS_2}},
                PState) ->
    % Downgrade QOS_2 to QOS_1
    process_request(?PUBLISH,
                    Frame#mqtt_frame{
                      fixed = Fixed#mqtt_frame_fixed{qos = ?QOS_1}},
                    PState);
process_request(?PUBLISH,
                #mqtt_frame{
                   fixed = #mqtt_frame_fixed{qos = Qos,
                                             retain = Retain,
                                             dup = Dup },
                   variable = #mqtt_frame_publish{topic_name = Topic,
                                                  message_id = MessageId },
                   payload = Payload},
                PState0 = #proc_state{retainer_pid = RPid,
                                      amqp2mqtt_fun = Amqp2MqttFun,
                                      unacked_client_pubs = U,
                                      proto_ver = ProtoVer}) ->
    rabbit_global_counters:messages_received(ProtoVer, 1),
    PState = maybe_increment_publisher(PState0),
    Publish = fun() ->
                      Msg = #mqtt_msg{retain     = Retain,
                                      qos        = Qos,
                                      topic      = Topic,
                                      dup        = Dup,
                                      message_id = MessageId,
                                      payload    = Payload},
                      case publish_to_queues(Msg, PState) of
                          {ok, _} = Ok ->
                              case Retain of
                                  false ->
                                      ok;
                                  true ->
                                      hand_off_to_retainer(RPid, Amqp2MqttFun, Topic, Msg)
                              end,
                              Ok;
                          Error ->
                              Error
                      end
              end,
    case Qos of
        N when N > ?QOS_0 ->
            rabbit_global_counters:messages_received_confirm(ProtoVer, 1),
            case rabbit_mqtt_confirms:contains(MessageId, U) of
                false ->
                    publish_to_queues_with_checks(Topic, Publish, PState);
                true ->
                    %% Client re-sent this PUBLISH packet.
                    %% We already sent this message to target queues awaiting confirmations.
                    %% Hence, we ignore this re-send.
                    {ok, PState}
            end;
        _ ->
            publish_to_queues_with_checks(Topic, Publish, PState)
    end;

process_request(?SUBSCRIBE,
                #mqtt_frame{
                   variable = #mqtt_frame_subscribe{
                                 message_id  = SubscribeMsgId,
                                 topic_table = Topics},
                   payload = undefined},
                #proc_state{send_fun = SendFun,
                            retainer_pid = RPid} = PState0) ->
    rabbit_log_connection:debug("Received a SUBSCRIBE for topic(s) ~p", [Topics]),
    HasSubsBefore = has_subs(PState0),
    {QosResponse, PState1} =
    lists:foldl(fun(_Topic, {[?SUBACK_FAILURE | _] = L, S}) ->
                        %% Once a subscription failed, mark all following subscriptions
                        %% as failed instead of creating bindings because we are going
                        %% to close the client connection anyways.
                        {[?SUBACK_FAILURE | L], S};
                   (#mqtt_topic{name = TopicName,
                                qos = Qos0},
                    {L, S0}) ->
                        QoS = supported_sub_qos(Qos0),
                        %%TODO check whether new subscription replaces an existing one
                        %% (i.e. same topic name and different QoS)
                        case ensure_queue(QoS, S0) of
                            {ok, Q} ->
                                QName = amqqueue:get_name(Q),
                                case bind(QName, TopicName, S0) of
                                    {ok, _Output, S1} ->
                                        %%TODO check what happens if we basic.consume multiple times
                                        %% for the same queue
                                        case consume(Q, QoS, S1) of
                                            {ok, S2} ->
                                                {[QoS | L], S2};
                                            {error, _Reason} ->
                                                {[?SUBACK_FAILURE | L], S1}
                                        end;
                                    {error, Reason, S} ->
                                        rabbit_log:error("Failed to bind ~s with topic ~s: ~p",
                                                         [rabbit_misc:rs(QName), TopicName, Reason]),
                                        {[?SUBACK_FAILURE | L], S}
                                end;
                            {error, _} ->
                                {[?SUBACK_FAILURE | L], S0}
                        end
                end, {[], PState0}, Topics),
    maybe_increment_consumer(HasSubsBefore, PState1),
    SendFun(
      #mqtt_frame{fixed    = #mqtt_frame_fixed{type = ?SUBACK},
                  variable = #mqtt_frame_suback{
                                message_id = SubscribeMsgId,
                                %%TODO check correct order of QosResponse
                                qos_table  = QosResponse}},
      PState1),
    case QosResponse of
        [?SUBACK_FAILURE | _] ->
            {error, subscribe_error, PState1};
        _ ->
            PState = lists:foldl(fun(Topic, S) ->
                                         maybe_send_retained_message(RPid, Topic, S)
                                 end, PState1, Topics),
            {ok, PState}
    end;

process_request(?UNSUBSCRIBE,
                #mqtt_frame{variable = #mqtt_frame_subscribe{message_id  = MessageId,
                                                             topic_table = Topics},
                            payload = undefined},
                PState0 = #proc_state{send_fun = SendFun}) ->
    rabbit_log_connection:debug("Received an UNSUBSCRIBE for topic(s) ~p", [Topics]),
    HasSubsBefore = has_subs(PState0),
    PState = lists:foldl(
        fun(#mqtt_topic{name = TopicName}, #proc_state{} = S0) ->
            case find_queue_name(TopicName, S0) of
                {ok, QName} ->
                    case unbind(QName, TopicName, S0) of
                        {ok, _, _} ->
                            PState0;
                        {error, Reason, State} ->
                            rabbit_log:error("Failed to unbind ~s with topic ~s: ~p",
                                            [rabbit_misc:rs(QName), TopicName, Reason]),
                            State
                    end;
                {not_found, _} ->
                    PState0
            end
        end, PState0, Topics),
    SendFun(
      #mqtt_frame{fixed    = #mqtt_frame_fixed {type       = ?UNSUBACK},
                  variable = #mqtt_frame_suback{message_id = MessageId}},
      PState),

    maybe_decrement_consumer(HasSubsBefore, PState),
    {ok, PState};

process_request(?PINGREQ, #mqtt_frame{}, PState = #proc_state{send_fun = SendFun}) ->
    rabbit_log_connection:debug("Received a PINGREQ"),
    SendFun(
      #mqtt_frame{fixed = #mqtt_frame_fixed{type = ?PINGRESP}},
      PState),
    rabbit_log_connection:debug("Sent a PINGRESP"),
    {ok, PState};

process_request(?DISCONNECT, #mqtt_frame{}, PState) ->
    rabbit_log_connection:debug("Received a DISCONNECT"),
    {stop, PState}.

process_connect(#mqtt_frame{
                   variable = #mqtt_frame_connect{
                                 username   = Username,
                                 proto_ver  = ProtoVersion,
                                 clean_sess = CleanSess,
                                 client_id  = ClientId,
                                 keep_alive = Keepalive} = FrameConnect},
                PState0 = #proc_state{send_fun = SendFun}) ->
    rabbit_log_connection:debug("Received a CONNECT, client ID: ~s, username: ~s, "
                                "clean session: ~s, protocol version: ~p, keepalive: ~p",
                                [ClientId, Username, CleanSess, ProtoVersion, Keepalive]),
    {ReturnCode, SessionPresent, PState} =
    case rabbit_misc:pipeline([fun check_protocol_version/1,
                               fun check_client_id/1,
                               fun check_credentials/2,
                               fun login/2,
                               fun register_client/2,
                               fun notify_connection_created/2,
                               fun start_keepalive/2,
                               fun handle_clean_session/2],
                              FrameConnect, PState0) of
        {ok, SessionPresent0, PState1} ->
            {?CONNACK_ACCEPT, SessionPresent0, PState1};
        {error, ReturnCode0, PState1} ->
            {ReturnCode0, false, PState1}
    end,
    ResponseFrame = #mqtt_frame{fixed    = #mqtt_frame_fixed{type = ?CONNACK},
                                variable = #mqtt_frame_connack{
                                              session_present = SessionPresent,
                                              return_code = ReturnCode}},
    SendFun(ResponseFrame, PState),
    return_connack(ReturnCode, PState).

client_id(<<>>) ->
    rabbit_mqtt_util:gen_client_id();
client_id(ClientId)
  when is_binary(ClientId) ->
    ClientId.

check_protocol_version(#mqtt_frame_connect{proto_ver = ProtoVersion}) ->
    case lists:member(ProtoVersion, proplists:get_keys(?PROTOCOL_NAMES)) of
        true ->
            ok;
        false ->
            {error, ?CONNACK_UNACCEPTABLE_PROTO_VER}
    end.

check_client_id(#mqtt_frame_connect{clean_sess = false,
                                    client_id = []}) ->
    {error, ?CONNACK_ID_REJECTED};
check_client_id(_) ->
    ok.

check_credentials(Frame = #mqtt_frame_connect{username = Username,
                                              password = Password},
                  PState = #proc_state{ssl_login_name = SslLoginName,
                                       peer_addr = PeerAddr}) ->
    Ip = list_to_binary(inet:ntoa(PeerAddr)),
    case creds(Username, Password, SslLoginName) of
        nocreds ->
            rabbit_core_metrics:auth_attempt_failed(Ip, <<>>, mqtt),
            rabbit_log_connection:error("MQTT login failed: no credentials provided"),
            {error, ?CONNACK_BAD_CREDENTIALS};
        {invalid_creds, {undefined, Pass}} when is_list(Pass) ->
            rabbit_core_metrics:auth_attempt_failed(Ip, <<>>, mqtt),
            rabbit_log_connection:error("MQTT login failed: no username is provided"),
            {error, ?CONNACK_BAD_CREDENTIALS};
        {invalid_creds, {User, undefined}} when is_list(User) ->
            rabbit_core_metrics:auth_attempt_failed(Ip, User, mqtt),
            rabbit_log_connection:error("MQTT login failed for user '~p': no password provided", [User]),
            {error, ?CONNACK_BAD_CREDENTIALS};
        {UserBin, PassBin} ->
            {ok, {UserBin, PassBin, Frame}, PState}
    end.

login({UserBin, PassBin,
       Frame = #mqtt_frame_connect{client_id = ClientId0,
                                   clean_sess = CleanSess}},
      PState0) ->
    ClientId = client_id(ClientId0),
    case process_login(UserBin, PassBin, ClientId, PState0) of
        already_connected ->
            {ok, already_connected};
        {ok, PState} ->
            {ok, Frame, PState#proc_state{clean_sess = CleanSess,
                                          client_id = ClientId}};
        {error, _Reason, _PState} = Err ->
            Err
    end.

register_client(already_connected, _PState) ->
    ok;
register_client(Frame = #mqtt_frame_connect{proto_ver = ProtoVersion},
                PState = #proc_state{client_id = ClientId,
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
            ProtoHumanReadable = {'MQTT', human_readable_mqtt_version(ProtoVersion)},
            PState#proc_state{
              exchange = ExchangeName,
              will_msg   = make_will_msg(Frame),
              retainer_pid = RetainerPid,
              register_state = RegisterState,
              proto_ver = protocol_integer_to_atom(ProtoVersion),
              info = #info{prefetch = Prefetch,
                           peer_host = PeerHost,
                           peer_port = PeerPort,
                           host = Host,
                           port = Port,
                           proto_human = ProtoHumanReadable
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
                    {error, ?CONNACK_SERVER_UNAVAILABLE};
                {timeout, _} ->
                    rabbit_log_connection:error("MQTT cannot accept a connection: "
                                                "client ID registration timed out"),
                    {error, ?CONNACK_SERVER_UNAVAILABLE}
            end;
        false ->
            {ok, NewProcState(undefined)}
    end.

notify_connection_created(already_connected, _PState) ->
    ok;
notify_connection_created(
  _Frame,
  #proc_state{socket = Sock,
              conn_name = ConnName,
              info = #info{proto_human = {ProtoName, ProtoVsn}},
              auth_state = #auth_state{vhost = VHost,
                                       username = Username}} = PState) ->
    {ok, {PeerHost, PeerPort, Host, Port}} = rabbit_net:socket_ends(Sock, inbound),
    ConnectedAt = os:system_time(milli_seconds),
    Infos = [{host, Host},
             {port, Port},
             {peer_host, PeerHost},
             {peer_port, PeerPort},
             {vhost, VHost},
             {node, node()},
             {user, Username},
             {name, ConnName},
             {connected_at, ConnectedAt},
             {pid, self()},
             {protocol, {ProtoName, binary_to_list(ProtoVsn)}},
             {type, network}
            ],
    rabbit_core_metrics:connection_created(self(), Infos),
    rabbit_event:notify(connection_created, Infos),
    rabbit_networking:register_non_amqp_connection(self()),
    {ok, PState#proc_state{
           %% We won't need conn_name anymore. Use less memmory by setting to undefined.
           conn_name = undefined}}.

human_readable_mqtt_version(3) ->
    <<"3.1.0">>;
human_readable_mqtt_version(4) ->
    <<"3.1.1">>.

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

start_keepalive(#mqtt_frame_connect{keep_alive = Seconds},
                #proc_state{socket = Socket}) ->
    ok = rabbit_mqtt_keepalive:start(Seconds, Socket).

handle_clean_session(_, PState0 = #proc_state{clean_sess = false,
                                              proto_ver = ProtoVer}) ->
    case get_queue(?QOS_1, PState0) of
        {error, not_found} ->
            %% Queue will be created later when client subscribes.
            {ok, _SessionPresent = false, PState0};
        {ok, Q} ->
            case consume(Q, ?QOS_1, PState0) of
                {ok, PState} ->
                    rabbit_global_counters:consumer_created(ProtoVer),
                    {ok, _SessionPresent = true, PState};
                {error, access_refused} ->
                    {error, ?CONNACK_NOT_AUTHORIZED};
                {error, _Reason} ->
                    %% Let's use most generic error return code.
                    {error, ?CONNACK_SERVER_UNAVAILABLE}
            end
    end;
handle_clean_session(_, PState = #proc_state{clean_sess = true,
                                             auth_state = #auth_state{user = User,
                                                                      username = Username,
                                                                      authz_ctx = AuthzCtx}}) ->
    case get_queue(?QOS_1, PState) of
        {error, not_found} ->
            {ok, _SessionPresent = false, PState};
        {ok, Q0} ->
            QName = amqqueue:get_name(Q0),
            %% configure access to queue required for queue.delete
            case check_resource_access(User, QName, configure, AuthzCtx) of
                ok ->
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
                      end),
                    {ok, _SessionPresent = false, PState};
                {error, access_refused} ->
                    {error, ?CONNACK_NOT_AUTHORIZED}
            end
    end.

-spec get_queue(qos(), proc_state()) ->
    {ok, amqqueue:amqqueue()} | {error, not_found}.
get_queue(QoS, PState) ->
    QName = queue_name(QoS, PState),
    rabbit_amqqueue:lookup(QName).

queue_name(QoS, #proc_state{client_id = ClientId,
                            auth_state = #auth_state{vhost = VHost}}) ->
    QNameBin = rabbit_mqtt_util:queue_name_bin(ClientId, QoS),
    rabbit_misc:r(VHost, queue, QNameBin).

find_queue_name(TopicName, #proc_state{exchange = Exchange,
                                       mqtt2amqp_fun = Mqtt2AmqpFun} = PState) ->
    RoutingKey = Mqtt2AmqpFun(TopicName),
    QNameQoS0 = queue_name(?QOS_0, PState),
    case lookup_binding(Exchange, QNameQoS0, RoutingKey) of
        true ->
            {ok, QNameQoS0};
        false ->
            QNameQoS1 = queue_name(?QOS_1, PState),
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

has_subs(#proc_state{exchange = Exchange} = PState) ->
    has_bindings_between(Exchange, queue_name(?QOS_0, PState)) orelse
    has_bindings_between(Exchange, queue_name(?QOS_1, PState)).
has_bindings_between(Exchange, QueueName) ->
    case rabbit_binding:list_for_source_and_destination(Exchange, QueueName) of
        [] ->
            false;
        _ ->
            true
    end.

hand_off_to_retainer(RetainerPid, Amqp2MqttFun, Topic0, #mqtt_msg{payload = <<"">>}) ->
    Topic1 = Amqp2MqttFun(Topic0),
    rabbit_mqtt_retainer:clear(RetainerPid, Topic1),
    ok;
hand_off_to_retainer(RetainerPid, Amqp2MqttFun, Topic0, Msg) ->
    Topic1 = Amqp2MqttFun(Topic0),
    rabbit_mqtt_retainer:retain(RetainerPid, Topic1, Msg),
    ok.

maybe_send_retained_message(RPid, #mqtt_topic{name = Topic0, qos = SubscribeQos},
                            #proc_state{amqp2mqtt_fun = Amqp2MqttFun,
                                        packet_id = PacketId0,
                                        send_fun = SendFun} = PState0) ->
    Topic1 = Amqp2MqttFun(Topic0),
    case rabbit_mqtt_retainer:fetch(RPid, Topic1) of
        undefined ->
            PState0;
        Msg ->
            Qos = effective_qos(Msg#mqtt_msg.qos, SubscribeQos),
            {PacketId, PState} = case Qos of
                                     ?QOS_0 ->
                                         {undefined, PState0};
                                     ?QOS_1 ->
                                         {PacketId0, PState0#proc_state{packet_id = increment_packet_id(PacketId0)}}
                                 end,
            SendFun(
              #mqtt_frame{fixed = #mqtt_frame_fixed{
                                     type = ?PUBLISH,
                                     qos  = Qos,
                                     dup  = false,
                                     retain = Msg#mqtt_msg.retain
                                    }, variable = #mqtt_frame_publish{
                                                     message_id = PacketId,
                                                     topic_name = Topic1
                                                    },
                          payload = Msg#mqtt_msg.payload},
              PState),
            PState
    end.

make_will_msg(#mqtt_frame_connect{will_flag   = false}) ->
    undefined;
make_will_msg(#mqtt_frame_connect{will_retain = Retain,
                                  will_qos    = Qos,
                                  will_topic  = Topic,
                                  will_msg    = Msg}) ->
    #mqtt_msg{retain  = Retain,
              qos     = Qos,
              topic   = Topic,
              dup     = false,
              payload = Msg}.

process_login(_UserBin, _PassBin, _ClientId,
              #proc_state{peer_addr  = Addr,
                          auth_state = #auth_state{username = Username,
                                                   user = User,
                                                   vhost = VHost
                                                  }})
  when Username =/= undefined, User =/= undefined, VHost =/= underfined ->
    UsernameStr = rabbit_data_coercion:to_list(Username),
    VHostStr = rabbit_data_coercion:to_list(VHost),
    rabbit_core_metrics:auth_attempt_failed(list_to_binary(inet:ntoa(Addr)), Username, mqtt),
    rabbit_log_connection:warning("MQTT detected duplicate connect/login attempt for user ~tp, vhost ~tp",
                                  [UsernameStr, VHostStr]),
    already_connected;
process_login(UserBin, PassBin, ClientId,
              #proc_state{socket = Sock,
                          ssl_login_name = SslLoginName,
                          peer_addr = Addr,
                          auth_state = undefined} = PState0) ->
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
           Input, PState0) of
        {ok, _Output, PState} ->
            rabbit_core_metrics:auth_attempt_succeeded(RemoteIpAddressBin, UsernameBin, mqtt),
            {ok, PState};
        {error, _Reason, _PState} = Err ->
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
                               username_bin := UsernameBin}) ->
    case rabbit_vhost_limit:is_over_connection_limit(VHost) of
        false ->
            ok;
        {true, Limit} ->
            rabbit_log_connection:error(
              "Error on MQTT connection ~p~n"
              "access to vhost '~s' refused for user '~s': "
              "vhost connection limit (~p) is reached",
              [self(), VHost, UsernameBin, Limit]),
            {error, ?CONNACK_NOT_AUTHORIZED}
    end.

check_vhost_alive(#{vhost := VHost,
                    username_bin := UsernameBin}) ->
    case rabbit_vhost_sup_sup:is_vhost_alive(VHost) of
        true  ->
            ok;
        false ->
            rabbit_log_connection:error(
              "Error on MQTT connection ~p~n"
              "access refused for user '~s': "
              "vhost is down",
              [self(), UsernameBin, VHost]),
            {error, ?CONNACK_NOT_AUTHORIZED}
    end.

check_user_login(#{vhost := VHost,
                   username_bin := UsernameBin,
                   pass_bin := PassBin,
                   client_id := ClientId
                  } = In, PState) ->
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
            notify_auth_result(user_authentication_success, Username, PState),
            {ok, maps:put(user, User, In), PState};
        {refused, Username, Msg, Args} ->
            rabbit_log_connection:error(
              "Error on MQTT connection ~p~n"
              "access refused for user '~s' in vhost '~s' "
              ++ Msg,
              [self(), Username, VHost] ++ Args),
            notify_auth_result(user_authentication_failure, Username, PState),
            {error, ?CONNACK_BAD_CREDENTIALS}
    end.

notify_auth_result(AuthResult, Username, #proc_state{conn_name = ConnName}) ->
    rabbit_event:notify(
      AuthResult,
      [
       {name, case Username of
                  none -> '';
                  _ -> Username
              end},
       {connection_name, ConnName},
       {connection_type, network}
      ]).

check_user_connection_limit(#{user := #user{username = Username}}) ->
    case rabbit_auth_backend_internal:is_over_connection_limit(Username) of
        false ->
            ok;
        {true, Limit} ->
            rabbit_log_connection:error(
              "Error on MQTT connection ~p~n"
              "access refused for user '~s': "
              "user connection limit (~p) is reached",
              [self(), Username, Limit]),
            {error, ?CONNACK_NOT_AUTHORIZED}
    end.


check_vhost_access(#{vhost := VHost,
                     client_id := ClientId,
                     user := User = #user{username = Username}
                    } = In,
                   #proc_state{peer_addr = PeerAddr} = PState) ->
    AuthzCtx = #{<<"client_id">> => ClientId},
    try rabbit_access_control:check_vhost_access(
          User,
          VHost,
          {ip, PeerAddr},
          AuthzCtx) of
        ok ->
            {ok, maps:put(authz_ctx, AuthzCtx, In), PState}
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
                    #proc_state{peer_addr = PeerAddr} = PState) ->
    case rabbit_access_control:check_user_loopback(UsernameBin, PeerAddr) of
        ok ->
            AuthState = #auth_state{user = User,
                                    username = UsernameBin,
                                    vhost = VHost,
                                    authz_ctx = AuthzCtx},
            {ok, PState#proc_state{auth_state = AuthState}};
        not_allowed ->
            rabbit_log_connection:warning(
              "MQTT login failed: user '~s' can only connect via localhost",
              [binary_to_list(UsernameBin)]),
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
    case application:get_env(?APP, ignore_colons_in_username) of
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
    case application:get_env(?APP, ignore_colons_in_username) of
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
    "plugin configuration or default";
human_readable_vhost_lookup_strategy(Val) ->
     atom_to_list(Val).

creds(User, Pass, SSLLoginName) ->
    DefaultUser   = rabbit_mqtt_util:env(default_user),
    DefaultPass   = rabbit_mqtt_util:env(default_pass),
    {ok, Anon}    = application:get_env(?APP, allow_anonymous),
    {ok, TLSAuth} = application:get_env(?APP, ssl_cert_login),
    HaveDefaultCreds = Anon =:= true andalso
                       is_binary(DefaultUser) andalso
                       is_binary(DefaultPass),

    CredentialsProvided = User =/= undefined orelse
                          Pass =/= undefined,

    CorrectCredentials = is_list(User) andalso
                         is_list(Pass),

    SSLLoginProvided = TLSAuth =:= true andalso
                       SSLLoginName =/= none,

    case {CredentialsProvided, CorrectCredentials, SSLLoginProvided, HaveDefaultCreds} of
        %% Username and password take priority
        {true, true, _, _}          -> {list_to_binary(User),
                                        list_to_binary(Pass)};
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

ensure_queue(QoS, #proc_state{
                     client_id = ClientId,
                     clean_sess = CleanSess,
                     auth_state = #auth_state{
                                     vhost = VHost,
                                     user = User = #user{username = Username},
                                     authz_ctx = AuthzCtx}
                    } = PState) ->
    case get_queue(QoS, PState) of
        {ok, Q} ->
            {ok, Q};
        {error, not_found} ->
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
            end
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

queue_type(?QOS_0, true, _) ->
    rabbit_mqtt_qos0_queue;
queue_type(_, _, QArgs) ->
    rabbit_amqqueue:get_queue_type(QArgs).

consume(Q, QoS, #proc_state{
                   queue_states = QStates0,
                   auth_state = #auth_state{
                                   user = User = #user{username = Username},
                                   authz_ctx = AuthzCtx},
                   info = #info{prefetch = Prefetch}
                  } = PState0) ->
    QName = amqqueue:get_name(Q),
    %% read access to queue required for basic.consume
    case check_resource_access(User, QName, read, AuthzCtx) of
        ok ->
            case amqqueue:get_type(Q) of
                rabbit_mqtt_qos0_queue ->
                    %% Messages get delivered directly to our process without
                    %% explicitly calling rabbit_queue_type:consume/3.
                    {ok, PState0};
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
                                      PState = PState0#proc_state{queue_states = QStates},
                                      {ok, PState};
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

bind(QueueName, TopicName, PState) ->
    binding_action_with_checks({QueueName, TopicName, fun rabbit_binding:add/2}, PState).
unbind(QueueName, TopicName, PState) ->
    binding_action_with_checks({QueueName, TopicName, fun rabbit_binding:remove/2}, PState).

binding_action_with_checks(Input, PState) ->
    %% Same permission checks required for both binding and unbinding
    %% queue to / from topic exchange.
    rabbit_misc:pipeline(
      [fun check_queue_write_access/2,
       fun check_exchange_read_access/2,
       fun check_topic_access/2,
       fun binding_action/2],
      Input, PState).

check_queue_write_access(
  {QueueName, _, _},
  #proc_state{auth_state = #auth_state{
                              user = User,
                              authz_ctx = AuthzCtx}}) ->
    %% write access to queue required for queue.(un)bind
    check_resource_access(User, QueueName, write, AuthzCtx).

check_exchange_read_access(
  _, #proc_state{exchange = ExchangeName,
              auth_state = #auth_state{
                              user = User,
                              authz_ctx = AuthzCtx}}) ->
    %% read access to exchange required for queue.(un)bind
    check_resource_access(User, ExchangeName, read, AuthzCtx).

check_topic_access({_, TopicName, _}, PState) ->
    check_topic_access(TopicName, read, PState).

binding_action(
  {QueueName, TopicName, BindingFun},
  #proc_state{exchange = ExchangeName,
              auth_state = #auth_state{
                              user = #user{username = Username}},
              mqtt2amqp_fun = Mqtt2AmqpFun}) ->
    RoutingKey = Mqtt2AmqpFun(TopicName),
    Binding = #binding{source = ExchangeName,
                       destination = QueueName,
                       key = RoutingKey},
    BindingFun(Binding, Username).

send_will(#proc_state{will_msg = undefined}) ->
    ok;
send_will(PState = #proc_state{will_msg = WillMsg = #mqtt_msg{retain = Retain,
                                                              topic = Topic},
                               retainer_pid = RPid,
                               amqp2mqtt_fun = Amqp2MqttFun}) ->
    case check_topic_access(Topic, write, PState) of
        ok ->
            publish_to_queues(WillMsg, PState),
            case Retain of
                false ->
                    ok;
                true ->
                    hand_off_to_retainer(RPid, Amqp2MqttFun, Topic, WillMsg)
            end;
        {error, access_refused = Reason}  ->
            rabbit_log:error("failed to send will message: ~p", [Reason])
    end.

publish_to_queues(undefined, PState) ->
    {ok, PState};
publish_to_queues(
  #mqtt_msg{qos        = Qos,
            topic      = Topic,
            dup        = Dup,
            message_id = MessageId,
            payload    = Payload},
  #proc_state{exchange       = ExchangeName,
              mqtt2amqp_fun  = Mqtt2AmqpFun,
              delivery_flow  = Flow} = PState) ->
    RoutingKey = Mqtt2AmqpFun(Topic),
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
                  msg_seq_no = MessageId,
                  flow = Flow
                 },
    case rabbit_exchange:lookup(ExchangeName) of
        {ok, Exchange} ->
            QNames = rabbit_exchange:route(Exchange, Delivery),
            deliver_to_queues(Delivery, QNames, PState);
        {error, not_found} ->
            rabbit_log:error("~s not found", [rabbit_misc:rs(ExchangeName)]),
            {error, exchange_not_found, PState}
    end.

deliver_to_queues(Delivery,
                  RoutedToQNames,
                  PState0 = #proc_state{queue_states = QStates0,
                                        proto_ver = ProtoVer}) ->
    %% TODO only lookup fields that are needed using ets:select / match?
    %% TODO Use ETS continuations to be more space efficient
    Qs0 = rabbit_amqqueue:lookup(RoutedToQNames),
    Qs = rabbit_amqqueue:prepend_extra_bcc(Qs0),
    case rabbit_queue_type:deliver(Qs, Delivery, QStates0) of
        {ok, QStates, Actions} ->
            rabbit_global_counters:messages_routed(ProtoVer, length(Qs)),
            PState = process_routing_confirm(Delivery, Qs,
                                             PState0#proc_state{queue_states = QStates}),
            %% Actions must be processed after registering confirms as actions may
            %% contain rejections of publishes.
            {ok, handle_queue_actions(Actions, PState)};
        {error, Reason} ->
            rabbit_log:error("Failed to deliver message with packet_id=~p to queues: ~p",
                             [Delivery#delivery.msg_seq_no, Reason]),
            {error, Reason, PState0}
    end.

process_routing_confirm(#delivery{confirm = false},
                        [], PState = #proc_state{proto_ver = ProtoVer}) ->
    rabbit_global_counters:messages_unroutable_dropped(ProtoVer, 1),
    PState;
process_routing_confirm(#delivery{confirm = true,
                                  msg_seq_no = undefined},
                        [], PState = #proc_state{proto_ver = ProtoVer}) ->
    %% unroutable will message with QoS > 0
    rabbit_global_counters:messages_unroutable_dropped(ProtoVer, 1),
    PState;
process_routing_confirm(#delivery{confirm = true,
                                  msg_seq_no = MsgId},
                        [], PState = #proc_state{proto_ver = ProtoVer}) ->
    rabbit_global_counters:messages_unroutable_returned(ProtoVer, 1),
    %% MQTT 5 spec:
    %% If the Server knows that there are no matching subscribers, it MAY use
    %% Reason Code 0x10 (No matching subscribers) instead of 0x00 (Success).
    send_puback(MsgId, PState),
    PState;
process_routing_confirm(#delivery{confirm = false}, _, PState) ->
    PState;
process_routing_confirm(#delivery{confirm = true,
                                  msg_seq_no = undefined}, [_|_], PState) ->
    %% routable will message with QoS > 0
    PState;
process_routing_confirm(#delivery{confirm = true,
                                  msg_seq_no = MsgId},
                        Qs, PState = #proc_state{unacked_client_pubs = U0}) ->
    QNames = lists:map(fun amqqueue:get_name/1, Qs),
    U = rabbit_mqtt_confirms:insert(MsgId, QNames, U0),
    PState#proc_state{unacked_client_pubs = U}.

send_puback(MsgIds0, PState)
  when is_list(MsgIds0) ->
    %% Classic queues confirm messages unordered.
    %% Let's sort them here assuming most MQTT clients send with an increasing packet identifier.
    MsgIds = lists:usort(MsgIds0),
    lists:foreach(fun(Id) ->
                          send_puback(Id, PState)
                  end, MsgIds);
send_puback(MsgId, PState = #proc_state{send_fun = SendFun,
                                        proto_ver = ProtoVer}) ->
    rabbit_global_counters:messages_confirmed(ProtoVer, 1),
    SendFun(
      #mqtt_frame{fixed = #mqtt_frame_fixed{type = ?PUBACK},
                  variable = #mqtt_frame_publish{message_id = MsgId}},
      PState).

serialise_and_send_to_client(Frame, #proc_state{proto_ver = ProtoVer, socket = Sock}) ->
    %%TODO Test sending large frames at high speed:
    %% Will we need garbage collection as done in rabbit_writer:maybe_gc_large_msg/1?
    %%TODO batch to fill up MTU if there are messages in the Erlang mailbox?
    %% Check rabbit_writer:maybe_flush/1
    Data = rabbit_mqtt_frame:serialise(Frame, ProtoVer),
    try rabbit_net:port_command(Sock, Data)
    catch _:Error ->
              rabbit_log_connection:error("MQTT: a socket write failed, the socket might already be closed"),
              rabbit_log_connection:debug("Failed to write to socket ~p, error: ~p, frame: ~p",
                                          [Sock, Error, Frame])
    end.

serialise(Frame, #proc_state{proto_ver = ProtoVer}) ->
    rabbit_mqtt_frame:serialise(Frame, ProtoVer).

terminate(PState, ConnName) ->
    Infos = [{name, ConnName},
             {node, node()},
             {pid, self()},
             {disconnected_at, os:system_time(milli_seconds)}
            ] ++ additional_connection_closed_info(PState),
    rabbit_event:notify(connection_closed, Infos),
    rabbit_networking:unregister_non_amqp_connection(self()),
    maybe_unregister_client(PState),
    maybe_decrement_consumer(PState),
    maybe_decrement_publisher(PState),
    maybe_delete_mqtt_qos0_queue(PState).

additional_connection_closed_info(
  #proc_state{info = #info{proto_human = {ProtoName, ProtoVsn}},
              auth_state = #auth_state{vhost = VHost,
                                       username = Username}}) ->
    [{protocol, {ProtoName, binary_to_list(ProtoVsn)}},
     {vhost, VHost},
     {user, Username}];
additional_connection_closed_info(_) ->
    [].

maybe_unregister_client(#proc_state{client_id = ClientId})
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

maybe_delete_mqtt_qos0_queue(PState = #proc_state{clean_sess = true,
                                                  auth_state = #auth_state{username = Username}}) ->
    case get_queue(?QOS_0, PState) of
        {ok, Q} ->
            %% double check we delete the right queue
            case {amqqueue:get_type(Q), amqqueue:get_pid(Q)} of
                {rabbit_mqtt_qos0_queue, Pid}
                  when Pid =:= self() ->
                    rabbit_queue_type:delete(Q, false, false, Username);
                _ ->
                    ok
            end;
        {error, not_found} ->
            ok
    end;
maybe_delete_mqtt_qos0_queue(_) ->
    ok.

handle_pre_hibernate() ->
    erase(permission_cache),
    erase(topic_permission_cache),
    ok.

handle_ra_event({applied, [{Corr, ok}]},
                PState = #proc_state{register_state = {pending, Corr}}) ->
    %% success case - command was applied transition into registered state
    PState#proc_state{register_state = registered};
handle_ra_event({not_leader, Leader, Corr},
                PState = #proc_state{register_state = {pending, Corr},
                                     client_id = ClientId}) ->
    case rabbit_mqtt_ff:track_client_id_in_ra() of
        true ->
            %% retry command against actual leader
            {ok, NewCorr} = rabbit_mqtt_collector:register(Leader, ClientId, self()),
            PState#proc_state{register_state = {pending, NewCorr}};
        false ->
            PState
    end;
handle_ra_event(register_timeout,
                PState = #proc_state{register_state = {pending, _Corr},
                                     client_id = ClientId}) ->
    case rabbit_mqtt_ff:track_client_id_in_ra() of
        true ->
            {ok, NewCorr} = rabbit_mqtt_collector:register(ClientId, self()),
            PState#proc_state{register_state = {pending, NewCorr}};
        false ->
            PState
    end;
handle_ra_event(register_timeout, PState) ->
    PState;
handle_ra_event(Evt, PState) ->
    %% log these?
    rabbit_log:debug("unhandled ra_event: ~w ", [Evt]),
    PState.

handle_down({{'DOWN', QName}, _MRef, process, QPid, Reason},
            PState0 = #proc_state{queue_states = QStates0,
                                  unacked_client_pubs = U0}) ->
    credit_flow:peer_down(QPid),
    case rabbit_queue_type:handle_down(QPid, QName, Reason, QStates0) of
        {ok, QStates1, Actions} ->
            PState1 = PState0#proc_state{queue_states = QStates1},
            try handle_queue_actions(Actions, PState1) of
                PState ->
                    {ok, PState}
            catch throw:consuming_queue_down ->
                      {error, consuming_queue_down}
            end;
        {eol, QStates1, QRef} ->
            {ConfirmMsgIds, U} = rabbit_mqtt_confirms:remove_queue(QRef, U0),
            QStates = rabbit_queue_type:remove(QRef, QStates1),
            PState = PState0#proc_state{queue_states = QStates,
                                        unacked_client_pubs = U},
            send_puback(ConfirmMsgIds, PState),
            {ok, PState}
    end.

handle_queue_event({queue_event, rabbit_mqtt_qos0_queue, Msg}, PState0) ->
    PState = deliver_one_to_client(Msg, false, PState0),
    {ok, PState};
handle_queue_event({queue_event, QName, Evt},
                   PState0 = #proc_state{queue_states = QStates0,
                                         unacked_client_pubs = U0}) ->
    case rabbit_queue_type:handle_event(QName, Evt, QStates0) of
        {ok, QStates, Actions} ->
            PState1 = PState0#proc_state{queue_states = QStates},
            PState = handle_queue_actions(Actions, PState1),
            {ok, PState};
        {eol, Actions} ->
            PState1 = handle_queue_actions(Actions, PState0),
            {ConfirmMsgIds, U} = rabbit_mqtt_confirms:remove_queue(QName, U0),
            QStates = rabbit_queue_type:remove(QName, QStates0),
            PState = PState1#proc_state{queue_states = QStates,
                                         unacked_client_pubs = U},
            send_puback(ConfirmMsgIds, PState),
            {ok, PState};
        {protocol_error, _Type, _Reason, _ReasonArgs} = Error ->
            {error, Error, PState0}
    end.

handle_queue_actions(Actions, #proc_state{} = PState0) ->
    lists:foldl(
      fun ({deliver, ?CONSUMER_TAG, Ack, Msgs}, S) ->
              deliver_to_client(Msgs, Ack, S);
          ({settled, QName, MsgIds}, S = #proc_state{unacked_client_pubs = U0}) ->
              {ConfirmMsgIds, U} = rabbit_mqtt_confirms:confirm(MsgIds, QName, U0),
              send_puback(ConfirmMsgIds, S),
              S#proc_state{unacked_client_pubs = U};
          ({rejected, _QName, MsgIds}, S = #proc_state{unacked_client_pubs = U0}) ->
              %% Negative acks are supported in MQTT v5 only.
              %% Therefore, in MQTT v3 and v4 we ignore rejected messages.
              U = lists:foldl(
                    fun(MsgId, Acc0) ->
                            case rabbit_mqtt_confirms:reject(MsgId, Acc0) of
                                {ok, Acc} -> Acc;
                                {error, not_found} -> Acc0
                            end
                    end, U0, MsgIds),
              S#proc_state{unacked_client_pubs = U};
          ({block, QName}, S = #proc_state{soft_limit_exceeded = SLE}) ->
              S#proc_state{soft_limit_exceeded = sets:add_element(QName, SLE)};
          ({unblock, QName}, S = #proc_state{soft_limit_exceeded = SLE}) ->
              S#proc_state{soft_limit_exceeded = sets:del_element(QName, SLE)};
          ({queue_down, QName}, S) ->
              handle_queue_down(QName, S)
      end, PState0, Actions).

handle_queue_down(QName, PState0 = #proc_state{client_id = ClientId}) ->
    %% Classic queue is down.
    case rabbit_amqqueue:lookup(QName) of
        {ok, Q} ->
            case rabbit_mqtt_util:qos_from_queue_name(QName, ClientId) of
                no_consuming_queue ->
                    PState0;
                QoS ->
                    %% Consuming classic queue is down.
                    %% Let's try to re-consume: HA failover for classic mirrored queues.
                    case consume(Q, QoS, PState0) of
                        {ok, PState} ->
                            PState;
                        {error, _Reason} ->
                            rabbit_log:info("Terminating MQTT connection because consuming ~s is down.",
                                            [rabbit_misc:rs(QName)]),
                            throw(consuming_queue_down)
                    end
            end;
        {error, not_found} ->
            PState0
    end.

deliver_to_client(Msgs, Ack, PState) ->
    lists:foldl(fun(Msg, S) ->
                        deliver_one_to_client(Msg, Ack, S)
                end, PState, Msgs).

deliver_one_to_client(Msg = {QNameOrType, QPid, QMsgId, _Redelivered,
                             #basic_message{content = #content{properties = #'P_basic'{headers = Headers}}}},
                      AckRequired, PState0) ->
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
    PState1 = maybe_publish_to_client(Msg, QoS, PState0),
    PState = maybe_ack(AckRequired, QoS, QNameOrType, QMsgId, PState1),
    %%TODO GC
    % case GCThreshold of
    %     undefined -> ok;
    %     _         -> rabbit_basic:maybe_gc_large_msg(Content, GCThreshold)
    % end,
    ok = maybe_notify_sent(QNameOrType, QPid, PState),
    PState.

-spec effective_qos(qos(), qos()) -> qos().
effective_qos(PublisherQoS, SubscriberQoS) ->
    %% "The QoS of Application Messages sent in response to a Subscription MUST be the minimum
    %% of the QoS of the originally published message and the Maximum QoS granted by the Server
    %% [MQTT-3.8.4-8]."
    erlang:min(PublisherQoS, SubscriberQoS).

maybe_publish_to_client({_, _, _, _Redelivered = true, _}, ?QOS_0, PState) ->
    %% Do not redeliver to MQTT subscriber who gets message at most once.
    PState;
maybe_publish_to_client(
  {_QName, _QPid, QMsgId, Redelivered,
   #basic_message{
      routing_keys = [RoutingKey | _CcRoutes],
      content = #content{payload_fragments_rev = FragmentsRev}}},
  QoS, PState0 = #proc_state{amqp2mqtt_fun = Amqp2MqttFun,
                             send_fun = SendFun}) ->
    {PacketId, PState} = queue_message_id_to_packet_id(QMsgId, QoS, PState0),
    %%TODO support iolists when sending to client
    Payload = list_to_binary(lists:reverse(FragmentsRev)),
    Frame =
    #mqtt_frame{
       fixed = #mqtt_frame_fixed{
                  type = ?PUBLISH,
                  qos = QoS,
                  %% "The value of the DUP flag from an incoming PUBLISH packet is not
                  %% propagated when the PUBLISH Packet is sent to subscribers by the Server.
                  %% The DUP flag in the outgoing PUBLISH packet is set independently to the
                  %% incoming PUBLISH packet, its value MUST be determined solely by whether
                  %% the outgoing PUBLISH packet is a retransmission [MQTT-3.3.1-3]."
                  %% Therefore, we do not consider header value <<"x-mqtt-dup">> here.
                  dup = Redelivered},
       variable = #mqtt_frame_publish{
                     message_id = PacketId,
                     topic_name = Amqp2MqttFun(RoutingKey)},
       payload = Payload},
    SendFun(Frame, PState),
    PState.

queue_message_id_to_packet_id(_, ?QOS_0, PState) ->
    %% "A PUBLISH packet MUST NOT contain a Packet Identifier if its QoS value is set to 0 [MQTT-2.2.1-2]."
    {undefined, PState};
queue_message_id_to_packet_id(QMsgId, ?QOS_1, #proc_state{packet_id = PktId,
                                                          unacked_server_pubs = U} = PState) ->
    {PktId, PState#proc_state{packet_id = increment_packet_id(PktId),
                              unacked_server_pubs = maps:put(PktId, QMsgId, U)}}.

-spec increment_packet_id(packet_id()) -> packet_id().
increment_packet_id(Id)
  when Id >= 16#ffff ->
    1;
increment_packet_id(Id) ->
    Id + 1.

maybe_ack(_AckRequired = true, ?QOS_0, QName, QMsgId,
          PState = #proc_state{queue_states = QStates0}) ->
    case rabbit_queue_type:settle(QName, complete, ?CONSUMER_TAG, [QMsgId], QStates0) of
        {ok, QStates, [] = _Actions} ->
            % incr_queue_stats(QRef, MsgIds, State),
            %%TODO handle actions
            PState#proc_state{queue_states = QStates};
        {protocol_error, _ErrorType, _Reason, _ReasonArgs} = Err ->
            %%TODO handle error
            throw(Err)
    end;
maybe_ack(_, _, _, _, PState) ->
    PState.

maybe_notify_sent(rabbit_mqtt_qos0_queue, _, _) ->
    ok;
maybe_notify_sent(QName, QPid, #proc_state{queue_states = QStates}) ->
    case rabbit_queue_type:module(QName, QStates) of
        {ok, rabbit_classic_queue} ->
            rabbit_amqqueue:notify_sent(QPid, self());
        _ ->
            ok
    end.

publish_to_queues_with_checks(
  TopicName, PublishFun,
  #proc_state{exchange = Exchange,
              auth_state = #auth_state{user = User,
                                       authz_ctx = AuthzCtx}} = PState) ->
    case check_resource_access(User, Exchange, write, AuthzCtx) of
        ok ->
            case check_topic_access(TopicName, write, PState) of
                ok ->
                    PublishFun();
                {error, access_refused} ->
                    {error, unauthorized, PState}
            end;
        {error, access_refused} ->
            {error, unauthorized, PState}
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
            catch exit:{amqp_error, access_refused, Msg, _AmqpMethod} ->
                      rabbit_log:error("MQTT resource access refused: ~s", [Msg]),
                      {error, access_refused}
            end
    end.

check_topic_access(TopicName, Access,
                   #proc_state{
                      auth_state = #auth_state{user = User = #user{username = Username},
                                               vhost = VHost,
                                               authz_ctx = AuthzCtx},
                      exchange = #resource{name = ExchangeBin},
                      client_id = ClientId,
                      mqtt2amqp_fun = Mqtt2AmqpFun}) ->
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
            RoutingKey = Mqtt2AmqpFun(TopicName),
            Context = #{routing_key  => RoutingKey,
                        variable_map => AuthzCtx},
            try rabbit_access_control:check_topic_access(User, Resource, Access, Context) of
                ok ->
                    CacheTail = lists:sublist(Cache, ?MAX_PERMISSION_CACHE_SIZE - 1),
                    put(topic_permission_cache, [Key | CacheTail]),
                    ok
            catch
                exit:{amqp_error, access_refused, Msg, _AmqpMethod} ->
                    rabbit_log:error("MQTT topic access refused: ~s", [Msg]),
                    {error, access_refused}
            end
    end.

info(protocol, #proc_state{info = #info{proto_human = Val}}) -> Val;
info(host, #proc_state{info = #info{host = Val}}) -> Val;
info(port, #proc_state{info = #info{port = Val}}) -> Val;
info(peer_host, #proc_state{info = #info{peer_host = Val}}) -> Val;
info(peer_port, #proc_state{info = #info{peer_port = Val}}) -> Val;
info(ssl_login_name, #proc_state{ssl_login_name = Val}) -> Val;
info(client_id, #proc_state{client_id = Val}) ->
    rabbit_data_coercion:to_binary(Val);
info(vhost, #proc_state{auth_state = #auth_state{vhost = Val}}) -> Val;
info(user, #proc_state{auth_state = #auth_state{username = Val}}) -> Val;
info(clean_sess, #proc_state{clean_sess = Val}) -> Val;
info(will_msg, #proc_state{will_msg = Val}) -> Val;
info(retainer_pid, #proc_state{retainer_pid = Val}) -> Val;
info(exchange, #proc_state{exchange = #resource{name = Val}}) -> Val;
info(prefetch, #proc_state{info = #info{prefetch = Val}}) -> Val;
info(messages_unconfirmed, #proc_state{unacked_client_pubs = Val}) ->
    rabbit_mqtt_confirms:size(Val);
info(messages_unacknowledged, #proc_state{unacked_server_pubs = Val}) ->
    maps:size(Val);
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

format_status(#proc_state{queue_states = QState,
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
                          info = Info
                         } = PState) ->
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
      soft_limit_exceeded => soft_limit_exceeded(PState)}.

soft_limit_exceeded(#proc_state{soft_limit_exceeded = SLE}) ->
    not sets:is_empty(SLE).

protocol_integer_to_atom(3) ->
    ?MQTT_PROTO_V3;
protocol_integer_to_atom(4) ->
    ?MQTT_PROTO_V4.

maybe_increment_publisher(PState = #proc_state{has_published = false,
                                               proto_ver = ProtoVer}) ->
    rabbit_global_counters:publisher_created(ProtoVer),
    PState#proc_state{has_published = true};
maybe_increment_publisher(PState) ->
    PState.

maybe_decrement_publisher(#proc_state{has_published = true,
                                      proto_ver = ProtoVer}) ->
    rabbit_global_counters:publisher_deleted(ProtoVer);
maybe_decrement_publisher(_) ->
    ok.

%% multiple subscriptions from the same connection count as one consumer
maybe_increment_consumer(_WasConsumer = false,
                         #proc_state{proto_ver = ProtoVer} = PState) ->
    case has_subs(PState) of
        true ->
            rabbit_global_counters:consumer_created(ProtoVer);
        false ->
            ok
    end;
maybe_increment_consumer(_, _) ->
    ok.

maybe_decrement_consumer(_WasConsumer = true,
                         #proc_state{proto_ver = ProtoVer} = PState) ->
    case has_subs(PState) of
        false ->
            rabbit_global_counters:consumer_deleted(ProtoVer);
        true ->
            ok
    end;
maybe_decrement_consumer(_, _) ->
    ok.

maybe_decrement_consumer(#proc_state{proto_ver = ProtoVer,
                                     auth_state = #auth_state{vhost = _Vhost}} = PState) ->
    case has_subs(PState) of
        true ->
            rabbit_global_counters:consumer_deleted(ProtoVer);
        false ->
            ok
    end;
maybe_decrement_consumer(_) ->
    ok.
