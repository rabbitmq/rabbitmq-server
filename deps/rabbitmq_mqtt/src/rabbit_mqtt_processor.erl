%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mqtt_processor).

-export([info/2, initial_state/1, initial_state/4,
         process_frame/2, amqp_pub/2, amqp_callback/2, send_will/1,
         close_connection/1, handle_pre_hibernate/0,
         handle_ra_event/2, handle_down/2, handle_queue_event/2]).

%% for testing purposes
-export([get_vhost_username/1, get_vhost/3, get_vhost_from_user_mapping/2, maybe_quorum/3]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbit/include/amqqueue.hrl").
-include("rabbit_mqtt_frame.hrl").
-include("rabbit_mqtt.hrl").

-define(APP, rabbitmq_mqtt).
-define(FRAME_TYPE(Frame, Type),
        Frame = #mqtt_frame{ fixed = #mqtt_frame_fixed{ type = Type }}).
-define(MAX_PERMISSION_CACHE_SIZE, 12).
-define(CONSUMER_TAG, mqtt_consumer).

initial_state(Socket) ->
    RealSocket = rabbit_net:unwrap_socket(Socket),
    SSLLoginName = ssl_login_name(RealSocket),
    {ok, {PeerAddr, _PeerPort}} = rabbit_net:peername(RealSocket),
    initial_state(RealSocket, SSLLoginName, fun serialise_and_send_to_client/2, PeerAddr).

initial_state(Socket, SSLLoginName, SendFun, PeerAddr) ->
    {ok, {mqtt2amqp_fun, M2A}, {amqp2mqtt_fun, A2M}} =
        rabbit_mqtt_util:get_topic_translation_funs(),
    %% MQTT connections use exactly one channel. The frame max is not
    %% applicable and there is no way to know what client is used.
    #proc_state{ unacked_pubs   = gb_trees:empty(),
                 awaiting_ack   = gb_trees:empty(),
                 message_id     = 1,
                 subscriptions  = #{},
                 queue_states   = rabbit_queue_type:init(),
                 consumer_tags  = {undefined, undefined},
                 channels       = {undefined, undefined},
                 socket         = Socket,
                 ssl_login_name = SSLLoginName,
                 send_fun       = SendFun,
                 peer_addr      = PeerAddr,
                 mqtt2amqp_fun  = M2A,
                 amqp2mqtt_fun  = A2M}.

process_frame(#mqtt_frame{ fixed = #mqtt_frame_fixed{ type = Type }},
              PState = #proc_state{ auth_state = undefined } )
  when Type =/= ?CONNECT ->
    {error, connect_expected, PState};
process_frame(Frame = #mqtt_frame{ fixed = #mqtt_frame_fixed{ type = Type }},
              PState) ->
    try process_request(Type, Frame, PState) of
        {ok, PState1} -> {ok, PState1, PState1#proc_state.connection};
        Ret -> Ret
    catch
        _:{{shutdown, {server_initiated_close, 403, _}}, _} ->
            %% NB: MQTT spec says we should ack normally, ie pretend
            %% there was no auth error, but here we are closing the
            %% connection with an error. This is what happens anyway
            %% if there is an authorization failure at the AMQP 0-9-1
            %% client level. And error was already logged by AMQP
            %% channel, so no need for custom logging.
            {error, access_refused, PState}
    end.

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
                  variable = #mqtt_frame_publish{ message_id = MessageId }},
                #proc_state{ channels     = {Channel, _},
                             awaiting_ack = Awaiting } = PState) ->
    %% tag can be missing because of bogus clients and QoS downgrades
    case gb_trees:is_defined(MessageId, Awaiting) of
      false ->
        {ok, PState};
      true ->
        Tag = gb_trees:get(MessageId, Awaiting),
        amqp_channel:cast(Channel, #'basic.ack'{ delivery_tag = Tag }),
        {ok, PState#proc_state{ awaiting_ack = gb_trees:delete(MessageId, Awaiting) }}
    end;

process_request(?PUBLISH,
                Frame = #mqtt_frame{
                    fixed = Fixed = #mqtt_frame_fixed{ qos = ?QOS_2 }},
                PState) ->
    % Downgrade QOS_2 to QOS_1
    process_request(?PUBLISH,
                    Frame#mqtt_frame{
                        fixed = Fixed#mqtt_frame_fixed{ qos = ?QOS_1 }},
                    PState);
process_request(?PUBLISH,
                #mqtt_frame{
                  fixed = #mqtt_frame_fixed{ qos    = Qos,
                                             retain = Retain,
                                             dup    = Dup },
                  variable = #mqtt_frame_publish{ topic_name = Topic,
                                                  message_id = MessageId },
                  payload = Payload },
                  PState = #proc_state{retainer_pid = RPid,
                                       amqp2mqtt_fun = Amqp2MqttFun}) ->
    check_publish(Topic, fun() ->
        Msg = #mqtt_msg{retain     = Retain,
                        qos        = Qos,
                        topic      = Topic,
                        dup        = Dup,
                        message_id = MessageId,
                        payload    = Payload},
        Result = amqp_pub(Msg, PState),
        case Retain of
          false -> ok;
          true  -> hand_off_to_retainer(RPid, Amqp2MqttFun, Topic, Msg)
        end,
        {ok, Result}
    end, PState);

process_request(?SUBSCRIBE,
                #mqtt_frame{
                  variable = #mqtt_frame_subscribe{
                              message_id  = SubscribeMsgId,
                              topic_table = Topics},
                  payload = undefined},
                #proc_state{retainer_pid = RPid,
                            send_fun = SendFun,
                            message_id  = StateMsgId} = PState0) ->
    rabbit_log_connection:debug("Received a SUBSCRIBE for topic(s) ~p", [Topics]),

    {QosResponse, PState1} =
        lists:foldl(fun (#mqtt_topic{name = TopicName,
                                     qos  = Qos}, {QosList, S0}) ->
                       SupportedQos = supported_subs_qos(Qos),
                       {QueueName, #proc_state{subscriptions = Subs} = S} =
                       ensure_queue(SupportedQos, S0),
                       bind(QueueName, TopicName, S),
                       SupportedQosList = case maps:find(TopicName, Subs) of
                           {ok, L} -> [SupportedQos|L];
                           error   -> [SupportedQos]
                       end,
                       {[SupportedQos | QosList],
                        S#proc_state{subscriptions = maps:put(TopicName, SupportedQosList, Subs)}}
                    end, {[], PState0}, Topics),
    SendFun(#mqtt_frame{fixed    = #mqtt_frame_fixed{type = ?SUBACK},
                        variable = #mqtt_frame_suback{
                                    message_id = SubscribeMsgId,
                                    qos_table  = QosResponse}}, PState1),
    %% we may need to send up to length(Topics) messages.
    %% if QoS is > 0 then we need to generate a message id,
    %% and increment the counter.
    StartMsgId = safe_max_id(SubscribeMsgId, StateMsgId),
    N = lists:foldl(fun (Topic, Acc) ->
                      case maybe_send_retained_message(RPid, Topic, Acc, PState1) of
                        {true, X} -> Acc + X;
                        false     -> Acc
                      end
                    end, StartMsgId, Topics),
    {ok, PState1#proc_state{message_id = N}};

process_request(?UNSUBSCRIBE,
                #mqtt_frame{
                  variable = #mqtt_frame_subscribe{ message_id  = MessageId,
                                                    topic_table = Topics },
                  payload = undefined }, #proc_state{ channels      = {Channel, _},
                                                      exchange      = Exchange,
                                                      client_id     = ClientId,
                                                      subscriptions = Subs0,
                                                      send_fun      = SendFun,
                                                      mqtt2amqp_fun = Mqtt2AmqpFun } = PState) ->
    rabbit_log_connection:debug("Received an UNSUBSCRIBE for topic(s) ~tp", [Topics]),
    Queues = rabbit_mqtt_util:subcription_queue_name(ClientId),
    Subs1 =
    lists:foldl(
      fun (#mqtt_topic{ name = TopicName }, Subs) ->
        QosSubs = case maps:find(TopicName, Subs) of
                      {ok, Val} when is_list(Val) -> lists:usort(Val);
                      error                       -> []
                  end,
        RoutingKey = Mqtt2AmqpFun(TopicName),
        lists:foreach(
          fun (QosSub) ->
                  Queue = element(QosSub + 1, Queues),
                  Binding = #'queue.unbind'{
                              queue       = Queue,
                              exchange    = Exchange,
                              routing_key = RoutingKey},
                  #'queue.unbind_ok'{} = amqp_channel:call(Channel, Binding)
          end, QosSubs),
        maps:remove(TopicName, Subs)
      end, Subs0, Topics),
    SendFun(#mqtt_frame{ fixed    = #mqtt_frame_fixed { type       = ?UNSUBACK },
                         variable = #mqtt_frame_suback{ message_id = MessageId }},
                PState),
    {ok, PState #proc_state{ subscriptions = Subs1 }};

process_request(?PINGREQ, #mqtt_frame{}, #proc_state{ send_fun = SendFun } = PState) ->
    rabbit_log_connection:debug("Received a PINGREQ"),
    SendFun(#mqtt_frame{ fixed = #mqtt_frame_fixed{ type = ?PINGRESP }},
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
                #proc_state{send_fun = SendFun} = PState0) ->
    rabbit_log_connection:debug("Received a CONNECT, client ID: ~p, username: ~p, "
                                "clean session: ~p, protocol version: ~p, keepalive: ~p",
                                [ClientId, Username, CleanSess, ProtoVersion, Keepalive]),
    {ReturnCode, SessionPresent, PState} =
    case rabbit_misc:pipeline([fun check_protocol_version/1,
                               fun check_client_id/1,
                               fun check_credentials/2,
                               fun login/2,
                               fun register_client/2],
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

client_id([]) ->
    rabbit_mqtt_util:gen_client_id();
client_id(ClientId)
  when is_list(ClientId) ->
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
        connack_dup_auth ->
            maybe_clean_sess(PState0);
        {ok, PState} ->
            {ok, Frame, PState#proc_state{clean_sess = CleanSess,
                                          client_id = ClientId}};
        {error, _Reason, _PState} = Err ->
            Err
    end.

register_client(Frame = #mqtt_frame_connect{
                           keep_alive = Keepalive,
                           proto_ver = ProtoVersion},
                PState0 = #proc_state{client_id = ClientId,
                                      socket = Socket,
                                      auth_state = #auth_state{
                                                      vhost = VHost}}) ->
    case rabbit_mqtt_collector:register(ClientId, self()) of
        {ok, Corr} ->
            RetainerPid = rabbit_mqtt_retainer_sup:child_for_vhost(VHost),
            Prefetch = rabbit_mqtt_util:env(prefetch),
            rabbit_mqtt_reader:start_keepalive(self(), Keepalive),
            {ok, {PeerHost, PeerPort, Host, Port}} = rabbit_net:socket_ends(Socket, inbound),
            ExchangeBin = rabbit_mqtt_util:env(exchange),
            ExchangeName = rabbit_misc:r(VHost, exchange, ExchangeBin),
            Protocol = {'MQTT', human_readable_mqtt_version(ProtoVersion)},
            PState = PState0#proc_state{
                       exchange = ExchangeName,
                       will_msg   = make_will_msg(Frame),
                       retainer_pid = RetainerPid,
                       register_state = {pending, Corr},
                       info = #info{prefetch = Prefetch,
                                    peer_host = PeerHost,
                                    peer_port = PeerPort,
                                    host = Host,
                                    port = Port,
                                    protocol = Protocol}},
            maybe_clean_sess(PState);
        {error, _} = Err ->
            %% e.g. this node was removed from the MQTT cluster members
            rabbit_log_connection:error("MQTT cannot accept a connection: "
                                        "client ID tracker is unavailable: ~p", [Err]),
            {error, ?CONNACK_SERVER_UNAVAILABLE};
        {timeout, _} ->
            rabbit_log_connection:error("MQTT cannot accept a connection: "
                                        "client ID registration timed out"),
            {error, ?CONNACK_SERVER_UNAVAILABLE}
    end.

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

maybe_clean_sess(PState = #proc_state {clean_sess = false,
                                       connection = Conn,
                                       auth_state = #auth_state{vhost = VHost},
                                       client_id  = ClientId }) ->
    SessionPresent = session_present(VHost, ClientId),
    case SessionPresent of
        false ->
            %% ensure_queue/2 not only ensures that queue is created, but also starts consuming from it.
            %% Let's avoid creating that queue until explicitly asked by a client.
            %% Then publish-only clients, that connect with clean_sess=true due to some misconfiguration,
            %% will consume less resources.
            {ok, SessionPresent, PState};
        true ->
            try ensure_queue(?QOS_1, PState) of
                {_Queue, PState1} ->
                    {ok, SessionPresent, PState1}
            catch
                exit:({{shutdown, {server_initiated_close, 403, _}}, _}) ->
                    %% Connection is not yet propagated to #proc_state{}, let's close it here
                    catch amqp_connection:close(Conn),
                    rabbit_log_connection:error("MQTT cannot recover a session, user is missing permissions"),
                    {error, ?CONNACK_SERVER_UNAVAILABLE};
                C:E:S ->
                    %% Connection is not yet propagated to
                    %% #proc_state{}, let's close it here.
                    %% This is an exceptional situation anyway, but
                    %% doing this will prevent second crash from
                    %% amqp client being logged.
                    catch amqp_connection:close(Conn),
                    erlang:raise(C, E, S)
            end
    end;
maybe_clean_sess(PState = #proc_state {clean_sess = true,
                                       client_id  = ClientId,
                                       auth_state = #auth_state{user = User,
                                                                username = Username,
                                                                vhost = VHost,
                                                                authz_ctx = AuthzCtx}}) ->
    {_, QueueName} = rabbit_mqtt_util:subcription_queue_name(ClientId),
    Queue = rabbit_misc:r(VHost, queue, QueueName),
    case rabbit_amqqueue:exists(Queue) of
        false ->
            {ok, false, PState};
        true ->
            ok = rabbit_access_control:check_resource_access(User, Queue, configure, AuthzCtx),
            rabbit_amqqueue:with(
              Queue,
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
            {ok, false, PState}
    end.

hand_off_to_retainer(RetainerPid, Amqp2MqttFun, Topic0, #mqtt_msg{payload = <<"">>}) ->
    Topic1 = Amqp2MqttFun(Topic0),
    rabbit_mqtt_retainer:clear(RetainerPid, Topic1),
    ok;
hand_off_to_retainer(RetainerPid, Amqp2MqttFun, Topic0, Msg) ->
    Topic1 = Amqp2MqttFun(Topic0),
    rabbit_mqtt_retainer:retain(RetainerPid, Topic1, Msg),
    ok.

maybe_send_retained_message(RPid, #mqtt_topic{name = Topic0, qos = SubscribeQos}, MsgId,
                            #proc_state{ send_fun = SendFun,
                                         amqp2mqtt_fun = Amqp2MqttFun } = PState) ->
    Topic1 = Amqp2MqttFun(Topic0),
    case rabbit_mqtt_retainer:fetch(RPid, Topic1) of
        undefined -> false;
        Msg       ->
            %% calculate effective QoS as the lower value of SUBSCRIBE frame QoS
            %% and retained message QoS. The spec isn't super clear on this, we
            %% do what Mosquitto does, per user feedback.
            Qos = erlang:min(SubscribeQos, Msg#mqtt_msg.qos),
            Id = case Qos of
                ?QOS_0 -> undefined;
                ?QOS_1 -> MsgId
            end,
            SendFun(#mqtt_frame{fixed = #mqtt_frame_fixed{
                type = ?PUBLISH,
                qos  = Qos,
                dup  = false,
                retain = Msg#mqtt_msg.retain
            }, variable = #mqtt_frame_publish{
                message_id = Id,
                topic_name = Topic1
            },
            payload = Msg#mqtt_msg.payload}, PState),
            case Qos of
            ?QOS_0 -> false;
            ?QOS_1 -> {true, 1}
        end
    end.

-spec amqp_callback(#'basic.ack'{} | {#'basic.deliver'{}, #amqp_msg{}, {pid(), pid(), pid()}}, #proc_state{}) -> {'ok', #proc_state{}} | {'error', term(), term()}.
amqp_callback({#'basic.deliver'{ consumer_tag = ConsumerTag,
                                 delivery_tag = DeliveryTag,
                                 routing_key  = RoutingKey },
               #amqp_msg{ props = #'P_basic'{ headers = Headers },
                          payload = Payload },
               DeliveryCtx} = Delivery,
              #proc_state{ channels      = {Channel, _},
                           awaiting_ack  = Awaiting,
                           message_id    = MsgId,
                           send_fun      = SendFun,
                           amqp2mqtt_fun = Amqp2MqttFun } = PState) ->
    notify_received(DeliveryCtx),
    case {delivery_dup(Delivery), delivery_qos(ConsumerTag, Headers, PState)} of
        {true, {?QOS_0, ?QOS_1}} ->
            amqp_channel:cast(
              Channel, #'basic.ack'{ delivery_tag = DeliveryTag }),
            {ok, PState};
        {true, {?QOS_0, ?QOS_0}} ->
            {ok, PState};
        {Dup, {DeliveryQos, _SubQos} = Qos}     ->
            TopicName = Amqp2MqttFun(RoutingKey),
            SendFun(
              #mqtt_frame{ fixed = #mqtt_frame_fixed{
                                     type = ?PUBLISH,
                                     qos  = DeliveryQos,
                                     dup  = Dup },
                           variable = #mqtt_frame_publish{
                                        message_id =
                                          case DeliveryQos of
                                              ?QOS_0 -> undefined;
                                              ?QOS_1 -> MsgId
                                          end,
                                        topic_name = TopicName },
                           payload = Payload}, PState),
              case Qos of
                  {?QOS_0, ?QOS_0} ->
                      {ok, PState};
                  {?QOS_1, ?QOS_1} ->
                      Awaiting1 = gb_trees:insert(MsgId, DeliveryTag, Awaiting),
                      PState1 = PState#proc_state{ awaiting_ack = Awaiting1 },
                      PState2 = next_msg_id(PState1),
                      {ok, PState2};
                  {?QOS_0, ?QOS_1} ->
                      amqp_channel:cast(
                        Channel, #'basic.ack'{ delivery_tag = DeliveryTag }),
                      {ok, PState}
              end
    end;

amqp_callback(#'basic.ack'{ multiple = true, delivery_tag = Tag } = Ack,
              PState = #proc_state{ unacked_pubs = UnackedPubs,
                                    send_fun     = SendFun }) ->
    case gb_trees:size(UnackedPubs) > 0 andalso
         gb_trees:take_smallest(UnackedPubs) of
        {TagSmall, MsgId, UnackedPubs1} when TagSmall =< Tag ->
            SendFun(
              #mqtt_frame{ fixed    = #mqtt_frame_fixed{ type = ?PUBACK },
                           variable = #mqtt_frame_publish{ message_id = MsgId }},
              PState),
            amqp_callback(Ack, PState #proc_state{ unacked_pubs = UnackedPubs1 });
        _ ->
            {ok, PState}
    end;

amqp_callback(#'basic.ack'{ multiple = false, delivery_tag = Tag },
              PState = #proc_state{ unacked_pubs = UnackedPubs,
                                    send_fun     = SendFun }) ->
    SendFun(
      #mqtt_frame{ fixed    = #mqtt_frame_fixed{ type = ?PUBACK },
                   variable = #mqtt_frame_publish{
                                message_id = gb_trees:get(
                                               Tag, UnackedPubs) }}, PState),
    {ok, PState #proc_state{ unacked_pubs = gb_trees:delete(Tag, UnackedPubs) }}.

delivery_dup({#'basic.deliver'{ redelivered = Redelivered },
              #amqp_msg{ props = #'P_basic'{ headers = Headers }},
              _DeliveryCtx}) ->
    case rabbit_mqtt_util:table_lookup(Headers, <<"x-mqtt-dup">>) of
        undefined   -> Redelivered;
        {bool, Dup} -> Redelivered orelse Dup
    end.

ensure_valid_mqtt_message_id(Id) when Id >= 16#ffff ->
    1;
ensure_valid_mqtt_message_id(Id) ->
    Id.

safe_max_id(Id0, Id1) ->
    ensure_valid_mqtt_message_id(erlang:max(Id0, Id1)).

next_msg_id(PState = #proc_state{ message_id = MsgId0 }) ->
    MsgId1 = ensure_valid_mqtt_message_id(MsgId0 + 1),
    PState#proc_state{ message_id = MsgId1 }.

%% decide at which qos level to deliver based on subscription
%% and the message publish qos level. non-MQTT publishes are
%% assumed to be qos 1, regardless of delivery_mode.
delivery_qos(Tag, _Headers,  #proc_state{ consumer_tags = {Tag, _} }) ->
    {?QOS_0, ?QOS_0};
delivery_qos(Tag, Headers,   #proc_state{ consumer_tags = {_, Tag} }) ->
    case rabbit_mqtt_util:table_lookup(Headers, <<"x-mqtt-publish-qos">>) of
        {byte, Qos} -> {lists:min([Qos, ?QOS_1]), ?QOS_1};
        undefined   -> {?QOS_1, ?QOS_1}
    end.

session_present(VHost, ClientId) ->
    {_, QueueQ1} = rabbit_mqtt_util:subcription_queue_name(ClientId),
    QueueName = rabbit_misc:r(VHost, queue, QueueQ1),
    rabbit_amqqueue:exists(QueueName).

make_will_msg(#mqtt_frame_connect{ will_flag   = false }) ->
    undefined;
make_will_msg(#mqtt_frame_connect{ will_retain = Retain,
                                   will_qos    = Qos,
                                   will_topic  = Topic,
                                   will_msg    = Msg }) ->
    #mqtt_msg{ retain  = Retain,
               qos     = Qos,
               topic   = Topic,
               dup     = false,
               payload = Msg }.

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
    connack_dup_auth;
process_login(UserBin, PassBin, ClientId0,
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
    ClientId = rabbit_data_coercion:to_binary(ClientId0),
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
            notify_auth_result(Username, user_authentication_success, []),
            {ok, maps:put(user, User, In), PState};
        {refused, Username, Msg, Args} ->
            rabbit_log_connection:error(
              "Error on MQTT connection ~p~n"
              "access refused for user '~s' in vhost '~s' "
              ++ Msg,
              [self(), Username, VHost] ++ Args),
            notify_auth_result(Username,
                               user_authentication_failure,
                               [{error, rabbit_misc:format(Msg, Args)}]),
            {error, ?CONNACK_BAD_CREDENTIALS}
    end.

notify_auth_result(Username, AuthResult, ExtraProps) ->
    EventProps = [{connection_type, mqtt},
                  {name, case Username of none -> ''; _ -> Username end}] ++
                 ExtraProps,
    rabbit_event:notify(AuthResult, [P || {_, V} = P <- EventProps, V =/= '']).

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
                    #proc_state{socket = Sock,
                                peer_addr = PeerAddr} = PState) ->
    case rabbit_access_control:check_user_loopback(UsernameBin, PeerAddr) of
        ok ->
            {ok, {PeerHost, PeerPort, Host, Port}} = rabbit_net:socket_ends(Sock, inbound),
            Infos = [{node, node()},
                     {host, Host},
                     {port, Port},
                     {peer_host, PeerHost},
                     {peer_port, PeerPort},
                     {user, UsernameBin},
                     {vhost, VHost}],
            rabbit_core_metrics:connection_created(self(), Infos),
            rabbit_event:notify(connection_created, Infos),
            AuthState = #auth_state{user = User,
                                    username = UsernameBin,
                                    vhost = VHost,
                                    authz_ctx = AuthzCtx},
            {ok, PState#proc_state{auth_state = AuthState}};
        not_allowed ->
            rabbit_log_connection:warning(
              "MQTT login failed for user ~s: "
              "this user's access is restricted to localhost",
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

supported_subs_qos(?QOS_0) -> ?QOS_0;
supported_subs_qos(?QOS_1) -> ?QOS_1;
supported_subs_qos(?QOS_2) -> ?QOS_1.

delivery_mode(?QOS_0) -> 1;
delivery_mode(?QOS_1) -> 2;
delivery_mode(?QOS_2) -> 2.

maybe_quorum(Qos1Args, CleanSession, Queue) ->
    case {rabbit_mqtt_util:env(durable_queue_type), CleanSession} of
      %% it is possible to Quorum queues only if Clean Session == False
      %% else always use Classic queues
      %% Clean Session == True sets auto-delete to True and quorum queues
      %% does not support auto-delete flag
       {quorum, false} -> lists:append(Qos1Args,
          [{<<"x-queue-type">>, longstr, <<"quorum">>}]);

      {quorum, true} ->
          rabbit_log:debug("Can't use quorum queue for ~ts. " ++
          "The clean session is true. Classic queue will be used", [Queue]),
          Qos1Args;
      _ -> Qos1Args
    end.

%% different qos subscriptions are received in different queues
%% with appropriate durability and timeout arguments
%% this will lead to duplicate messages for overlapping subscriptions
%% with different qos values - todo: prevent duplicates
%ensure_queue(Qos, #proc_state{ channels      = {Channel, _},
%                               client_id     = ClientId,
%                               clean_sess    = CleanSess,
%                          consumer_tags = {TagQ0, TagQ1} = Tags} = PState) ->
%    {QueueQ0, QueueQ1} = rabbit_mqtt_util:subcription_queue_name(ClientId),
%    Qos1Args = case {rabbit_mqtt_util:env(subscription_ttl), CleanSess} of
%                   {undefined, _} ->
%                       [];
%                   {Ms, false} when is_integer(Ms) ->
%                       [{<<"x-expires">>, long, Ms}];
%                   _ ->
%                       []
%               end,
%    QueueSetup =
%        case {TagQ0, TagQ1, Qos} of
%            {undefined, _, ?QOS_0} ->
%                {QueueQ0,
%                 #'queue.declare'{ queue       = QueueQ0,
%                                   durable     = false,
%                                   auto_delete = true },
%                 #'basic.consume'{ queue  = QueueQ0,
%                                   no_ack = true }};
%            {_, undefined, ?QOS_1} ->
%                {QueueQ1,
%                 #'queue.declare'{ queue       = QueueQ1,
%                                   durable     = true,
%                                   %% Clean session means a transient connection,
%                                   %% translating into auto-delete.
%                                   %%
%                                   %% see rabbitmq/rabbitmq-mqtt#37
%                                   auto_delete = CleanSess,
%                                   arguments   = maybe_quorum(Qos1Args, CleanSess, QueueQ1)},
%                 #'basic.consume'{ queue  = QueueQ1,
%                                   no_ack = false }};
%            {_, _, ?QOS_0} ->
%                {exists, QueueQ0};
%            {_, _, ?QOS_1} ->
%                {exists, QueueQ1}
%          end,
%    case QueueSetup of
%        {Queue, Declare, Consume} ->
%            #'queue.declare_ok'{} = amqp_channel:call(Channel, Declare),
%            #'basic.consume_ok'{ consumer_tag = Tag } =
%                amqp_channel:call(Channel, Consume),
%            {Queue, PState #proc_state{ consumer_tags = setelement(Qos+1, Tags, Tag) }};
%        {exists, Q} ->
%            {Q, PState}
%    end.

ensure_queue(?QOS_0, %% spike handles only QoS0
             #proc_state{
                client_id = ClientId,
                clean_sess = _CleanSess,
                queue_states = QueueStates0,
                auth_state = #auth_state{
                                vhost = VHost,
                                user = User = #user{username = Username},
                                authz_ctx = AuthzCtx},
                info = #info{prefetch = Prefetch}
               } = PState0) ->
    {QueueBin, _QueueQos1Bin} = rabbit_mqtt_util:subcription_queue_name(ClientId),
    QueueName = rabbit_misc:r(VHost, queue, QueueBin),
    case rabbit_amqqueue:exists(QueueName) of
        true ->
            {QueueName, PState0};
        false ->
            check_resource_access(User, QueueName, read, AuthzCtx),
            check_resource_access(User, QueueName, configure, AuthzCtx),
            rabbit_core_metrics:queue_declared(QueueName),
            Durable = false,
            AutoDelete = true,
            case rabbit_amqqueue:with(
                   QueueName,
                   fun (Q) -> ok = rabbit_amqqueue:assert_equivalence(
                                     Q, Durable, AutoDelete, [], none),
                              rabbit_amqqueue:stat(Q)
                   end) of
                {error, not_found} ->
                    case rabbit_vhost_limit:is_over_queue_limit(VHost) of
                        false ->
                            case rabbit_amqqueue:declare(QueueName, Durable, AutoDelete,
                                                         [], none, Username) of
                                {new, Q} when ?is_amqqueue(Q) ->
                                    rabbit_core_metrics:queue_created(QueueName),
                                    Spec = #{no_ack => true,
                                             channel_pid => self(),
                                             limiter_pid => none,
                                             limiter_active => false,
                                             prefetch_count => Prefetch,
                                             consumer_tag => ?CONSUMER_TAG,
                                             exclusive_consume => false,
                                             args => [],
                                             ok_msg => undefined,
                                             acting_user =>  Username},
                                    case rabbit_queue_type:consume(Q, Spec, QueueStates0) of
                                        {ok, QueueStates, _Actions = []} ->
                                            % rabbit_global_counters:consumer_created(mqtt),
                                            PState = PState0#proc_state{queue_states = QueueStates},
                                            {QueueName, PState};
                                        Other ->
                                            exit(
                                              lists:flatten(
                                                io_lib:format("Failed to consume from ~s: ~p",
                                                              [rabbit_misc:rs(QueueName), Other])))
                                    end;
                                Other ->
                                    exit(lists:flatten(
                                           io_lib:format("Failed to declare ~s: ~p",
                                                         [rabbit_misc:rs(QueueName), Other])))
                            end;
                        {true, Limit} ->
                            exit(
                              lists:flatten(
                                io_lib:format("cannot declare ~s because "
                                              "queue limit ~p in vhost '~s' is reached",
                                              [rabbit_misc:rs(QueueName), Limit, VHost])))
                    end;
                Other ->
                    exit(
                      lists:flatten(
                        io_lib:format("Expected ~s to not exist, got: ~p",
                                      [rabbit_misc:rs(QueueName), Other])))
            end
    end.

bind(QueueName,
     TopicName,
     #proc_state{exchange = ExchangeName,
                 auth_state = #auth_state{
                                 user = User = #user{username = Username},
                                 authz_ctx = AuthzCtx},
                 mqtt2amqp_fun = Mqtt2AmqpFun} = PState) ->
    ok = rabbit_access_control:check_resource_access(User, QueueName, write, AuthzCtx),
    ok = rabbit_access_control:check_resource_access(User, ExchangeName, read, AuthzCtx),
    ok = check_topic_access(TopicName, read, PState),
    RoutingKey = Mqtt2AmqpFun(TopicName),
    Binding = #binding{source = ExchangeName,
                       destination = QueueName,
                       key = RoutingKey},
    ok = rabbit_binding:add(Binding, Username).

send_will(PState = #proc_state{will_msg = undefined}) ->
    PState;

send_will(PState = #proc_state{will_msg = WillMsg = #mqtt_msg{retain = Retain,
                                                              topic = Topic},
                               retainer_pid = RPid,
                               channels = {ChQos0, ChQos1},
                               amqp2mqtt_fun = Amqp2MqttFun}) ->
    case check_topic_access(Topic, write, PState) of
        ok ->
            amqp_pub(WillMsg, PState),
            case Retain of
                false -> ok;
                true  ->
                    hand_off_to_retainer(RPid, Amqp2MqttFun, Topic, WillMsg)
            end;
        Error  ->
            rabbit_log:warning(
                "Could not send last will: ~tp",
                [Error])
    end,
    case ChQos1 of
        undefined -> ok;
        _         -> amqp_channel:close(ChQos1)
    end,
    case ChQos0 of
        undefined -> ok;
        _         -> amqp_channel:close(ChQos0)
    end,
    PState #proc_state{ channels = {undefined, undefined} }.

amqp_pub(undefined, PState) ->
    PState;
amqp_pub(#mqtt_msg{qos        = Qos,
                   topic      = Topic,
                   dup        = Dup,
                   message_id = _MessageId, %% spike handles only QoS0
                   payload    = Payload},
         PState = #proc_state{exchange       = ExchangeName,
                              % unacked_pubs   = UnackedPubs,
                              % awaiting_seqno = SeqNo,
                              mqtt2amqp_fun  = Mqtt2AmqpFun}) ->
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
                      id = <<>>,
                      is_persistent = Confirm
                     },
    Delivery = #delivery{
                  mandatory = false,
                  confirm = Confirm,
                  sender = self(),
                  message = BasicMessage,
                  msg_seq_no = undefined, %% spike handles only QoS0
                  flow = noflow %%TODO enable flow control
                 },

    Exchange = rabbit_exchange:lookup_or_die(ExchangeName),
    QNames = rabbit_exchange:route(Exchange, Delivery),
    deliver_to_queues(Delivery, QNames, PState).

deliver_to_queues(#delivery{confirm = false},
                  _RoutedToQueueNames = [],
                  PState) ->
    % rabbit_global_counters:messages_unroutable_dropped(mqtt, 1),
    PState;
deliver_to_queues(Delivery = #delivery{message    = _Message = #basic_message{exchange_name = _XName},
                                       confirm    = _Confirm,
                                       msg_seq_no = _MsgSeqNo},
                  RoutedToQueueNames,
                  PState = #proc_state{queue_states = QueueStates0}) ->
    Qs0 = rabbit_amqqueue:lookup(RoutedToQueueNames),
    Qs = rabbit_amqqueue:prepend_extra_bcc(Qs0),
    % QueueNames = lists:map(fun amqqueue:get_name/1, Qs),

    {ok, QueueStates, _Actions} =  rabbit_queue_type:deliver(Qs, Delivery, QueueStates0),
    % rabbit_global_counters:messages_routed(mqtt, length(Qs)),

    %% NB: the order here is important since basic.returns must be
    %% sent before confirms.
    %% TODO: AMQP 0.9.1 mandatory flag corresponds to MQTT 5 PUBACK reason code "No matching subscribers"
    % ok = process_routing_mandatory(Mandatory, Qs, Message, State0),
    %% spike handles only QoS0
    % State1 = process_routing_confirm(Confirm, QueueNames,
    %                                  MsgSeqNo, XName, State0),

    %% Actions must be processed after registering confirms as actions may
    %% contain rejections of publishes
    %% TODO handle Actions: For example if the messages is rejected, MQTT 5 allows to send a NACK
    %% back to the client (via PUBACK Reason Code).
    % State = handle_queue_actions(Actions, State1#ch{queue_states = QueueStates}),
    PState#proc_state{queue_states = QueueStates}.

human_readable_mqtt_version(3) ->
    "3.1.0";
human_readable_mqtt_version(4) ->
    "3.1.1";
human_readable_mqtt_version(_) ->
    "N/A".

serialise_and_send_to_client(Frame, #proc_state{ socket = Sock }) ->
    %%TODO Test sending large frames at high speed: Will we need garbage collection as done
    %% in rabbit_writer:maybe_gc_large_msg()?
    try rabbit_net:port_command(Sock, rabbit_mqtt_frame:serialise(Frame)) of
      Res ->
        Res
    catch _:Error ->
      rabbit_log_connection:error("MQTT: a socket write failed, the socket might already be closed"),
      rabbit_log_connection:debug("Failed to write to socket ~tp, error: ~tp, frame: ~tp",
                                  [Sock, Error, Frame])
    end.

close_connection(PState = #proc_state{ connection = undefined }) ->
    PState;
close_connection(PState = #proc_state{ connection = Connection,
                                       client_id  = ClientId }) ->
    % todo: maybe clean session
    case ClientId of
        undefined -> ok;
        _         ->
            case rabbit_mqtt_collector:unregister(ClientId, self()) of
                ok           -> ok;
                %% ignore as we are shutting down
                {timeout, _} -> ok
            end
    end,
    %% ignore noproc or other exceptions, we are shutting down
    catch amqp_connection:close(Connection),
    PState #proc_state{ channels   = {undefined, undefined},
                        connection = undefined }.

handle_pre_hibernate() ->
    erase(topic_permission_cache),
    erase(permission_cache),
    ok.

handle_ra_event({applied, [{Corr, ok}]},
                PState = #proc_state{register_state = {pending, Corr}}) ->
    %% success case - command was applied transition into registered state
    PState#proc_state{register_state = registered};
handle_ra_event({not_leader, Leader, Corr},
                PState = #proc_state{register_state = {pending, Corr},
                                     client_id = ClientId}) ->
    %% retry command against actual leader
    {ok, NewCorr} = rabbit_mqtt_collector:register(Leader, ClientId, self()),
    PState#proc_state{register_state = {pending, NewCorr}};
handle_ra_event(register_timeout,
                PState = #proc_state{register_state = {pending, _Corr},
                                     client_id = ClientId}) ->
    {ok, NewCorr} = rabbit_mqtt_collector:register(ClientId, self()),
    PState#proc_state{register_state = {pending, NewCorr}};
handle_ra_event(register_timeout, PState) ->
    PState;
handle_ra_event(Evt, PState) ->
    %% log these?
    rabbit_log:debug("unhandled ra_event: ~w ", [Evt]),
    PState.

handle_down({'DOWN', _MRef, process, QPid, Reason},
            PState0 = #proc_state{queue_states = QStates0}) ->
    %% spike handles only QoS0
    case rabbit_queue_type:handle_down(QPid, Reason, QStates0) of
        {ok, QStates1, Actions} ->
            PState = PState0#proc_state{queue_states = QStates1},
            handle_queue_actions(Actions, PState);
        {eol, QStates1, QRef} ->
            QStates = rabbit_queue_type:remove(QRef, QStates1),
            PState0#proc_state{queue_states = QStates}
    end.

handle_queue_event({queue_event, QRef, Evt},
                   PState0 = #proc_state{queue_states = QueueStates0}) ->
    case rabbit_queue_type:handle_event(QRef, Evt, QueueStates0) of
        {ok, QueueStates, Actions} ->
            PState1 = PState0#proc_state{queue_states = QueueStates},
            PState = handle_queue_actions(Actions, PState1),
            {ok, PState};
        eol ->
            {error, queue_eol, PState0};
        {protocol_error, _Type, _Reason, _ReasonArgs} = Error ->
            {error, Error, PState0}
    end.

handle_queue_actions(Actions, #proc_state{} = PState0) ->
    lists:foldl(
      fun ({deliver, ?CONSUMER_TAG, _AckRequired = false, Msgs}, S) ->
              handle_deliver(Msgs, S)
      end, PState0, Actions).

handle_deliver(Msgs, PState)
  when is_list(Msgs) ->
    lists:foldl(fun(Msg, S) ->
                        handle_deliver0(Msg, S)
                end, PState, Msgs).

handle_deliver0({_QName, _QPid, _MsgId, Redelivered,
                 #basic_message{routing_keys  = [RoutingKey | _CcRoutes],
                                content = #content{
                                             properties = #'P_basic'{headers = Headers},
                                             payload_fragments_rev = FragmentsRev}}},
                PState = #proc_state{send_fun = SendFun,
                                     amqp2mqtt_fun = Amqp2MqttFun}) ->
    Dup = case rabbit_mqtt_util:table_lookup(Headers, <<"x-mqtt-dup">>) of
              undefined   -> Redelivered;
              {bool, Dup0} -> Redelivered orelse Dup0
          end,
    %%TODO support iolists when sending to client
    Payload = list_to_binary(lists:reverse(FragmentsRev)),
    Frame = #mqtt_frame{fixed = #mqtt_frame_fixed{
                                   type = ?PUBLISH,
                                   qos  = ?QOS_0, %% spike handles only QoS0
                                   dup  = Dup},
                        variable = #mqtt_frame_publish{
                                      message_id = undefined, %% spike handles only QoS0
                                      topic_name = Amqp2MqttFun(RoutingKey)},
                        payload = Payload},
    SendFun(Frame, PState),
    PState.

check_publish(TopicName, Fn, PState) ->
  %%TODO check additionally write access to exchange as done in channel?
  case check_topic_access(TopicName, write, PState) of
    ok -> Fn();
    _ -> {error, unauthorized, PState}
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
                    ok;
                R ->
                    R
            catch
                _:{amqp_error, access_refused, Msg, _} ->
                    rabbit_log:error("operation resulted in an error (access_refused): ~tp", [Msg]),
                    {error, access_refused};
                _:Error ->
                    rabbit_log:error("~tp", [Error]),
                    {error, access_refused}
            end
    end.

%% TODO copied from channel, remove duplication
check_resource_access(User, Resource, Perm, Context) ->
    V = {Resource, Context, Perm},
    Cache = case get(permission_cache) of
                undefined -> [];
                Other     -> Other
            end,
    case lists:member(V, Cache) of
        true  -> ok;
        false -> ok = rabbit_access_control:check_resource_access(
                        User, Resource, Perm, Context),
                 CacheTail = lists:sublist(Cache, ?MAX_PERMISSION_CACHE_SIZE-1),
                 put(permission_cache, [V | CacheTail])
    end.

info(consumer_tags, #proc_state{consumer_tags = Val}) -> Val;
info(unacked_pubs, #proc_state{unacked_pubs = Val}) -> Val;
info(awaiting_ack, #proc_state{awaiting_ack = Val}) -> Val;
info(awaiting_seqno, #proc_state{awaiting_seqno = Val}) -> Val;
info(message_id, #proc_state{message_id = Val}) -> Val;
info(client_id, #proc_state{client_id = Val}) ->
    rabbit_data_coercion:to_binary(Val);
info(clean_sess, #proc_state{clean_sess = Val}) -> Val;
info(will_msg, #proc_state{will_msg = Val}) -> Val;
info(channels, #proc_state{channels = Val}) -> Val;
info(exchange, #proc_state{exchange = #resource{name = Val}}) -> Val;
info(ssl_login_name, #proc_state{ssl_login_name = Val}) -> Val;
info(retainer_pid, #proc_state{retainer_pid = Val}) -> Val;
info(user, #proc_state{auth_state = #auth_state{username = Val}}) -> Val;
info(vhost, #proc_state{auth_state = #auth_state{vhost = Val}}) -> Val;
info(host, #proc_state{info = #info{host = Val}}) -> Val;
info(port, #proc_state{info = #info{port = Val}}) -> Val;
info(peer_host, #proc_state{info = #info{peer_host = Val}}) -> Val;
info(peer_port, #proc_state{info = #info{peer_port = Val}}) -> Val;
info(protocol, #proc_state{info = #info{protocol = Val}}) ->
    case Val of
        {Proto, Version} -> {Proto, rabbit_data_coercion:to_binary(Version)};
        Other -> Other
    end;
% info(channels, PState) -> additional_info(channels, PState);
% info(channel_max, PState) -> additional_info(channel_max, PState);
% info(frame_max, PState) -> additional_info(frame_max, PState);
% info(client_properties, PState) -> additional_info(client_properties, PState);
% info(ssl, PState) -> additional_info(ssl, PState);
% info(ssl_protocol, PState) -> additional_info(ssl_protocol, PState);
% info(ssl_key_exchange, PState) -> additional_info(ssl_key_exchange, PState);
% info(ssl_cipher, PState) -> additional_info(ssl_cipher, PState);
% info(ssl_hash, PState) -> additional_info(ssl_hash, PState);
info(Other, _) -> throw({bad_argument, Other}).


% additional_info(Key,
%                 #proc_state{adapter_info =
%                             #amqp_adapter_info{additional_info = AddInfo}}) ->
%     proplists:get_value(Key, AddInfo).

notify_received(undefined) ->
    %% no notification for quorum queues and streams
    ok;
notify_received(DeliveryCtx) ->
    %% notification for flow control
    amqp_channel:notify_received(DeliveryCtx).

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
