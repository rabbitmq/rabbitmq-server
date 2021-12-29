%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mqtt_processor).

-export([info/2, initial_state/2, initial_state/5,
         process_frame/2, amqp_pub/2, amqp_callback/2, send_will/1,
         close_connection/1, handle_pre_hibernate/0,
         handle_ra_event/2]).

%% for testing purposes
-export([get_vhost_username/1, get_vhost/3, get_vhost_from_user_mapping/2,
         add_client_id_to_adapter_info/2]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_mqtt_frame.hrl").
-include("rabbit_mqtt.hrl").

-define(APP, rabbitmq_mqtt).
-define(FRAME_TYPE(Frame, Type),
        Frame = #mqtt_frame{ fixed = #mqtt_frame_fixed{ type = Type }}).
-define(MAX_TOPIC_PERMISSION_CACHE_SIZE, 12).

initial_state(Socket, SSLLoginName) ->
    RealSocket = rabbit_net:unwrap_socket(Socket),
    {ok, {PeerAddr, _PeerPort}} = rabbit_net:peername(RealSocket),
    initial_state(RealSocket, SSLLoginName,
        adapter_info(Socket, 'MQTT'),
        fun serialise_and_send_to_client/2, PeerAddr).

initial_state(Socket, SSLLoginName,
              AdapterInfo0 = #amqp_adapter_info{additional_info = Extra},
              SendFun, PeerAddr) ->
    {ok, {mqtt2amqp_fun, M2A}, {amqp2mqtt_fun, A2M}} =
        rabbit_mqtt_util:get_topic_translation_funs(),
    %% MQTT connections use exactly one channel. The frame max is not
    %% applicable and there is no way to know what client is used.
    AdapterInfo = AdapterInfo0#amqp_adapter_info{additional_info = [
        {channels, 1},
        {channel_max, 1},
        {frame_max, 0},
        {client_properties,
         [{<<"product">>, longstr, <<"MQTT client">>}]} | Extra]},
    #proc_state{ unacked_pubs   = gb_trees:empty(),
                 awaiting_ack   = gb_trees:empty(),
                 message_id     = 1,
                 subscriptions  = #{},
                 consumer_tags  = {undefined, undefined},
                 channels       = {undefined, undefined},
                 exchange       = rabbit_mqtt_util:env(exchange),
                 socket         = Socket,
                 adapter_info   = AdapterInfo,
                 ssl_login_name = SSLLoginName,
                 send_fun       = SendFun,
                 peer_addr      = PeerAddr,
                 mqtt2amqp_fun  = M2A,
                 amqp2mqtt_fun  = A2M}.

process_frame(#mqtt_frame{ fixed = #mqtt_frame_fixed{ type = Type }},
              PState = #proc_state{ connection = undefined } )
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

add_client_id_to_adapter_info(ClientId, #amqp_adapter_info{additional_info = AdditionalInfo0} = AdapterInfo) ->
    AdditionalInfo1 = [{variable_map, #{<<"client_id">> => ClientId}}
        | AdditionalInfo0],
    ClientProperties = proplists:get_value(client_properties, AdditionalInfo1, [])
        ++ [{client_id, longstr, ClientId}],
    AdditionalInfo2 = case lists:keysearch(client_properties, 1, AdditionalInfo1) of
                          {value, _} ->
                              lists:keyreplace(client_properties,
                                               1,
                                               AdditionalInfo1,
                                               {client_properties, ClientProperties});
                          false ->
                              [{client_properties, ClientProperties} | AdditionalInfo1]
                      end,
    AdapterInfo#amqp_adapter_info{additional_info = AdditionalInfo2}.

process_request(?CONNECT,
                #mqtt_frame{ variable = #mqtt_frame_connect{
                                           username   = Username,
                                           password   = Password,
                                           proto_ver  = ProtoVersion,
                                           clean_sess = CleanSess,
                                           client_id  = ClientId0,
                                           keep_alive = Keepalive} = Var},
                PState0 = #proc_state{ ssl_login_name = SSLLoginName,
                                       send_fun       = SendFun,
                                       adapter_info   = AdapterInfo,
                                       peer_addr      = Addr}) ->
    ClientId = case ClientId0 of
                   []    -> rabbit_mqtt_util:gen_client_id();
                   [_|_] -> ClientId0
               end,
     rabbit_log_connection:debug("Received a CONNECT, client ID: ~p (expanded to ~p), username: ~p, "
                                 "clean session: ~p, protocol version: ~p, keepalive: ~p",
                                 [ClientId0, ClientId, Username, CleanSess, ProtoVersion, Keepalive]),
    AdapterInfo1 = add_client_id_to_adapter_info(rabbit_data_coercion:to_binary(ClientId), AdapterInfo),
    PState1 = PState0#proc_state{adapter_info = AdapterInfo1},
    Ip = list_to_binary(inet:ntoa(Addr)),
    {Return, PState5} =
        case {lists:member(ProtoVersion, proplists:get_keys(?PROTOCOL_NAMES)),
              ClientId0 =:= [] andalso CleanSess =:= false} of
            {false, _} ->
                {?CONNACK_PROTO_VER, PState1};
            {_, true} ->
                {?CONNACK_INVALID_ID, PState1};
            _ ->
                case creds(Username, Password, SSLLoginName) of
                    nocreds ->
                        rabbit_core_metrics:auth_attempt_failed(Ip, <<>>, mqtt),
                        rabbit_log_connection:error("MQTT login failed: no credentials provided"),
                        {?CONNACK_CREDENTIALS, PState1};
                    {invalid_creds, {undefined, Pass}} when is_list(Pass) ->
                        rabbit_core_metrics:auth_attempt_failed(Ip, <<>>, mqtt),
                        rabbit_log_connection:error("MQTT login failed: no username is provided"),
                        {?CONNACK_CREDENTIALS, PState1};
                    {invalid_creds, {User, undefined}} when is_list(User) ->
                        rabbit_core_metrics:auth_attempt_failed(Ip, User, mqtt),
                        rabbit_log_connection:error("MQTT login failed for user '~p': no password provided", [User]),
                        {?CONNACK_CREDENTIALS, PState1};
                    {UserBin, PassBin} ->
                        case process_login(UserBin, PassBin, ProtoVersion, PState1) of
                            connack_dup_auth ->
                                maybe_clean_sess(PState1);
                            {?CONNACK_ACCEPT, Conn, VHost, AState} ->
                                case rabbit_mqtt_collector:register(ClientId, self()) of
                                    {ok, Corr} ->
                                        RetainerPid = rabbit_mqtt_retainer_sup:child_for_vhost(VHost),
                                        link(Conn),
                                    {ok, Ch} = amqp_connection:open_channel(Conn),
                                        link(Ch),
                                        amqp_channel:enable_delivery_flow_control(Ch),
                                        Prefetch = rabbit_mqtt_util:env(prefetch),
                                        #'basic.qos_ok'{} = amqp_channel:call(Ch,
                                            #'basic.qos'{prefetch_count = Prefetch}),
                                    rabbit_mqtt_reader:start_keepalive(self(), Keepalive),
                                    PState3 = PState1#proc_state{
                                                will_msg   = make_will_msg(Var),
                                                clean_sess = CleanSess,
                                                channels   = {Ch, undefined},
                                                connection = Conn,
                                                client_id  = ClientId,
                                                retainer_pid = RetainerPid,
                                                auth_state = AState,
                                                register_state = {pending, Corr}},
                                    maybe_clean_sess(PState3);
                                  %% e.g. this node was removed from the MQTT cluster members
                                  {error, _} = Err ->
                                    rabbit_log_connection:error("MQTT cannot accept a connection: "
                                                                "client ID tracker is unavailable: ~p", [Err]),
                                    %% ignore all exceptions, we are shutting down
                                    catch amqp_connection:close(Conn),
                                    {?CONNACK_SERVER, PState1};
                                  {timeout, _} ->
                                    rabbit_log_connection:error("MQTT cannot accept a connection: "
                                                                "client ID registration timed out"),
                                    %% ignore all exceptions, we are shutting down
                                    catch amqp_connection:close(Conn),
                                    {?CONNACK_SERVER, PState1}
                                end;
                            ConnAck -> {ConnAck, PState1}
                        end
                end
        end,
    {ReturnCode, SessionPresent} = case Return of
                                       {?CONNACK_ACCEPT, Bool} -> {?CONNACK_ACCEPT, Bool};
                                       Other                   -> {Other, false}
                                   end,
    SendFun(#mqtt_frame{fixed    = #mqtt_frame_fixed{type = ?CONNACK},
                        variable = #mqtt_frame_connack{
                                      session_present = SessionPresent,
                                      return_code = ReturnCode}},
            PState5),
    case ReturnCode of
      ?CONNACK_ACCEPT      -> {ok, PState5};
      ?CONNACK_CREDENTIALS -> {error, unauthenticated, PState5};
      ?CONNACK_AUTH        -> {error, unauthorized, PState5};
      ?CONNACK_SERVER      -> {error, unavailable, PState5};
      ?CONNACK_INVALID_ID  -> {error, invalid_client_id, PState5};
      ?CONNACK_PROTO_VER   -> {error, unsupported_protocol_version, PState5}
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
                #proc_state{channels = {Channel, _},
                            exchange = Exchange,
                            retainer_pid = RPid,
                            send_fun = SendFun,
                            message_id  = StateMsgId,
                            mqtt2amqp_fun = Mqtt2AmqpFun} = PState0) ->
    rabbit_log_connection:debug("Received a SUBSCRIBE for topic(s) ~p", [Topics]),

    {QosResponse, PState1} =
        lists:foldl(fun (#mqtt_topic{name = TopicName,
                                     qos  = Qos}, {QosList, PState}) ->
                       SupportedQos = supported_subs_qos(Qos),
                       {Queue, #proc_state{subscriptions = Subs} = PState1} =
                           ensure_queue(SupportedQos, PState),
                       RoutingKey = Mqtt2AmqpFun(TopicName),
                       Binding = #'queue.bind'{
                                   queue       = Queue,
                                   exchange    = Exchange,
                                   routing_key = RoutingKey},
                       #'queue.bind_ok'{} = amqp_channel:call(Channel, Binding),
                       SupportedQosList = case maps:find(TopicName, Subs) of
                           {ok, L} -> [SupportedQos|L];
                           error   -> [SupportedQos]
                       end,
                       {[SupportedQos | QosList],
                        PState1 #proc_state{
                            subscriptions =
                                maps:put(TopicName, SupportedQosList, Subs)}}
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
    rabbit_log_connection:debug("Received an UNSUBSCRIBE for topic(s) ~p", [Topics]),
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
    amqp_channel:notify_received(DeliveryCtx),
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

maybe_clean_sess(PState = #proc_state { clean_sess = false,
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
            {{?CONNACK_ACCEPT, SessionPresent}, PState};
        true ->
            try ensure_queue(?QOS_1, PState) of
                {_Queue, PState1} -> {{?CONNACK_ACCEPT, SessionPresent}, PState1}
            catch
                exit:({{shutdown, {server_initiated_close, 403, _}}, _}) ->
                    %% Connection is not yet propagated to #proc_state{}, let's close it here
                    catch amqp_connection:close(Conn),
                    rabbit_log_connection:error("MQTT cannot recover a session, user is missing permissions"),
                    {?CONNACK_SERVER, PState};
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
maybe_clean_sess(PState = #proc_state { clean_sess = true,
                                        connection = Conn,
                                        auth_state = #auth_state{vhost = VHost},
                                        client_id  = ClientId }) ->
    {_, Queue} = rabbit_mqtt_util:subcription_queue_name(ClientId),
    {ok, Channel} = amqp_connection:open_channel(Conn),
    case session_present(VHost, ClientId) of
        false ->
            {{?CONNACK_ACCEPT, false}, PState};
        true ->
            try amqp_channel:call(Channel, #'queue.delete'{ queue = Queue }) of
                #'queue.delete_ok'{} -> {{?CONNACK_ACCEPT, false}, PState}
            catch
                exit:({{shutdown, {server_initiated_close, 403, _}}, _}) ->
                    %% Connection is not yet propagated to #proc_state{}, let's close it here
                    catch amqp_connection:close(Conn),
                    rabbit_log_connection:error("MQTT cannot start a clean session: "
                                                "`configure` permission missing for queue `~p`", [Queue]),
                    {?CONNACK_SERVER, PState}
            after
                catch amqp_channel:close(Channel)
            end
    end.

session_present(VHost, ClientId)  ->
    {_, QueueQ1} = rabbit_mqtt_util:subcription_queue_name(ClientId),
    QueueName = rabbit_misc:r(VHost, queue, QueueQ1),
    case rabbit_amqqueue:lookup(QueueName) of
        {ok, _} -> true;
        {error, not_found} -> false
    end.

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

process_login(_UserBin, _PassBin, _ProtoVersion,
              #proc_state{channels   = {Channel, _},
                          peer_addr  = Addr,
                          auth_state = #auth_state{username = Username,
                                                   vhost = VHost}}) when is_pid(Channel) ->
    UsernameStr = rabbit_data_coercion:to_list(Username),
    VHostStr = rabbit_data_coercion:to_list(VHost),
    rabbit_core_metrics:auth_attempt_failed(list_to_binary(inet:ntoa(Addr)), Username, mqtt),
    rabbit_log_connection:warning("MQTT detected duplicate connect/login attempt for user ~p, vhost ~p",
                                  [UsernameStr, VHostStr]),
    connack_dup_auth;
process_login(UserBin, PassBin, ProtoVersion,
              #proc_state{channels     = {undefined, undefined},
                          socket       = Sock,
                          adapter_info = AdapterInfo,
                          ssl_login_name = SslLoginName,
                          peer_addr    = Addr}) ->
    {ok, {_, _, _, ToPort}} = rabbit_net:socket_ends(Sock, inbound),
    {VHostPickedUsing, {VHost, UsernameBin}} = get_vhost(UserBin, SslLoginName, ToPort),
    rabbit_log_connection:debug(
        "MQTT vhost picked using ~s",
        [human_readable_vhost_lookup_strategy(VHostPickedUsing)]),
    RemoteAddress = list_to_binary(inet:ntoa(Addr)),
    case rabbit_vhost:exists(VHost) of
        true  ->
            case amqp_connection:start(#amqp_params_direct{
                username     = UsernameBin,
                password     = PassBin,
                virtual_host = VHost,
                adapter_info = set_proto_version(AdapterInfo, ProtoVersion)}) of
                {ok, Connection} ->
                    case rabbit_access_control:check_user_loopback(UsernameBin, Addr) of
                        ok          ->
                            rabbit_core_metrics:auth_attempt_succeeded(RemoteAddress, UsernameBin,
                                                                       mqtt),
                            [{internal_user, InternalUser}] = amqp_connection:info(
                                Connection, [internal_user]),
                            {?CONNACK_ACCEPT, Connection, VHost,
                                #auth_state{user = InternalUser,
                                    username = UsernameBin,
                                    vhost = VHost}};
                        not_allowed ->
                            rabbit_core_metrics:auth_attempt_failed(RemoteAddress, UsernameBin,
                                                                    mqtt),
                            amqp_connection:close(Connection),
                            rabbit_log_connection:warning(
                                "MQTT login failed for user ~s: "
                                "this user's access is restricted to localhost",
                                [binary_to_list(UsernameBin)]),
                            ?CONNACK_AUTH
                    end;
                {error, {auth_failure, Explanation}} ->
                    rabbit_core_metrics:auth_attempt_failed(RemoteAddress, UsernameBin, mqtt),
                    rabbit_log_connection:error("MQTT login failed for user '~s', authentication failed: ~s",
                        [binary_to_list(UserBin), Explanation]),
                    ?CONNACK_CREDENTIALS;
                {error, access_refused} ->
                    rabbit_core_metrics:auth_attempt_failed(RemoteAddress, UsernameBin, mqtt),
                    rabbit_log_connection:warning("MQTT login failed for user '~s': "
                        "virtual host access not allowed",
                        [binary_to_list(UserBin)]),
                    ?CONNACK_AUTH;
                {error, not_allowed} ->
                    rabbit_core_metrics:auth_attempt_failed(RemoteAddress, UsernameBin, mqtt),
                    %% when vhost allowed for TLS connection
                    rabbit_log_connection:warning("MQTT login failed for user '~s': "
                        "virtual host access not allowed",
                        [binary_to_list(UserBin)]),
                    ?CONNACK_AUTH
            end;
        false ->
            rabbit_core_metrics:auth_attempt_failed(RemoteAddress, UsernameBin, mqtt),
            rabbit_log_connection:error("MQTT login failed for user '~s': virtual host '~s' does not exist",
                [UserBin, VHost]),
            ?CONNACK_CREDENTIALS
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

%% different qos subscriptions are received in different queues
%% with appropriate durability and timeout arguments
%% this will lead to duplicate messages for overlapping subscriptions
%% with different qos values - todo: prevent duplicates
ensure_queue(Qos, #proc_state{ channels      = {Channel, _},
                               client_id     = ClientId,
                               clean_sess    = CleanSess,
                          consumer_tags = {TagQ0, TagQ1} = Tags} = PState) ->
    {QueueQ0, QueueQ1} = rabbit_mqtt_util:subcription_queue_name(ClientId),
    Qos1Args = case {rabbit_mqtt_util:env(subscription_ttl), CleanSess} of
                   {undefined, _} ->
                       [];
                   {Ms, false} when is_integer(Ms) ->
                       [{<<"x-expires">>, long, Ms}];
                   _ ->
                       []
               end,
    QueueSetup =
        case {TagQ0, TagQ1, Qos} of
            {undefined, _, ?QOS_0} ->
                {QueueQ0,
                 #'queue.declare'{ queue       = QueueQ0,
                                   durable     = false,
                                   auto_delete = true },
                 #'basic.consume'{ queue  = QueueQ0,
                                   no_ack = true }};
            {_, undefined, ?QOS_1} ->
                {QueueQ1,
                 #'queue.declare'{ queue       = QueueQ1,
                                   durable     = true,
                                   %% Clean session means a transient connection,
                                   %% translating into auto-delete.
                                   %%
                                   %% see rabbitmq/rabbitmq-mqtt#37
                                   auto_delete = CleanSess,
                                   arguments   = Qos1Args },
                 #'basic.consume'{ queue  = QueueQ1,
                                   no_ack = false }};
            {_, _, ?QOS_0} ->
                {exists, QueueQ0};
            {_, _, ?QOS_1} ->
                {exists, QueueQ1}
          end,
    case QueueSetup of
        {Queue, Declare, Consume} ->
            #'queue.declare_ok'{} = amqp_channel:call(Channel, Declare),
            #'basic.consume_ok'{ consumer_tag = Tag } =
                amqp_channel:call(Channel, Consume),
            {Queue, PState #proc_state{ consumer_tags = setelement(Qos+1, Tags, Tag) }};
        {exists, Q} ->
            {Q, PState}
    end.

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
                "Could not send last will: ~p",
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

%% TODO amqp_pub/2 is publishing messages asynchronously, using
%% amqp_channel:cast_flow/3
%%
%% It does access check using check_publish/3 before submitting, but
%% this is superfluous, as actual publishing will do the same
%% check. While check results cached, it's still some unnecessary
%% work.
%%
%% And the only reason to keep it that way is that it prevents useless
%% crash messages flooding logs, as there is no code to handle async
%% channel crash gracefully.
%%
%% It'd be better to rework the whole thing, removing performance
%% penalty and some 50 lines of duplicate code. Maybe unlinking from
%% channel, and adding it as a child of connection supervisor instead.
%% But exact details are not yet clear.
amqp_pub(undefined, PState) ->
    PState;

%% set up a qos1 publishing channel if necessary
%% this channel will only be used for publishing, not consuming
amqp_pub(Msg   = #mqtt_msg{ qos = ?QOS_1 },
         PState = #proc_state{ channels       = {ChQos0, undefined},
                               awaiting_seqno = undefined,
                               connection     = Conn }) ->
    {ok, Channel} = amqp_connection:open_channel(Conn),
    #'confirm.select_ok'{} = amqp_channel:call(Channel, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Channel, self()),
    amqp_pub(Msg, PState #proc_state{ channels       = {ChQos0, Channel},
                                      awaiting_seqno = 1 });

amqp_pub(#mqtt_msg{ qos        = Qos,
                    topic      = Topic,
                    dup        = Dup,
                    message_id = MessageId,
                    payload    = Payload },
         PState = #proc_state{ channels       = {ChQos0, ChQos1},
                               exchange       = Exchange,
                               unacked_pubs   = UnackedPubs,
                               awaiting_seqno = SeqNo,
                               mqtt2amqp_fun  = Mqtt2AmqpFun }) ->
    RoutingKey = Mqtt2AmqpFun(Topic),
    Method = #'basic.publish'{ exchange    = Exchange,
                               routing_key = RoutingKey },
    Headers = [{<<"x-mqtt-publish-qos">>, byte, Qos},
               {<<"x-mqtt-dup">>, bool, Dup}],
    Msg = #amqp_msg{ props   = #'P_basic'{ headers       = Headers,
                                           delivery_mode = delivery_mode(Qos)},
                     payload = Payload },
    {UnackedPubs1, Ch, SeqNo1} =
        case Qos =:= ?QOS_1 andalso MessageId =/= undefined of
            true  -> {gb_trees:enter(SeqNo, MessageId, UnackedPubs), ChQos1,
                      SeqNo + 1};
            false -> {UnackedPubs, ChQos0, SeqNo}
        end,
    amqp_channel:cast_flow(Ch, Method, Msg),
    PState #proc_state{ unacked_pubs   = UnackedPubs1,
                        awaiting_seqno = SeqNo1 }.

adapter_info(Sock, ProtoName) ->
    amqp_connection:socket_adapter_info(Sock, {ProtoName, "N/A"}).

set_proto_version(AdapterInfo = #amqp_adapter_info{protocol = {Proto, _}}, Vsn) ->
    AdapterInfo#amqp_adapter_info{protocol = {Proto,
        human_readable_mqtt_version(Vsn)}}.

human_readable_mqtt_version(3) ->
    "3.1.0";
human_readable_mqtt_version(4) ->
    "3.1.1";
human_readable_mqtt_version(_) ->
    "N/A".

serialise_and_send_to_client(Frame, #proc_state{ socket = Sock }) ->
    try rabbit_net:port_command(Sock, rabbit_mqtt_frame:serialise(Frame)) of
      Res ->
        Res
    catch _:Error ->
      rabbit_log_connection:error("MQTT: a socket write failed, the socket might already be closed"),
      rabbit_log_connection:debug("Failed to write to socket ~p, error: ~p, frame: ~p",
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

check_publish(TopicName, Fn, PState) ->
  case check_topic_access(TopicName, write, PState) of
    ok -> Fn();
    _ -> {error, unauthorized, PState}
  end.

check_topic_access(TopicName, Access,
                   #proc_state{
                        auth_state = #auth_state{user = User = #user{username = Username},
                                                 vhost = VHost},
                        exchange = Exchange,
                        client_id = ClientId,
                        mqtt2amqp_fun = Mqtt2AmqpFun }) ->
    Cache =
        case get(topic_permission_cache) of
            undefined -> [];
            Other     -> Other
        end,

    Key = {TopicName, Username, ClientId, VHost, Exchange, Access},
    case lists:member(Key, Cache) of
        true ->
            ok;
        false ->
            Resource = #resource{virtual_host = VHost,
                                 kind = topic,
                                 name = Exchange},

            RoutingKey = Mqtt2AmqpFun(TopicName),
            Context = #{routing_key  => RoutingKey,
                        variable_map => #{
                                          <<"username">>  => Username,
                                          <<"vhost">>     => VHost,
                                          <<"client_id">> => rabbit_data_coercion:to_binary(ClientId)
                                         }
                       },

            try rabbit_access_control:check_topic_access(User, Resource, Access, Context) of
                ok ->
                    CacheTail = lists:sublist(Cache, ?MAX_TOPIC_PERMISSION_CACHE_SIZE - 1),
                    put(topic_permission_cache, [Key | CacheTail]),
                    ok;
                R ->
                    R
            catch
                _:{amqp_error, access_refused, Msg, _} ->
                    rabbit_log:error("operation resulted in an error (access_refused): ~p", [Msg]),
                    {error, access_refused};
                _:Error ->
                    rabbit_log:error("~p", [Error]),
                    {error, access_refused}
            end
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
info(exchange, #proc_state{exchange = Val}) -> Val;
info(adapter_info, #proc_state{adapter_info = Val}) -> Val;
info(ssl_login_name, #proc_state{ssl_login_name = Val}) -> Val;
info(retainer_pid, #proc_state{retainer_pid = Val}) -> Val;
info(user, #proc_state{auth_state = #auth_state{username = Val}}) -> Val;
info(vhost, #proc_state{auth_state = #auth_state{vhost = Val}}) -> Val;
info(host, #proc_state{adapter_info = #amqp_adapter_info{host = Val}}) -> Val;
info(port, #proc_state{adapter_info = #amqp_adapter_info{port = Val}}) -> Val;
info(peer_host, #proc_state{adapter_info = #amqp_adapter_info{peer_host = Val}}) -> Val;
info(peer_port, #proc_state{adapter_info = #amqp_adapter_info{peer_port = Val}}) -> Val;
info(protocol, #proc_state{adapter_info = #amqp_adapter_info{protocol = Val}}) ->
    case Val of
        {Proto, Version} -> {Proto, rabbit_data_coercion:to_binary(Version)};
        Other -> Other
    end;
info(channels, PState) -> additional_info(channels, PState);
info(channel_max, PState) -> additional_info(channel_max, PState);
info(frame_max, PState) -> additional_info(frame_max, PState);
info(client_properties, PState) -> additional_info(client_properties, PState);
info(ssl, PState) -> additional_info(ssl, PState);
info(ssl_protocol, PState) -> additional_info(ssl_protocol, PState);
info(ssl_key_exchange, PState) -> additional_info(ssl_key_exchange, PState);
info(ssl_cipher, PState) -> additional_info(ssl_cipher, PState);
info(ssl_hash, PState) -> additional_info(ssl_hash, PState);
info(Other, _) -> throw({bad_argument, Other}).


additional_info(Key,
                #proc_state{adapter_info =
                            #amqp_adapter_info{additional_info = AddInfo}}) ->
    proplists:get_value(Key, AddInfo).
