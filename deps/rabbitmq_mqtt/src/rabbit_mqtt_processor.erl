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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_mqtt_processor).

-export([info/2, initial_state/1,
         process_frame/2, amqp_pub/2, amqp_callback/2, send_will/1,
         close_connection/1]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_mqtt_frame.hrl").
-include("rabbit_mqtt.hrl").

-define(FRAME_TYPE(Frame, Type),
        Frame = #mqtt_frame{ fixed = #mqtt_frame_fixed{ type = Type }}).

initial_state(Socket) ->
    #proc_state{ unacked_pubs  = gb_trees:empty(),
                 awaiting_ack  = gb_trees:empty(),
                 message_id    = 1,
                 subscriptions = dict:new(),
                 consumer_tags = {undefined, undefined},
                 channels      = {undefined, undefined},
                 exchange      = rabbit_mqtt_util:env(exchange),
                 socket        = Socket }.

info(client_id, #proc_state{ client_id = ClientId }) -> ClientId.

process_frame(#mqtt_frame{ fixed = #mqtt_frame_fixed{ type = Type }},
              PState = #proc_state{ connection = undefined } )
  when Type =/= ?CONNECT ->
    {err, connect_expected, PState};
process_frame(Frame = #mqtt_frame{ fixed = #mqtt_frame_fixed{ type = Type }},
              PState ) ->
    %rabbit_log:info("MQTT received frame ~p ~n", [Frame]),
    process_request(Type, Frame, PState).

process_request(?CONNECT,
                #mqtt_frame{ variable = #mqtt_frame_connect{
                                          username   = Username,
                                          password   = Password,
                                          proto_ver  = ProtoVersion,
                                          clean_sess = CleanSess,
                                          client_id  = ClientId } = Var}, PState) ->
    {ReturnCode, PState1} =
        case {ProtoVersion =:= ?MQTT_PROTO_MAJOR,
              rabbit_mqtt_util:valid_client_id(ClientId)} of
            {false, _} ->
                {?CONNACK_PROTO_VER, PState};
            {_, false} ->
                {?CONNACK_INVALID_ID, PState};
            _ ->
                case creds(Username, Password) of
                    nocreds ->
                        rabbit_log:error("MQTT login failed - no credentials~n"),
                        {?CONNACK_CREDENTIALS, PState};
                    {UserBin, Creds} ->
                        case process_login(UserBin, Creds, PState) of
                            {?CONNACK_ACCEPT, Conn} ->
                                link(Conn),
                                maybe_clean_sess(CleanSess, Conn, ClientId),
                                {ok, Ch} = amqp_connection:open_channel(Conn),
                                ok = rabbit_mqtt_collector:register(
                                  ClientId, self()),
                                Prefetch = rabbit_mqtt_util:env(prefetch),
                                #'basic.qos_ok'{} = amqp_channel:call(
                                  Ch, #'basic.qos'{prefetch_count = Prefetch}),
                                {?CONNACK_ACCEPT,
                                 PState #proc_state{ will_msg   = make_will_msg(Var),
                                                     clean_sess = CleanSess,
                                                     channels   = {Ch, undefined},
                                                     connection = Conn,
                                                     client_id  = ClientId }};
                            ConnAck ->
                                {ConnAck, PState}
                        end
                end
        end,
    send_client(#mqtt_frame{ fixed    = #mqtt_frame_fixed{ type = ?CONNACK},
                             variable = #mqtt_frame_connack{
                                         return_code = ReturnCode }}, PState1),
    {ok, PState1};

process_request(?PUBACK,
                #mqtt_frame{
                  variable = #mqtt_frame_publish{ message_id = MessageId }},
                #proc_state{ channels     = {Channel, _},
                             awaiting_ack = Awaiting } = PState) ->
    Tag = gb_trees:get(MessageId, Awaiting),
    amqp_channel:cast(
       Channel, #'basic.ack'{ delivery_tag = Tag }),
    {ok, PState #proc_state{ awaiting_ack = gb_trees:delete( MessageId, Awaiting)}};

process_request(?PUBLISH,
                #mqtt_frame{
                  fixed = #mqtt_frame_fixed{ qos = ?QOS_2 }}, PState) ->
    {err, qos2_not_supported, PState};
process_request(?PUBLISH,
                #mqtt_frame{
                  fixed = #mqtt_frame_fixed{ qos    = Qos,
                                             retain = Retain,
                                             dup    = Dup },
                  variable = #mqtt_frame_publish{ topic_name = Topic,
                                                  message_id = MessageId },
                  payload = Payload }, PState) ->
    {ok, amqp_pub(#mqtt_msg{ retain     = Retain,
                             qos        = Qos,
                             topic      = Topic,
                             dup        = Dup,
                             message_id = MessageId,
                             payload    = Payload }, PState)};

process_request(?SUBSCRIBE,
                #mqtt_frame{
                  variable = #mqtt_frame_subscribe{ message_id  = MessageId,
                                                    topic_table = Topics },
                  payload = undefined },
                #proc_state{ channels = {Channel, _},
                             exchange = Exchange} = PState0) ->
    {QosResponse, PState1} =
        lists:foldl(fun (#mqtt_topic{ name = TopicName,
                                       qos  = Qos }, {QosList, PState}) ->
                       SupportedQos = supported_subs_qos(Qos),
                       {Queue, #proc_state{ subscriptions = Subs } = PState1} =
                           ensure_queue(SupportedQos, PState),
                       Binding = #'queue.bind'{
                                   queue       = Queue,
                                   exchange    = Exchange,
                                   routing_key = rabbit_mqtt_util:mqtt2amqp(
                                                   TopicName)},
                       #'queue.bind_ok'{} = amqp_channel:call(Channel, Binding),
                       {[SupportedQos | QosList],
                        PState1 #proc_state{ subscriptions =
                                             dict:append(TopicName, SupportedQos, Subs) }}
                   end, {[], PState0}, Topics),
    send_client(#mqtt_frame{ fixed    = #mqtt_frame_fixed{ type = ?SUBACK },
                             variable = #mqtt_frame_suback{
                                         message_id = MessageId,
                                         qos_table  = QosResponse }}, PState1),

    {ok, PState1};

process_request(?UNSUBSCRIBE,
                #mqtt_frame{
                  variable = #mqtt_frame_subscribe{ message_id  = MessageId,
                                                    topic_table = Topics },
                  payload = undefined }, #proc_state{ channels      = {Channel, _},
                                                      exchange      = Exchange,
                                                      client_id     = ClientId,
                                                      subscriptions = Subs0} = PState) ->
    Queues = rabbit_mqtt_util:subcription_queue_name(ClientId),
    Subs1 =
    lists:foldl(
      fun (#mqtt_topic{ name = TopicName }, Subs) ->
        QosSubs = case dict:find(TopicName, Subs) of
                      {ok, Val} when is_list(Val) -> lists:usort(Val);
                      error                       -> []
                  end,
        lists:foreach(
          fun (QosSub) ->
                  Queue = element(QosSub + 1, Queues),
                  Binding = #'queue.unbind'{
                              queue       = Queue,
                              exchange    = Exchange,
                              routing_key =
                                  rabbit_mqtt_util:mqtt2amqp(TopicName)},
                  #'queue.unbind_ok'{} = amqp_channel:call(Channel, Binding)
          end, QosSubs),
        dict:erase(TopicName, Subs)
      end, Subs0, Topics),
    send_client(#mqtt_frame{ fixed    = #mqtt_frame_fixed { type       = ?UNSUBACK },
                             variable = #mqtt_frame_suback{ message_id = MessageId }},
                PState),
    {ok, PState #proc_state{ subscriptions = Subs1 }};

process_request(?PINGREQ, #mqtt_frame{}, PState) ->
    send_client(#mqtt_frame{ fixed = #mqtt_frame_fixed{ type = ?PINGRESP }},
                PState),
    {ok, PState};

process_request(?DISCONNECT, #mqtt_frame{}, PState) ->
    {stop, PState}.

%%----------------------------------------------------------------------------

amqp_callback({#'basic.deliver'{ consumer_tag = ConsumerTag,
                                 delivery_tag = DeliveryTag,
                                 routing_key  = RoutingKey },
               #amqp_msg{ props = #'P_basic'{ headers = Headers },
                          payload = Payload }} = Delivery,
              #proc_state{ channels      = {Channel, _},
                           awaiting_ack  = Awaiting,
                           message_id    = MsgId } = PState) ->
    case {delivery_dup(Delivery), delivery_qos(ConsumerTag, Headers, PState)} of
        {true, {?QOS_0, ?QOS_1}} ->
            amqp_channel:cast(
              Channel, #'basic.ack'{ delivery_tag = DeliveryTag }),
            {ok, PState};
        {true, {?QOS_0, ?QOS_0}} ->
            {ok, PState};
        {Dup, {DeliveryQos, _SubQos} = Qos}     ->
            send_client(
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
                                        topic_name =
                                          rabbit_mqtt_util:amqp2mqtt(
                                            RoutingKey) },
                           payload = Payload}, PState),
              case Qos of
                  {?QOS_0, ?QOS_0} ->
                      {ok, PState};
                  {?QOS_1, ?QOS_1} ->
                      {ok,
                       next_msg_id(
                         PState #proc_state{
                           awaiting_ack =
                             gb_trees:insert(MsgId, DeliveryTag, Awaiting)})};
                  {?QOS_0, ?QOS_1} ->
                      amqp_channel:cast(
                        Channel, #'basic.ack'{ delivery_tag = DeliveryTag }),
                      {ok, PState}
              end
    end;

amqp_callback(#'basic.ack'{ multiple = true, delivery_tag = Tag } = Ack,
              PState = #proc_state{ unacked_pubs = UnackedPubs }) ->
    case gb_trees:take_smallest(UnackedPubs) of
        {TagSmall, MsgId, UnackedPubs1} when TagSmall =< Tag ->
            send_client(
              #mqtt_frame{ fixed    = #mqtt_frame_fixed{ type = ?PUBACK },
                           variable = #mqtt_frame_publish{ message_id = MsgId }},
              PState),
            amqp_callback(Ack, PState #proc_state{ unacked_pubs = UnackedPubs1 });
        _ ->
            {ok, PState}
    end;

amqp_callback(#'basic.ack'{ multiple = false, delivery_tag = Tag },
              PState = #proc_state{ unacked_pubs = UnackedPubs }) ->
    send_client(
      #mqtt_frame{ fixed    = #mqtt_frame_fixed{ type = ?PUBACK },
                   variable = #mqtt_frame_publish{
                                message_id = gb_trees:get(
                                               Tag, UnackedPubs) }}, PState),
    {ok, PState #proc_state{ unacked_pubs = gb_trees:delete(Tag, UnackedPubs) }}.

delivery_dup({#'basic.deliver'{ redelivered = Redelivered },
              #amqp_msg{ props = #'P_basic'{ headers = Headers }}}) ->
    case rabbit_misc:table_lookup(Headers, 'x-mqtt-dup') of
        undefined   -> Redelivered;
        {bool, Dup} -> Redelivered orelse Dup
    end.

next_msg_id(PState = #proc_state{ message_id = 16#ffff }) ->
    PState #proc_state{ message_id = 1 };
next_msg_id(PState = #proc_state{ message_id = MsgId }) ->
    PState #proc_state{ message_id = MsgId + 1 }.

%% decide at which qos level to deliver based on subscription
%% and the message publish qos level
delivery_qos(Tag, _Headers, #proc_state{ consumer_tags = {Tag, _} }) ->
    {?QOS_0, ?QOS_0};
delivery_qos(Tag, Headers, #proc_state{ consumer_tags = {_, Tag} }) ->
    {byte, Qos} = rabbit_misc:table_lookup(Headers, 'x-mqtt-publish-qos'),
    {lists:min([Qos, ?QOS_1]), ?QOS_1}.

maybe_clean_sess(false, _Conn, _ClientId) ->
    % todo: establish subscription to deliver old unacknowledged messages
    ok;
maybe_clean_sess(true, Conn, ClientId) ->
    {_, Queue} = rabbit_mqtt_util:subcription_queue_name(ClientId),
    {ok, Channel} = amqp_connection:open_channel(Conn),
    try amqp_channel:call(Channel, #'queue.delete'{ queue = Queue }) of
        #'queue.delete_ok'{} -> ok = amqp_channel:close(Channel)
    catch
        exit:_Error -> ok
    end.

%%----------------------------------------------------------------------------

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

process_login(UserBin, Creds, #proc_state{ channels  = {undefined, undefined},
                                           socket    = Sock }) ->
    case rabbit_access_control:check_user_login(UserBin, Creds) of
         {ok, _User} ->
             VHost = rabbit_mqtt_util:env(vhost),
             case amqp_connection:start(
                    #amqp_params_direct{
                      username     = UserBin,
                      virtual_host = VHost,
                      adapter_info = adapter_info(Sock)}) of
                 {ok, Connection} ->
                     {?CONNACK_ACCEPT, Connection};
                 {error, auth_failure} ->
                     rabbit_log:error("MQTT login failed - " ++
                                      "auth_failure " ++
                                      "(user vanished)~n"),
                     ?CONNACK_CREDENTIALS;
                 {error, access_refused} ->
                     rabbit_log:warning("MQTT login failed - " ++
                                        "access_refused " ++
                                        "(vhost access not allowed)~n"),
                     ?CONNACK_AUTH
              end;
         {refused, Msg, Args} ->
             rabbit_log:warning("MQTT login failed: " ++ Msg ++
                                "\n", Args),
             ?CONNACK_CREDENTIALS
    end.

creds(User, Pass) ->
    DefaultUser = rabbit_mqtt_util:env(default_user),
    DefaultPass = rabbit_mqtt_util:env(default_pass),
    Anon        = rabbit_mqtt_util:env(allow_anonymous),
    U = case {User =/= undefined, is_binary(DefaultUser), Anon =:= true} of
             {true,  _,    _   } -> list_to_binary(User);
             {false, true, true} -> DefaultUser;
             _                   -> nocreds
        end,
    case U of
        nocreds ->
            nocreds;
        _ ->
            case {Pass =/= undefined, is_binary(DefaultPass), Anon =:= true} of
                 {true,  _,    _   } -> {U, [{password, list_to_binary(Pass)}]};
                 {false, true, true} -> {U, [{password, DefaultPass}]};
                 _                   -> {U, []}
            end
    end.

supported_subs_qos(?QOS_0) -> ?QOS_0;
supported_subs_qos(?QOS_1) -> ?QOS_1;
supported_subs_qos(?QOS_2) -> ?QOS_1.

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
                   {undefined, _}                  -> [];
                   {Ms, false} when is_integer(Ms) -> [{"x-expires", long, Ms}];
                   _                               -> []
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

send_will(PState = #proc_state{ will_msg = WillMsg }) ->
    amqp_pub(WillMsg, PState).

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
                               awaiting_seqno = SeqNo }) ->
    Method = #'basic.publish'{ exchange    = Exchange,
                               routing_key =
                                   rabbit_mqtt_util:mqtt2amqp(Topic)},
    Headers = [{'x-mqtt-publish-qos', byte, Qos}, {'x-mqtt-dup', bool, Dup}],
    Msg = #amqp_msg{ props   = #'P_basic'{ headers = Headers },
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

adapter_info(Sock) ->
    {Addr, Port} = case rabbit_net:sockname(Sock) of
                       {ok, Res} -> Res;
                       _         -> {unknown, unknown}
                   end,
    {PeerAddr, PeerPort} = case rabbit_net:peername(Sock) of
                               {ok, Res2} -> Res2;
                               _          -> {unknown, unknown}
                           end,
    Name = case rabbit_net:connection_string(Sock, inbound) of
               {ok, Res3} -> Res3;
               _          -> unknown
           end,
    #amqp_adapter_info{ protocol     = {'MQTT', {?MQTT_PROTO_MAJOR,
                                                 ?MQTT_PROTO_MINOR}},
                        name         = list_to_binary(Name),
                        address      = Addr,
                        port         = Port,
                        peer_address = PeerAddr,
                        peer_port    = PeerPort}.

send_client(Frame, #proc_state{ socket = Sock }) ->
    %rabbit_log:info("MQTT sending frame ~p ~n", [Frame]),
    rabbit_net:port_command(Sock, rabbit_mqtt_frame:serialise(Frame)).

close_connection(PState = #proc_state{ connection = undefined }) ->
    PState;
close_connection(PState = #proc_state{ connection = Connection }) ->
    %% ignore noproc or other exceptions to avoid debris
    catch amqp_connection:close(Connection),
    PState #proc_state{ channels   = {undefined, undefined},
                        connection = undefined }.

