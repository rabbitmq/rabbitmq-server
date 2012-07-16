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

-export([process_frame/2]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("include/rabbit_mqtt_frame.hrl").
-include("include/rabbit_mqtt.hrl").

-define(FRAME_TYPE(Frame, Type),
        Frame = #mqtt_frame{fixed = #mqtt_frame_fixed { type = Type }}).

process_frame(Frame = #mqtt_frame{ fixed = #mqtt_frame_fixed{ type = Type }},
              State ) ->
    %rabbit_log:error("received frame ~p ~n", [Frame]),
    process_request(Type, Frame, State).

process_request(?CONNECT,
                #mqtt_frame{ variable = #mqtt_frame_connect {
                                          username   = Username,
                                          password   = Password,
                                          proto_ver  = ProtoVersion,
                                          clean_sess = CleanSess,
                                          client_id  = ClientId }}, State) ->
    {ReturnCode, State1} =
        case {ProtoVersion =:= ?MQTT_PROTO_MAJOR,
              rabbit_mqtt_util:valid_client_id(ClientId)} of
            {false, _} ->
                {?CONNACK_PROTO_VER, State};
            {_, false} ->
                {?CONNACK_INVALID_ID, State};
            _ ->
                case creds(Username, Password) of
                    nocreds ->
                        rabbit_log:error("MQTT login failed - no credentials~n"),
                        {?CONNACK_CREDENTIALS, State};
                    {UserBin, Creds} ->
                        case process_login(UserBin, Creds, State) of
                            {?CONNACK_ACCEPT, Conn} ->
                                link(Conn),
                                maybe_clean_sess(CleanSess, Conn, ClientId),
                                {ok, Ch} = amqp_connection:open_channel(Conn),
                                ok = ensure_unique_client_id(ClientId),
                                {?CONNACK_ACCEPT,
                                 State #state { clean_sess = CleanSess,
                                                channel    = Ch,
                                                connection = Conn,
                                                client_id  = ClientId }};
                            ConnAck -> {ConnAck, State}
                        end
                end
        end,
    send_frame(#mqtt_frame{ fixed    = #mqtt_frame_fixed { type = ?CONNACK},
                            variable = #mqtt_frame_connack {
                                         return_code = ReturnCode }}, State1),
    {ok, State1};

process_request(?PUBLISH,
                #mqtt_frame {
                  fixed = #mqtt_frame_fixed { qos = Qos,
                                              dup = Dup },
                  variable = #mqtt_frame_publish { topic_name = TopicName,
                                                   message_id = MessageId },
                  payload = Payload },
                #state { channel      = Channel,
                         confirms     = Confirms,
                         unacked_pubs = UnackedPubs } = State) ->
    State1 = case not Confirms andalso Qos =:= 1 of
                 true -> #'confirm.select_ok'{} =
                           amqp_channel:call(Channel, #'confirm.select'{}),
                         amqp_channel:register_confirm_handler(Channel, self()),
                         State #state { confirms = true };
                 _    -> State
             end,
    SeqNo = amqp_channel:next_publish_seqno(Channel),
    Method = #'basic.publish'{ exchange    = ?DEFAULT_EXCHANGE,
                               routing_key =
                                   rabbit_mqtt_util:translate_topic(TopicName)},
    Props = case Dup of
                true  -> #'P_basic'{headers = [{'x-mqtt-dup', bool, true}]};
                false -> #'P_basic'{}
            end,
    Msg = #amqp_msg{ props   = Props,
                     payload = Payload },
    amqp_channel:cast(Channel, Method, Msg),
    {ok, State1 #state { unacked_pubs =
                           case Qos of
                               0 -> UnackedPubs;
                               1 -> gb_trees:enter(SeqNo, MessageId, UnackedPubs)
                           end }};

process_request(?SUBSCRIBE,
                #mqtt_frame {
                  variable = #mqtt_frame_subscribe { message_id  = MessageId,
                                                     topic_table = Topics },
                  payload = undefined }, #state { channel = Channel,
                                                  client_id = ClientId,
                                                  consumer_tag = Tag0} = State) ->
    Queue = rabbit_mqtt_util:subcription_queue_name(ClientId),
    Tag1 = case Tag0 of
               undefined ->
                   #'queue.declare_ok'{} =
                       amqp_channel:call(Channel, #'queue.declare'{
                                                    queue = Queue }),
                   Method = #'basic.consume'{ queue = Queue },
                   #'basic.consume_ok'{ consumer_tag = Tag } =
                       amqp_channel:call(Channel, Method),
                   Tag;
               _ -> Tag0
           end,
    QosResponse =
      [begin
         Binding = #'queue.bind'{
                       queue       = Queue,
                       exchange    = ?DEFAULT_EXCHANGE,
                       routing_key = rabbit_mqtt_util:translate_topic(TopicName)},
         #'queue.bind_ok'{} = amqp_channel:call(Channel, Binding),
         ?QOS_0
       end || #mqtt_topic { name = TopicName } <- Topics ],
    send_frame(#mqtt_frame{ fixed    = #mqtt_frame_fixed { type = ?SUBACK },
                            variable = #mqtt_frame_suback {
                                         message_id = MessageId,
                                         qos_table  = QosResponse }}, State),
    {ok, State #state { consumer_tag = Tag1 }};

process_request(?UNSUBSCRIBE,
                #mqtt_frame {
                  variable = #mqtt_frame_subscribe { message_id  = MessageId,
                                                     topic_table = Topics },
                  payload = undefined }, #state { channel   = Channel,
                                                  client_id = ClientId} = State) ->
    Queue = rabbit_mqtt_util:subcription_queue_name(ClientId),
    [begin
        Binding = #'queue.unbind'{queue       = Queue,
                                  exchange    = ?DEFAULT_EXCHANGE,
                                  routing_key = rabbit_mqtt_util:translate_topic(TopicName)},
        #'queue.unbind_ok'{} = amqp_channel:call(Channel, Binding)
     end || #mqtt_topic { name = TopicName } <- Topics ],
    send_frame(#mqtt_frame{ fixed    = #mqtt_frame_fixed  {type       = ?UNSUBACK},
                            variable = #mqtt_frame_suback {message_id = MessageId }}, State),
    {ok, State};

process_request(?PINGREQ, #mqtt_frame{}, State) ->
    send_frame(#mqtt_frame { fixed = #mqtt_frame_fixed { type = ?PINGRESP }},
               State),
    {ok, State};

process_request(?DISCONNECT, #mqtt_frame {}, State) ->
    {stop, normal, State}.

maybe_clean_sess(false, _Conn, _ClientId) ->
    ok;
maybe_clean_sess(true, Conn, ClientId) ->
    Queue = rabbit_mqtt_util:subcription_queue_name(ClientId),
    {ok, Channel} = amqp_connection:open_channel(Conn),
    try amqp_channel:call(Channel, #'queue.declare'{ queue = Queue,
                                                     passive = true }) of
        #'queue.declare_ok'{} -> #'queue.delete_ok'{} =
                                   amqp_channel:call(Channel,
                                                     #'queue.delete'{
                                                       queue = Queue }),
                                 ok = amqp_channel:close(Channel)
    catch
        exit:_Error -> ok
    end.

ensure_unique_client_id(_ClientId) ->
    %% todo spec section 3.1:
    %% If a client with the same Client ID is already connected to the server,
    %% the "older" client must be disconnected by the server before completing
    %% the CONNECT flow of the new client.
    ok.

process_login(UserBin, Creds, State = #state{ channel      = undefined,
                                              adapter_info = AdapterInfo }) ->
    case rabbit_access_control:check_user_login(UserBin, Creds) of
         {ok, _User} ->
             {ok, VHost} = application:get_env(rabbitmq_mqtt, vhost),
             case amqp_connection:start(
                    #amqp_params_direct{username     = UserBin,
                                        virtual_host = VHost,
                                        adapter_info = AdapterInfo}) of
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

send_frame(Frame, #state{ socket = Sock }) ->
    rabbit_mqtt_reader:send_frame(Sock, Frame).
