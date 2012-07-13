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
                #mqtt_frame{
                  variable = #mqtt_frame_connect { username  = Username,
                                                   password  = Password,
                                                   proto_ver = ProtoVersion,
                                                   client_id = ClientId }},
                State = #state{channel      = undefined,
                               adapter_info = AdapterInfo}) ->
    {ReturnCode, State1} =
        case {ProtoVersion =:= ?MQTT_PROTO_MAJOR,
              rabbit_mqtt_util:valid_client_id(ClientId)} of
            {false, _} ->
                {?CONNACK_PROTO_VER, State};
            {_, false} ->
                {?CONNACK_INVALID_ID, State};
            _ ->
                {UserBin, Creds} = creds(Username, Password),
                case rabbit_access_control:check_user_login(UserBin, Creds) of
                     {ok, _User} ->
                         {ok, VHost} = application:get_env(rabbitmq_mqtt, vhost),
                         case amqp_connection:start(
                                #amqp_params_direct{username = UserBin,
                                                    virtual_host = VHost,
                                                    adapter_info = AdapterInfo}) of
                             {ok, Connection} ->
                                 link(Connection),
                                 {ok, Channel} =
                                     amqp_connection:open_channel(Connection),
                                 ok = ensure_unique_client_id(ClientId),
                                 {?CONNACK_ACCEPT,
                                     State#state{connection = Connection,
                                                 channel    = Channel,
                                                 client_id  = ClientId}};
                             {error, auth_failure} ->
                                 rabbit_log:error("MQTT login failed - " ++
                                                  "auth_failure " ++
                                                  "(user vanished)~n"),
                                 {?CONNACK_CREDENTIALS, State};
                             {error, access_refused} ->
                                 rabbit_log:warning("MQTT login failed - " ++
                                                    "access_refused " ++
                                                    "(vhost access not allowed)~n"),
                                 {?CONNACK_AUTH, State}
                          end;
                     {refused, Msg, Args} ->
                         rabbit_log:warning("MQTT login failed: " ++ Msg ++
                                            "\n", Args),
                         {?CONNACK_CREDENTIALS, State}
                end
        end,
    send_frame(#mqtt_frame{ fixed    = #mqtt_frame_fixed {type = ?CONNACK},
                            variable = #mqtt_frame_connack {
                                         return_code = ReturnCode }}, State1),
    {ok, State1};

process_request(?PUBLISH,
                #mqtt_frame {
                  variable = #mqtt_frame_publish { topic_name = TopicName,
                                                   message_id = _MessageId },
                  payload = Payload }, #state { channel = Channel } = State) ->
    Method = #'basic.publish'{ exchange    = ?DEFAULT_EXCHANGE,
                               routing_key =
                                   rabbit_mqtt_util:translate_topic(TopicName)},
    amqp_channel:cast(Channel, Method, #amqp_msg{payload = Payload}),
    {ok, State};

process_request(?SUBSCRIBE,
                #mqtt_frame {
                  variable = #mqtt_frame_subscribe { message_id  = MessageId,
                                                     topic_table = Topics },
                  payload = undefined }, #state { channel = Channel,
                                                  client_id = ClientId} = State) ->
    Queue = rabbit_mqtt_util:subcription_queue_name(ClientId),
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{ queue = Queue,
                                                     exclusive = false,
                                                     auto_delete = false}),
    QosResponse =
      [begin
         Binding = #'queue.bind'{
                       queue       = Queue,
                       exchange    = ?DEFAULT_EXCHANGE,
                       routing_key = rabbit_mqtt_util:translate_topic(TopicName)},
         #'queue.bind_ok'{} = amqp_channel:call(Channel, Binding),
         ?QOS_0
       end || #mqtt_topic { name = TopicName } <- Topics ],
    Method = #'basic.consume'{ queue = Queue },
    #'basic.consume_ok'{consumer_tag = _Tag} = amqp_channel:call(Channel, Method),
    send_frame(#mqtt_frame{ fixed    = #mqtt_frame_fixed { type = ?SUBACK },
                            variable = #mqtt_frame_suback {
                                         message_id = MessageId,
                                         qos_table  = QosResponse }}, State),
    {ok, State};

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

ensure_unique_client_id(_ClientId) ->
    %% todo spec section 3.1:
    %% If a client with the same Client ID is already connected to the server,
    %% the "older" client must be disconnected by the server before completing
    %% the CONNECT flow of the new client.
    ok.

creds(Username, Password) ->
    {ok, DefaultUser} = application:get_env(rabbitmq_mqtt, default_user),
    {ok, DefaultPass} = application:get_env(rabbitmq_mqtt, default_pass),
    U = case Username of
            undefined -> DefaultUser;
            _         -> list_to_binary(Username)
        end,
    P = case Password of
            undefined -> DefaultPass;
            _         -> list_to_binary(Password)
        end,
    {U, [{password, P}]}.

send_frame(Frame, #state{socket = Sock}) ->
    rabbit_mqtt_reader:send_frame(Sock, Frame).
