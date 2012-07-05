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
-behaviour(gen_server2).

-export([start_link/1, process_frame/2, flush_and_die/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("include/rabbit_mqtt_frame.hrl").

-record(state, {
                client_id,
                message_id,
                channel,
                connection,
                adapter_info,
                send_fun
               }).

-define(DEFAULT_EXCHANGE, <<"amq.topic">>).
-define(MQTT_PROTOCOL_VERSION, 3).
-define(FLUSH_TIMEOUT, 60000).

%%----------------------------------------------------------------------------
%% Public API
%%----------------------------------------------------------------------------
start_link(Args) ->
    gen_server2:start_link(?MODULE, Args, []).

process_frame(Pid, Frame = #mqtt_frame{ fixed = #mqtt_frame_fixed{ type = ?PUBLISH }} ) ->
    credit_flow:send(Pid),
    gen_server2:cast(Pid, {?PUBLISH, Frame, self()});
process_frame(Pid, Frame = #mqtt_frame{ fixed = #mqtt_frame_fixed{ type = Type }} ) ->
    gen_server2:cast(Pid, {Type, Frame, noflow}).

flush_and_die(Pid) ->
    gen_server2:cast(Pid, flush_and_die).

%%----------------------------------------------------------------------------
%% Basic gen_server2 callbacks
%%----------------------------------------------------------------------------

init([SendFun, AdapterInfo, _Configuration]) ->
    process_flag(trap_exit, true),
    {ok,
     #state {
       client_id           = none,
       message_id          = 0,
       channel             = none,
       connection          = none,
       adapter_info        = AdapterInfo,
       send_fun            = SendFun
       },
     hibernate,
     {backoff, 1000, 1000, 10000}
    }.

terminate(_Reason, State) ->
    close_connection(State).

handle_cast(flush_and_die, State) ->
    {stop, normal, close_connection(State)};

handle_cast({?CONNECT, Frame, noflow}, State) ->
    process_connect(Frame, State);

handle_cast({_Type, Frame, _FlowPid}, State = #state{channel = none}) ->
    rabbit_log:error("Ignoring invalid MQTT frame prior to CONNECT: ~p~n",
                     [Frame]),
    {noreply, State, hibernate};

handle_cast({Type, Frame, FlowPid}, State) ->
    case FlowPid of
        noflow -> ok;
        _      -> credit_flow:ack(FlowPid)
    end,
    process_request(Type, Frame, State);

handle_cast(client_timeout, State) ->
    {stop, client_timeout, State}.

handle_info({#'basic.deliver'{delivery_tag = Tag,
                              routing_key = RoutingKey },
             #amqp_msg{ payload = Payload }},
            #state { channel = Channel,
                     message_id = _MessageId } = State ) ->
    send_frame(
      #mqtt_frame{ fixed = #mqtt_frame_fixed {type = ?PUBLISH},
                   variable = #mqtt_frame_publish {
                                topic_name = untranslate_topic( RoutingKey) },
                   payload = Payload},
      State),
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),
    {noreply, State, hibernate};
handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State, hibernate};
handle_info(#'basic.cancel_ok'{}, State) ->
    {noreply, State, hibernate};
handle_info({'EXIT', Conn, Reason}, State = #state{connection = Conn}) ->
    {stop, {conn_died, Reason}, State};
handle_info({inet_reply, _, ok}, State) ->
    {noreply, State, hibernate};
handle_info({bump_credit, Msg}, State) ->
    credit_flow:handle_bump_msg(Msg),
    {noreply, State, hibernate};

handle_info({inet_reply, _, Status}, State) ->
    {stop, Status, State}.

process_connect(#mqtt_frame{
                    variable = #mqtt_frame_connect { username  = Username,
                                                     password  = Password,
                                                     proto_ver = ProtoVersion,
                                                     client_id = ClientId }},
                State = #state{channel      = none,
                               adapter_info = AdapterInfo}) ->
    {ReturnCode, State1} =
        case {ProtoVersion =:= ?MQTT_PROTO_MAJOR, valid_client_id(ClientId)} of
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
    send_frame(#mqtt_frame{ fixed = #mqtt_frame_fixed {type = ?CONNACK},
                            variable = #mqtt_frame_connack {
                                         return_code = ReturnCode }}, State),
    {noreply, State1, hibernate}.

process_request(?PUBLISH,
                #mqtt_frame {
                  variable = #mqtt_frame_publish { topic_name = TopicName,
                                                   message_id = _MessageId },
                  payload = Payload }, #state { channel = Channel } = State) ->
    Method = #'basic.publish'{ exchange    = ?DEFAULT_EXCHANGE,
                               routing_key = translate_topic(TopicName)},
    amqp_channel:cast(Channel, Method, #amqp_msg{payload = Payload}),
    {noreply, State, hibernate};

process_request(?SUBSCRIBE,
                #mqtt_frame {
                  variable = #mqtt_frame_subscribe { message_id = MessageId,
                                                     topic_table = Topics },
                  payload = undefined }, #state { channel = Channel,
                                                  client_id = ClientId} = State) ->
    Queue = subcription_queue_name(ClientId),
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{ queue = Queue,
                                                     exclusive = false,
                                                     auto_delete = false}),
    QosResponse =
        [begin
            Binding = #'queue.bind'{queue       = Queue,
                                    exchange    = ?DEFAULT_EXCHANGE,
                                    routing_key = translate_topic(TopicName)},
            #'queue.bind_ok'{} = amqp_channel:call(Channel, Binding),
            ?QOS_0
         end || #mqtt_topic { name = TopicName } <- Topics ],
    Method = #'basic.consume'{queue = Queue},
    #'basic.consume_ok'{consumer_tag = _Tag} = amqp_channel:call(Channel, Method),
    send_frame(#mqtt_frame{ fixed = #mqtt_frame_fixed {type = ?SUBACK},
                            variable = #mqtt_frame_suback {
                                         message_id = MessageId,
                                         qos_table = QosResponse }}, State),
    {noreply, State, hibernate};

process_request(?UNSUBSCRIBE,
                #mqtt_frame {
                  variable = #mqtt_frame_subscribe { message_id = MessageId,
                                                     topic_table = Topics },
                  payload = undefined }, #state { channel = Channel,
                                                  client_id = ClientId} = State) ->
    Queue = subcription_queue_name(ClientId),
    [begin
        Binding = #'queue.unbind'{queue       = Queue,
                                  exchange    = ?DEFAULT_EXCHANGE,
                                  routing_key = translate_topic(TopicName)},
        #'queue.unbind_ok'{} = amqp_channel:call(Channel, Binding)
     end || #mqtt_topic { name = TopicName } <- Topics ],
    send_frame(#mqtt_frame{ fixed    = #mqtt_frame_fixed  {type = ?UNSUBACK},
                            variable = #mqtt_frame_suback {
                                         message_id = MessageId }}, State),
    {noreply, State, hibernate};

process_request(?PINGREQ, _, State) ->
    send_frame(#mqtt_frame{ fixed = #mqtt_frame_fixed {type = ?PINGRESP}}, State),
    {noreply, State, hibernate};

process_request(?DISCONNECT, _, State) ->
    {stop, normal, close_connection(State)}.

subcription_queue_name(ClientId) ->
    list_to_binary("MQTT_subscription_" ++ ClientId).

%% amqp mqtt descr
%% *    +    match one topic level
%% #    #    match multiple topic levels
%% .    /    topic level separator
translate_topic(Topic) ->
    erlang:iolist_to_binary(
      re:replace(re:replace(Topic, "/", ".", [global]),
                 "[\+]", "*", [global])).

untranslate_topic(Topic) ->
    erlang:iolist_to_binary(
      re:replace(re:replace(Topic, "[\*]", "+", [global]),
                 "[\.]", "/", [global])).

valid_client_id(ClientId) ->
    ClientIdLen = length(ClientId),
    1 =< ClientIdLen andalso ClientIdLen =< 23.

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

send_frame(Frame, State = #state{send_fun = SendFun}) ->
    SendFun(async, rabbit_mqtt_frame:serialise(Frame)),
    State.

%% Closing the connection will close the channel and subchannels
close_connection(State = #state{connection = Connection}) ->
    %% ignore noproc or other exceptions to avoid debris
    catch amqp_connection:close(Connection),
    State#state{channel = none, connection = none}.

%%----------------------------------------------------------------------------
%% Skeleton gen_server2 callbacks
%%----------------------------------------------------------------------------
handle_call(_Msg, _From, State) ->
    {noreply, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

