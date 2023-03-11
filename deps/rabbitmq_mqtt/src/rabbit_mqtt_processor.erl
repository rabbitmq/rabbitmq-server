%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% This module contains code that is common to MQTT and Web MQTT connections.
-module(rabbit_mqtt_processor).

-feature(maybe_expr, enable).

-export([info/2, init/4, process_packet/2,
         terminate/4, handle_pre_hibernate/0,
         handle_ra_event/2, handle_down/2, handle_queue_event/2,
         proto_version_tuple/1, throttle/2, format_status/1,
         remove_duplicate_client_id_connections/2,
         update_trace/2]).

-ifdef(TEST).
-export([get_vhost_username/1, get_vhost/3, get_vhost_from_user_mapping/2]).
-endif.

-export_type([state/0,
              send_fun/0]).

-import(rabbit_mqtt_util, [mqtt_to_amqp/1,
                           amqp_to_mqtt/1,
                           ip_address_to_binary/1]).

-include_lib("kernel/include/logger.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("rabbit/include/amqqueue.hrl").
-include("rabbit_mqtt.hrl").
-include("rabbit_mqtt_packet.hrl").

-define(MAX_PERMISSION_CACHE_SIZE, 12).
-define(CONSUMER_TAG, <<"mqtt">>).

-type send_fun() :: fun((iodata()) -> ok).

-record(auth_state,
        {user :: #user{},
         authz_ctx :: #{binary() := binary()}
        }).

-record(cfg,
        {socket :: rabbit_net:socket(),
         proto_ver :: mqtt310 | mqtt311,
         clean_sess :: boolean(),
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
         client_id :: binary(),
         conn_name :: option(binary()),
         ip_addr :: inet:ip_address(),
         port :: inet:port_number(),
         peer_ip_addr :: inet:ip_address(),
         peer_port :: inet:port_number(),
         connected_at = os:system_time(milli_seconds) :: pos_integer(),
         send_fun :: send_fun()
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
         subscriptions = #{} :: #{Topic :: binary() => QoS :: ?QOS_0..?QOS_1},
         auth_state = #auth_state{},
         ra_register_state :: option(registered | {pending, reference()}),
         %% quorum queues and streams whose soft limit has been exceeded
         queues_soft_limit_exceeded = sets:new([{version, 2}]) :: sets:set(),
         qos0_messages_dropped = 0 :: non_neg_integer()
        }).

-opaque state() :: #state{}.

%% NB: If init/4 returns an error, it must clean up itself because terminate/4 will not be called.
-spec init(ConnectPacket :: mqtt_packet(),
           RawSocket :: rabbit_net:socket(),
           ConnectionName :: binary(),
           SendFun :: send_fun()) ->
    {ok, state()} | {error, {socket_ends, any()} | connack_return_code()}.
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
     clean_sess = CleanSess,
     client_id  = ClientId0,
     keep_alive = KeepaliveSecs} = Packet,
  Socket, ConnName0, SendFun, {PeerIp, PeerPort, Ip, Port}) ->
    ?LOG_DEBUG("Received a CONNECT, client ID: ~s, username: ~s, "
               "clean session: ~s, protocol version: ~p, keepalive: ~p",
               [ClientId0, Username0, CleanSess, ProtoVer, KeepaliveSecs]),
    SslLoginName = ssl_login_name(Socket),
    ProtoVerAtom = proto_integer_to_atom(ProtoVer),
    Flow = case rabbit_misc:get_env(rabbit, mirroring_flow_control, true) of
               true   -> flow;
               false  -> noflow
           end,
    Result0 =
    maybe
        ok ?= check_protocol_version(ProtoVer),
        {ok, ClientId} ?= ensure_client_id(ClientId0, CleanSess),
        {ok, {Username1, Password}} ?= check_credentials(Username0, Password0, SslLoginName, PeerIp),

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
        rabbit_core_metrics:auth_attempt_succeeded(PeerIp, Username, mqtt),
        {ok, RaRegisterState} ?= register_client_id(VHost, ClientId),
        {TraceState, ConnName} = init_trace(VHost, ConnName0),
        ok = rabbit_mqtt_keepalive:start(KeepaliveSecs, Socket),
        self() ! connection_created,
        {ok,
         #state{
            cfg = #cfg{socket = Socket,
                       proto_ver = ProtoVerAtom,
                       clean_sess = CleanSess,
                       ssl_login_name = SslLoginName,
                       delivery_flow = Flow,
                       trace_state = TraceState,
                       prefetch = rabbit_mqtt_util:env(prefetch),
                       conn_name = ConnName,
                       ip_addr = Ip,
                       port = Port,
                       peer_ip_addr = PeerIp,
                       peer_port = PeerPort,
                       send_fun = SendFun,
                       exchange = rabbit_misc:r(VHost, exchange, rabbit_mqtt_util:env(exchange)),
                       retainer_pid = rabbit_mqtt_retainer_sup:start_child_for_vhost(VHost),
                       vhost = VHost,
                       client_id = ClientId,
                       will_msg = make_will_msg(Packet)},
            auth_state = #auth_state{
                            user = User,
                            authz_ctx = AuthzCtx},
            ra_register_state = RaRegisterState}}
    end,
    Result = case Result0 of
                 {ok, State0 = #state{}} ->
                     process_connect(State0);
                 {error, _} = Err0 ->
                     Err0
             end,
    case Result of
        {ok, SessPresent, State = #state{}} ->
            send_conn_ack(?CONNACK_ACCEPT, SessPresent, ProtoVerAtom, SendFun),
            {ok, State};
        {error, ReturnErrCode} = Err
          when is_integer(ReturnErrCode) ->
            %% If a server sends a CONNACK packet containing a non-zero return
            %% code it MUST set Session Present to 0 [MQTT-3.2.2-4].
            SessPresent = false,
            send_conn_ack(ReturnErrCode, SessPresent, ProtoVerAtom, SendFun),
            Err
    end.

send_conn_ack(ReturnCode, SessPresent, ProtoVer, SendFun) ->
    Packet = #mqtt_packet{fixed = #mqtt_packet_fixed{type = ?CONNACK},
                          variable = #mqtt_packet_connack{
                                        session_present = SessPresent,
                                        return_code = ReturnCode}},
    ok = send(Packet, ProtoVer, SendFun).

process_connect(State0) ->
    maybe
        {ok, QoS0SessPresent, State1} ?= handle_clean_sess_qos0(State0),
        {ok, SessPresent, State2} ?= handle_clean_sess_qos1(QoS0SessPresent, State1),
        State = cache_subscriptions(SessPresent, State2),
        rabbit_networking:register_non_amqp_connection(self()),
        {ok, SessPresent, State}
    else
        {error, _} = Error ->
            unregister_client(State0),
            Error
    end.

-spec process_packet(mqtt_packet(), state()) ->
    {ok, state()} |
    {stop, disconnect, state()} |
    {error, Reason :: term(), state()}.
process_packet(Packet = #mqtt_packet{fixed = #mqtt_packet_fixed{type = Type}},
               State = #state{auth_state = #auth_state{}})
  when Type =/= ?CONNECT ->
    process_request(Type, Packet, State).

-spec process_request(packet_type(), mqtt_packet(), state()) ->
    {ok, state()} |
    {stop, disconnect, state()} |
    {error, Reason :: term(), state()}.
process_request(?PUBACK,
                #mqtt_packet{variable = #mqtt_packet_publish{packet_id = PacketId}},
                #state{unacked_server_pubs = U0,
                       queue_states = QStates0,
                       cfg = #cfg{queue_qos1 = QName}} = State) ->
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
                #mqtt_packet{
                   fixed = #mqtt_packet_fixed{qos = Qos,
                                              retain = Retain,
                                              dup = Dup },
                   variable = #mqtt_packet_publish{topic_name = Topic,
                                                   packet_id = PacketId },
                   payload = Payload},
                State0 = #state{unacked_client_pubs = U,
                                cfg = #cfg{proto_ver = ProtoVer}}) ->
    EffectiveQos = maybe_downgrade_qos(Qos),
    rabbit_global_counters:messages_received(ProtoVer, 1),
    State = maybe_increment_publisher(State0),
    Msg = #mqtt_msg{retain     = Retain,
                    qos        = EffectiveQos,
                    topic      = Topic,
                    dup        = Dup,
                    packet_id  = PacketId,
                    payload    = Payload},
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

process_request(?SUBSCRIBE,
                #mqtt_packet{
                   variable = #mqtt_packet_subscribe{
                                 packet_id  = SubscribePktId,
                                 topic_table = Topics},
                   payload = undefined},
                #state{cfg = #cfg{retainer_pid = RPid}} = State0) ->
    ?LOG_DEBUG("Received a SUBSCRIBE for topic(s) ~p", [Topics]),
    {QosResponse, State1} =
    lists:foldl(
      fun(_Topic, {[?SUBACK_FAILURE | _] = L, S}) ->
              %% Once a subscription failed, mark all following subscriptions
              %% as failed instead of creating bindings because we are going
              %% to close the client connection anyway.
              {[?SUBACK_FAILURE | L], S};
         (#mqtt_topic{name = TopicName,
                      qos = TopicQos},
          {L, S0}) ->
              QoS = maybe_downgrade_qos(TopicQos),
              maybe
                  ok ?= maybe_replace_old_sub(TopicName, QoS, S0),
                  {ok, Q} ?= ensure_queue(QoS, S0),
                  QName = amqqueue:get_name(Q),
                  ok ?= bind(QName, TopicName, S0),
                  Subs = maps:put(TopicName, QoS, S0#state.subscriptions),
                  S1 = S0#state{subscriptions = Subs},
                  maybe_increment_consumer(S0, S1),
                  case self_consumes(Q) of
                      false ->
                          case consume(Q, QoS, S1) of
                              {ok, S2} ->
                                  {[QoS | L], S2};
                              {error, _} ->
                                  {[?SUBACK_FAILURE | L], S1}
                          end;
                      true ->
                          {[QoS | L], S1}
                  end
              else
                  {error, _} -> {[?SUBACK_FAILURE | L], S0}
              end
      end, {[], State0}, Topics),
    Reply = #mqtt_packet{fixed    = #mqtt_packet_fixed{type = ?SUBACK},
                         variable = #mqtt_packet_suback{
                                       packet_id = SubscribePktId,
                                       qos_table  = QosResponse}},
    send(Reply, State1),
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
                State0) ->
    ?LOG_DEBUG("Received an UNSUBSCRIBE for topic(s) ~p", [Topics]),
    State = lists:foldl(
              fun(#mqtt_topic{name = TopicName}, #state{subscriptions = Subs0} = S0) ->
                      case maps:take(TopicName, Subs0) of
                          {QoS, Subs} ->
                              QName = queue_name(QoS, S0),
                              case unbind(QName, TopicName, S0) of
                                  ok ->
                                      S = S0#state{subscriptions = Subs},
                                      maybe_decrement_consumer(S0, S),
                                      S;
                                  {error, _} ->
                                      S0
                              end;
                          error ->
                              S0
                      end
              end, State0, Topics),
    Reply = #mqtt_packet{fixed = #mqtt_packet_fixed{type = ?UNSUBACK},
                         variable = #mqtt_packet_suback{packet_id = PacketId}},
    send(Reply, State),
    {ok, State};

process_request(?PINGREQ, #mqtt_packet{}, State = #state{cfg = #cfg{client_id = ClientId}}) ->
    ?LOG_DEBUG("Received a PINGREQ from client ID ~s", [ClientId]),
    Reply = #mqtt_packet{fixed = #mqtt_packet_fixed{type = ?PINGRESP}},
    send(Reply, State),
    ?LOG_DEBUG("Sent a PINGRESP to client ID ~s", [ClientId]),
    {ok, State};

process_request(?DISCONNECT, #mqtt_packet{}, State) ->
    ?LOG_DEBUG("Received a DISCONNECT"),
    {stop, disconnect, State}.

check_protocol_version(ProtoVersion) ->
    case lists:member(ProtoVersion, proplists:get_keys(?PROTOCOL_NAMES)) of
        true ->
            ok;
        false ->
            ?LOG_ERROR("unacceptable MQTT protocol version: ~p", [ProtoVersion]),
            {error, ?CONNACK_UNACCEPTABLE_PROTO_VER}
    end.

check_credentials(Username, Password, SslLoginName, PeerIp) ->
    case creds(Username, Password, SslLoginName) of
        nocreds ->
            auth_attempt_failed(PeerIp, <<>>),
            ?LOG_ERROR("MQTT login failed: no credentials provided"),
            {error, ?CONNACK_BAD_CREDENTIALS};
        {invalid_creds, {undefined, Pass}} when is_binary(Pass) ->
            auth_attempt_failed(PeerIp, <<>>),
            ?LOG_ERROR("MQTT login failed: no username is provided"),
            {error, ?CONNACK_BAD_CREDENTIALS};
        {invalid_creds, {User, undefined}} when is_binary(User) ->
            auth_attempt_failed(PeerIp, User),
            ?LOG_ERROR("MQTT login failed for user '~p': no password provided", [User]),
            {error, ?CONNACK_BAD_CREDENTIALS};
        {UserBin, PassBin} ->
            {ok, {UserBin, PassBin}}
    end.

ensure_client_id(<<>>, _CleanSess = false) ->
    ?LOG_ERROR("MQTT client ID must be provided for non-clean session"),
    {error, ?CONNACK_ID_REJECTED};
ensure_client_id(<<>>, _CleanSess = true) ->
    {ok, rabbit_data_coercion:to_binary(
           rabbit_misc:base64url(
             rabbit_guid:gen_secure()))};
ensure_client_id(ClientId, _CleanSess)
  when is_binary(ClientId) ->
    {ok, ClientId}.

-spec register_client_id(rabbit_types:vhost(), binary()) ->
    {ok, RaRegisterState :: undefined | {pending, reference()}} |
    {error, ConnAckErrorCode :: pos_integer()}.
register_client_id(VHost, ClientId)
  when is_binary(VHost), is_binary(ClientId) ->
    %% Always register client ID in pg.
    PgGroup = {VHost, ClientId},
    ok = pg:join(persistent_term:get(?PG_SCOPE), PgGroup, self()),

    case rabbit_mqtt_ff:track_client_id_in_ra() of
        true ->
            case rabbit_mqtt_collector:register(ClientId, self()) of
                {ok, Corr} ->
                    %% Ra node takes care of removing duplicate client ID connections.
                    {ok, {pending, Corr}};
                {error, _} = Err ->
                    %% e.g. this node was removed from the MQTT cluster members
                    ?LOG_ERROR("MQTT connection failed to register client ID ~s in vhost ~s in Ra: ~p",
                               [ClientId, VHost, Err]),
                    {error, ?CONNACK_SERVER_UNAVAILABLE}
            end;
        false ->
            ok = erpc:multicast([node() | nodes()],
                                ?MODULE,
                                remove_duplicate_client_id_connections,
                                [PgGroup, self()]),
            {ok, undefined}
    end.

-spec remove_duplicate_client_id_connections({rabbit_types:vhost(), binary()}, pid()) -> ok.
remove_duplicate_client_id_connections(PgGroup, PidToKeep) ->
    try persistent_term:get(?PG_SCOPE) of
        PgScope ->
            Pids = pg:get_local_members(PgScope, PgGroup),
            lists:foreach(fun(Pid) ->
                                  gen_server:cast(Pid, duplicate_id)
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

handle_clean_sess_qos0(State) ->
    handle_clean_sess(false, ?QOS_0, State).

handle_clean_sess_qos1(QoS0SessPresent, State) ->
    handle_clean_sess(QoS0SessPresent, ?QOS_1, State).

handle_clean_sess(_, QoS,
                  State = #state{cfg = #cfg{clean_sess = true},
                                 auth_state = #auth_state{user = User = #user{username = Username},
                                                          authz_ctx = AuthzCtx}}) ->
    %% "If the Server accepts a connection with CleanSession set to 1, the Server
    %% MUST set Session Present to 0 in the CONNACK packet [MQTT-3.2.2-1].
    SessPresent = false,
    case get_queue(QoS, State) of
        {error, _} ->
            {ok, SessPresent, State};
        {ok, Q0} ->
            QName = amqqueue:get_name(Q0),
            %% configure access to queue required for queue.delete
            case check_resource_access(User, QName, configure, AuthzCtx) of
                ok ->
                    delete_queue(QName, Username),
                    {ok, SessPresent, State};
                {error, access_refused} ->
                    {error, ?CONNACK_NOT_AUTHORIZED}
            end
    end;
handle_clean_sess(SessPresent, QoS,
                  State0 = #state{cfg = #cfg{clean_sess = false}}) ->
    case get_queue(QoS, State0) of
        {error, _} ->
            %% Queue will be created later when client subscribes.
            {ok, SessPresent, State0};
        {ok, Q} ->
            case consume(Q, QoS, State0) of
                {ok, State} ->
                    {ok, _SessionPresent = true, State};
                {error, access_refused} ->
                    {error, ?CONNACK_NOT_AUTHORIZED};
                {error, _Reason} ->
                    %% Let's use most generic error return code.
                    {error, ?CONNACK_SERVER_UNAVAILABLE}
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

queue_name(?QOS_1, #state{cfg = #cfg{queue_qos1 = #resource{kind = queue} = Name}}) ->
    Name;
queue_name(QoS, #state{cfg = #cfg{client_id = ClientId,
                                  vhost = VHost}}) ->
    QNameBin = rabbit_mqtt_util:queue_name_bin(ClientId, QoS),
    rabbit_misc:r(VHost, queue, QNameBin).

%% Query subscriptions from the database and hold them in process state
%% to avoid future mnesia:match_object/3 queries.
cache_subscriptions(_SessionPresent = _SubscriptionsPresent = true,
                    State = #state{cfg = #cfg{proto_ver = ProtoVer}}) ->
    SubsQos0 = topic_names(?QOS_0, State),
    SubsQos1 = topic_names(?QOS_1, State),
    Subs = maps:merge(maps:from_keys(SubsQos0, ?QOS_0),
                      maps:from_keys(SubsQos1, ?QOS_1)),
    rabbit_global_counters:consumer_created(ProtoVer),
    State#state{subscriptions = Subs};
cache_subscriptions(_, State) ->
    State.

topic_names(QoS, State = #state{cfg = #cfg{exchange = Exchange}}) ->
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
    lists:map(fun(B) -> amqp_to_mqtt(B#binding.key) end, Bindings).

%% "If a Server receives a SUBSCRIBE Packet containing a Topic Filter that is identical
%% to an existing Subscription’s Topic Filter then it MUST completely replace that
%% existing Subscription with a new Subscription. The Topic Filter in the new Subscription
%% will be identical to that in the previous Subscription, although its maximum QoS value
%% could be different." [MQTT-3.8.4-3].
maybe_replace_old_sub(TopicName, QoS, State = #state{subscriptions = Subs}) ->
    case Subs of
        #{TopicName := OldQoS} when OldQoS =/= QoS ->
            QName = queue_name(OldQoS, State),
            unbind(QName, TopicName, State);
        _ ->
            ok
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
                            State0 = #state{packet_id = PacketId0}) ->
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
            Packet = #mqtt_packet{fixed = #mqtt_packet_fixed{
                                             type = ?PUBLISH,
                                             qos  = Qos,
                                             dup  = false,
                                             retain = Msg#mqtt_msg.retain
                                            },
                                  variable = #mqtt_packet_publish{
                                                packet_id = PacketId,
                                                topic_name = Topic1
                                               },
                                  payload = Msg#mqtt_msg.payload},
            send(Packet, State),
            State
    end.

make_will_msg(#mqtt_packet_connect{will_flag = false}) ->
    undefined;
make_will_msg(#mqtt_packet_connect{will_flag = true,
                                   will_retain = Retain,
                                   will_qos = Qos,
                                   will_topic = Topic,
                                   will_msg = Msg}) ->
    EffectiveQos = maybe_downgrade_qos(Qos),
    Correlation = case EffectiveQos of
                      ?QOS_0 -> undefined;
                      ?QOS_1 -> ?WILL_MSG_QOS_1_CORRELATION
                  end,
    #mqtt_msg{retain = Retain,
              qos = EffectiveQos,
              packet_id = Correlation,
              topic = Topic,
              dup = false,
              payload = Msg}.

check_vhost_exists(VHost, Username, PeerIp) ->
    case rabbit_vhost:exists(VHost) of
        true  ->
            ok;
        false ->
            auth_attempt_failed(PeerIp, Username),
            ?LOG_ERROR("MQTT connection failed: virtual host '~s' does not exist", [VHost]),
            {error, ?CONNACK_BAD_CREDENTIALS}
    end.

check_vhost_connection_limit(VHost) ->
    case rabbit_vhost_limit:is_over_connection_limit(VHost) of
        false ->
            ok;
        {true, Limit} ->
            ?LOG_ERROR("MQTT connection failed: connection limit ~p is reached for vhost '~s'",
                       [Limit, VHost]),
            {error, ?CONNACK_NOT_AUTHORIZED}
    end.

check_vhost_alive(VHost) ->
    case rabbit_vhost_sup_sup:is_vhost_alive(VHost) of
        true  ->
            ok;
        false ->
            ?LOG_ERROR("MQTT connection failed: vhost '~s' is down", [VHost]),
            {error, ?CONNACK_NOT_AUTHORIZED}
    end.

check_user_login(VHost, Username, Password, ClientId, PeerIp, ConnName) ->
    AuthProps = case Password of
                    none ->
                        %% SSL user name provided.
                        %% Authenticating using username only.
                        [];
                    _ ->
                        [{password, Password},
                         {vhost, VHost},
                         {client_id, ClientId}]
                end,
    case rabbit_access_control:check_user_login(Username, AuthProps) of
        {ok, User = #user{username = Username1}} ->
            notify_auth_result(user_authentication_success, Username1, ConnName),
            {ok, User};
        {refused, Username, Msg, Args} ->
            auth_attempt_failed(PeerIp, Username),
            ?LOG_ERROR("MQTT connection failed: access refused for user '~s':" ++ Msg,
                       [Username | Args]),
            notify_auth_result(user_authentication_failure, Username, ConnName),
            {error, ?CONNACK_BAD_CREDENTIALS}
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
            {error, ?CONNACK_NOT_AUTHORIZED}
    end.


check_vhost_access(VHost, User = #user{username = Username}, ClientId, PeerIp) ->
    AuthzCtx = #{<<"client_id">> => ClientId},
    try rabbit_access_control:check_vhost_access(
          User, VHost, {ip, PeerIp}, AuthzCtx) of
        ok ->
            {ok, AuthzCtx}
    catch exit:#amqp_error{name = not_allowed} ->
              auth_attempt_failed(PeerIp, Username),
              ?LOG_ERROR("MQTT connection failed: access refused for user '~s' to vhost '~s'",
                         [Username, VHost]),
              {error, ?CONNACK_NOT_AUTHORIZED}
    end.

check_user_loopback(Username, PeerIp) ->
    case rabbit_access_control:check_user_loopback(Username, PeerIp) of
        ok ->
            ok;
        not_allowed ->
            auth_attempt_failed(PeerIp, Username),
            ?LOG_WARNING(
              "MQTT login failed: user '~s' can only connect via localhost", [Username]),
            {error, ?CONNACK_NOT_AUTHORIZED}
    end.

get_vhost(UserBin, none, Port) ->
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
                    {plugin_configuration_or_default_vhost, {rabbit_mqtt_util:env(vhost), UserBin}};
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
            {client_cert_to_vhost_mapping, {VHostFromCertMapping, UserBin}}
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

-spec auth_attempt_failed(inet:ip_address(), binary()) -> ok.
auth_attempt_failed(PeerIp, Username) ->
    rabbit_core_metrics:auth_attempt_failed(PeerIp, Username, mqtt).

delivery_mode(?QOS_0) -> 1;
delivery_mode(?QOS_1) -> 2;
delivery_mode(?QOS_2) -> 2.

maybe_downgrade_qos(?QOS_0) -> ?QOS_0;
maybe_downgrade_qos(?QOS_1) -> ?QOS_1;
maybe_downgrade_qos(?QOS_2) -> ?QOS_1.

ensure_queue(QoS, State = #state{auth_state = #auth_state{user = #user{username = Username}}}) ->
    case get_queue(QoS, State) of
        {ok, Q} ->
            {ok, Q};
        {error, {resource_locked, Q}} ->
            QName = amqqueue:get_name(Q),
            ?LOG_DEBUG("MQTT deleting exclusive ~s owned by ~p",
                       [rabbit_misc:rs(QName), ?amqqueue_v2_field_exclusive_owner(Q)]),
            delete_queue(QName, Username),
            create_queue(QoS, State);
        {error, not_found} ->
            create_queue(QoS, State)
    end.

create_queue(
  QoS, #state{cfg = #cfg{
                       vhost = VHost,
                       client_id = ClientId,
                       clean_sess = CleanSess},
              auth_state = #auth_state{
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
                                      queue_owner(CleanSess),
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
                            ?LOG_ERROR("Failed to declare ~s: ~p",
                                       [rabbit_misc:rs(QName), Other]),
                            {error, queue_declare}
                    end;
                {true, Limit} ->
                    ?LOG_ERROR("cannot declare ~s because "
                               "queue limit ~p in vhost '~s' is reached",
                               [rabbit_misc:rs(QName), Limit, VHost]),
                    {error, access_refused}
            end;
        {error, access_refused} = E ->
            E
    end.

-spec queue_owner(CleanSession :: boolean()) ->
    pid() | none.
queue_owner(true) ->
    %% Exclusive queues are auto-deleted after node restart while auto-delete queues are not.
    %% Therefore make durable queue exclusive.
    self();
queue_owner(false) ->
    none.

queue_args(QoS, false) ->
    Args = case rabbit_mqtt_util:env(subscription_ttl) of
               Ms when is_integer(Ms) ->
                   [{<<"x-expires">>, long, Ms}];
               _ ->
                   []
           end,
    case {QoS, rabbit_mqtt_util:env(durable_queue_type)} of
        {?QOS_1, quorum} ->
            [{<<"x-queue-type">>, longstr, <<"quorum">>} | Args];
        _ ->
            Args
    end;
queue_args(_, _) ->
    [].

queue_type(?QOS_0, true, QArgs) ->
    case rabbit_queue_type:is_enabled(?QUEUE_TYPE_QOS_0) of
        true ->
            ?QUEUE_TYPE_QOS_0;
        false ->
            rabbit_amqqueue:get_queue_type(QArgs)
    end;
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
                                      ?LOG_ERROR("Failed to consume from ~s: ~p",
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
maybe_set_queue_qos1(?QOS_1, State = #state{cfg = Cfg = #cfg{queue_qos1 = undefined}}) ->
    State#state{cfg = Cfg#cfg{queue_qos1 = queue_name(?QOS_1, State)}};
maybe_set_queue_qos1(_, State) ->
    State.

bind(QName, TopicName, State) ->
    binding_action_with_checks(QName, TopicName, add, State).

unbind(QName, TopicName, State) ->
    binding_action_with_checks(QName, TopicName, remove, State).

binding_action_with_checks(QName, TopicName, Action,
                           State = #state{cfg = #cfg{exchange = ExchangeName},
                                          auth_state = AuthState}) ->
    %% Same permissions required for binding or unbinding queue to/from topic exchange.
    maybe
        ok ?= check_queue_write_access(QName, AuthState),
        ok ?= check_exchange_read_access(ExchangeName, AuthState),
        ok ?= check_topic_access(TopicName, read, State),
        ok ?= binding_action(ExchangeName, TopicName, QName, fun rabbit_binding:Action/2, AuthState)
    else
        {error, Reason} = Err ->
            ?LOG_ERROR("Failed to ~s binding between ~s and ~s for topic ~s: ~p",
                       [Action, rabbit_misc:rs(ExchangeName), rabbit_misc:rs(QName), TopicName, Reason]),
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

binding_action(ExchangeName, TopicName, QName, BindingFun, #auth_state{user = #user{username = Username}}) ->
    RoutingKey = mqtt_to_amqp(TopicName),
    Binding = #binding{source = ExchangeName,
                       destination = QName,
                       key = RoutingKey},
    BindingFun(Binding, Username).

publish_to_queues(
  #mqtt_msg{qos        = Qos,
            topic      = Topic,
            packet_id  = PacketId,
            payload    = Payload},
  #state{cfg = #cfg{exchange = ExchangeName,
                    delivery_flow = Flow,
                    conn_name = ConnName,
                    trace_state = TraceState},
         auth_state = #auth_state{user = #user{username = Username}}
        } = State) ->
    RoutingKey = mqtt_to_amqp(Topic),
    Confirm = Qos > ?QOS_0,
    Props = #'P_basic'{
               headers = [{<<"x-mqtt-publish-qos">>, byte, Qos}],
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
            rabbit_trace:tap_in(BasicMessage, QNames, ConnName, Username, TraceState),
            deliver_to_queues(Delivery, QNames, State);
        {error, not_found} ->
            ?LOG_ERROR("~s not found", [rabbit_misc:rs(ExchangeName)]),
            {error, exchange_not_found, State}
    end.

deliver_to_queues(Delivery,
                  RoutedToQNames,
                  State0 = #state{queue_states = QStates0,
                                  cfg = #cfg{proto_ver = ProtoVer}}) ->
    Qs0 = rabbit_amqqueue:lookup_many(RoutedToQNames),
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
            ?LOG_ERROR("Failed to deliver message with packet_id=~p to queues: ~p",
                       [Delivery#delivery.msg_seq_no, Reason]),
            {error, Reason, State0}
    end.

process_routing_confirm(#delivery{confirm = false},
                        [], State = #state{cfg = #cfg{proto_ver = ProtoVer}}) ->
    rabbit_global_counters:messages_unroutable_dropped(ProtoVer, 1),
    State;
process_routing_confirm(#delivery{confirm = true,
                                  msg_seq_no = ?WILL_MSG_QOS_1_CORRELATION},
                        [], State = #state{cfg = #cfg{proto_ver = ProtoVer}}) ->
    %% unroutable will message with QoS 1
    rabbit_global_counters:messages_unroutable_dropped(ProtoVer, 1),
    State;
process_routing_confirm(#delivery{confirm = true,
                                  msg_seq_no = PktId},
                        [], State = #state{cfg = #cfg{proto_ver = ProtoVer}}) ->
    rabbit_global_counters:messages_unroutable_returned(ProtoVer, 1),
    %% MQTT 5 spec:
    %% If the Server knows that there are no matching subscribers, it MAY use
    %% Reason Code 0x10 (No matching subscribers) instead of 0x00 (Success).
    send_puback(PktId, State),
    State;
process_routing_confirm(#delivery{confirm = false}, _, State) ->
    State;
process_routing_confirm(#delivery{confirm = true,
                                  msg_seq_no = ?WILL_MSG_QOS_1_CORRELATION}, [_|_], State) ->
    %% routable will message with QoS 1
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
send_puback(PktId, State = #state{cfg = #cfg{proto_ver = ProtoVer}}) ->
    rabbit_global_counters:messages_confirmed(ProtoVer, 1),
    Packet = #mqtt_packet{fixed = #mqtt_packet_fixed{type = ?PUBACK},
                          variable = #mqtt_packet_publish{packet_id = PktId}},
    send(Packet, State).

-spec send(mqtt_packet(), state()) -> ok.
send(Packet, #state{cfg = #cfg{proto_ver = ProtoVer,
                               send_fun = SendFun}}) ->
    send(Packet, ProtoVer, SendFun).

-spec send(mqtt_packet(), mqtt310 | mqtt311, send_fun()) -> ok.
send(Packet, ProtoVer, SendFun) ->
    Data = rabbit_mqtt_packet:serialise(Packet, ProtoVer),
    ok = SendFun(Data).

-spec terminate(boolean(), binary(), atom(), state()) -> ok.
terminate(SendWill, ConnName, ProtoFamily,
          State = #state{cfg = #cfg{vhost = VHost},
                         auth_state = #auth_state{user = #user{username = Username}}
                        }) ->
    maybe_send_will(SendWill, ConnName, State),
    Infos = [{name, ConnName},
             {node, node()},
             {pid, self()},
             {disconnected_at, os:system_time(milli_seconds)},
             {protocol, {ProtoFamily, proto_version_tuple(State)}},
             {vhost, VHost},
             {user, Username}],
    rabbit_core_metrics:connection_closed(self()),
    rabbit_event:notify(connection_closed, Infos),
    rabbit_networking:unregister_non_amqp_connection(self()),
    unregister_client(State),
    maybe_decrement_consumer(State),
    maybe_decrement_publisher(State),
    maybe_delete_mqtt_qos0_queue(State).

-spec maybe_send_will(boolean(), binary(), state()) -> ok.
maybe_send_will(true, ConnStr,
                State = #state{cfg = #cfg{will_msg = WillMsg = #mqtt_msg{topic = Topic}}}) ->
    case publish_to_queues_with_checks(WillMsg, State) of
        {ok, _} ->
            ?LOG_DEBUG("sent MQTT will message to topic ~s on connection ~s",
                       [Topic, ConnStr]);
        {error, Reason, _} ->
            ?LOG_DEBUG("failed to send MQTT will message to topic ~s on connection ~s: ~p",
                       [Topic, ConnStr, Reason])
    end;
maybe_send_will(_, _, _) ->
    ok.

unregister_client(#state{cfg = #cfg{client_id = ClientId}}) ->
    case rabbit_mqtt_ff:track_client_id_in_ra() of
        true ->
            rabbit_mqtt_collector:unregister(ClientId, self());
        false ->
            ok
    end.

maybe_delete_mqtt_qos0_queue(
  State = #state{cfg = #cfg{clean_sess = true},
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

-spec handle_pre_hibernate() -> ok.
handle_pre_hibernate() ->
    erase(permission_cache),
    erase(topic_permission_cache),
    ok.

-spec handle_ra_event(register_timeout
| {applied, [{reference(), ok}]}
| {not_leader, term(), reference()}, state()) -> state().
handle_ra_event({applied, [{Corr, ok}]},
                State = #state{ra_register_state = {pending, Corr}}) ->
    %% success case - command was applied transition into registered state
    State#state{ra_register_state = registered};
handle_ra_event({not_leader, Leader, Corr},
                State = #state{ra_register_state = {pending, Corr},
                               cfg = #cfg{client_id = ClientId}}) ->
    case rabbit_mqtt_ff:track_client_id_in_ra() of
        true ->
            %% retry command against actual leader
            {ok, NewCorr} = rabbit_mqtt_collector:register(Leader, ClientId, self()),
            State#state{ra_register_state = {pending, NewCorr}};
        false ->
            State
    end;
handle_ra_event(register_timeout,
                State = #state{ra_register_state = {pending, _Corr},
                               cfg = #cfg{client_id = ClientId}}) ->
    case rabbit_mqtt_ff:track_client_id_in_ra() of
        true ->
            {ok, NewCorr} = rabbit_mqtt_collector:register(ClientId, self()),
            State#state{ra_register_state = {pending, NewCorr}};
        false ->
            State
    end;
handle_ra_event(register_timeout, State) ->
    State;
handle_ra_event(Evt, State) ->
    ?LOG_DEBUG("unhandled ra_event: ~w ", [Evt]),
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
          ({block, QName}, S = #state{queues_soft_limit_exceeded = QSLE}) ->
              S#state{queues_soft_limit_exceeded = sets:add_element(QName, QSLE)};
          ({unblock, QName}, S = #state{queues_soft_limit_exceeded = QSLE}) ->
              S#state{queues_soft_limit_exceeded = sets:del_element(QName, QSLE)};
          ({queue_down, QName}, S) ->
              handle_queue_down(QName, S)
      end, State0, Actions).

handle_queue_down(QName, State0 = #state{cfg = #cfg{client_id = ClientId}}) ->
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
                            ?LOG_INFO("Terminating MQTT connection because consuming ~s is down.",
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
      content = #content{payload_fragments_rev = FragmentsRev}}} = Msg,
  QoS, State0) ->
    {PacketId, State} = msg_id_to_packet_id(QMsgId, QoS, State0),
    Packet =
    #mqtt_packet{
       fixed = #mqtt_packet_fixed{
                  type = ?PUBLISH,
                  qos = QoS,
                  dup = Redelivered},
       variable = #mqtt_packet_publish{
                     packet_id = PacketId,
                     topic_name = amqp_to_mqtt(RoutingKey)},
       payload = lists:reverse(FragmentsRev)},
    send(Packet, State),
    trace_tap_out(Msg, State),
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
                            retainer_pid = RPid},
                 auth_state = #auth_state{user = User,
                                          authz_ctx = AuthzCtx}}) ->
    case check_resource_access(User, Exchange, write, AuthzCtx) of
        ok ->
            case check_topic_access(Topic, write, State) of
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
                    ?LOG_ERROR("MQTT resource access refused: ~s", [Msg]),
                    {error, access_refused}
            end
    end.

check_topic_access(
  TopicName, Access,
  #state{auth_state = #auth_state{user = User = #user{username = Username}},
         cfg = #cfg{client_id = ClientId,
                    vhost = VHost,
                    exchange = #resource{name = ExchangeBin}}}) ->
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
info(clean_sess, #state{cfg = #cfg{clean_sess = Val}}) -> Val;
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
info(vhost, #state{cfg = #cfg{vhost = Val}}) -> Val;
%% for rabbitmq_management/priv/www/js/tmpl/connection.ejs
info(client_properties, #state{cfg = #cfg{client_id = Val}}) ->
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

proto_integer_to_atom(3) ->
    ?MQTT_PROTO_V3;
proto_integer_to_atom(4) ->
    ?MQTT_PROTO_V4.

-spec proto_version_tuple(state()) -> tuple().
proto_version_tuple(#state{cfg = #cfg{proto_ver = ?MQTT_PROTO_V3}}) ->
    {3, 1, 0};
proto_version_tuple(#state{cfg = #cfg{proto_ver = ?MQTT_PROTO_V4}}) ->
    {3, 1, 1}.

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

-spec format_status(state()) -> map().
format_status(
  #state{queue_states = QState,
         unacked_client_pubs = UnackClientPubs,
         unacked_server_pubs = UnackSerPubs,
         packet_id = PackID,
         subscriptions = Subscriptions,
         auth_state = AuthState,
         ra_register_state = RaRegisterState,
         queues_soft_limit_exceeded = QSLE,
         qos0_messages_dropped = Qos0MsgsDropped,
         cfg = #cfg{
                  socket = Socket,
                  proto_ver = ProtoVersion,
                  clean_sess = CleanSess,
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
            clean_sess => CleanSess,
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
      ra_register_state => RaRegisterState,
      queues_soft_limit_exceeded => QSLE,
      qos0_messages_dropped => Qos0MsgsDropped}.
