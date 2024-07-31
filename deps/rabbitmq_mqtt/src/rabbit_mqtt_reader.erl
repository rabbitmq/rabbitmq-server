%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mqtt_reader).

-behaviour(gen_server).
-behaviour(ranch_protocol).

-include_lib("kernel/include/logger.hrl").
-include_lib("rabbit_common/include/logging.hrl").

-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, format_status/1]).

-export([conserve_resources/3,
         close_connection/2]).

-export([info/2]).

-include("rabbit_mqtt.hrl").
-include("rabbit_mqtt_packet.hrl").

-define(HIBERNATE_AFTER, 1000).

-record(state,
        {socket :: rabbit_net:socket(),
         proxy_socket :: option({rabbit_proxy_socket, any(), any()}),
         await_recv :: boolean(),
         deferred_recv :: option(binary()),
         parse_state :: rabbit_mqtt_packet:state(),
         proc_state = connect_packet_unprocessed :: connect_packet_unprocessed |
                                                    rabbit_mqtt_processor:state(),
         connection_state :: running | blocked,
         conserve :: boolean(),
         stats_timer :: option(rabbit_event:state()),
         keepalive = rabbit_mqtt_keepalive:init() :: rabbit_mqtt_keepalive:state(),
         conn_name :: binary()
        }).

-type(state() :: #state{}).

%%----------------------------------------------------------------------------

start_link(Ref, _Transport, []) ->
    Pid = proc_lib:spawn_link(?MODULE, init, [Ref]),
    {ok, Pid}.

-spec conserve_resources(pid(),
                         rabbit_alarm:resource_alarm_source(),
                         rabbit_alarm:resource_alert()) -> ok.
conserve_resources(Pid, _, {_, Conserve, _}) ->
    Pid ! {conserve_resources, Conserve},
    ok.

-spec info(pid(), rabbit_types:info_keys()) ->
    rabbit_types:infos().
info(Pid, Items) ->
    gen_server:call(Pid, {info, Items}).

-spec close_connection(pid(), Reason :: any()) -> ok.
close_connection(Pid, Reason) ->
    gen_server:cast(Pid, {close_connection, Reason}).

%%----------------------------------------------------------------------------

init(Ref) ->
    process_flag(trap_exit, true),
    logger:set_process_metadata(#{domain => ?RMQLOG_DOMAIN_CONN ++ [mqtt]}),
    ProxyProtocolEnabled = application:get_env(?APP_NAME, proxy_protocol, false),
    case rabbit_networking:handshake(Ref, ProxyProtocolEnabled) of
        {error, Reason} ->
            ?LOG_ERROR("MQTT could not establish connection: ~s", [Reason]),
            {stop, Reason};
        {ok, Sock} ->
            RealSocket = rabbit_net:unwrap_socket(Sock),
            case rabbit_net:connection_string(Sock, inbound) of
                {ok, ConnStr} ->
                    ConnName = rabbit_data_coercion:to_binary(ConnStr),
                    ?LOG_DEBUG("MQTT accepting TCP connection ~tp (~ts)", [self(), ConnName]),
                    _ = rabbit_alarm:register(self(), {?MODULE, conserve_resources, []}),
                    LoginTimeout = application:get_env(?APP_NAME, login_timeout, 10_000),
                    erlang:send_after(LoginTimeout, self(), login_timeout),
                    State0 = #state{socket = RealSocket,
                                    proxy_socket = rabbit_net:maybe_get_proxy_socket(Sock),
                                    conn_name = ConnName,
                                    await_recv = false,
                                    connection_state = running,
                                    conserve = false,
                                    parse_state = rabbit_mqtt_packet:init_state()},
                    State1 = control_throttle(State0),
                    State = rabbit_event:init_stats_timer(State1, #state.stats_timer),
                    gen_server:enter_loop(?MODULE, [], State);
                {error, Reason = enotconn} ->
                    ?LOG_INFO("MQTT could not get connection string: ~s", [Reason]),
                    rabbit_net:fast_close(RealSocket),
                    ignore;
                {error, Reason} ->
                    ?LOG_ERROR("MQTT could not get connection string: ~p", [Reason]),
                    rabbit_net:fast_close(RealSocket),
                    {stop, Reason}
            end
    end.

handle_call({info, InfoItems}, _From, State) ->
    {reply, infos(InfoItems, State), State, ?HIBERNATE_AFTER};

handle_call(Msg, From, State) ->
    {stop, {mqtt_unexpected_call, Msg, From}, State}.

handle_cast({duplicate_id, SendWill},
            State = #state{proc_state = PState,
                           conn_name = ConnName}) ->
    ?LOG_WARNING("MQTT disconnecting client ~tp with duplicate id '~ts'",
                 [ConnName, rabbit_mqtt_processor:info(client_id, PState)]),
    rabbit_mqtt_processor:send_disconnect(?RC_SESSION_TAKEN_OVER, PState),
    {stop, {shutdown, duplicate_id}, {SendWill, State}};

handle_cast({close_connection, Reason},
            State = #state{conn_name = ConnName,
                           proc_state = PState}) ->
    ?LOG_WARNING("MQTT disconnecting client ~tp with client ID '~ts', reason: ~ts",
                 [ConnName, rabbit_mqtt_processor:info(client_id, PState), Reason]),
    case Reason of
        maintenance -> rabbit_mqtt_processor:send_disconnect(?RC_SERVER_SHUTTING_DOWN, PState);
        _ -> ok
    end,
    {stop, {shutdown, {disconnect, server_initiated}}, State};

handle_cast(QueueEvent = {queue_event, _, _},
            State = #state{proc_state = PState0}) ->
    try rabbit_mqtt_processor:handle_queue_event(QueueEvent, PState0) of
        {ok, PState} ->
            maybe_process_deferred_recv(control_throttle(pstate(State, PState)));
        {error, Reason0, PState} ->
            {stop, Reason0, pstate(State, PState)}
    catch throw:{send_failed, Reason1} ->
              network_error(Reason1, State)
    end;

handle_cast({force_event_refresh, Ref}, State0) ->
    Infos = infos(?EVENT_KEYS, State0),
    rabbit_event:notify(connection_created, Infos, Ref),
    State = rabbit_event:init_stats_timer(State0, #state.stats_timer),
    {noreply, State, ?HIBERNATE_AFTER};

handle_cast(refresh_config, State = #state{proc_state = PState0,
                                           conn_name = ConnName}) ->
    PState = rabbit_mqtt_processor:update_trace(ConnName, PState0),
    {noreply, pstate(State, PState), ?HIBERNATE_AFTER};

handle_cast(Msg, State) ->
    {stop, {mqtt_unexpected_cast, Msg}, State}.

handle_info(connection_created, State) ->
    Infos = infos(?EVENT_KEYS, State),
    rabbit_core_metrics:connection_created(self(), Infos),
    rabbit_event:notify(connection_created, Infos),
    {noreply, State, ?HIBERNATE_AFTER};

handle_info(timeout, State) ->
    rabbit_mqtt_processor:handle_pre_hibernate(),
    {noreply, State, hibernate};

handle_info({'EXIT', _Conn, Reason}, State) ->
    {stop, {connection_died, Reason}, State};

handle_info({Tag, Sock, Data},
            State = #state{ socket = Sock, connection_state = blocked })
  when Tag =:= tcp; Tag =:= ssl ->
    {noreply, State#state{ deferred_recv = Data }, ?HIBERNATE_AFTER};

handle_info({Tag, Sock, Data},
            State = #state{ socket = Sock, connection_state = running })
            when Tag =:= tcp; Tag =:= ssl ->
    process_received_bytes(
      Data, control_throttle(State #state{ await_recv = false }));

handle_info({Tag, Sock}, State = #state{socket = Sock})
            when Tag =:= tcp_closed; Tag =:= ssl_closed ->
    network_error(closed, State);

handle_info({Tag, Sock, Reason}, State = #state{socket = Sock})
            when Tag =:= tcp_error; Tag =:= ssl_error ->
    network_error(Reason, State);

handle_info({conserve_resources, Conserve}, State) ->
    maybe_process_deferred_recv(
        control_throttle(State #state{ conserve = Conserve }));

handle_info({bump_credit, Msg}, State) ->
    credit_flow:handle_bump_msg(Msg),
    maybe_process_deferred_recv(control_throttle(State));

handle_info({keepalive, Req}, State = #state{proc_state = PState,
                                             keepalive = KState0,
                                             conn_name = ConnName}) ->
    case rabbit_mqtt_keepalive:handle(Req, KState0) of
        {ok, KState} ->
            {noreply, State#state{keepalive = KState}, ?HIBERNATE_AFTER};
        {error, timeout} ->
            ?LOG_ERROR("closing MQTT connection ~p (keepalive timeout)", [ConnName]),
            rabbit_mqtt_processor:send_disconnect(?RC_KEEP_ALIVE_TIMEOUT, PState),
            {stop, {shutdown, keepalive_timeout}, State};
        {error, Reason} ->
            {stop, Reason, State}
    end;

handle_info(credential_expired,
            State = #state{conn_name = ConnName,
                           proc_state = PState}) ->
    ?LOG_WARNING("MQTT disconnecting client ~tp with client ID '~ts' because credential expired",
                 [ConnName, rabbit_mqtt_processor:info(client_id, PState)]),
    rabbit_mqtt_processor:send_disconnect(?RC_MAXIMUM_CONNECT_TIME, PState),
    {stop, {shutdown, {disconnect, server_initiated}}, State};

handle_info(login_timeout, State = #state{proc_state = connect_packet_unprocessed,
                                          conn_name = ConnName}) ->
    %% The connection is also closed if the CONNECT packet happens to
    %% be already in the `deferred_recv' buffer. This can happen while
    %% the connection is blocked because of a resource alarm. However
    %% we don't know what is in the buffer, it can be arbitrary bytes,
    %% and we don't want to skip closing the connection in that case.
    ?LOG_ERROR("closing MQTT connection ~tp (login timeout)", [ConnName]),
    {stop, {shutdown, login_timeout}, State};
handle_info(login_timeout, State) ->
    {noreply, State, ?HIBERNATE_AFTER};

handle_info(emit_stats, State) ->
    {noreply, emit_stats(State), ?HIBERNATE_AFTER};

handle_info({{'DOWN', _QName}, _MRef, process, _Pid, _Reason} = Evt,
            #state{proc_state = PState0} = State) ->
    case rabbit_mqtt_processor:handle_down(Evt, PState0) of
        {ok, PState} ->
            maybe_process_deferred_recv(control_throttle(pstate(State, PState)));
        {error, Reason} ->
            {stop, {shutdown, Reason}, State}
    end;

handle_info({'DOWN', _MRef, process, QPid, _Reason}, State) ->
    rabbit_amqqueue_common:notify_sent_queue_down(QPid),
    {noreply, State, ?HIBERNATE_AFTER};

handle_info({shutdown, Explanation} = Reason, State = #state{conn_name = ConnName}) ->
    %% rabbitmq_management plugin requests to close connection.
    ?LOG_INFO("MQTT closing connection ~tp: ~p", [ConnName, Explanation]),
    {stop, Reason, State};

handle_info(Msg, State) ->
    {stop, {mqtt_unexpected_msg, Msg}, State}.

terminate(Reason, State = #state{}) ->
    terminate(Reason, {true, State});
terminate(Reason, {SendWill, State = #state{keepalive = KState0,
                                            proc_state = PState}}) ->
    KState = rabbit_mqtt_keepalive:cancel_timer(KState0),
    maybe_emit_stats(State#state{keepalive = KState}),
    case PState of
        connect_packet_unprocessed ->
            ok;
        _ ->
            Infos = infos(?EVENT_KEYS, State),
            rabbit_mqtt_processor:terminate(SendWill, Infos, PState)
    end,
    log_terminate(Reason, State).

log_terminate({network_error, {ssl_upgrade_error, closed}, ConnName}, _State) ->
    ?LOG_ERROR("MQTT detected TLS upgrade error on ~s: connection closed", [ConnName]);

log_terminate({network_error,
               {ssl_upgrade_error,
                {tls_alert, "handshake failure"}}, ConnName}, _State) ->
    log_tls_alert(handshake_failure, ConnName);
log_terminate({network_error,
               {ssl_upgrade_error,
                {tls_alert, "unknown ca"}}, ConnName}, _State) ->
    log_tls_alert(unknown_ca, ConnName);
log_terminate({network_error,
               {ssl_upgrade_error,
                {tls_alert, {Err, _}}}, ConnName}, _State) ->
    log_tls_alert(Err, ConnName);
log_terminate({network_error,
               {ssl_upgrade_error,
                {tls_alert, Alert}}, ConnName}, _State) ->
    log_tls_alert(Alert, ConnName);
log_terminate({network_error, {ssl_upgrade_error, Reason}, ConnName}, _State) ->
    ?LOG_ERROR("MQTT detected TLS upgrade error on ~s: ~p", [ConnName, Reason]);
log_terminate({network_error, Reason, ConnName}, _State) ->
    ?LOG_ERROR("MQTT detected network error on ~s: ~p", [ConnName, Reason]);
log_terminate({network_error, Reason}, _State) ->
    ?LOG_ERROR("MQTT detected network error: ~p", [Reason]);
log_terminate(normal, #state{conn_name  = ConnName}) ->
    ?LOG_INFO("closing MQTT connection ~p (~s)", [self(), ConnName]),
    ok;
log_terminate(_Reason, _State) ->
    ok.

%%----------------------------------------------------------------------------

log_tls_alert(handshake_failure, ConnName) ->
    ?LOG_ERROR("MQTT detected TLS upgrade error on ~ts: handshake failure", [ConnName]);
log_tls_alert(unknown_ca, ConnName) ->
    ?LOG_ERROR("MQTT detected TLS certificate verification error on ~ts: alert 'unknown CA'",
               [ConnName]);
log_tls_alert(Alert, ConnName) ->
    ?LOG_ERROR("MQTT detected TLS upgrade error on ~ts: alert ~ts", [ConnName, Alert]).

process_received_bytes(<<>>, State) ->
    {noreply, ensure_stats_timer(State), ?HIBERNATE_AFTER};
process_received_bytes(Bytes, State = #state{socket = Socket,
                                             parse_state = ParseState,
                                             proc_state = ProcState,
                                             conn_name = ConnName}) ->
    try rabbit_mqtt_packet:parse(Bytes, ParseState) of
        {more, ParseState1} ->
            {noreply,
             ensure_stats_timer(State#state{parse_state = ParseState1}),
             ?HIBERNATE_AFTER};
        {ok, Packet, Rest, ParseState1} ->
            case ProcState of
                connect_packet_unprocessed ->
                    SendFun = fun(Data) ->
                                      case rabbit_net:send(Socket, Data) of
                                          ok ->
                                              ok;
                                          {error, Reason} ->
                                              ?LOG_ERROR("writing to MQTT socket ~p failed: ~p",
                                                         [Socket, Reason]),
                                              throw({send_failed, Reason})
                                      end
                              end,
                    try rabbit_mqtt_processor:init(Packet, Socket, ConnName, SendFun) of
                        {ok, ProcState1} ->
                            ?LOG_INFO("Accepted MQTT connection ~ts for client ID ~ts",
                                      [ConnName, rabbit_mqtt_processor:info(client_id, ProcState1)]),
                            process_received_bytes(
                              Rest, State#state{parse_state = ParseState1,
                                                proc_state = ProcState1});
                        {error, {socket_ends, Reason} = R} ->
                            ?LOG_ERROR("MQTT connection ~ts failed to establish because socket "
                                       "addresses could not be determined: ~tp",
                                       [ConnName, Reason]),
                            {stop, {shutdown, R}, {_SendWill = false, State}};
                        {error, ConnectReasonCode} ->
                            ?LOG_ERROR("Rejected MQTT connection ~ts with Connect Reason Code ~p",
                                       [ConnName, ConnectReasonCode]),
                            {stop, shutdown, {_SendWill = false, State}}
                    catch throw:{send_failed, Reason} ->
                              network_error(Reason, State)
                    end;
                _ ->
                    try rabbit_mqtt_processor:process_packet(Packet, ProcState) of
                        {ok, ProcState1} ->
                            process_received_bytes(
                              Rest,
                              State #state{parse_state = ParseState1,
                                           proc_state = ProcState1});
                        {error, access_refused = Reason, ProcState1} ->
                            ?LOG_ERROR("MQTT connection ~ts is closing due to an authorization failure", [ConnName]),
                            {stop, {shutdown, Reason}, pstate(State, ProcState1)};
                        {error, Reason, ProcState1} ->
                            ?LOG_ERROR("MQTT protocol error on connection ~ts: ~tp", [ConnName, Reason]),
                            {stop, {shutdown, Reason}, pstate(State, ProcState1)};
                        {stop, {disconnect, server_initiated} = Reason, ProcState1} ->
                            ?LOG_ERROR("MQTT protocol error on connection ~ts: ~tp", [ConnName, Reason]),
                            {stop, {shutdown, Reason}, pstate(State, ProcState1)};
                        {stop, {disconnect, {client_initiated, SendWill}}, ProcState1} ->
                            {stop, normal, {SendWill, pstate(State, ProcState1)}}
                    catch throw:{send_failed, Reason} ->
                              network_error(Reason, State)
                    end
            end;
        {error, {disconnect_reason_code, ReasonCode} = Reason} ->
            rabbit_mqtt_processor:send_disconnect(ReasonCode, ProcState),
            {stop, {shutdown, Reason}, State};
        {error, Error} ->
            ?LOG_ERROR("MQTT detected a packet error on connection ~ts: ~tp", [ConnName, Error]),
            {stop, {shutdown, Error}, State}
    catch _:Reason:Stacktrace ->
              ?LOG_ERROR("Unparseable MQTT packet received from connection ~ts", [ConnName]),
              ?LOG_DEBUG("MQTT cannot parse a packet on connection '~ts', reason: ~tp, "
                         "stacktrace: ~tp, payload (first 100 bytes): ~tp",
                         [ConnName, Reason, Stacktrace, rabbit_mqtt_util:truncate_binary(Bytes, 100)]),
              {stop, {shutdown, Reason}, State}
    end.

-spec pstate(state(), rabbit_mqtt_processor:state()) -> state().
pstate(State = #state {}, PState) ->
    State #state{ proc_state = PState }.

network_error(closed,
              State = #state{conn_name  = ConnName,
                             proc_state = ProcState}) ->
    Fmt = "MQTT connection ~p will terminate because peer closed TCP connection",
    Args = [ConnName],
    case ProcState of
        connect_packet_unprocessed -> ?LOG_DEBUG(Fmt, Args);
        _ -> ?LOG_INFO(Fmt, Args)
    end,
    {stop, {shutdown, conn_closed}, State};

network_error(Reason,
              State = #state{conn_name  = ConnName}) ->
    ?LOG_INFO("MQTT detected network error for ~p: ~p", [ConnName, Reason]),
    {stop, {shutdown, conn_closed}, State}.

run_socket(State = #state{ connection_state = blocked }) ->
    State;
run_socket(State = #state{ deferred_recv = Data }) when Data =/= undefined ->
    State;
run_socket(State = #state{ await_recv = true }) ->
    State;
run_socket(State = #state{ socket = Sock }) ->
    ok = rabbit_net:setopts(Sock, [{active, once}]),
    State#state{ await_recv = true }.

control_throttle(State = #state{connection_state = ConnState,
                                conserve = Conserve,
                                proc_state = PState,
                                keepalive = KState
                               }) ->
    Throttle = case PState of
                   connect_packet_unprocessed -> Conserve;
                   _ -> rabbit_mqtt_processor:throttle(Conserve, PState)
               end,
    case {ConnState, Throttle} of
        {running, true} ->
            State#state{connection_state = blocked,
                        keepalive = rabbit_mqtt_keepalive:cancel_timer(KState)};
        {blocked, false} ->
            run_socket(State#state{connection_state = running,
                                   keepalive = rabbit_mqtt_keepalive:start_timer(KState)});
        {_, _} ->
            run_socket(State)
    end.

maybe_process_deferred_recv(State = #state{ deferred_recv = undefined }) ->
    {noreply, State, ?HIBERNATE_AFTER};
maybe_process_deferred_recv(State = #state{ deferred_recv = Data, socket = Sock }) ->
    handle_info({tcp, Sock, Data},
                State#state{ deferred_recv = undefined }).

maybe_emit_stats(#state{stats_timer = undefined}) ->
    ok;
maybe_emit_stats(State) ->
    rabbit_event:if_enabled(State, #state.stats_timer,
                            fun() -> emit_stats(State) end).

emit_stats(State=#state{proc_state = connect_packet_unprocessed}) ->
    %% Avoid emitting stats on terminate when the connection has not yet been
    %% established, as this causes orphan entries on the stats database
    State1 = rabbit_event:reset_stats_timer(State, #state.stats_timer),
    ensure_stats_timer(State1);
emit_stats(State) ->
    [{_, Pid},
     {_, RecvOct},
     {_, SendOct},
     {_, Reductions}] = infos(?SIMPLE_METRICS, State),
    Infos = infos(?OTHER_METRICS, State),
    rabbit_core_metrics:connection_stats(Pid, Infos),
    rabbit_core_metrics:connection_stats(Pid, RecvOct, SendOct, Reductions),
    State1 = rabbit_event:reset_stats_timer(State, #state.stats_timer),
    ensure_stats_timer(State1).

ensure_stats_timer(State = #state{}) ->
    rabbit_event:ensure_stats_timer(State, #state.stats_timer, emit_stats).

infos(Items, State) ->
    [{Item, i(Item, State)} || Item <- Items].

i(SockStat, #state{socket = Sock})
  when SockStat =:= recv_oct;
       SockStat =:= recv_cnt;
       SockStat =:= send_oct;
       SockStat =:= send_cnt;
       SockStat =:= send_pend ->
    case rabbit_net:getstat(Sock, [SockStat]) of
        {ok, [{_, N}]} when is_number(N) ->
            N;
        _ ->
            0
    end;
i(state, S) ->
    i(connection_state, S);
i(garbage_collection, _) ->
    rabbit_misc:get_gc_info(self());
i(reductions, _) ->
    {reductions, Reductions} = erlang:process_info(self(), reductions),
    Reductions;
i(name, S) ->
    i(conn_name, S);
i(conn_name, #state{conn_name = Val}) ->
    Val;
i(connection_state, #state{proc_state = connect_packet_unprocessed}) ->
    starting;
i(connection_state, #state{connection_state = Val}) ->
    Val;
i(pid, _) ->
    self();
i(SSL, #state{socket = Sock, proxy_socket = ProxySock})
  when SSL =:= ssl;
       SSL =:= ssl_protocol;
       SSL =:= ssl_key_exchange;
       SSL =:= ssl_cipher;
       SSL =:= ssl_hash ->
    rabbit_ssl:info(SSL, {Sock, ProxySock});
i(Cert, #state{socket = Sock})
  when Cert =:= peer_cert_issuer;
       Cert =:= peer_cert_subject;
       Cert =:= peer_cert_validity ->
    rabbit_ssl:cert_info(Cert, Sock);
i(timeout, #state{keepalive = KState}) ->
    rabbit_mqtt_keepalive:interval_secs(KState);
i(protocol, #state{proc_state = ProcState}) ->
    {'MQTT', rabbit_mqtt_processor:proto_version_tuple(ProcState)};
i(Key, #state{proc_state = ProcState}) ->
    rabbit_mqtt_processor:info(Key, ProcState).

-spec format_status(Status) -> Status when
      Status :: #{state => term(),
                  message => term(),
                  reason => term(),
                  log => [sys:system_event()]}.
format_status(Status) ->
    maps:map(
      fun(state, State) ->
              format_state(State);
         (_, Value) ->
              Value
      end, Status).

-spec format_state(state()) -> map().
format_state(#state{socket = Socket,
                    proxy_socket = ProxySock,
                    await_recv = AwaitRecv,
                    deferred_recv = DeferredRecv,
                    parse_state = _,
                    proc_state = PState,
                    connection_state = ConnectionState,
                    conserve = Conserve,
                    stats_timer = StatsTimer,
                    keepalive = Keepalive,
                    conn_name = ConnName
                   }) ->
    #{socket => Socket,
      proxy_socket => ProxySock,
      await_recv => AwaitRecv,
      deferred_recv => DeferredRecv =/= undefined,
      proc_state => if PState =:= connect_packet_unprocessed ->
                           PState;
                       true ->
                           rabbit_mqtt_processor:format_status(PState)
                    end,
      connection_state => ConnectionState,
      conserve => Conserve,
      stats_timer => StatsTimer,
      keepalive => Keepalive,
      conn_name => ConnName}.
