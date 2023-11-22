%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_web_mqtt_handler).
-behaviour(cowboy_websocket).
-behaviour(cowboy_sub_protocol).

-include_lib("kernel/include/logger.hrl").
-include_lib("rabbit_common/include/logging.hrl").
-include_lib("rabbitmq_mqtt/include/rabbit_mqtt.hrl").
-include_lib("rabbitmq_mqtt/include/rabbit_mqtt_packet.hrl").

-export([
    init/2,
    websocket_init/1,
    websocket_handle/2,
    websocket_info/2,
    terminate/3
]).

-export([conserve_resources/3]).

%% cowboy_sub_protocol
-export([upgrade/4,
         upgrade/5,
         takeover/7]).

-define(APP, rabbitmq_web_mqtt).

-record(state, {
          socket :: {rabbit_proxy_socket, any(), any()} | rabbit_net:socket(),
          parse_state = rabbit_mqtt_packet:init_state() :: rabbit_mqtt_packet:state(),
          proc_state = connect_packet_unprocessed :: connect_packet_unprocessed |
                                                     rabbit_mqtt_processor:state(),
          connection_state = running :: running | blocked,
          conserve = false :: boolean(),
          stats_timer :: option(rabbit_event:state()),
          keepalive = rabbit_mqtt_keepalive:init() :: rabbit_mqtt_keepalive:state(),
          conn_name :: option(binary()),
          should_use_fhc :: rabbit_types:option(boolean())
        }).

-type state() :: #state{}.

%% Close frame status codes as defined in https://www.rfc-editor.org/rfc/rfc6455#section-7.4.1
-define(CLOSE_NORMAL, 1000).
-define(CLOSE_SERVER_GOING_DOWN, 1001).
-define(CLOSE_PROTOCOL_ERROR, 1002).
-define(CLOSE_UNACCEPTABLE_DATA_TYPE, 1003).

%% cowboy_sub_protcol
upgrade(Req, Env, Handler, HandlerState) ->
    upgrade(Req, Env, Handler, HandlerState, #{}).

upgrade(Req, Env, Handler, HandlerState, Opts) ->
    cowboy_websocket:upgrade(Req, Env, Handler, HandlerState, Opts).

takeover(Parent, Ref, Socket, Transport, Opts, Buffer, {Handler, HandlerState}) ->
    Sock = case HandlerState#state.socket of
               undefined ->
                   Socket;
               ProxyInfo ->
                   {rabbit_proxy_socket, Socket, ProxyInfo}
           end,
    cowboy_websocket:takeover(Parent, Ref, Socket, Transport, Opts, Buffer,
                              {Handler, HandlerState#state{socket = Sock}}).

%% cowboy_websocket
init(Req, Opts) ->
    case cowboy_req:parse_header(<<"sec-websocket-protocol">>, Req) of
        undefined ->
            no_supported_sub_protocol(undefined, Req);
        Protocol ->
            case lists:member(<<"mqtt">>, Protocol) of
                false ->
                    no_supported_sub_protocol(Protocol, Req);
                true ->
                    WsOpts0 = proplists:get_value(ws_opts, Opts, #{}),
                    WsOpts  = maps:merge(#{compress => true}, WsOpts0),
                    ShouldUseFHC = application:get_env(?APP, use_file_handle_cache, true),
                    case ShouldUseFHC of
                      true  -> ?LOG_INFO("Web MQTT: file handle cache use is enabled");
                      false -> ?LOG_INFO("Web MQTT: file handle cache use is disabled")
                    end,

                    {?MODULE,
                     cowboy_req:set_resp_header(<<"sec-websocket-protocol">>, <<"mqtt">>, Req),
                     #state{socket = maps:get(proxy_header, Req, undefined), should_use_fhc = ShouldUseFHC},
                     WsOpts}
            end
    end.

-spec websocket_init(state()) ->
    {cowboy_websocket:commands(), state()} |
    {cowboy_websocket:commands(), state(), hibernate}.
websocket_init(State0 = #state{socket = Sock, should_use_fhc = ShouldUseFHC}) ->
    logger:set_process_metadata(#{domain => ?RMQLOG_DOMAIN_CONN ++ [web_mqtt]}),
    case ShouldUseFHC of
      true  ->
        ok = file_handle_cache:obtain();
      false -> ok;
      undefined ->
        ok = file_handle_cache:obtain()
    end,
    case rabbit_net:connection_string(Sock, inbound) of
        {ok, ConnStr} ->
            ConnName = rabbit_data_coercion:to_binary(ConnStr),
            ?LOG_INFO("Accepting Web MQTT connection ~s", [ConnName]),
            _ = rabbit_alarm:register(self(), {?MODULE, conserve_resources, []}),
            State1 = State0#state{conn_name = ConnName},
            State = rabbit_event:init_stats_timer(State1, #state.stats_timer),
            process_flag(trap_exit, true),
            {[], State, hibernate};
        {error, Reason} ->
            {[{shutdown_reason, Reason}], State0}
    end.

-spec conserve_resources(pid(),
                         rabbit_alarm:resource_alarm_source(),
                         rabbit_alarm:resource_alert()) -> ok.
conserve_resources(Pid, _, {_, Conserve, _}) ->
    Pid ! {conserve_resources, Conserve},
    ok.

-spec websocket_handle(ping | pong | {text | binary | ping | pong, binary()}, State) ->
    {cowboy_websocket:commands(), State} |
    {cowboy_websocket:commands(), State, hibernate}.
websocket_handle({binary, Data}, State) ->
    handle_data(Data, State);
%% Silently ignore ping and pong frames as Cowboy will automatically reply to ping frames.
websocket_handle({Ping, _}, State)
  when Ping =:= ping orelse Ping =:= pong ->
    {[], State, hibernate};
websocket_handle(Ping, State)
  when Ping =:= ping orelse Ping =:= pong ->
    {[], State, hibernate};
%% Log and close connection when receiving any other unexpected frames.
websocket_handle(Frame, State) ->
    ?LOG_INFO("Web MQTT: unexpected WebSocket frame ~tp", [Frame]),
    stop(State, ?CLOSE_UNACCEPTABLE_DATA_TYPE, <<"unexpected WebSocket frame">>).

-spec websocket_info(any(), State) ->
    {cowboy_websocket:commands(), State} |
    {cowboy_websocket:commands(), State, hibernate}.
websocket_info({conserve_resources, Conserve}, State) ->
    handle_credits(State#state{conserve = Conserve});
websocket_info({bump_credit, Msg}, State) ->
    credit_flow:handle_bump_msg(Msg),
    handle_credits(State);
websocket_info({reply, Data}, State) ->
    {[{binary, Data}], State, hibernate};
websocket_info({stop, CloseCode, Error, SendWill}, State) ->
    stop({SendWill, State}, CloseCode, Error);
websocket_info({'EXIT', _, _}, State) ->
    stop(State);
websocket_info({'$gen_cast', QueueEvent = {queue_event, _, _}},
               State = #state{proc_state = PState0}) ->
    case rabbit_mqtt_processor:handle_queue_event(QueueEvent, PState0) of
        {ok, PState} ->
            handle_credits(State#state{proc_state = PState});
        {error, Reason, PState} ->
            ?LOG_ERROR("Web MQTT connection ~p failed to handle queue event: ~p",
                       [State#state.conn_name, Reason]),
            stop(State#state{proc_state = PState})
    end;
websocket_info({'$gen_cast', duplicate_id}, State) ->
    %% Delete this backward compatibility clause when feature flag
    %% delete_ra_cluster_mqtt_node becomes required.
    websocket_info({'$gen_cast', {duplicate_id, true}}, State);
websocket_info({'$gen_cast', {duplicate_id, SendWill}},
               State = #state{proc_state = ProcState,
                              conn_name = ConnName}) ->
    ?LOG_WARNING("Web MQTT disconnecting a client with duplicate ID '~s' (~p)",
                 [rabbit_mqtt_processor:info(client_id, ProcState), ConnName]),
    rabbit_mqtt_processor:send_disconnect(?RC_SESSION_TAKEN_OVER, ProcState),
    defer_close(?CLOSE_NORMAL, SendWill),
    {[], State};
websocket_info({'$gen_cast', {close_connection, Reason}}, State = #state{proc_state = ProcState,
                                                                         conn_name = ConnName}) ->
    ?LOG_WARNING("Web MQTT disconnecting client with ID '~s' (~p), reason: ~s",
                 [rabbit_mqtt_processor:info(client_id, ProcState), ConnName, Reason]),
    case Reason of
        maintenance ->
            rabbit_mqtt_processor:send_disconnect(?RC_SERVER_SHUTTING_DOWN, ProcState),
            defer_close(?CLOSE_SERVER_GOING_DOWN),
            {[], State};
        _ ->
            stop(State)
    end;
websocket_info({'$gen_cast', {force_event_refresh, Ref}}, State0) ->
    Infos = infos(?EVENT_KEYS, State0),
    rabbit_event:notify(connection_created, Infos, Ref),
    State = rabbit_event:init_stats_timer(State0, #state.stats_timer),
    {[], State, hibernate};
websocket_info({'$gen_cast', refresh_config},
               State0 = #state{proc_state = PState0,
                               conn_name = ConnName}) ->
    PState = rabbit_mqtt_processor:update_trace(ConnName, PState0),
    State = State0#state{proc_state = PState},
    {[], State, hibernate};
websocket_info({keepalive, Req}, State = #state{proc_state = ProcState,
                                                keepalive = KState0,
                                                conn_name = ConnName}) ->
    case rabbit_mqtt_keepalive:handle(Req, KState0) of
        {ok, KState} ->
            {[], State#state{keepalive = KState}, hibernate};
        {error, timeout} ->
            ?LOG_ERROR("keepalive timeout in Web MQTT connection ~p", [ConnName]),
            rabbit_mqtt_processor:send_disconnect(?RC_KEEP_ALIVE_TIMEOUT, ProcState),
            defer_close(?CLOSE_NORMAL),
            {[], State};
        {error, Reason} ->
            ?LOG_ERROR("keepalive error in Web MQTT connection ~p: ~p",
                       [ConnName, Reason]),
            stop(State)
    end;
websocket_info(emit_stats, State) ->
    {[], emit_stats(State), hibernate};
websocket_info({ra_event, _From, Evt},
               #state{proc_state = PState0} = State) ->
    PState = rabbit_mqtt_processor:handle_ra_event(Evt, PState0),
    {[], State#state{proc_state = PState}, hibernate};
websocket_info({{'DOWN', _QName}, _MRef, process, _Pid, _Reason} = Evt,
               State = #state{proc_state = PState0}) ->
    case rabbit_mqtt_processor:handle_down(Evt, PState0) of
        {ok, PState} ->
            handle_credits(State#state{proc_state = PState});
        {error, Reason} ->
            stop(State, ?CLOSE_NORMAL, Reason)
    end;
websocket_info({'DOWN', _MRef, process, QPid, _Reason}, State) ->
    rabbit_amqqueue_common:notify_sent_queue_down(QPid),
    {[], State, hibernate};
websocket_info({shutdown, Reason}, #state{conn_name = ConnName} = State) ->
    %% rabbitmq_management plugin requests to close connection.
    ?LOG_INFO("Web MQTT closing connection ~tp: ~tp", [ConnName, Reason]),
    stop(State, ?CLOSE_NORMAL, Reason);
websocket_info(connection_created, State) ->
    Infos = infos(?EVENT_KEYS, State),
    rabbit_core_metrics:connection_created(self(), Infos),
    rabbit_event:notify(connection_created, Infos),
    {[], State, hibernate};
websocket_info(Msg, State) ->
    ?LOG_WARNING("Web MQTT: unexpected message ~tp", [Msg]),
    {[], State, hibernate}.

terminate(Reason, Request, #state{} = State) ->
    terminate(Reason, Request, {true, State});
terminate(_Reason, _Request,
          {SendWill, #state{conn_name = ConnName,
                            proc_state = PState,
                            keepalive = KState,
                            should_use_fhc = ShouldUseFHC} = State}) ->
    ?LOG_INFO("Web MQTT closing connection ~ts", [ConnName]),
    maybe_emit_stats(State),
    _ = rabbit_mqtt_keepalive:cancel_timer(KState),
    case ShouldUseFHC of
      true  ->
        ok = file_handle_cache:release();
      false -> ok;
      undefined ->
        ok = file_handle_cache:release()
    end,
    case PState of
        connect_packet_unprocessed ->
            ok;
        _ ->
            Infos = infos(?EVENT_KEYS, State),
            rabbit_mqtt_processor:terminate(SendWill, Infos, PState)
    end.

%% Internal.

no_supported_sub_protocol(Protocol, Req) ->
    %% The client MUST include “mqtt” in the list of WebSocket Sub Protocols it offers [MQTT-6.0.0-3].
    ?LOG_ERROR("Web MQTT: 'mqtt' not included in client offered subprotocols: ~tp", [Protocol]),
    %% Set should_use_fhc to false, because at this early stage of init no fhc
    %% obtain was called, so terminate/3 should not call fhc release
    %% (even if use_file_handle_cache is true)
    {ok,
     cowboy_req:reply(400, #{<<"connection">> => <<"close">>}, Req),
     #state{should_use_fhc = false}}.

handle_data(Data, State0 = #state{}) ->
    case handle_data1(Data, State0) of
        {ok, State1 = #state{connection_state = blocked}, hibernate} ->
            {[{active, false}], State1, hibernate};
        Other ->
            Other
    end.

handle_data1(<<>>, State) ->
    {ok, ensure_stats_timer(control_throttle(State)), hibernate};
handle_data1(Data, State = #state{socket = Socket,
                                  parse_state = ParseState,
                                  proc_state = ProcState,
                                  conn_name = ConnName}) ->
    try rabbit_mqtt_packet:parse(Data, ParseState) of
        {more, ParseState1} ->
            {ok, ensure_stats_timer(
                   control_throttle(
                     State#state{parse_state = ParseState1})), hibernate};
        {ok, Packet, Rest, ParseState1} ->
            case ProcState of
                connect_packet_unprocessed ->
                    case rabbit_mqtt_processor:init(Packet, rabbit_net:unwrap_socket(Socket),
                                                    ConnName, fun send_reply/1) of
                        {ok, ProcState1} ->
                            ?LOG_INFO("Accepted Web MQTT connection ~ts for client ID ~ts",
                                      [ConnName, rabbit_mqtt_processor:info(client_id, ProcState1)]),
                            handle_data1(
                              Rest, State#state{parse_state = ParseState1,
                                                proc_state = ProcState1});
                        {error, Reason} ->
                            ?LOG_ERROR("Rejected Web MQTT connection ~ts: ~p", [ConnName, Reason]),
                            self() ! {stop, ?CLOSE_PROTOCOL_ERROR, connect_packet_rejected,
                                      _SendWill = false},
                            {[], State}
                    end;
                _ ->
                    case rabbit_mqtt_processor:process_packet(Packet, ProcState) of
                        {ok, ProcState1} ->
                            handle_data1(
                              Rest,
                              State#state{parse_state = ParseState1,
                                          proc_state = ProcState1});
                        {error, Reason, _} ->
                            stop_mqtt_protocol_error(State, Reason, ConnName);
                        {stop, {disconnect, server_initiated}, _} ->
                            defer_close(?CLOSE_PROTOCOL_ERROR),
                            {[], State};
                        {stop, {disconnect, {client_initiated, SendWill}}, ProcState1} ->
                            stop({SendWill, State#state{proc_state = ProcState1}})
                    end
            end;
        {error, {disconnect_reason_code, ReasonCode}} ->
            rabbit_mqtt_processor:send_disconnect(ReasonCode, ProcState),
            defer_close(?CLOSE_PROTOCOL_ERROR),
            {[], State};
        {error, Reason} ->
            stop_mqtt_protocol_error(State, Reason, ConnName)
    catch _:Reason:Stacktrace ->
              ?LOG_DEBUG("Web MQTT cannot parse a packet, reason: ~tp, "
                         "stacktrace: ~tp, payload (first 100 bytes): ~tp",
                         [Reason, Stacktrace, rabbit_mqtt_util:truncate_binary(Data, 100)]),
              stop_mqtt_protocol_error(State, cannot_parse, ConnName)
    end.

%% Allow DISCONNECT packet to be sent to client before closing the connection.
defer_close(CloseStatusCode) ->
    defer_close(CloseStatusCode, true).

defer_close(CloseStatusCode, SendWill) ->
    self() ! {stop, CloseStatusCode, server_initiated_disconnect, SendWill},
    ok.

stop_mqtt_protocol_error(State, Reason, ConnName) ->
    ?LOG_WARNING("Web MQTT protocol error ~tp for connection ~tp", [Reason, ConnName]),
    stop(State, ?CLOSE_PROTOCOL_ERROR, Reason).

stop(State) ->
    stop(State, ?CLOSE_NORMAL, "MQTT died").

stop(State, CloseCode, Error0) ->
    Error = rabbit_data_coercion:to_binary(Error0),
    {[{close, CloseCode, Error}], State}.

handle_credits(State0) ->
    State = #state{connection_state = CS} = control_throttle(State0),
    Active = case CS of
                 running -> true;
                 blocked -> false
             end,
    {[{active, Active}], State, hibernate}.

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
        {blocked,false} ->
            State#state{connection_state = running,
                        keepalive = rabbit_mqtt_keepalive:start_timer(KState)};
        {_, _} ->
            State
    end.

-spec send_reply(iodata()) -> ok.
send_reply(Data) ->
    self() ! {reply, Data},
    ok.

ensure_stats_timer(State) ->
    rabbit_event:ensure_stats_timer(State, #state.stats_timer, emit_stats).

maybe_emit_stats(#state{stats_timer = undefined}) ->
    ok;
maybe_emit_stats(State) ->
    rabbit_event:if_enabled(State, #state.stats_timer,
                                fun() -> emit_stats(State) end).

emit_stats(State=#state{proc_state = connect_packet_unprocessed}) ->
    %% Avoid emitting stats on terminate when the connection has not yet been
    %% established, as this causes orphan entries on the stats database
    rabbit_event:reset_stats_timer(State, #state.stats_timer);
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

infos(Items, State) ->
    [{Item, i(Item, State)} || Item <- Items].

i(pid, _) ->
    self();
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
i(reductions, _) ->
    {reductions, Reductions} = erlang:process_info(self(), reductions),
    Reductions;
i(garbage_collection, _) ->
    rabbit_misc:get_gc_info(self());
i(protocol, #state{proc_state = PState}) ->
    {'Web MQTT', rabbit_mqtt_processor:proto_version_tuple(PState)};
i(SSL, #state{socket = Sock})
  when SSL =:= ssl;
       SSL =:= ssl_protocol;
       SSL =:= ssl_key_exchange;
       SSL =:= ssl_cipher;
       SSL =:= ssl_hash ->
    rabbit_ssl:info(SSL, {rabbit_net:unwrap_socket(Sock),
                          rabbit_net:maybe_get_proxy_socket(Sock)});
i(name, S) ->
    i(conn_name, S);
i(conn_name, #state{conn_name = Val}) ->
    Val;
i(Cert, #state{socket = Sock})
  when Cert =:= peer_cert_issuer;
       Cert =:= peer_cert_subject;
       Cert =:= peer_cert_validity ->
    rabbit_ssl:cert_info(Cert, rabbit_net:unwrap_socket(Sock));
i(state, S) ->
    i(connection_state, S);
i(connection_state, #state{proc_state = connect_packet_unprocessed}) ->
    starting;
i(connection_state, #state{connection_state = Val}) ->
    Val;
i(timeout, #state{keepalive = KState}) ->
    rabbit_mqtt_keepalive:interval_secs(KState);
i(Key, #state{proc_state = PState}) ->
    rabbit_mqtt_processor:info(Key, PState).
