%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mqtt_reader).

-behaviour(gen_server).
-behaviour(ranch_protocol).

-export([start_link/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).

%%TODO check where to best 'hibernate' when returning from callback
%%TODO use rabbit_global_counters for MQTT protocol

-export([conserve_resources/3,
         close_connection/2]).

-export([info/2]).

-include("rabbit_mqtt.hrl").

-define(SIMPLE_METRICS, [pid, recv_oct, send_oct, reductions]).
-define(OTHER_METRICS, [recv_cnt, send_cnt, send_pend, garbage_collection, state]).
-define(HIBERNATE_AFTER, 1000).

%%----------------------------------------------------------------------------

start_link(Ref, _Transport, []) ->
    Pid = proc_lib:spawn_link(?MODULE, init, [Ref]),
    {ok, Pid}.

conserve_resources(Pid, _, {_, Conserve, _}) ->
    Pid ! {conserve_resources, Conserve},
    ok.

info(Pid, InfoItems) ->
    case InfoItems -- ?INFO_ITEMS of
        [] -> gen_server:call(Pid, {info, InfoItems});
        UnknownItems -> throw({bad_argument, UnknownItems})
    end.

close_connection(Pid, Reason) ->
    gen_server:cast(Pid, {close_connection, Reason}).

%%----------------------------------------------------------------------------

init(Ref) ->
    process_flag(trap_exit, true),
    {ok, Sock} = rabbit_networking:handshake(Ref,
        application:get_env(rabbitmq_mqtt, proxy_protocol, false)),
    RealSocket = rabbit_net:unwrap_socket(Sock),
    case rabbit_net:connection_string(Sock, inbound) of
        {ok, ConnStr} ->
            rabbit_log_connection:debug("MQTT accepting TCP connection ~tp (~ts)", [self(), ConnStr]),
            rabbit_alarm:register(
              self(), {?MODULE, conserve_resources, []}),
            LoginTimeout = application:get_env(rabbitmq_mqtt, login_timeout, 10_000),
            erlang:send_after(LoginTimeout, self(), login_timeout),
            ProcessorState = rabbit_mqtt_processor:initial_state(RealSocket, ConnStr),
            %%TODO call rabbit_networking:register_non_amqp_connection/1 so that we are notified
            %% when need to force load the 'connection_created' event for the management plugin, see
            %% https://github.com/rabbitmq/rabbitmq-management-agent/issues/58
            %% https://github.com/rabbitmq/rabbitmq-server/blob/90cc0e2abf944141feedaf3190d7b6d8b4741b11/deps/rabbitmq_stream/src/rabbit_stream_reader.erl#L536
            %% https://github.com/rabbitmq/rabbitmq-server/blob/90cc0e2abf944141feedaf3190d7b6d8b4741b11/deps/rabbitmq_stream/src/rabbit_stream_reader.erl#L189
            %% https://github.com/rabbitmq/rabbitmq-server/blob/7eb4084cf879fe6b1d26dd3c837bd180cb3a8546/deps/rabbitmq_management_agent/src/rabbit_mgmt_db_handler.erl#L72
            gen_server:enter_loop(?MODULE, [],
             rabbit_event:init_stats_timer(
              control_throttle(
               #state{socket                 = RealSocket,
                      proxy_socket           = rabbit_net:maybe_get_proxy_socket(Sock),
                      conn_name              = ConnStr,
                      await_recv             = false,
                      connection_state       = running,
                      received_connect_frame = false,
                      conserve               = false,
                      parse_state            = rabbit_mqtt_frame:initial_state(),
                      proc_state             = ProcessorState }), #state.stats_timer)
             );
        {network_error, Reason} ->
            rabbit_net:fast_close(RealSocket),
            terminate({shutdown, Reason}, undefined);
        {error, enotconn} ->
            rabbit_net:fast_close(RealSocket),
            terminate(shutdown, undefined);
        {error, Reason} ->
            rabbit_net:fast_close(RealSocket),
            terminate({network_error, Reason}, undefined)
    end.

handle_call({info, InfoItems}, _From, State) ->
    Infos = lists:map(
        fun(InfoItem) ->
            {InfoItem, info_internal(InfoItem, State)}
        end,
        InfoItems),
    {reply, Infos, State};

handle_call(Msg, From, State) ->
    {stop, {mqtt_unexpected_call, Msg, From}, State}.

handle_cast(duplicate_id,
            State = #state{ proc_state = PState,
                            conn_name  = ConnName }) ->
    rabbit_log_connection:warning("MQTT disconnecting client ~tp with duplicate id '~ts'",
                 [ConnName, rabbit_mqtt_processor:info(client_id, PState)]),
    {stop, {shutdown, duplicate_id}, State};

handle_cast(decommission_node,
            State = #state{ proc_state = PState,
                            conn_name  = ConnName }) ->
    rabbit_log_connection:warning("MQTT disconnecting client ~tp with client ID '~ts' as its node is about"
                                  " to be decommissioned",
                 [ConnName, rabbit_mqtt_processor:info(client_id, PState)]),
    {stop, {shutdown, decommission_node}, State};

handle_cast({close_connection, Reason},
            State = #state{conn_name = ConnName, proc_state = PState}) ->
    rabbit_log_connection:warning("MQTT disconnecting client ~tp with client ID '~ts', reason: ~ts",
                                  [ConnName, rabbit_mqtt_processor:info(client_id, PState), Reason]),
    {stop, {shutdown, server_initiated_close}, State};

handle_cast(QueueEvent = {queue_event, _, _},
            State = #state{proc_state = PState}) ->
    callback_reply(State, rabbit_mqtt_processor:handle_queue_event(QueueEvent, PState));

handle_cast(Msg, State) ->
    {stop, {mqtt_unexpected_cast, Msg}, State}.

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

handle_info({inet_reply, Sock, ok}, State = #state{socket = Sock}) ->
    {noreply, State, ?HIBERNATE_AFTER};

handle_info({inet_reply, Sock, {error, Reason}}, State = #state{socket = Sock}) ->
    network_error(Reason, State);

handle_info({conserve_resources, Conserve}, State) ->
    maybe_process_deferred_recv(
        control_throttle(State #state{ conserve = Conserve }));

handle_info({bump_credit, Msg}, State) ->
    credit_flow:handle_bump_msg(Msg),
    maybe_process_deferred_recv(control_throttle(State));

handle_info({keepalive, Req}, State = #state{keepalive = KState0,
                                             conn_name = ConnName}) ->
    case rabbit_mqtt_keepalive:handle(Req, KState0) of
        {ok, KState} ->
            {noreply, State#state{keepalive = KState}, ?HIBERNATE_AFTER};
        {error, timeout} ->
            rabbit_log_connection:error("closing MQTT connection ~p (keepalive timeout)", [ConnName]),
            send_will_and_terminate({shutdown, keepalive_timeout}, State);
        {error, Reason} ->
            {stop, Reason, State}
    end;

handle_info(login_timeout, State = #state{received_connect_frame = true}) ->
    {noreply, State};
handle_info(login_timeout, State = #state{conn_name = ConnStr}) ->
    %% The connection is also closed if the CONNECT frame happens to
    %% be already in the `deferred_recv' buffer. This can happen while
    %% the connection is blocked because of a resource alarm. However
    %% we don't know what is in the buffer, it can be arbitrary bytes,
    %% and we don't want to skip closing the connection in that case.
    rabbit_log_connection:error("closing MQTT connection ~tp (login timeout)", [ConnStr]),
    {stop, {shutdown, login_timeout}, State};

handle_info(emit_stats, State) ->
    {noreply, emit_stats(State), ?HIBERNATE_AFTER};

handle_info({ra_event, _From, Evt},
            #state{proc_state = PState0} = State) ->
    %% handle applied event to ensure registration command actually got applied
    %% handle not_leader notification in case we send the command to a non-leader
    PState = rabbit_mqtt_processor:handle_ra_event(Evt, PState0),
    {noreply, pstate(State, PState), ?HIBERNATE_AFTER};

handle_info({{'DOWN', _QName}, _MRef, process, _Pid, _Reason} = Evt,
            #state{proc_state = PState0} = State) ->
    PState = rabbit_mqtt_processor:handle_down(Evt, PState0),
    {noreply, pstate(State, PState), ?HIBERNATE_AFTER};

handle_info(Msg, State) ->
    {stop, {mqtt_unexpected_msg, Msg}, State}.

terminate(Reason, State = #state{keepalive = KState0,
                                 proc_state = PState}) ->
    KState = rabbit_mqtt_keepalive:cancel_timer(KState0),
    maybe_emit_stats(State#state{keepalive = KState}),
    rabbit_mqtt_processor:terminate(PState),
    log_terminate(Reason, State).

log_terminate({network_error, {ssl_upgrade_error, closed}, ConnStr}, _State) ->
    rabbit_log_connection:error("MQTT detected TLS upgrade error on ~s: connection closed",
                                [ConnStr]);

log_terminate({network_error,
               {ssl_upgrade_error,
                {tls_alert, "handshake failure"}}, ConnStr}, _State) ->
    log_tls_alert(handshake_failure, ConnStr);
log_terminate({network_error,
               {ssl_upgrade_error,
                {tls_alert, "unknown ca"}}, ConnStr}, _State) ->
    log_tls_alert(unknown_ca, ConnStr);
log_terminate({network_error,
               {ssl_upgrade_error,
                {tls_alert, {Err, _}}}, ConnStr}, _State) ->
    log_tls_alert(Err, ConnStr);
log_terminate({network_error,
               {ssl_upgrade_error,
                {tls_alert, Alert}}, ConnStr}, _State) ->
    log_tls_alert(Alert, ConnStr);
log_terminate({network_error, {ssl_upgrade_error, Reason}, ConnStr}, _State) ->
    rabbit_log_connection:error("MQTT detected TLS upgrade error on ~s: ~p",
                                [ConnStr, Reason]);

log_terminate({network_error, Reason, ConnStr}, _State) ->
    rabbit_log_connection:error("MQTT detected network error on ~s: ~p",
                                [ConnStr, Reason]);

log_terminate({network_error, Reason}, _State) ->
    rabbit_log_connection:error("MQTT detected network error: ~p", [Reason]);

log_terminate(normal, #state{conn_name  = ConnName}) ->
    rabbit_log_connection:info("closing MQTT connection ~p (~s)", [self(), ConnName]),
    ok;

log_terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

log_tls_alert(handshake_failure, ConnStr) ->
    rabbit_log_connection:error("MQTT detected TLS upgrade error on ~ts: handshake failure",
       [ConnStr]);
log_tls_alert(unknown_ca, ConnStr) ->
    rabbit_log_connection:error("MQTT detected TLS certificate verification error on ~ts: alert 'unknown CA'",
       [ConnStr]);
log_tls_alert(Alert, ConnStr) ->
    rabbit_log_connection:error("MQTT detected TLS upgrade error on ~ts: alert ~ts",
       [ConnStr, Alert]).

process_received_bytes(<<>>, State = #state{received_connect_frame = false,
                                            proc_state = PState,
                                            conn_name = ConnStr}) ->
    rabbit_log_connection:info("Accepted MQTT connection ~p (~s, client id: ~s)",
                               [self(), ConnStr, rabbit_mqtt_processor:info(client_id, PState)]),
    {noreply, ensure_stats_timer(State#state{received_connect_frame = true}), ?HIBERNATE_AFTER};
process_received_bytes(<<>>, State) ->
    {noreply, ensure_stats_timer(State), ?HIBERNATE_AFTER};
process_received_bytes(Bytes,
                       State = #state{ parse_state = ParseState,
                                       proc_state  = ProcState,
                                       conn_name   = ConnStr }) ->
    case parse(Bytes, ParseState) of
        {more, ParseState1} ->
            {noreply,
             ensure_stats_timer( State #state{ parse_state = ParseState1 }),
             ?HIBERNATE_AFTER};
        {ok, Frame, Rest} ->
            case rabbit_mqtt_processor:process_frame(Frame, ProcState) of
                {ok, ProcState1} ->
                    PS = rabbit_mqtt_frame:initial_state(),
                    process_received_bytes(
                      Rest,
                      State #state{parse_state = PS,
                                   proc_state = ProcState1});
                %% PUBLISH and more
                {error, unauthorized = Reason, ProcState1} ->
                    rabbit_log_connection:error("MQTT connection ~ts is closing due to an authorization failure", [ConnStr]),
                    {stop, {shutdown, Reason}, pstate(State, ProcState1)};
                %% CONNECT frames only
                {error, unauthenticated = Reason, ProcState1} ->
                    rabbit_log_connection:error("MQTT connection ~ts is closing due to an authentication failure", [ConnStr]),
                    {stop, {shutdown, Reason}, pstate(State, ProcState1)};
                %% CONNECT frames only
                {error, invalid_client_id = Reason, ProcState1} ->
                    rabbit_log_connection:error("MQTT cannot accept connection ~ts: client uses an invalid ID", [ConnStr]),
                    {stop, {shutdown, Reason}, pstate(State, ProcState1)};
                %% CONNECT frames only
                {error, unsupported_protocol_version = Reason, ProcState1} ->
                    rabbit_log_connection:error("MQTT cannot accept connection ~ts: incompatible protocol version", [ConnStr]),
                    {stop, {shutdown, Reason}, pstate(State, ProcState1)};
                {error, unavailable = Reason, ProcState1} ->
                    rabbit_log_connection:error("MQTT cannot accept connection ~ts due to an internal error or unavailable component",
                        [ConnStr]),
                    {stop, {shutdown, Reason}, pstate(State, ProcState1)};
                {error, Reason, ProcState1} ->
                    rabbit_log_connection:error("MQTT protocol error on connection ~ts: ~tp",
                        [ConnStr, Reason]),
                    {stop, {shutdown, Reason}, pstate(State, ProcState1)};
                {error, Error} ->
                    rabbit_log_connection:error("MQTT detected a framing error on connection ~ts: ~tp",
                        [ConnStr, Error]),
                    {stop, {shutdown, Error}, State};
                {stop, ProcState1} ->
                    {stop, normal, pstate(State, ProcState1)}
            end;
        {error, {cannot_parse, Error, Stacktrace}} ->
            rabbit_log_connection:error("MQTT cannot parse a frame on connection '~ts', unparseable payload: ~tp, error: {~tp, ~tp} ",
                [ConnStr, Bytes, Error, Stacktrace]),
            {stop, {shutdown, Error}, State};
        {error, Error} ->
            rabbit_log_connection:error("MQTT detected a framing error on connection ~ts: ~tp",
                [ConnStr, Error]),
            {stop, {shutdown, Error}, State}
    end.

callback_reply(State, {ok, ProcState}) ->
    {noreply, pstate(State, ProcState), ?HIBERNATE_AFTER};
callback_reply(State, {error, Reason, ProcState}) ->
    {stop, Reason, pstate(State, ProcState)}.

pstate(State = #state {}, PState = #proc_state{}) ->
    State #state{ proc_state = PState }.

%%----------------------------------------------------------------------------
parse(Bytes, ParseState) ->
    try
        rabbit_mqtt_frame:parse(Bytes, ParseState)
    catch
        _:Reason:Stacktrace ->
            {error, {cannot_parse, Reason, Stacktrace}}
    end.

%% TODO Send will message in all abnormal shutdowns?
%% => in terminate/2 depending on Reason
%% "The Will Message MUST be published when the Network Connection is subsequently
%% closed unless the Will Message has been deleted by the Server on receipt of a
%% DISCONNECT Packet [MQTT-3.1.2-8]."
send_will_and_terminate(State) ->
    send_will_and_terminate({shutdown, conn_closed}, State).

send_will_and_terminate(Reason, State = #state{conn_name = ConnStr,
                                               proc_state = PState}) ->
    rabbit_log_connection:debug("MQTT: about to send will message (if any) on connection ~p", [ConnStr]),
    rabbit_mqtt_processor:send_will(PState),
    {stop, Reason, State}.

network_error(closed,
              State = #state{conn_name  = ConnStr,
                             received_connect_frame = Connected}) ->
    Fmt = "MQTT connection ~p will terminate because peer closed TCP connection",
    Args = [ConnStr],
    case Connected of
        true -> rabbit_log_connection:info(Fmt, Args);
        false  -> rabbit_log_connection:debug(Fmt, Args)
    end,
    send_will_and_terminate(State);

network_error(Reason,
              State = #state{conn_name  = ConnStr}) ->
    rabbit_log_connection:info("MQTT detected network error for ~p: ~p",
                               [ConnStr, Reason]),
    send_will_and_terminate(State).

run_socket(State = #state{ connection_state = blocked }) ->
    State;
run_socket(State = #state{ deferred_recv = Data }) when Data =/= undefined ->
    State;
run_socket(State = #state{ await_recv = true }) ->
    State;
run_socket(State = #state{ socket = Sock }) ->
    rabbit_net:setopts(Sock, [{active, once}]),
    State#state{ await_recv = true }.

control_throttle(State = #state{connection_state = Flow,
                                conserve = Conserve,
                                keepalive = KState}) ->
    case {Flow, Conserve orelse credit_flow:blocked()} of
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

maybe_emit_stats(undefined) ->
    ok;
maybe_emit_stats(State) ->
    rabbit_event:if_enabled(State, #state.stats_timer,
                            fun() -> emit_stats(State) end).

emit_stats(State=#state{received_connect_frame = false}) ->
    %% Avoid emitting stats on terminate when the connection has not yet been
    %% established, as this causes orphan entries on the stats database
    State1 = rabbit_event:reset_stats_timer(State, #state.stats_timer),
    ensure_stats_timer(State1);
emit_stats(State) ->
    [{_, Pid},
     {_, Recv_oct},
     {_, Send_oct},
     {_, Reductions}] = infos(?SIMPLE_METRICS, State),
    Infos = infos(?OTHER_METRICS, State),
    rabbit_core_metrics:connection_stats(Pid, Infos),
    rabbit_core_metrics:connection_stats(Pid, Recv_oct, Send_oct, Reductions),
    State1 = rabbit_event:reset_stats_timer(State, #state.stats_timer),
    ensure_stats_timer(State1).

ensure_stats_timer(State = #state{}) ->
    rabbit_event:ensure_stats_timer(State, #state.stats_timer, emit_stats).

infos(Items, State) -> [{Item, info_internal(Item, State)} || Item <- Items].

info_internal(pid, State) -> info_internal(connection, State);
info_internal(SockStat, #state{socket = Sock}) when SockStat =:= recv_oct;
                                                    SockStat =:= recv_cnt;
                                                    SockStat =:= send_oct;
                                                    SockStat =:= send_cnt;
                                                    SockStat =:= send_pend ->
    case rabbit_net:getstat(Sock, [SockStat]) of
        {ok, [{_, N}]} when is_number(N) -> N;
        _ -> 0
    end;
info_internal(state, State) -> info_internal(connection_state, State);
info_internal(garbage_collection, _State) ->
    rabbit_misc:get_gc_info(self());
info_internal(reductions, _State) ->
    {reductions, Reductions} = erlang:process_info(self(), reductions),
    Reductions;
info_internal(conn_name, #state{conn_name = Val}) ->
    rabbit_data_coercion:to_binary(Val);
info_internal(connection_state, #state{received_connect_frame = false}) ->
    starting;
info_internal(connection_state, #state{connection_state = Val}) ->
    Val;
info_internal(connection, _State) ->
    self();
info_internal(ssl, #state{socket = Sock, proxy_socket = ProxySock}) ->
    rabbit_net:proxy_ssl_info(Sock, ProxySock) /= nossl;
info_internal(ssl_protocol,       S) -> ssl_info(fun ({P,         _}) -> P end, S);
info_internal(ssl_key_exchange,   S) -> ssl_info(fun ({_, {K, _, _}}) -> K end, S);
info_internal(ssl_cipher,         S) -> ssl_info(fun ({_, {_, C, _}}) -> C end, S);
info_internal(ssl_hash,           S) -> ssl_info(fun ({_, {_, _, H}}) -> H end, S);
info_internal(Key, #state{proc_state = ProcState}) ->
    rabbit_mqtt_processor:info(Key, ProcState).

ssl_info(F, #state{socket = Sock, proxy_socket = ProxySock}) ->
    case rabbit_net:proxy_ssl_info(Sock, ProxySock) of
        nossl       -> '';
        {error, _}  -> '';
        {ok, Items} ->
            P = proplists:get_value(protocol, Items),
            #{cipher := C,
              key_exchange := K,
              mac := H} = proplists:get_value(selected_cipher_suite, Items),
            F({P, {K, C, H}})
    end.
