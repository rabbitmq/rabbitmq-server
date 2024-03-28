%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_stomp_reader).
-behaviour(gen_server2).

-export([start_link/3]).
-export([conserve_resources/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3, terminate/2]).
-export([start_heartbeats/2]).
-export([info/2, close_connection/2]).
-export([ssl_login_name/2]).

-include("rabbit_stomp.hrl").
-include("rabbit_stomp_frame.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-record(reader_state, {
    socket,
    conn_name,
    parse_state,
    processor_state,
    state,
    conserve_resources,
    recv_outstanding,
    max_frame_size,
    current_frame_size,
    stats_timer,
    parent,
    connection,
    heartbeat_sup, heartbeat,
    %% heartbeat timeout value used, 0 means
    %% heartbeats are disabled
    timeout_sec,
    parser_config
}).

%%----------------------------------------------------------------------------

start_link(SupHelperPid, Ref, Configuration) ->
    Pid = proc_lib:spawn_link(?MODULE, init,
                              [[SupHelperPid, Ref, Configuration]]),
    {ok, Pid}.

info(Pid, InfoItems) ->
    case InfoItems -- ?INFO_ITEMS of
        [] ->
            gen_server2:call(Pid, {info, InfoItems});
        UnknownItems -> throw({bad_argument, UnknownItems})
    end.

close_connection(Pid, Reason) ->
    gen_server:cast(Pid, {close_connection, Reason}).


init([SupHelperPid, Ref, Configuration]) ->
    process_flag(trap_exit, true),
    {ok, Sock} = rabbit_networking:handshake(Ref,
        application:get_env(rabbitmq_stomp, proxy_protocol, false)),
    RealSocket = rabbit_net:unwrap_socket(Sock),

    case rabbit_net:connection_string(Sock, inbound) of
        {ok, ConnStr} ->
            ConnName = rabbit_data_coercion:to_binary(ConnStr),
            ProcInitArgs = processor_args(Configuration, Sock),
            ProcState = rabbit_stomp_processor:initial_state(Configuration,
                                                             ProcInitArgs),

            rabbit_log_connection:info("accepting STOMP connection ~tp (~ts)",
                [self(), ConnName]),

            ParseState = rabbit_stomp_frame:initial_state(),
            ParserConfig = #stomp_parser_config{
                              max_headers        = Configuration#stomp_configuration.max_headers,
                              max_header_length  = Configuration#stomp_configuration.max_header_length,
                              max_body_length    = Configuration#stomp_configuration.max_body_length
                             },
            ParseState = rabbit_stomp_frame:initial_state(ParserConfig),
            _ = register_resource_alarm(),

            LoginTimeout = application:get_env(rabbitmq_stomp, login_timeout, 10_000),
            MaxFrameSize = application:get_env(rabbitmq_stomp, max_frame_size, ?DEFAULT_MAX_FRAME_SIZE),
            erlang:send_after(LoginTimeout, self(), login_timeout),

            rabbit_networking:register_non_amqp_connection(self()),

            gen_server2:enter_loop(?MODULE, [],
              rabbit_event:init_stats_timer(
                run_socket(control_throttle(
                  #reader_state{socket             = RealSocket,
                                conn_name          = ConnName,
                                parse_state        = ParseState,
                                parser_config      = ParserConfig,
                                processor_state    = ProcState,
                                heartbeat_sup      = SupHelperPid,
                                heartbeat          = {none, none},
                                max_frame_size     = MaxFrameSize,
                                current_frame_size = 0,
                                state              = running,
                                conserve_resources = false,
                                recv_outstanding   = false})), #reader_state.stats_timer),
              {backoff, 1000, 1000, 10000});
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
    {stop, {stomp_unexpected_call, Msg, From}, State}.

handle_cast(QueueEvent = {queue_event, _, _}, State) ->
    ProcState = processor_state(State),
    case rabbit_stomp_processor:handle_queue_event(QueueEvent, ProcState) of
        {ok, NewProcState} ->
            {noreply, processor_state(NewProcState, State), hibernate};
        {error, Reason, NewProcState} ->
            {stop, Reason, processor_state(NewProcState, State)}
    end;
handle_cast({close_connection, Reason}, State) ->
    {stop, {shutdown, {server_initiated_close, Reason}}, State};
handle_cast(client_timeout, State) ->
    {stop, {shutdown, client_heartbeat_timeout}, State};
handle_cast(Msg, State) ->
    {stop, {stomp_unexpected_cast, Msg}, State}.

handle_info(connection_created, State) ->
    Infos = infos(?INFO_ITEMS ++ ?OTHER_METRICS, State),
    rabbit_core_metrics:connection_created(self(), Infos),
    rabbit_event:notify(connection_created, Infos),
    {noreply, State, hibernate};

handle_info({Tag, Sock, Data}, State=#reader_state{socket=Sock})
        when Tag =:= tcp; Tag =:= ssl ->
    case process_received_bytes(Data, State#reader_state{recv_outstanding = false}) of
      {ok, NewState} ->
          {noreply, ensure_stats_timer(run_socket(control_throttle(NewState))), hibernate};
      {stop, Reason, NewState} ->
          {stop, Reason, NewState}
    end;
handle_info({Tag, Sock}, State=#reader_state{socket=Sock})
        when Tag =:= tcp_closed; Tag =:= ssl_closed ->
    {stop, normal, State};
handle_info({Tag, Sock, Reason}, State=#reader_state{socket=Sock})
        when Tag =:= tcp_error; Tag =:= ssl_error ->
    {stop, {inet_error, Reason}, State};
handle_info(emit_stats, State) ->
    {noreply, emit_stats(State), hibernate};
handle_info({conserve_resources, Conserve}, State) ->
    NewState = State#reader_state{conserve_resources = Conserve},
    {noreply, run_socket(control_throttle(NewState)), hibernate};
handle_info({bump_credit, Msg}, State) ->
    credit_flow:handle_bump_msg(Msg),
    {noreply, run_socket(control_throttle(State)), hibernate};

handle_info({{'DOWN', _QName}, _MRef, process, _Pid, _Reason} = Evt, State) ->
    ProcState = processor_state(State),
    {ok, NewProcState} = rabbit_stomp_processor:handle_down(Evt, ProcState),
    {noreply, processor_state(NewProcState, State), hibernate};

handle_info({'DOWN', _MRef, process, QPid, _Reason}, State) ->
    rabbit_amqqueue_common:notify_sent_queue_down(QPid),
    {noreply, State, hibernate};

%%----------------------------------------------------------------------------

handle_info(client_timeout, State) ->
    {stop, {shutdown, client_heartbeat_timeout}, State};

handle_info(login_timeout, State) ->
    ProcState = processor_state(State),
    case rabbit_stomp_processor:info(user, ProcState) of
        undefined ->
            {stop, {shutdown, login_timeout}, State};
        _ ->
            {noreply, State, hibernate}
    end;

%%----------------------------------------------------------------------------

handle_info({start_heartbeats, {0, 0}}, State) ->
    {noreply, State#reader_state{timeout_sec = {0, 0}}};

handle_info({start_heartbeats, {SendTimeout, ReceiveTimeout}},
            State = #reader_state{heartbeat_sup = SupPid, socket = Sock}) ->

    SendFun = fun() -> catch rabbit_net:send(Sock, <<$\n>>) end,
    Pid = self(),
    ReceiveFun = fun() -> gen_server2:cast(Pid, client_timeout) end,
    Heartbeat = rabbit_heartbeat:start(SupPid, Sock, SendTimeout,
                                       SendFun, ReceiveTimeout, ReceiveFun),
    {noreply, State#reader_state{heartbeat = Heartbeat,
                                 timeout_sec = {SendTimeout, ReceiveTimeout}}};


%%----------------------------------------------------------------------------
handle_info({'EXIT', _From, Reason}, State) ->
    {stop, {connection_died, Reason}, State}.
%%----------------------------------------------------------------------------

process_received_bytes([], State) ->
    {ok, State};
process_received_bytes(Bytes,
                       State = #reader_state{
                         max_frame_size = MaxFrameSize,
                         current_frame_size = FrameLength,
                         processor_state  = ProcState,
                         parse_state      = ParseState}) ->
    case rabbit_stomp_frame:parse(Bytes, ParseState) of
        {more, ParseState1} ->
            FrameLength1 = FrameLength + byte_size(Bytes),
            case FrameLength1 > MaxFrameSize of
                true ->
                    log_reason({network_error, {frame_too_big, {FrameLength1, MaxFrameSize}}}, State),
                    {stop, normal, State};
                false ->
                    {ok, State#reader_state{parse_state = ParseState1,
                                            current_frame_size = FrameLength1}}
            end;
        {ok, Frame, Rest} ->
            FrameLength1 = FrameLength + byte_size(Bytes) - byte_size(Rest),
            case FrameLength1 > MaxFrameSize of
                true ->
                    log_reason({network_error, {frame_too_big, {FrameLength1, MaxFrameSize}}}, State),
                    {stop, normal, State};
                false ->
                    try rabbit_stomp_processor:process_frame(Frame, ProcState) of
                        {ok, NewProcState} ->
                            PS = rabbit_stomp_frame:initial_state(),
                            NextState = maybe_block(State, Frame),
                            process_received_bytes(Rest, NextState#reader_state{
                                                           current_frame_size = 0,
                                                           processor_state = NewProcState,
                                                           parse_state     = PS});
                        {stop, Reason, NewProcState} ->
                            {stop, Reason,
                             processor_state(NewProcState, State)}
                    catch exit:{send_failed, closed} ->
                              {stop, normal, State};
                          exit:{send_failed, Reason} ->
                              {stop, Reason, State}
                    end
            end;
        {error, Reason} ->
            %% The parser couldn't parse data. We log the reason right
            %% now and stop with the reason 'normal' instead of the
            %% actual parsing error, because the supervisor would log
            %% a crash report (which is not that useful) and handle
            %% recovery, but it's too slow.
            log_reason({network_error, Reason}, State),
            {stop, normal, State}
    end.

-spec conserve_resources(pid(),
                         rabbit_alarm:resource_alarm_source(),
                         rabbit_alarm:resource_alert()) -> ok.
conserve_resources(Pid, _Source, {_, Conserve, _}) ->
    Pid ! {conserve_resources, Conserve},
    ok.

register_resource_alarm() ->
    rabbit_alarm:register(self(), {?MODULE, conserve_resources, []}).


control_throttle(State = #reader_state{state              = CS,
                                       conserve_resources = Mem,
                                       heartbeat = Heartbeat}) ->
    case {CS, Mem orelse credit_flow:blocked()} of
        {running,   true} -> State#reader_state{state = blocking};
        {blocking, false} -> rabbit_heartbeat:resume_monitor(Heartbeat),
                             State#reader_state{state = running};
        {blocked,  false} -> rabbit_heartbeat:resume_monitor(Heartbeat),
                             State#reader_state{state = running};
        {_,            _} -> State
    end.

maybe_block(State = #reader_state{state = blocking, heartbeat = Heartbeat},
            #stomp_frame{command = 'SEND'}) ->
    rabbit_heartbeat:pause_monitor(Heartbeat),
    State#reader_state{state = blocked};
maybe_block(State, _) ->
    State.

run_socket(State = #reader_state{state = blocked}) ->
    State;
run_socket(State = #reader_state{recv_outstanding = true}) ->
    State;
run_socket(State = #reader_state{socket = Sock}) ->
    _ = rabbit_net:setopts(Sock, [{active, once}]),
    State#reader_state{recv_outstanding = true}.


terminate(Reason, undefined) ->
    log_reason(Reason, undefined),
    {stop, Reason};
terminate(Reason, State = #reader_state{processor_state = ProcState}) ->
    maybe_emit_stats(State),
    rabbit_core_metrics:connection_closed(self()),
    Infos = infos(?OTHER_METRICS, State),
    rabbit_event:notify(connection_closed, Infos),
    rabbit_networking:unregister_non_amqp_connection(self()),
    log_reason(Reason, State),
    _ = rabbit_stomp_processor:flush_and_die(ProcState),
    {stop, Reason}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


log_reason({network_error, {ssl_upgrade_error, closed}, ConnName}, _State) ->
    rabbit_log_connection:error("STOMP detected TLS upgrade error on ~ts: connection closed",
        [ConnName]);


log_reason({network_error,
            {ssl_upgrade_error,
             {tls_alert, "handshake failure"}}, ConnName}, _State) ->
    log_tls_alert(handshake_failure, ConnName);
log_reason({network_error,
            {ssl_upgrade_error,
             {tls_alert, "unknown ca"}}, ConnName}, _State) ->
    log_tls_alert(unknown_ca, ConnName);
log_reason({network_error,
            {ssl_upgrade_error,
             {tls_alert, {Err, _}}}, ConnName}, _State) ->
    log_tls_alert(Err, ConnName);
log_reason({network_error,
            {ssl_upgrade_error,
             {tls_alert, Alert}}, ConnName}, _State) ->
    log_tls_alert(Alert, ConnName);
log_reason({network_error, {ssl_upgrade_error, Reason}, ConnName}, _State) ->
    rabbit_log_connection:error("STOMP detected TLS upgrade error on ~ts: ~tp",
        [ConnName, Reason]);

log_reason({network_error, Reason, ConnName}, _State) ->
    rabbit_log_connection:error("STOMP detected network error on ~ts: ~tp",
        [ConnName, Reason]);

log_reason({network_error, Reason}, _State) ->
    rabbit_log_connection:error("STOMP detected network error: ~tp", [Reason]);

log_reason({shutdown, client_heartbeat_timeout},
           #reader_state{ processor_state = ProcState }) ->
    AdapterName = rabbit_stomp_processor:adapter_name(ProcState),
    rabbit_log_connection:warning("STOMP detected missed client heartbeat(s) "
                                  "on connection ~ts, closing it", [AdapterName]);

log_reason({shutdown, {server_initiated_close, Reason}},
           #reader_state{conn_name = ConnName}) ->
    rabbit_log_connection:info("closing STOMP connection ~tp (~ts), reason: ~ts",
                               [self(), ConnName, Reason]);

log_reason(normal, #reader_state{conn_name  = ConnName}) ->
    rabbit_log_connection:info("closing STOMP connection ~tp (~ts)", [self(), ConnName]);

log_reason(shutdown, undefined) ->
    rabbit_log_connection:error("closing STOMP connection that never completed connection handshake (negotiation)");

log_reason(Reason, #reader_state{processor_state = ProcState}) ->
    AdapterName = rabbit_stomp_processor:adapter_name(ProcState),
    rabbit_log_connection:warning("STOMP connection ~ts terminated"
                                  " with reason ~tp, closing it", [AdapterName, Reason]).

log_tls_alert(handshake_failure, ConnName) ->
    rabbit_log_connection:error("STOMP detected TLS upgrade error on ~ts: handshake failure",
        [ConnName]);
log_tls_alert(unknown_ca, ConnName) ->
    rabbit_log_connection:error("STOMP detected TLS certificate verification error on ~ts: alert 'unknown CA'",
        [ConnName]);
log_tls_alert(Alert, ConnName) ->
    rabbit_log_connection:error("STOMP detected TLS upgrade error on ~ts: alert ~ts",
        [ConnName, Alert]).


%%----------------------------------------------------------------------------

processor_args(Configuration, Sock) ->
    RealSocket = rabbit_net:unwrap_socket(Sock),
    SendFun = fun(IoData) ->
                      case rabbit_net:send(RealSocket, IoData) of
                          ok ->
                              ok;
                          {error, Reason} ->
                              exit({send_failed, Reason})
                      end
              end,
    {ok, {PeerAddr, _PeerPort}} = rabbit_net:sockname(RealSocket),
    {SendFun, adapter_info(Sock),
     ssl_login_name(RealSocket, Configuration), PeerAddr}.

adapter_info(Sock) ->
    amqp_connection:socket_adapter_info(Sock, {'STOMP', 0}).

ssl_login_name(_Sock, #stomp_configuration{ssl_cert_login = false}) ->
    none;
ssl_login_name(Sock, #stomp_configuration{ssl_cert_login = true}) ->
    case rabbit_net:peercert(Sock) of
        {ok, C}              -> case rabbit_ssl:peer_cert_auth_name(C) of
                                    unsafe    -> none;
                                    not_found -> none;
                                    Name      -> Name
                                end;
        {error, no_peercert} -> none;
        nossl                -> none
    end.

%%----------------------------------------------------------------------------

start_heartbeats(_,   {0,0}    ) -> ok;
start_heartbeats(Pid, Heartbeat) -> Pid ! {start_heartbeats, Heartbeat}.

maybe_emit_stats(State) ->
    rabbit_event:if_enabled(State, #reader_state.stats_timer,
                            fun() -> emit_stats(State) end).

%% emit_stats(State=#reader_state{connection = C}) when C == none; C == undefined ->
%%     %% Avoid emitting stats on terminate when the connection has not yet been
%%     %% established, as this causes orphan entries on the stats database
%%     State1 = rabbit_event:reset_stats_timer(State, #reader_state.stats_timer),
%%     ensure_stats_timer(State1);
emit_stats(State) ->
    [{_, Pid},
     {_, Recv_oct},
     {_, Send_oct},
     {_, Reductions}] = infos(?SIMPLE_METRICS, State),
    Infos = infos(?OTHER_METRICS, State),
    rabbit_core_metrics:connection_stats(Pid, Infos),
    rabbit_core_metrics:connection_stats(Pid, Recv_oct, Send_oct, Reductions),
    State1 = rabbit_event:reset_stats_timer(State, #reader_state.stats_timer),
    ensure_stats_timer(State1).

ensure_stats_timer(State = #reader_state{}) ->
    rabbit_event:ensure_stats_timer(State, #reader_state.stats_timer, emit_stats).

%%----------------------------------------------------------------------------


processor_state(#reader_state{ processor_state = ProcState }) -> ProcState.
processor_state(ProcState, #reader_state{} = State) ->
    State#reader_state{ processor_state = ProcState}.

%%----------------------------------------------------------------------------

infos(Items, State) -> [{Item, info_internal(Item, State)} || Item <- Items].

info_internal(pid, _) -> self();
info_internal(SockStat, #reader_state{socket = Sock}) when SockStat =:= recv_oct;
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
info_internal(timeout, #reader_state{timeout_sec = {_, Receive}}) ->
    Receive;
info_internal(timeout, #reader_state{timeout_sec = undefined}) ->
    0;
info_internal(conn_name, #reader_state{conn_name = Val}) ->
    rabbit_data_coercion:to_binary(Val);
info_internal(name, #reader_state{conn_name = Val}) ->
    rabbit_data_coercion:to_binary(Val);
info_internal(connection, #reader_state{connection = _Val}) ->
    self();
info_internal(connection_state, #reader_state{state = Val}) ->
    Val;
info_internal(Key, #reader_state{processor_state = ProcState}) ->
    rabbit_stomp_processor:info(Key, ProcState).
