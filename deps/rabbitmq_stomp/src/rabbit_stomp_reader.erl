%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
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

-define(SIMPLE_METRICS, [pid, recv_oct, send_oct, reductions]).
-define(OTHER_METRICS, [recv_cnt, send_cnt, send_pend, garbage_collection, state,
                        timeout]).

-record(reader_state, {socket, conn_name, parse_state, processor_state, state,
                       conserve_resources, recv_outstanding, stats_timer,
                       parent, connection, heartbeat_sup, heartbeat,
                       timeout_sec %% heartbeat timeout value used, 0 means
                                   %% heartbeats are disabled
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
            ProcInitArgs = processor_args(Configuration, Sock),
            ProcState = rabbit_stomp_processor:initial_state(Configuration,
                                                             ProcInitArgs),

            _ = rabbit_log_connection:info("accepting STOMP connection ~p (~s)~n",
                [self(), ConnStr]),

            ParseState = rabbit_stomp_frame:initial_state(),
            _ = register_resource_alarm(),
            gen_server2:enter_loop(?MODULE, [],
              rabbit_event:init_stats_timer(
                run_socket(control_throttle(
                  #reader_state{socket             = RealSocket,
                                conn_name          = ConnStr,
                                parse_state        = ParseState,
                                processor_state    = ProcState,
                                heartbeat_sup      = SupHelperPid,
                                heartbeat          = {none, none},
                                state              = running,
                                conserve_resources = false,
                                recv_outstanding   = false})), #reader_state.stats_timer),
              {backoff, 1000, 1000, 10000});
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
    {stop, {stomp_unexpected_call, Msg, From}, State}.

handle_cast({close_connection, Reason}, State) ->
    {stop, {shutdown, {server_initiated_close, Reason}}, State};
handle_cast(client_timeout, State) ->
    {stop, {shutdown, client_heartbeat_timeout}, State};
handle_cast(Msg, State) ->
    {stop, {stomp_unexpected_cast, Msg}, State}.


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
handle_info({inet_reply, _Sock, {error, closed}}, State) ->
    {stop, normal, State};
handle_info({inet_reply, _, ok}, State) ->
    {noreply, State, hibernate};
handle_info({inet_reply, _, Status}, State) ->
    {stop, Status, State};
handle_info(emit_stats, State) ->
    {noreply, emit_stats(State), hibernate};
handle_info({conserve_resources, Conserve}, State) ->
    NewState = State#reader_state{conserve_resources = Conserve},
    {noreply, run_socket(control_throttle(NewState)), hibernate};
handle_info({bump_credit, Msg}, State) ->
    credit_flow:handle_bump_msg(Msg),
    {noreply, run_socket(control_throttle(State)), hibernate};

%%----------------------------------------------------------------------------

handle_info(client_timeout, State) ->
    {stop, {shutdown, client_heartbeat_timeout}, State};

%%----------------------------------------------------------------------------

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State, hibernate};
handle_info(#'basic.cancel_ok'{}, State) ->
    {noreply, State, hibernate};
handle_info(#'basic.ack'{delivery_tag = Tag, multiple = IsMulti}, State) ->
    ProcState = processor_state(State),
    NewProcState = rabbit_stomp_processor:flush_pending_receipts(Tag,
                                                                 IsMulti,
                                                                 ProcState),
    {noreply, processor_state(NewProcState, State), hibernate};
handle_info({Delivery = #'basic.deliver'{},
             Message = #amqp_msg{}},
             State) ->
    %% receiving a message from a quorum queue
    %% no delivery context
    handle_info({Delivery, Message, undefined}, State);
handle_info({Delivery = #'basic.deliver'{},
             #amqp_msg{props = Props, payload = Payload},
             DeliveryCtx},
             State) ->
    ProcState = processor_state(State),
    NewProcState = rabbit_stomp_processor:send_delivery(Delivery,
                                                        Props,
                                                        Payload,
                                                        DeliveryCtx,
                                                        ProcState),
    {noreply, processor_state(NewProcState, State), hibernate};
handle_info(#'basic.cancel'{consumer_tag = Ctag}, State) ->
    ProcState = processor_state(State),
    case rabbit_stomp_processor:cancel_consumer(Ctag, ProcState) of
      {ok, NewProcState, _} ->
        {noreply, processor_state(NewProcState, State), hibernate};
      {stop, Reason, NewProcState} ->
        {stop, Reason, processor_state(NewProcState, State)}
    end;

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
handle_info({'EXIT', From, Reason}, State) ->
  ProcState = processor_state(State),
  case rabbit_stomp_processor:handle_exit(From, Reason, ProcState) of
    {stop, NewReason, NewProcState} ->
        {stop, NewReason, processor_state(NewProcState, State)};
    unknown_exit ->
        {stop, {connection_died, Reason}, State}
  end.
%%----------------------------------------------------------------------------

process_received_bytes([], State) ->
    {ok, State};
process_received_bytes(Bytes,
                       State = #reader_state{
                         processor_state = ProcState,
                         parse_state     = ParseState}) ->
    case rabbit_stomp_frame:parse(Bytes, ParseState) of
        {more, ParseState1} ->
            {ok, State#reader_state{parse_state = ParseState1}};
        {ok, Frame, Rest} ->
            case rabbit_stomp_processor:process_frame(Frame, ProcState) of
                {ok, NewProcState, Conn} ->
                    PS = rabbit_stomp_frame:initial_state(),
                    NextState = maybe_block(State, Frame),
                    process_received_bytes(Rest, NextState#reader_state{
                        processor_state = NewProcState,
                        parse_state     = PS,
                        connection      = Conn});
                {stop, Reason, NewProcState} ->
                    {stop, Reason,
                     processor_state(NewProcState, State)}
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
            #stomp_frame{command = "SEND"}) ->
    rabbit_heartbeat:pause_monitor(Heartbeat),
    State#reader_state{state = blocked};
maybe_block(State, _) ->
    State.

run_socket(State = #reader_state{state = blocked}) ->
    State;
run_socket(State = #reader_state{recv_outstanding = true}) ->
    State;
run_socket(State = #reader_state{socket = Sock}) ->
    rabbit_net:setopts(Sock, [{active, once}]),
    State#reader_state{recv_outstanding = true}.


terminate(Reason, undefined) ->
    log_reason(Reason, undefined),
    {stop, Reason};
terminate(Reason, State = #reader_state{processor_state = ProcState}) ->
  maybe_emit_stats(State),
  log_reason(Reason, State),
  _ = rabbit_stomp_processor:flush_and_die(ProcState),
    {stop, Reason}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


log_reason({network_error, {ssl_upgrade_error, closed}, ConnStr}, _State) ->
    _ = rabbit_log_connection:error("STOMP detected TLS upgrade error on ~s: connection closed~n",
        [ConnStr]);


log_reason({network_error,
            {ssl_upgrade_error,
             {tls_alert, "handshake failure"}}, ConnStr}, _State) ->
    log_tls_alert(handshake_failure, ConnStr);
log_reason({network_error,
            {ssl_upgrade_error,
             {tls_alert, "unknown ca"}}, ConnStr}, _State) ->
    log_tls_alert(unknown_ca, ConnStr);
log_reason({network_error,
            {ssl_upgrade_error,
             {tls_alert, {Err, _}}}, ConnStr}, _State) ->
    log_tls_alert(Err, ConnStr);
log_reason({network_error,
            {ssl_upgrade_error,
             {tls_alert, Alert}}, ConnStr}, _State) ->
    log_tls_alert(Alert, ConnStr);
log_reason({network_error, {ssl_upgrade_error, Reason}, ConnStr}, _State) ->
    _ = rabbit_log_connection:error("STOMP detected TLS upgrade error on ~s: ~p~n",
        [ConnStr, Reason]);

log_reason({network_error, Reason, ConnStr}, _State) ->
    _ = rabbit_log_connection:error("STOMP detected network error on ~s: ~p~n",
        [ConnStr, Reason]);

log_reason({network_error, Reason}, _State) ->
    _ = rabbit_log_connection:error("STOMP detected network error: ~p~n", [Reason]);

log_reason({shutdown, client_heartbeat_timeout},
           #reader_state{ processor_state = ProcState }) ->
    AdapterName = rabbit_stomp_processor:adapter_name(ProcState),
    _ = rabbit_log_connection:warning("STOMP detected missed client heartbeat(s) "
                                  "on connection ~s, closing it~n", [AdapterName]);

log_reason({shutdown, {server_initiated_close, Reason}},
           #reader_state{conn_name = ConnName}) ->
    _ = rabbit_log_connection:info("closing STOMP connection ~p (~s), reason: ~s~n",
                               [self(), ConnName, Reason]);

log_reason(normal, #reader_state{conn_name  = ConnName}) ->
    _ = rabbit_log_connection:info("closing STOMP connection ~p (~s)~n", [self(), ConnName]);

log_reason(shutdown, undefined) ->
    _ = rabbit_log_connection:error("closing STOMP connection that never completed connection handshake (negotiation)~n", []);

log_reason(Reason, #reader_state{processor_state = ProcState}) ->
    AdapterName = rabbit_stomp_processor:adapter_name(ProcState),
    _ = rabbit_log_connection:warning("STOMP connection ~s terminated"
                                  " with reason ~p, closing it~n", [AdapterName, Reason]).

log_tls_alert(handshake_failure, ConnStr) ->
    _ = rabbit_log_connection:error("STOMP detected TLS upgrade error on ~s: handshake failure~n",
        [ConnStr]);
log_tls_alert(unknown_ca, ConnStr) ->
    _ = rabbit_log_connection:error("STOMP detected TLS certificate verification error on ~s: alert 'unknown CA'~n",
        [ConnStr]);
log_tls_alert(Alert, ConnStr) ->
    _ = rabbit_log_connection:error("STOMP detected TLS upgrade error on ~s: alert ~s~n",
        [ConnStr, Alert]).


%%----------------------------------------------------------------------------

processor_args(Configuration, Sock) ->
    RealSocket = rabbit_net:unwrap_socket(Sock),
    SendFun = fun (sync, IoData) ->
                      %% no messages emitted
                      catch rabbit_net:send(RealSocket, IoData);
                  (async, IoData) ->
                      %% {inet_reply, _, _} will appear soon
                      %% We ignore certain errors here, as we will be
                      %% receiving an asynchronous notification of the
                      %% same (or a related) fault shortly anyway. See
                      %% bug 21365.
                      catch rabbit_net:port_command(RealSocket, IoData)
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

emit_stats(State=#reader_state{connection = C}) when C == none; C == undefined ->
    %% Avoid emitting stats on terminate when the connection has not yet been
    %% established, as this causes orphan entries on the stats database
    State1 = rabbit_event:reset_stats_timer(State, #reader_state.stats_timer),
    ensure_stats_timer(State1);
emit_stats(State) ->
    [{_, Pid}, {_, Recv_oct}, {_, Send_oct}, {_, Reductions}] = I
	= infos(?SIMPLE_METRICS, State),
    Infos = infos(?OTHER_METRICS, State),
    rabbit_core_metrics:connection_stats(Pid, Infos),
    rabbit_core_metrics:connection_stats(Pid, Recv_oct, Send_oct, Reductions),
    rabbit_event:notify(connection_stats, Infos ++ I),
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

info_internal(pid, State) -> info_internal(connection, State);
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
info_internal(connection, #reader_state{connection = Val}) ->
    Val;
info_internal(connection_state, #reader_state{state = Val}) ->
    Val;
info_internal(Key, #reader_state{processor_state = ProcState}) ->
    rabbit_stomp_processor:info(Key, ProcState).
