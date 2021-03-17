%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_web_stomp_handler).
-behaviour(cowboy_websocket).
-behaviour(cowboy_sub_protocol).

-include_lib("rabbitmq_stomp/include/rabbit_stomp.hrl").
-include_lib("rabbitmq_stomp/include/rabbit_stomp_frame.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%% Websocket.
-export([
    init/2,
    websocket_init/1,
    websocket_handle/2,
    websocket_info/2,
    terminate/3
]).
-export([close_connection/2]).

%% cowboy_sub_protocol
-export([upgrade/4,
         upgrade/5,
         takeover/7]).

-record(state, {
    frame_type,
    heartbeat_mode,
    heartbeat,
    heartbeat_sup,
    parse_state,
    proc_state,
    state,
    conserve_resources,
    socket,
    peername,
    auth_hd,
    stats_timer,
    connection
}).

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
    Env0 = maps:get(env, Opts, #{}),
    Env = Env0#{socket => Sock},
    cowboy_websocket:takeover(Parent, Ref, Socket, Transport, maps:put(env, Env, Opts), Buffer,
                              {Handler, HandlerState#state{socket = Sock}}).

%% Websocket.
init(Req0, Opts) ->
    {PeerAddr, _PeerPort} = maps:get(peer, Req0),
    {_, KeepaliveSup} = lists:keyfind(keepalive_sup, 1, Opts),
    SockInfo = maps:get(proxy_header, Req0, undefined),
    Req = case cowboy_req:parse_header(<<"sec-websocket-protocol">>, Req0) of
        undefined  -> Req0;
        Protocols ->
            case filter_stomp_protocols(Protocols) of
                [] -> Req0;
                [StompProtocol|_] ->
                    cowboy_req:set_resp_header(<<"sec-websocket-protocol">>,
                        StompProtocol, Req0)
            end
    end,
    WsOpts0 = proplists:get_value(ws_opts, Opts, #{}),
    WsOpts  = maps:merge(#{compress => true}, WsOpts0),
    {?MODULE, Req, #state{
        frame_type         = proplists:get_value(type, Opts, text),
        heartbeat_sup      = KeepaliveSup,
        heartbeat          = {none, none},
        heartbeat_mode     = heartbeat,
        state              = running,
        conserve_resources = false,
        socket             = SockInfo,
        peername           = PeerAddr,
        auth_hd            = cowboy_req:header(<<"authorization">>, Req)
    }, WsOpts}.

websocket_init(State) ->
    ok = file_handle_cache:obtain(),
    process_flag(trap_exit, true),
    {ok, ProcessorState} = init_processor_state(State),
    {ok, rabbit_event:init_stats_timer(
           State#state{proc_state     = ProcessorState,
                       parse_state    = rabbit_stomp_frame:initial_state()},
           #state.stats_timer)}.

-spec close_connection(pid(), string()) -> 'ok'.
close_connection(Pid, Reason) ->
    _ = rabbit_log_connection:info("Web STOMP: will terminate connection process ~p, reason: ~s",
                               [Pid, Reason]),
    sys:terminate(Pid, Reason),
    ok.

init_processor_state(#state{socket=Sock, peername=PeerAddr, auth_hd=AuthHd}) ->
    Self = self(),
    SendFun = fun (_Sync, Data) ->
                      Self ! {send, Data},
                      ok
              end,

    SSLLogin = application:get_env(rabbitmq_stomp, ssl_cert_login, false),
    StompConfig0 = #stomp_configuration{ssl_cert_login = SSLLogin, implicit_connect = false},
    UseHTTPAuth = application:get_env(rabbitmq_web_stomp, use_http_auth, false),
    UserConfig = application:get_env(rabbitmq_stomp, default_user, undefined),
    StompConfig1 = rabbit_stomp:parse_default_user(UserConfig, StompConfig0),
    StompConfig2 = case UseHTTPAuth of
        true ->
            case AuthHd of
                undefined ->
                    %% We fall back to the default STOMP credentials.
                    StompConfig1#stomp_configuration{force_default_creds = true};
                _ ->
                    {basic, HTTPLogin, HTTPPassCode}
                        = cow_http_hd:parse_authorization(AuthHd),
                    StompConfig0#stomp_configuration{
                      default_login = HTTPLogin,
                      default_passcode = HTTPPassCode,
                      force_default_creds = true}
            end;
        false ->
            StompConfig1
    end,

    AdapterInfo = amqp_connection:socket_adapter_info(Sock, {'Web STOMP', 0}),
    RealSocket = rabbit_net:unwrap_socket(Sock),
    LoginNameFromCertificate = rabbit_stomp_reader:ssl_login_name(RealSocket, StompConfig2),
    ProcessorState = rabbit_stomp_processor:initial_state(
        StompConfig2,
        {SendFun, AdapterInfo, LoginNameFromCertificate, PeerAddr}),
    {ok, ProcessorState}.

websocket_handle({text, Data}, State) ->
    handle_data(Data, State);
websocket_handle({binary, Data}, State) ->
    handle_data(Data, State);
websocket_handle(_Frame, State) ->
    {ok, State}.

websocket_info({send, Msg}, State=#state{frame_type=FrameType}) ->
    {reply, {FrameType, Msg}, State};

websocket_info({conserve_resources, Conserve}, State) ->
    NewState = State#state{conserve_resources = Conserve},
    handle_credits(control_throttle(NewState));
websocket_info({bump_credit, Msg}, State) ->
    credit_flow:handle_bump_msg(Msg),
    handle_credits(control_throttle(State));

websocket_info(#'basic.consume_ok'{}, State) ->
    {ok, State};
websocket_info(#'basic.cancel_ok'{}, State) ->
    {ok, State};
websocket_info(#'basic.ack'{delivery_tag = Tag, multiple = IsMulti},
               State=#state{ proc_state = ProcState0 }) ->
    ProcState = rabbit_stomp_processor:flush_pending_receipts(Tag,
                                                              IsMulti,
                                                              ProcState0),
    {ok, State#state{ proc_state = ProcState }};
websocket_info({Delivery = #'basic.deliver'{},
               #amqp_msg{props = Props, payload = Payload},
               DeliveryCtx},
               State=#state{ proc_state = ProcState0 }) ->
    ProcState = rabbit_stomp_processor:send_delivery(Delivery,
                                                     Props,
                                                     Payload,
                                                     DeliveryCtx,
                                                     ProcState0),
    {ok, State#state{ proc_state = ProcState }};
websocket_info(#'basic.cancel'{consumer_tag = Ctag},
               State=#state{ proc_state = ProcState0 }) ->
    case rabbit_stomp_processor:cancel_consumer(Ctag, ProcState0) of
      {ok, ProcState, _Connection} ->
        {ok, State#state{ proc_state = ProcState }};
      {stop, _Reason, ProcState} ->
        stop(State#state{ proc_state = ProcState })
    end;

websocket_info({start_heartbeats, _},
               State = #state{heartbeat_mode = no_heartbeat}) ->
    {ok, State};

websocket_info({start_heartbeats, {0, 0}}, State) ->
    {ok, State};
websocket_info({start_heartbeats, {SendTimeout, ReceiveTimeout}},
               State = #state{socket         = Sock,
                              heartbeat_sup  = SupPid,
                              heartbeat_mode = heartbeat}) ->
    Self = self(),
    SendFun = fun () -> Self ! {send, <<$\n>>}, ok end,
    ReceiveFun = fun() -> Self ! client_timeout end,
    Heartbeat = rabbit_heartbeat:start(SupPid, Sock, SendTimeout,
                                       SendFun, ReceiveTimeout, ReceiveFun),
    {ok, State#state{heartbeat = Heartbeat}};
websocket_info(client_timeout, State) ->
    stop(State);

%%----------------------------------------------------------------------------
websocket_info({'EXIT', From, Reason},
               State=#state{ proc_state = ProcState0 }) ->
  case rabbit_stomp_processor:handle_exit(From, Reason, ProcState0) of
    {stop, _Reason, ProcState} ->
        stop(State#state{ proc_state = ProcState });
    unknown_exit ->
        stop(State)
  end;
%%----------------------------------------------------------------------------

websocket_info(emit_stats, State) ->
    {ok, emit_stats(State)};

websocket_info(Msg, State) ->
    _ = rabbit_log_connection:info("Web STOMP: unexpected message ~p~n",
                    [Msg]),
    {ok, State}.

terminate(_Reason, _Req, #state{proc_state = undefined}) ->
    ok;
terminate(_Reason, _Req, #state{proc_state = ProcState}) ->
    rabbit_stomp_processor:flush_and_die(ProcState),
    ok.

%%----------------------------------------------------------------------------

%% The protocols v10.stomp, v11.stomp and v12.stomp are registered
%% at IANA: https://www.iana.org/assignments/websocket/websocket.xhtml

filter_stomp_protocols(Protocols) ->
    lists:reverse(lists:sort(lists:filter(
        fun(<< "v1", C, ".stomp">>)
            when C =:= $2; C =:= $1; C =:= $0 -> true;
           (_) ->
            false
        end,
        Protocols))).

%%----------------------------------------------------------------------------

handle_data(Data, State0) ->
    case handle_data1(Data, State0) of
        {ok, State1 = #state{state = blocked}} ->
            {[{active, false}], State1};
        {error, Error0} ->
            Error1 = rabbit_misc:format("~p", [Error0]),
            _ = rabbit_log_connection:error("STOMP detected framing error '~s'~n", [Error1]),
            stop(State0, 1007, Error1);
        Other ->
            Other
    end.

handle_data1(<<>>, State) ->
    {ok, ensure_stats_timer(State)};
handle_data1(Bytes, State = #state{proc_state  = ProcState,
                                   parse_state = ParseState}) ->
    case rabbit_stomp_frame:parse(Bytes, ParseState) of
        {more, ParseState1} ->
            {ok, ensure_stats_timer(State#state{ parse_state = ParseState1 })};
        {ok, Frame, Rest} ->
            case rabbit_stomp_processor:process_frame(Frame, ProcState) of
                {ok, ProcState1, ConnPid} ->
                    ParseState1 = rabbit_stomp_frame:initial_state(),
                    State1 = maybe_block(State, Frame),
                    handle_data1(
                      Rest,
                      State1 #state{ parse_state = ParseState1,
                                     proc_state  = ProcState1,
                                     connection  = ConnPid });
                {stop, _Reason, ProcState1} ->
                    stop(State#state{ proc_state = ProcState1 })
            end;
        Other ->
            Other
    end.

maybe_block(State = #state{state = blocking, heartbeat = Heartbeat},
            #stomp_frame{command = "SEND"}) ->
    rabbit_heartbeat:pause_monitor(Heartbeat),
    State#state{state = blocked};
maybe_block(State, _) ->
    State.

stop(State) ->
    stop(State, 1000, "STOMP died").

stop(State = #state{proc_state = ProcState}, CloseCode, Error0) ->
    maybe_emit_stats(State),
    ok = file_handle_cache:release(),
    rabbit_stomp_processor:flush_and_die(ProcState),
    Error1 = rabbit_data_coercion:to_binary(Error0),
    {[{close, CloseCode, Error1}], State}.

%%----------------------------------------------------------------------------

handle_credits(State0) ->
    case control_throttle(State0) of
        State = #state{state = running} ->
            {[{active, true}], State};
        State ->
            {ok, State}
    end.

control_throttle(State = #state{state              = CS,
                                conserve_resources = Mem}) ->
    case {CS, Mem orelse credit_flow:blocked()} of
        {running,   true} -> blocking(State);
        {blocking, false} -> running(State);
        {blocked,  false} -> running(State);
        {_,            _} -> State
    end.

blocking(State) ->
    State#state{state = blocking}.

running(State = #state{heartbeat=Heartbeat}) ->
    rabbit_heartbeat:resume_monitor(Heartbeat),
    State#state{state = running}.

%%----------------------------------------------------------------------------

ensure_stats_timer(State) ->
    rabbit_event:ensure_stats_timer(State, #state.stats_timer, emit_stats).

maybe_emit_stats(State) ->
    rabbit_event:if_enabled(State, #state.stats_timer,
                                fun() -> emit_stats(State) end).

emit_stats(State=#state{connection = C}) when C == none; C == undefined ->
    %% Avoid emitting stats on terminate when the connection has not yet been
    %% established, as this causes orphan entries on the stats database
    State1 = rabbit_event:reset_stats_timer(State, #state.stats_timer),
    State1;
emit_stats(State=#state{socket=Sock, state=RunningState, connection=Conn}) ->
    SockInfos = case rabbit_net:getstat(Sock,
            [recv_oct, recv_cnt, send_oct, send_cnt, send_pend]) of
        {ok,    SI} -> SI;
        {error,  _} -> []
    end,
    Infos = [{pid, Conn}, {state, RunningState}|SockInfos],
    rabbit_core_metrics:connection_stats(Conn, Infos),
    rabbit_event:notify(connection_stats, Infos),
    State1 = rabbit_event:reset_stats_timer(State, #state.stats_timer),
    State1.
