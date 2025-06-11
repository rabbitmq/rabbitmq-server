%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_amqp_reader).

-include_lib("kernel/include/logger.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("amqp10_common/include/amqp10_types.hrl").
-include("rabbit_amqp.hrl").
-include("rabbit_amqp_metrics.hrl").
-include("rabbit_amqp_reader.hrl").

-export([init/1,
         info/2,
         mainloop/2,
         set_credential/2]).

-export([system_continue/3,
         system_terminate/4,
         system_code_change/4]).

-export([advertise_sasl_mechanism/1,
         handle_input/2,
         handle_other/2,
         ensure_stats_timer/1]).

-import(rabbit_amqp_util, [protocol_error/3]).

-define(IS_RUNNING(State), State#v1.connection_state =:= running).

unpack_from_0_9_1(
  {Sock, PendingRecv, SupPid, Buf, BufLen, ProxySocket,
   ConnectionName, Host, PeerHost, Port, PeerPort, ConnectedAt, StatsTimer},
  Parent) ->
    logger:update_process_metadata(#{connection => ConnectionName}),
    #v1{parent           = Parent,
        websocket        = false,
        sock             = Sock,
        callback         = {frame_header, sasl},
        pending_recv     = PendingRecv,
        helper_sup       = SupPid,
        buf              = Buf,
        buf_len          = BufLen,
        proxy_socket     = ProxySocket,
        connection_state = received_amqp3100,
        stats_timer      = StatsTimer,
        connection = #v1_connection{
                        name = ConnectionName,
                        host = Host,
                        peer_host = PeerHost,
                        port = Port,
                        peer_port = PeerPort,
                        connected_at = ConnectedAt}}.

-spec system_continue(pid(), [sys:dbg_opt()], state()) -> no_return() | ok.
system_continue(Parent, Deb, State) ->
    ?MODULE:mainloop(Deb, State#v1{parent = Parent}).

-spec system_terminate(term(), pid(), [sys:dbg_opt()], term()) -> no_return().
system_terminate(Reason, _Parent, _Deb, _State) ->
    exit(Reason).

-spec system_code_change(term(), module(), undefined | term(), term()) -> {ok, term()}.
system_code_change(Misc, _Module, _OldVsn, _Extra) ->
    {ok, Misc}.

server_properties() ->
    Props0 = rabbit_reader:server_properties(amqp_1_0),
    Props1 = [{{symbol, K}, {utf8, V}} || {K, longstr, V} <- Props0],
    Props = [{{symbol, <<"node">>}, {utf8, atom_to_binary(node())}} | Props1],
    {map, Props}.

-spec set_credential(pid(), binary()) -> ok.
set_credential(Pid, Credential) ->
    Pid ! {set_credential, Credential},
    ok.

%%--------------------------------------------------------------------------

recvloop(Deb, State = #v1{pending_recv = true}) ->
    mainloop(Deb, State);
recvloop(Deb, State = #v1{sock = Sock,
                          recv_len = RecvLen,
                          buf_len = BufLen})
  when BufLen < RecvLen ->
    case rabbit_net:setopts(Sock, [{active, once}]) of
        ok ->
            mainloop(Deb, State#v1{pending_recv = true});
        {error, Reason} ->
            throw({inet_error, Reason})
    end;
recvloop(Deb, State0 = #v1{recv_len = RecvLen,
                           buf = Buf,
                           buf_len = BufLen}) ->
    Bin = case Buf of
              [B] -> B;
              _ -> list_to_binary(lists:reverse(Buf))
          end,
    {Data, Rest} = split_binary(Bin, RecvLen),
    State1 = State0#v1{buf = [Rest],
                       buf_len = BufLen - RecvLen},
    State = handle_input(Data, State1),
    recvloop(Deb, State).

-spec mainloop([sys:dbg_opt()], state()) ->
    no_return() | ok.
mainloop(Deb, State = #v1{sock = Sock, buf = Buf, buf_len = BufLen}) ->
    case rabbit_net:recv(Sock) of
        {data, Data} ->
            State1 = maybe_resize_buffer(State, Data),
            recvloop(Deb, State1#v1{buf = [Data | Buf],
                                    buf_len = BufLen + byte_size(Data),
                                    pending_recv = false});
        closed when State#v1.connection_state =:= closed ->
            ok;
        closed ->
            throw(connection_closed_abruptly);
        {error, Reason} ->
            throw({inet_error, Reason});
        {other, {system, From, Request}} ->
            sys:handle_system_msg(Request, From, State#v1.parent,
                                  ?MODULE, Deb, State);
        {other, Other} ->
            case handle_other(Other, State) of
                stop     -> ok;
                NewState -> recvloop(Deb, NewState)
            end
    end.

maybe_resize_buffer(State=#v1{sock=Sock, dynamic_buffer_size=BufferSize0,
        dynamic_buffer_moving_average=MovingAvg0}, Data) ->
    LowDynamicBuffer = 128,
    HighDynamicBuffer = 131072,
    DataLen = byte_size(Data),
    MovingAvg = (MovingAvg0 * 7 + DataLen) / 8,
    if
        BufferSize0 < HighDynamicBuffer andalso MovingAvg > BufferSize0 * 0.9 ->
            BufferSize = min(BufferSize0 * 2, HighDynamicBuffer),
            case rabbit_net:setopts(Sock, [{buffer, BufferSize}]) of
                ok -> State#v1{
                    dynamic_buffer_size=BufferSize,
                    dynamic_buffer_moving_average=MovingAvg
                };
                {error, Reason} ->
                    throw({inet_error, Reason})
            end;
        BufferSize0 > LowDynamicBuffer andalso MovingAvg < BufferSize0 * 0.4 ->
            BufferSize = max(BufferSize0 div 2, LowDynamicBuffer),
            case rabbit_net:setopts(Sock, [{buffer, BufferSize}]) of
                ok -> State#v1{
                    dynamic_buffer_size=BufferSize,
                    dynamic_buffer_moving_average=MovingAvg
                };
                {error, Reason} ->
                    throw({inet_error, Reason})
            end;
        true ->
            State#v1{dynamic_buffer_moving_average=MovingAvg}
    end.

-spec handle_other(any(), state()) -> state() | stop.
handle_other(emit_stats, State) ->
    emit_stats(State);
handle_other(ensure_stats_timer, State) ->
    ensure_stats_timer(State);
handle_other({'EXIT', Parent, Reason}, State = #v1{parent = Parent}) ->
    ReasonString = rabbit_misc:format("broker forced connection closure with reason '~w'",
                                      [Reason]),
    _ = terminate(ReasonString, State),
    %% this is what we are expected to do according to
    %% http://www.erlang.org/doc/man/sys.html
    %%
    %% If we wanted to be *really* nice we should wait for a while for
    %% clients to close the socket at their end, just as we do in the
    %% ordinary error case. However, since this termination is
    %% initiated by our parent it is probably more important to exit
    %% quickly.
    exit(Reason);
handle_other({{'DOWN', ChannelNum}, _MRef, process, SessionPid, Reason}, State) ->
    handle_session_exit(ChannelNum, SessionPid, Reason, State);
handle_other(handshake_timeout, State = #v1{connection_state = ConnState})
  when ConnState =:= running orelse
       ConnState =:= closing orelse
       ConnState =:= closed ->
    State;
handle_other(handshake_timeout, State) ->
    throw({handshake_timeout, State#v1.callback});
handle_other(heartbeat_timeout, State = #v1{connection_state = closed}) ->
    State;
handle_other(heartbeat_timeout, State) ->
    Error = error_frame(?V_1_0_AMQP_ERROR_RESOURCE_LIMIT_EXCEEDED,
                        "no frame received from client within idle timeout threshold", []),
    handle_exception(State, 0, Error);
handle_other({rabbit_call, From, {shutdown, Explanation}},
             State = #v1{connection = #v1_connection{properties = Properties}}) ->
    Ret = case Explanation =:= "Node was put into maintenance mode" andalso
               ignore_maintenance(Properties) of
              true -> State;
              false -> terminate(Explanation, State)
          end,
    gen_server:reply(From, ok),
    Ret;
handle_other({rabbit_call, From, {info, Items}}, State) ->
    Reply = try infos(Items, State) of
                Infos ->
                    {ok, Infos}
            catch Error ->
                      {error, Error}
            end,
    gen_server:reply(From, Reply),
    State;
handle_other({'$gen_call', From, Req}, State) ->
    %% Delete this function clause when feature flag 'rabbitmq_4.1.0' becomes required.
    handle_other({rabbit_call, From, Req}, State);
handle_other({'$gen_cast', {force_event_refresh, Ref}}, State) ->
    case ?IS_RUNNING(State) of
        true ->
            Infos = infos(?CONNECTION_EVENT_KEYS, State),
            rabbit_event:notify(connection_created, Infos, Ref),
            rabbit_event:init_stats_timer(State, #v1.stats_timer);
        false ->
            %% Ignore, we will emit a connection_created event once we start running.
            State
    end;
handle_other(terminate_connection, _State) ->
    stop;
handle_other({set_credential, Cred}, State) ->
    set_credential0(Cred, State);
handle_other(credential_expired, State) ->
    Error = error_frame(?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS, "credential expired", []),
    handle_exception(State, 0, Error);
handle_other(Other, _State) ->
    %% internal error -> something worth dying for
    exit({unexpected_message, Other}).

switch_callback(State, Callback, Length) ->
    State#v1{callback = Callback,
             recv_len = Length}.

terminate(Reason, State)
  when ?IS_RUNNING(State) ->
    handle_exception(State, 0,
                     error_frame(?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                                 "Connection forced: ~tp", [Reason]));
terminate(_, _) ->
    stop.

%%--------------------------------------------------------------------------
%% error handling / termination

close(Error, State0 = #v1{connection = #v1_connection{timeout = Timeout}}) ->
    %% Client properties will be emitted in the connection_closed event by rabbit_reader.
    ClientProperties = i(client_properties, State0),
    put(client_properties, ClientProperties),

    %% "It is illegal to send any more frames (or bytes of any other kind)
    %% after sending a close frame." [2.7.9]
    %% Sessions might send frames via the writer proc.
    %% Therefore, let's first try to orderly shutdown our sessions.
    State = shutdown_sessions(State0),

    Time = case Timeout > 0 andalso
                Timeout < ?CLOSING_TIMEOUT of
               true -> Timeout;
               false -> ?CLOSING_TIMEOUT
           end,
    _TRef = erlang:send_after(Time, self(), terminate_connection),
    ok = send_on_channel0(State, #'v1_0.close'{error = Error}, amqp10_framing),
    State#v1{connection_state = closed}.

shutdown_sessions(#v1{tracked_channels = Channels} = State) ->
    maps:foreach(fun(_ChannelNum, Pid) ->
                         gen_server:cast(Pid, shutdown)
                 end, Channels),
    TimerRef = erlang:send_after(?SHUTDOWN_SESSIONS_TIMEOUT,
                                 self(),
                                 shutdown_sessions_timeout),
    wait_for_shutdown_sessions(TimerRef, State).

wait_for_shutdown_sessions(TimerRef, #v1{tracked_channels = Channels} = State)
  when map_size(Channels) =:= 0 ->
    ok = erlang:cancel_timer(TimerRef, [{async, false},
                                        {info, false}]),
    State;
wait_for_shutdown_sessions(TimerRef, #v1{tracked_channels = Channels} = State0) ->
    receive
        {{'DOWN', ChannelNum}, _MRef, process, SessionPid, _Reason} ->
            State = untrack_channel(ChannelNum, SessionPid, State0),
            wait_for_shutdown_sessions(TimerRef, State);
        shutdown_sessions_timeout ->
            ?LOG_INFO("sessions running ~b ms after requested to be shut down: ~p",
                      [?SHUTDOWN_SESSIONS_TIMEOUT, maps:values(Channels)]),
            State0
    end.

handle_session_exit(ChannelNum, SessionPid, Reason, State0) ->
    State = untrack_channel(ChannelNum, SessionPid, State0),
    S = case terminated_normally(Reason) of
            true ->
                State;
            false ->
                R = case Reason of
                        {RealReason, Trace} ->
                            error_frame(?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                                        "Session error: ~tp~n~tp",
                                        [RealReason, Trace]);
                        _ ->
                            error_frame(?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                                        "Session error: ~tp",
                                        [Reason])
                    end,
                handle_exception(State, ChannelNum, R)
        end,
    maybe_close(S).

terminated_normally(normal) ->
    true;
terminated_normally(shutdown) ->
    true;
terminated_normally({shutdown, _Term}) ->
    true;
terminated_normally(_Reason) ->
    false.

maybe_close(State = #v1{connection_state = closing}) ->
    close(undefined, State);
maybe_close(State) ->
    State.

error_frame(Condition, Fmt, Args) ->
    Description = list_to_binary(rabbit_misc:format(Fmt, Args)),
    #'v1_0.error'{condition = Condition,
                  description = {utf8, Description}}.

handle_exception(State = #v1{connection_state = closed}, Channel,
                 #'v1_0.error'{description = {utf8, Desc}}) ->
    ?LOG_ERROR("Error on AMQP 1.0 connection ~tp (~tp), channel number ~b:~n~tp",
               [self(), closed, Channel, Desc]),
    State;
handle_exception(State = #v1{connection_state = CS}, Channel,
                 Error = #'v1_0.error'{description = {utf8, Desc}})
  when ?IS_RUNNING(State) orelse CS =:= closing ->
    ?LOG_ERROR("Error on AMQP 1.0 connection ~tp (~tp), channel number ~b:~n~tp",
               [self(), CS, Channel, Desc]),
    close(Error, State);
handle_exception(State, _Channel, Error) ->
    silent_close_delay(),
    throw({handshake_error, State#v1.connection_state, Error}).

is_connection_frame(#'v1_0.open'{})  -> true;
is_connection_frame(#'v1_0.close'{}) -> true;
is_connection_frame(_)               -> false.

handle_frame(Mode, Channel, Body, State) ->
    try
        handle_frame0(Mode, Channel, Body, State)
    catch
        _:#'v1_0.error'{} = Reason ->
            handle_exception(State, Channel, Reason);
        _:{error, {not_allowed, Username}} ->
            %% section 2.8.15 in http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-complete-v1.0-os.pdf
            handle_exception(State,
                             Channel,
                             error_frame(
                               ?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS,
                               "Access for user '~ts' was refused: insufficient permissions",
                               [Username]));
        _:Reason:Trace ->
            handle_exception(State,
                             Channel,
                             error_frame(
                               ?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                               "Reader error: ~tp~n~tp",
                               [Reason, Trace]))
    end.

handle_frame0(amqp, Channel, _Body,
              #v1{connection = #v1_connection{channel_max = ChannelMax}})
  when Channel > ChannelMax ->
    protocol_error(?V_1_0_CONNECTION_ERROR_FRAMING_ERROR,
                   "channel number (~b) exceeds maximum channel number (~b)",
                   [Channel, ChannelMax]);
handle_frame0(_Mode, Channel, Body,
              State = #v1{connection_state = CS})
  when CS =:= closing orelse
       CS =:= closed ->
    Performative = parse_frame_body(Body, Channel),
    case is_connection_frame(Performative) of
        true  -> handle_connection_frame(Performative, State);
        false -> State
    end;
handle_frame0(Mode, Channel, Body, State) ->
    Performative = parse_frame_body(Body, Channel),
    case {Mode, is_connection_frame(Performative)} of
        {amqp, true}  -> handle_connection_frame(Performative, State);
        {amqp, false} -> handle_session_frame(Channel, Performative, State);
        {sasl, false} -> handle_sasl_frame(Performative, State)
    end.

%% "The frame body is defined as a performative followed by an opaque payload." [2.3.2]
parse_frame_body(Body, _Channel) ->
    BytesBody = byte_size(Body),
    {DescribedPerformative, BytesParsed} = amqp10_binary_parser:parse(Body),
    Performative = amqp10_framing:decode(DescribedPerformative),
    if BytesParsed < BytesBody ->
           Payload = binary_part(Body, BytesParsed, BytesBody - BytesParsed),
           ?TRACE("channel ~b ->~n ~tp~n followed by ~tb bytes of payload",
                  [_Channel, amqp10_framing:pprint(Performative), iolist_size(Payload)]),
           {Performative, Payload};
       BytesParsed =:= BytesBody ->
           ?TRACE("channel ~b ->~n ~tp",
                  [_Channel, amqp10_framing:pprint(Performative)]),
           Performative
    end.

handle_connection_frame(
  #'v1_0.open'{container_id = {utf8, ContainerId},
               max_frame_size = ClientMaxFrame,
               channel_max = ClientChannelMax,
               idle_time_out = IdleTimeout,
               hostname = Hostname,
               properties = Properties},
  #v1{connection_state = waiting_open,
      connection = Connection = #v1_connection{
                                   name = ConnectionName,
                                   user = User = #user{username = Username},
                                   auth_mechanism = {Mechanism, _Mod}
                                  },
      helper_sup = HelperSupPid,
      sock = Sock} = State0) ->
    Vhost = vhost(Hostname),
    logger:update_process_metadata(#{amqp_container => ContainerId,
                                     vhost => Vhost,
                                     user => Username}),
    ok = check_user_loopback(State0),
    ok = check_vhost_exists(Vhost, State0),
    ok = check_vhost_alive(Vhost),
    ok = rabbit_access_control:check_vhost_access(User, Vhost, {socket, Sock}, #{}),
    ok = check_vhost_connection_limit(Vhost, Username),
    ok = check_user_connection_limit(Username),
    Timer = maybe_start_credential_expiry_timer(User),
    rabbit_core_metrics:auth_attempt_succeeded(<<>>, Username, amqp10),
    notify_auth(user_authentication_success, Username, State0),
    ?LOG_INFO(
       "Connection from AMQP 1.0 container '~ts': user '~ts' authenticated "
       "using SASL mechanism ~s and granted access to vhost '~ts'",
       [ContainerId, Username, Mechanism, Vhost]),

    OutgoingMaxFrameSize = case ClientMaxFrame of
                               undefined ->
                                   unlimited;
                               {uint, Bytes}
                                 when Bytes >= ?MIN_MAX_FRAME_1_0_SIZE ->
                                   Bytes;
                               {uint, Bytes} ->
                                   protocol_error(
                                     ?V_1_0_AMQP_ERROR_FRAME_SIZE_TOO_SMALL,
                                     "max_frame_size (~w) < minimum maximum frame size (~w)",
                                     [Bytes, ?MIN_MAX_FRAME_1_0_SIZE])
                           end,
    SendTimeoutSec =
    case IdleTimeout of
        undefined ->
            0;
        {uint, Interval} ->
            if Interval =:= 0 ->
                   0;
               Interval < 1000 ->
                   %% "If a peer can not, for any reason support a proposed idle timeout, then it SHOULD
                   %% close the connection using a close frame with an error explaining why. There is no
                   %% requirement for peers to support arbitrarily short or long idle timeouts." [2.4.5]
                   %% rabbit_heartbeat does not want to support sub-second timeouts.
                   protocol_error(
                     ?V_1_0_AMQP_ERROR_NOT_ALLOWED,
                     "idle-time-out (~b ms) < minimum idle-time-out (1000 ms)",
                     [Interval]);
               Interval >= 1000 ->
                   Interval div 1000
            end
    end,
    {ok, ReceiveTimeoutSec} = application:get_env(rabbit, heartbeat),
    ReceiveTimeoutMillis = ReceiveTimeoutSec * 1000,
    Reader = self(),
    ReceiveFun = fun() -> Reader ! heartbeat_timeout end,
    SendFun = heartbeat_send_fun(Reader, State0),
    %% TODO: only start heartbeat receive timer at next next frame
    Heartbeater = rabbit_heartbeat:start(
                    HelperSupPid, Sock, ConnectionName,
                    SendTimeoutSec, SendFun,
                    ReceiveTimeoutSec, ReceiveFun),
    {ok, IncomingMaxFrameSize} = application:get_env(rabbit, frame_max),
    {ok, SessionMax} = application:get_env(rabbit, session_max_per_connection),
    %% "The channel-max value is the highest channel number that can be used on the connection.
    %% This value plus one is the maximum number of sessions that can be simultaneously active
    %% on the connection." [2.7.1]
    ChannelMax = SessionMax - 1,
    %% Assert config is valid.
    true = ChannelMax >= 0 andalso ChannelMax =< 16#ff_ff,
    EffectiveChannelMax = case ClientChannelMax of
                              undefined ->
                                  ChannelMax;
                              {ushort, N} ->
                                  min(N, ChannelMax)
                          end,
    State1 = State0#v1{connection_state = running,
                       connection = Connection#v1_connection{
                                      container_id = ContainerId,
                                      vhost = Vhost,
                                      incoming_max_frame_size = IncomingMaxFrameSize,
                                      outgoing_max_frame_size = OutgoingMaxFrameSize,
                                      channel_max = EffectiveChannelMax,
                                      properties = Properties,
                                      timeout = ReceiveTimeoutMillis,
                                      credential_timer = Timer},
                       heartbeater = Heartbeater},
    State = start_writer(State1),
    HostnameVal = case Hostname of
                      undefined -> undefined;
                      null -> undefined;
                      {utf8, Val} -> Val
                  end,
    ?LOG_DEBUG(
       "AMQP 1.0 connection.open frame: hostname = ~ts, extracted vhost = ~ts, idle-time-out = ~p",
       [HostnameVal, Vhost, IdleTimeout]),

    Infos = infos(?CONNECTION_EVENT_KEYS, State),
    ok = rabbit_core_metrics:connection_created(
           proplists:get_value(pid, Infos),
           Infos),
    ok = rabbit_event:notify(connection_created, Infos),
    ok = maybe_emit_stats(State),
    ok = rabbit_amqp1_0:register_connection(self()),
    Caps = [%% https://docs.oasis-open.org/amqp/linkpair/v1.0/cs01/linkpair-v1.0-cs01.html#_Toc51331306
            <<"LINK_PAIR_V1_0">>,
            %% https://docs.oasis-open.org/amqp/anonterm/v1.0/cs01/anonterm-v1.0-cs01.html#doc-anonymous-relay
            <<"ANONYMOUS-RELAY">>],
    Open = #'v1_0.open'{
              channel_max = {ushort, EffectiveChannelMax},
              max_frame_size = {uint, IncomingMaxFrameSize},
              %% "the value in idle-time-out SHOULD be half the peer's actual timeout threshold" [2.4.5]
              idle_time_out = {uint, ReceiveTimeoutMillis div 2},
              container_id = {utf8, rabbit_nodes:cluster_name()},
              offered_capabilities = rabbit_amqp_util:capabilities(Caps),
              properties = server_properties()},
    ok = send_on_channel0(State, Open, amqp10_framing),
    State;
handle_connection_frame(#'v1_0.close'{}, State0) ->
    State = State0#v1{connection_state = closing},
    close(undefined, State).

start_writer(#v1{helper_sup = SupPid,
                 websocket = WebSocket,
                 sock = Sock} = State) ->
    Socket = case WebSocket of
                 true -> websocket;
                 false -> Sock
             end,
    ChildSpec = #{id => writer,
                  start => {rabbit_amqp_writer, start_link, [Socket, self()]},
                  restart => transient,
                  significant => true,
                  shutdown => ?WORKER_WAIT,
                  type => worker
                 },
    {ok, Pid} = supervisor:start_child(SupPid, ChildSpec),
    State#v1{writer = Pid}.

handle_session_frame(Channel, Body, #v1{tracked_channels = Channels} = State) ->
    case Channels of
        #{Channel := SessionPid} ->
            rabbit_amqp_session:process_frame(SessionPid, Body),
            State;
        _ ->
            case ?IS_RUNNING(State) of
                true ->
                    case Body of
                        #'v1_0.begin'{} ->
                            send_to_new_session(Channel, Body, State);
                        _ ->
                            State
                    end;
                false ->
                    throw({channel_frame_while_connection_not_running,
                           Channel,
                           State#v1.connection_state,
                           Body})
            end
    end.

handle_sasl_frame(#'v1_0.sasl_init'{mechanism = {symbol, Mechanism},
                                    initial_response = Response,
                                    hostname = _},
                  State0 = #v1{connection_state = waiting_sasl_init,
                               connection = Connection,
                               sock = Sock}) ->
    ResponseBin = case Response of
                      undefined -> <<>>;
                      {binary, Bin} -> Bin
                  end,
    AuthMechanism = auth_mechanism_to_module(Mechanism, Sock),
    AuthState = AuthMechanism:init(Sock),
    State = State0#v1{
              connection = Connection#v1_connection{
                             auth_mechanism = {Mechanism, AuthMechanism},
                             auth_state = AuthState},
              connection_state = securing},
    auth_phase(ResponseBin, State);
handle_sasl_frame(#'v1_0.sasl_response'{response = {binary, Response}},
                  State = #v1{connection_state = securing}) ->
    auth_phase(Response, State);
handle_sasl_frame(Performative, State) ->
    throw({unexpected_1_0_sasl_frame, Performative, State}).

-spec handle_input(binary(), state()) -> state().
handle_input(Handshake = <<"AMQP",0,1,0,0>>,
             #v1{callback = handshake,
                 connection_state = waiting_amqp0100,
                 connection = #v1_connection{user = #user{}},
                 helper_sup = HelperSup
                } = State0) ->
    %% At this point, client already got successfully authenticated by SASL.
    send(State0, Handshake),
    ChildSpec = #{id => session_sup,
                  start => {rabbit_amqp_session_sup, start_link, [self()]},
                  restart => transient,
                  significant => true,
                  shutdown => infinity,
                  type => supervisor},
    {ok, SessionSupPid} = supervisor:start_child(HelperSup, ChildSpec),
    State = State0#v1{
              session_sup = SessionSupPid,
              %% "After establishing or accepting a TCP connection and sending
              %% the protocol header, each peer MUST send an open frame before
              %% sending any other frames." [2.4.1]
              connection_state = waiting_open},
    switch_callback(State, {frame_header, amqp}, 8);
handle_input(Header = <<Size:32, DOff:8, Type:8, Channel:16>>,
             State0 = #v1{callback = {frame_header, Mode}})
  when DOff >= 2 ->
    case {Mode, Type} of
        {amqp, 0} -> ok;
        {sasl, 1} -> ok;
        _ -> throw({bad_1_0_header_type, Header, Mode})
    end,
    MaxFrameSize = State0#v1.connection#v1_connection.incoming_max_frame_size,
    State = if Size =:= 8 ->
                   %% heartbeat
                   State0;
               Size > MaxFrameSize ->
                   Err = error_frame(
                           ?V_1_0_CONNECTION_ERROR_FRAMING_ERROR,
                           "frame size (~b bytes) > maximum frame size (~b bytes)",
                           [Size, MaxFrameSize]),
                   handle_exception(State0, Channel, Err);
               true ->
                   switch_callback(State0, {frame_body, Mode, DOff, Channel}, Size - 8)
            end,
    ensure_stats_timer(State);
handle_input(Malformed, #v1{callback = {frame_header, _Mode}}) ->
    throw({bad_1_0_header, Malformed});
handle_input(FrameBin, State0 = #v1{callback = {frame_body, Mode, DOff, Channel}}) ->
    %% Figure 2.16
    %% DOff = 4-byte words minus 8 bytes we've already read
    ExtendedHeaderSize = (DOff * 32 - 64),
    <<_IgnoreExtendedHeader:ExtendedHeaderSize, FrameBody/binary>> = FrameBin,
    State = switch_callback(State0, {frame_header, Mode}, 8),
    handle_frame(Mode, Channel, FrameBody, State);
handle_input(Data, #v1{callback = Callback}) ->
    throw({bad_input, Callback, Data}).

-spec init(tuple()) -> no_return().
init(PackedState) ->
    {parent, Parent} = erlang:process_info(self(), parent),
    ok = rabbit_connection_sup:remove_connection_helper_sup(Parent, helper_sup_amqp_091),
    State0 = unpack_from_0_9_1(PackedState, Parent),
    State = advertise_sasl_mechanism(State0),
    %% By invoking recvloop here we become 1.0.
    recvloop(sys:debug_options([]), State).

-spec advertise_sasl_mechanism(state()) -> state().
advertise_sasl_mechanism(State0 = #v1{connection_state = received_amqp3100,
                                      sock = Sock}) ->
    send(State0, <<"AMQP",3,1,0,0>>),
    Ms0 = [{symbol, atom_to_binary(M)} || M <- auth_mechanisms(Sock)],
    Ms1 = {array, symbol, Ms0},
    Ms = #'v1_0.sasl_mechanisms'{sasl_server_mechanisms = Ms1},
    ok = send_on_channel0(State0, Ms, rabbit_amqp_sasl),
    State = State0#v1{connection_state = waiting_sasl_init},
    switch_callback(State, {frame_header, sasl}, 8).

send_on_channel0(State, Performative, Framing) ->
    Data = rabbit_amqp_writer:assemble_frame(0, Performative, Framing),
    send(State, Data).

send(#v1{websocket = true}, Data) ->
    self() ! {send_ws, self(), Data},
    ok;
send(#v1{websocket = false,
         sock = Sock}, Data) ->
    rabbit_misc:throw_on_error(
      inet_error,
      fun() -> rabbit_net:send(Sock, Data) end).

heartbeat_send_fun(Reader, #v1{websocket = true}) ->
    fun() ->
            Frame = amqp10_binary_generator:build_heartbeat_frame(),
            Reader ! {send_ws, self(), Frame},
            ok
    end;
heartbeat_send_fun(_, #v1{websocket = false,
                          sock = Sock}) ->
    fun() ->
            Frame = amqp10_binary_generator:build_heartbeat_frame(),
            catch rabbit_net:send(Sock, Frame)
    end.

auth_mechanism_to_module(TypeBin, Sock) ->
    case rabbit_registry:binary_to_type(TypeBin) of
        {error, not_found} ->
            protocol_error(?V_1_0_AMQP_ERROR_NOT_FOUND,
                           "unknown authentication mechanism '~ts'", [TypeBin]);
        T ->
            case {lists:member(T, auth_mechanisms(Sock)),
                  rabbit_registry:lookup_module(auth_mechanism, T)} of
                {true, {ok, Module}} ->
                    Module;
                _ ->
                    protocol_error(?V_1_0_AMQP_ERROR_NOT_FOUND,
                                   "invalid authentication mechanism '~ts'", [T])
            end
    end.

%% Returns mechanisms ordered in decreasing level of preference (as configured).
auth_mechanisms(Sock) ->
    {ok, ConfiguredMechs} = application:get_env(rabbit, auth_mechanisms),
    RegisteredMechs = rabbit_registry:lookup_all(auth_mechanism),
    lists:filter(
      fun(Mech) ->
              case proplists:lookup(Mech, RegisteredMechs) of
                  {Mech, Mod} ->
                      Mod:should_offer(Sock);
                  none ->
                      false
              end
      end, ConfiguredMechs).

auth_phase(
  Response,
  State = #v1{connection = Conn = #v1_connection{auth_mechanism = {Name, AuthMechanism},
                                                 auth_state = AuthState}}) ->
    case AuthMechanism:handle_response(Response, AuthState) of
        {refused, Username, Msg, Args} ->
            %% We don't trust the client at this point - force them to wait
            %% for a bit before sending the sasl outcome frame
            %% so they can't DOS us with repeated failed logins etc.
            auth_fail(Username, State),
            silent_close_delay(),
            Outcome = #'v1_0.sasl_outcome'{code = ?V_1_0_SASL_CODE_AUTH},
            ok = send_on_channel0(State, Outcome, rabbit_amqp_sasl),
            protocol_error(
              ?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS, "~ts login refused: ~ts",
              [Name, io_lib:format(Msg, Args)]);
        {protocol_error, Msg, Args} ->
            auth_fail(none, State),
            protocol_error(?V_1_0_AMQP_ERROR_DECODE_ERROR, Msg, Args);
        {challenge, Challenge, AuthState1} ->
            Challenge = #'v1_0.sasl_challenge'{challenge = {binary, Challenge}},
            ok = send_on_channel0(State, Challenge, rabbit_amqp_sasl),
            State1 = State#v1{connection = Conn#v1_connection{auth_state = AuthState1}},
            switch_callback(State1, {frame_header, sasl}, 8);
        {ok, User} ->
            Outcome = #'v1_0.sasl_outcome'{code = ?V_1_0_SASL_CODE_OK},
            ok = send_on_channel0(State, Outcome, rabbit_amqp_sasl),
            State1 = State#v1{connection_state = waiting_amqp0100,
                              connection = Conn#v1_connection{user = User,
                                                              auth_state = authenticated}},
            switch_callback(State1, handshake, 8)
    end.

auth_fail(Username, State) ->
    rabbit_core_metrics:auth_attempt_failed(<<>>, Username, amqp10),
    notify_auth(user_authentication_failure, Username, State).

notify_auth(EventType, Username, State) ->
    Name = case Username of
               none -> [];
               _ -> [{name, Username}]
           end,
    AuthEventItems = lists:filtermap(
                       fun(Item = name) ->
                               {true, {connection_name, i(Item, State)}};
                          (Item) ->
                               case i(Item, State) of
                                   '' -> false;
                                   Val -> {true, {Item, Val}}
                               end
                       end, ?AUTH_EVENT_KEYS),
    EventProps = Name ++ AuthEventItems,
    rabbit_event:notify(EventType, EventProps).

track_channel(ChannelNum, SessionPid, #v1{tracked_channels = Channels} = State) ->
    ?LOG_DEBUG("AMQP 1.0 created session process ~p for channel number ~b",
               [SessionPid, ChannelNum]),
    _Ref = erlang:monitor(process, SessionPid, [{tag, {'DOWN', ChannelNum}}]),
    State#v1{tracked_channels = maps:put(ChannelNum, SessionPid, Channels)}.

untrack_channel(ChannelNum, SessionPid, #v1{tracked_channels = Channels0} = State) ->
    case maps:take(ChannelNum, Channels0) of
        {SessionPid, Channels} ->
            ?LOG_DEBUG("AMQP 1.0 closed session process ~p with channel number ~b",
                       [SessionPid, ChannelNum]),
            State#v1{tracked_channels = Channels};
        _ ->
            State
    end.

send_to_new_session(
  ChannelNum, BeginFrame,
  #v1{session_sup = SessionSup,
      connection = #v1_connection{outgoing_max_frame_size = MaxFrame,
                                  vhost = Vhost,
                                  user = User,
                                  container_id = ContainerId,
                                  name = ConnName},
      writer = WriterPid} = State) ->
    %% Subtract fixed frame header size.
    OutgoingMaxFrameSize = case MaxFrame of
                               unlimited -> unlimited;
                               _ -> MaxFrame - 8
                           end,
    ChildArgs = [WriterPid,
                 ChannelNum,
                 OutgoingMaxFrameSize,
                 User,
                 Vhost,
                 ContainerId,
                 ConnName,
                 BeginFrame],
    case rabbit_amqp_session_sup:start_session(SessionSup, ChildArgs) of
        {ok, SessionPid} ->
            track_channel(ChannelNum, SessionPid, State);
        {error, _} = E ->
            throw(E)
    end.

vhost({utf8, <<"vhost:", VHost/binary>>}) ->
    VHost;
vhost(_) ->
    application:get_env(rabbit, default_vhost, <<"/">>).

check_user_loopback(#v1{connection = #v1_connection{user = #user{username = Username}},
                        sock = Socket} = State) ->
    case rabbit_access_control:check_user_loopback(Username, Socket) of
        ok ->
            ok;
        not_allowed ->
            auth_fail(Username, State),
            protocol_error(?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS,
                           "user '~ts' can only connect via localhost",
                           [Username])
    end.

check_vhost_exists(Vhost, State) ->
    case rabbit_vhost:exists(Vhost) of
        true ->
            ok;
        false ->
            auth_fail(State#v1.connection#v1_connection.user#user.username, State),
            protocol_error(?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS,
                           "AMQP 1.0 connection failed: virtual host '~s' does not exist",
                           [Vhost])
    end.

check_vhost_alive(Vhost) ->
    case rabbit_vhost_sup_sup:is_vhost_alive(Vhost) of
        true ->
            ok;
        false ->
            protocol_error(?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                           "AMQP 1.0 connection failed: virtual host '~s' is down",
                           [Vhost])
    end.

check_vhost_connection_limit(Vhost, Username) ->
    case rabbit_vhost_limit:is_over_connection_limit(Vhost) of
        false ->
            ok;
        {true, Limit} ->
            protocol_error(
              ?V_1_0_AMQP_ERROR_RESOURCE_LIMIT_EXCEEDED,
              "access to vhost '~ts' refused for user '~ts': vhost connection limit (~p) is reached",
              [Vhost, Username, Limit])
    end.

check_user_connection_limit(Username) ->
    case rabbit_auth_backend_internal:is_over_connection_limit(Username) of
        false ->
            ok;
        {true, Limit} ->
            protocol_error(
              ?V_1_0_AMQP_ERROR_RESOURCE_LIMIT_EXCEEDED,
              "connection refused for user '~ts': user connection limit (~p) is reached",
              [Username, Limit])
    end.


set_credential0(Cred,
                State = #v1{connection = #v1_connection{
                                            user = User0,
                                            vhost = Vhost,
                                            credential_timer = OldTimer} = Conn,
                            tracked_channels = Chans,
                            sock = Sock}) ->
    ?LOG_INFO("updating credential", []),
    case rabbit_access_control:update_state(User0, Cred) of
        {ok, User} ->
            try rabbit_access_control:check_vhost_access(User, Vhost, {socket, Sock}, #{}) of
                ok ->
                    maps:foreach(fun(_ChanNum, Pid) ->
                                         rabbit_amqp_session:reset_authz(Pid, User)
                                 end, Chans),
                    case OldTimer of
                        undefined -> ok;
                        Ref -> ok = erlang:cancel_timer(Ref, [{info, false}])
                    end,
                    NewTimer = maybe_start_credential_expiry_timer(User),
                    State#v1{connection = Conn#v1_connection{
                                            user = User,
                                            credential_timer = NewTimer}}
            catch _:Reason ->
                      Error = error_frame(?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS,
                                          "access to vhost ~s failed for new credential: ~p",
                                          [Vhost, Reason]),
                      handle_exception(State, 0, Error)
            end;
        Err ->
            Error = error_frame(?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS,
                                "credential update failed: ~p",
                                [Err]),
            handle_exception(State, 0, Error)
    end.

maybe_start_credential_expiry_timer(User) ->
    case rabbit_access_control:expiry_timestamp(User) of
        never ->
            undefined;
        Ts when is_integer(Ts) ->
            Time = (Ts - os:system_time(second)) * 1000,
            ?LOG_DEBUG(
               "credential expires in ~b ms frow now (absolute timestamp = ~b seconds since epoch)",
               [Time, Ts]),
            case Time > 0 of
                true ->
                    erlang:send_after(Time, self(), credential_expired);
                false ->
                    protocol_error(?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS,
                                   "credential expired ~b ms ago", [abs(Time)])
            end
    end.

%% We don't trust the client at this point - force them to wait
%% for a bit so they can't DOS us with repeated failed logins etc.
silent_close_delay() ->
    timer:sleep(?SILENT_CLOSE_DELAY).

%% This function is deprecated.
%% It could be called in 3.13 / 4.0 mixed version clusters by the old 3.13 CLI command
%% rabbitmqctl list_amqp10_connections
%%
%% rabbitmqctl list_connections
%% listing AMQP 1.0 connections in 4.0 uses rabbit_reader:info/2 instead.
-spec info(rabbit_types:connection(), rabbit_types:info_keys()) ->
    rabbit_types:infos().
info(Pid, InfoItems) ->
    case gen_server:call(Pid, {info, InfoItems}, infinity) of
        {ok, InfoList} ->
            InfoList;
        {error, Reason} ->
            throw(Reason)
    end.

infos(Items, State) ->
    [{Item, i(Item, State)} || Item <- Items].

i(pid, #v1{}) ->
    self();
i(type, #v1{}) ->
    network;
i(protocol, #v1{websocket = WebSocket}) ->
    Vsn = {1, 0},
    case WebSocket of
        true -> {'Web AMQP', Vsn};
        false -> Vsn
    end;
i(connection, #v1{connection = Val}) ->
    Val;
i(node, #v1{}) ->
    node();
i(auth_mechanism, #v1{connection = #v1_connection{auth_mechanism = Val}}) ->
    case Val of
        {Name, _Mod} -> Name;
        _ -> Val
    end;
i(frame_max, #v1{connection = #v1_connection{outgoing_max_frame_size = Val}}) ->
    %% Some HTTP API clients expect an integer to be reported.
    %% https://github.com/rabbitmq/rabbitmq-server/issues/11838
    if Val =:= unlimited -> ?UINT_MAX;
       is_integer(Val) -> Val
    end;
i(timeout, #v1{connection = #v1_connection{timeout = Millis}}) ->
    Millis div 1000;
i(user, #v1{connection = #v1_connection{user = User}}) ->
    case User of
        #user{username = Val} -> Val;
        unauthenticated -> ''
    end;
i(state, S) ->
    i(connection_state, S);
i(connection_state, #v1{connection_state = Val}) ->
    Val;
i(connected_at, #v1{connection = #v1_connection{connected_at = Val}}) ->
    Val;
i(name, #v1{connection = #v1_connection{name = Val}}) ->
    Val;
i(container_id, #v1{connection = #v1_connection{container_id = Val}}) ->
    Val;
i(vhost, #v1{connection = #v1_connection{vhost = Val}}) ->
    Val;
i(host, #v1{connection = #v1_connection{host = Val}}) ->
    Val;
i(port, #v1{connection = #v1_connection{port = Val}}) ->
    Val;
i(peer_host, #v1{connection = #v1_connection{peer_host = Val}}) ->
    Val;
i(peer_port, #v1{connection = #v1_connection{peer_port = Val}}) ->
    Val;
i(SockStat, #v1{sock = Sock})
  when SockStat =:= recv_oct;
       SockStat =:= recv_cnt;
       SockStat =:= send_oct;
       SockStat =:= send_cnt;
       SockStat =:= send_pend ->
    case rabbit_net:getstat(Sock, [SockStat]) of
        {ok, [{SockStat, Val}]} ->
            Val;
        {error, _} ->
            ''
    end;
i(ssl, #v1{sock = Sock}) -> rabbit_net:is_ssl(Sock);
i(SSL, #v1{sock = Sock, proxy_socket = ProxySock})
  when SSL =:= ssl_protocol;
       SSL =:= ssl_key_exchange;
       SSL =:= ssl_cipher;
       SSL =:= ssl_hash ->
    rabbit_ssl:info(SSL, {Sock, ProxySock});
i(Cert, #v1{sock = Sock})
  when Cert =:= peer_cert_issuer;
       Cert =:= peer_cert_subject;
       Cert =:= peer_cert_validity ->
    rabbit_ssl:cert_info(Cert, Sock);
i(client_properties, #v1{connection = #v1_connection{properties = Props}}) ->
    %% Connection properties sent by the client.
    %% Displayed in rabbitmq_management/priv/www/js/tmpl/connection.ejs
    case Props of
        undefined ->
            [];
        {map, Fields} ->
            [mc_amqpl:to_091(Key, TypeVal) || {{symbol, Key}, TypeVal} <- Fields]
    end;
i(channels, #v1{tracked_channels = Channels}) ->
    maps:size(Channels);
i(session_pids, #v1{tracked_channels = Map}) ->
    maps:values(Map);
i(channel_max, #v1{connection = #v1_connection{channel_max = Max}}) ->
    Max;
i(reductions = Item, _State) ->
    {Item, Reductions} = erlang:process_info(self(), Item),
    Reductions;
i(garbage_collection, _State) ->
    rabbit_misc:get_gc_info(self());
i(Item, #v1{}) ->
    throw({bad_argument, Item}).

maybe_emit_stats(State) ->
    ok = rabbit_event:if_enabled(
           State,
           #v1.stats_timer,
           fun() -> emit_stats(State) end).

emit_stats(State) ->
    [{_, Pid},
     {_, RecvOct},
     {_, SendOct},
     {_, Reductions}] = infos(?SIMPLE_METRICS, State),
    Infos = infos(?OTHER_METRICS, State),
    rabbit_core_metrics:connection_stats(Pid, Infos),
    rabbit_core_metrics:connection_stats(Pid, RecvOct, SendOct, Reductions),
    %% NB: Don't call ensure_stats_timer because it becomes expensive
    %% if all idle non-hibernating connections emit stats.
    rabbit_event:reset_stats_timer(State, #v1.stats_timer).

ensure_stats_timer(State)
  when ?IS_RUNNING(State) ->
    rabbit_event:ensure_stats_timer(State, #v1.stats_timer, emit_stats);
ensure_stats_timer(State) ->
    State.

ignore_maintenance({map, Properties}) ->
    lists:member(
      {{symbol, <<"ignore-maintenance">>}, true},
      Properties);
ignore_maintenance(_) ->
    false.
