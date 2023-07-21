%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.
%%

-module(rabbit_amqp1_0_reader).

-include_lib("rabbit_common/include/rabbit.hrl").
-include("rabbit_amqp1_0.hrl").

-export([init/2, mainloop/2]).
-export([info/2]).

%% TODO which of these are needed?
-export([shutdown/2]).
-export([system_continue/3, system_terminate/4, system_code_change/4]).

-import(rabbit_amqp1_0_util, [protocol_error/3]).

-define(HANDSHAKE_TIMEOUT, 10).
-define(NORMAL_TIMEOUT, 3).
-define(CLOSING_TIMEOUT, 30).
-define(SILENT_CLOSE_DELAY, 3).

%%--------------------------------------------------------------------------

-record(v1_connection,
        {name :: binary(),
         vhost :: none | rabbit_types:vhost(),
         host,
         peer_host,
         port :: inet:port_number(),
         peer_port :: inet:port_number(),
         connected_at :: integer(),
         user :: rabbit_types:option(rabbit_types:user()),
         timeout_sec :: non_neg_integer(),
         incoming_max_frame_size :: pos_integer(),
         outgoing_max_frame_size :: unlimited | pos_integer(),
         auth_mechanism,
         auth_state :: term(),
         hostname,
         properties :: undefined | {map, list(tuple())}
        }).

-record(v1,
        {
         parent :: pid(),
         helper_sup :: pid(),
         writer :: none | pid(),
         heartbeater :: none | rabbit_heartbeat:heartbeaters(),
         session_sup :: rabbit_types:option(pid()),
         sock :: rabbit_net:socket(),
         proxy_socket :: undefined | {rabbit_proxy_socket, any(), any()},
         connection :: #v1_connection{},
         connection_state :: pre_init | starting | waiting_amqp0100 | securing | running | closing | closed,
         callback,
         recv_len :: non_neg_integer(),
         pending_recv :: boolean(),
         buf :: list(),
         buf_len :: non_neg_integer(),
         tracked_channels :: #{Channel :: non_neg_integer() => Session :: pid()}
        }).

-define(IS_RUNNING(State), State#v1.connection_state =:= running).

%%--------------------------------------------------------------------------

unpack_from_0_9_1(
  {Parent, Sock,RecvLen, PendingRecv, HelperSupPid, Buf, BufLen, ProxySocket,
   ConnectionName, Host, PeerHost, Port, PeerPort, ConnectedAt}) ->
    #v1{parent              = Parent,
        sock                = Sock,
        callback            = handshake,
        recv_len            = RecvLen,
        pending_recv        = PendingRecv,
        connection_state    = pre_init,
        heartbeater         = none,
        helper_sup          = HelperSupPid,
        buf                 = Buf,
        buf_len             = BufLen,
        connection = #v1_connection{
                        name = ConnectionName,
                        vhost = none,
                        host = Host,
                        peer_host = PeerHost,
                        port = Port,
                        peer_port = PeerPort,
                        connected_at = ConnectedAt,
                        user = none,
                        timeout_sec = ?HANDSHAKE_TIMEOUT,
                        %% "Prior to any explicit negotiation, the maximum
                        %% frame size is 512 (MIN-MAX-FRAME-SIZE)" [2.4.1]
                        incoming_max_frame_size = ?MIN_MAX_FRAME_1_0_SIZE,
                        outgoing_max_frame_size = ?MIN_MAX_FRAME_1_0_SIZE,
                        auth_mechanism = none,
                        auth_state = none},
        proxy_socket        = ProxySocket,
        tracked_channels    = maps:new(),
        writer = none}.

shutdown(Pid, Explanation) ->
    gen_server:call(Pid, {shutdown, Explanation}, infinity).

system_continue(Parent, Deb, State) ->
    ?MODULE:mainloop(Deb, State#v1{parent = Parent}).

-spec system_terminate(term(), term(), term(), term()) -> no_return().
system_terminate(Reason, _Parent, _Deb, _State) ->
    exit(Reason).

system_code_change(Misc, _Module, _OldVsn, _Extra) ->
    {ok, Misc}.

server_properties() ->
    %% The atom doesn't match anything, it's just "not 0-9-1".
    Raw = lists:keydelete(
          <<"capabilities">>, 1, rabbit_reader:server_properties(amqp_1_0)),
    {map, [{{symbol, K}, {utf8, V}} || {K, longstr, V}  <- Raw]}.

%%--------------------------------------------------------------------------

inet_op(F) -> rabbit_misc:throw_on_error(inet_error, F).

recvloop(Deb, State = #v1{pending_recv = true}) ->
    mainloop(Deb, State);
recvloop(Deb, State = #v1{sock = Sock, recv_len = RecvLen, buf_len = BufLen})
  when BufLen < RecvLen ->
    case rabbit_net:setopts(Sock, [{active, once}]) of
        ok ->
            mainloop(Deb, State#v1{pending_recv = true});
        {error, Reason} ->
            throw({inet_error, Reason})
    end;
recvloop(Deb, State = #v1{recv_len = RecvLen, buf = Buf, buf_len = BufLen}) ->
    {Data, Rest} = split_binary(case Buf of
                                    [B] -> B;
                                    _   -> list_to_binary(lists:reverse(Buf))
                                end, RecvLen),
    recvloop(Deb, handle_input(State#v1.callback, Data,
                               State#v1{buf = [Rest],
                                        buf_len = BufLen - RecvLen})).

mainloop(Deb, State = #v1{sock = Sock, buf = Buf, buf_len = BufLen}) ->
    case rabbit_net:recv(Sock) of
        {data, Data} ->
            recvloop(Deb, State#v1{buf = [Data | Buf],
                                   buf_len = BufLen + size(Data),
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

handle_other({'EXIT', Parent, Reason}, State = #v1{parent = Parent}) ->
    _ = terminate(io_lib:format("broker forced connection closure "
                                "with reason '~w'", [Reason]), State),
    %% this is what we are expected to do according to
    %% http://www.erlang.org/doc/man/sys.html
    %%
    %% If we wanted to be *really* nice we should wait for a while for
    %% clients to close the socket at their end, just as we do in the
    %% ordinary error case. However, since this termination is
    %% initiated by our parent it is probably more important to exit
    %% quickly.
    exit(Reason);
handle_other({'DOWN', _MRef, process, ChPid, Reason}, State) ->
    handle_dependent_exit(ChPid, Reason, State);
handle_other(handshake_timeout, State)
  when ?IS_RUNNING(State) orelse
       State#v1.connection_state =:= closing orelse
       State#v1.connection_state =:= closed ->
    State;
handle_other(handshake_timeout, State) ->
    throw({handshake_timeout, State#v1.callback});
handle_other(heartbeat_timeout, State = #v1{connection_state = closed}) ->
    State;
handle_other(heartbeat_timeout, #v1{connection_state = S}) ->
    throw({heartbeat_timeout, S});
handle_other({'$gen_call', From, {shutdown, Explanation}}, State) ->
    {ForceTermination, NewState} = terminate(Explanation, State),
    gen_server:reply(From, ok),
    case ForceTermination of
        force  -> stop;
        normal -> NewState
    end;
handle_other({'$gen_cast', force_event_refresh}, State) ->
    %% Ignore, the broker sent us this as it thinks we are a 0-9-1 connection
    State;
handle_other(terminate_connection, State) ->
    %%TODO terminate?
    State;
handle_other({info, InfoItems, From}, State) ->
    Infos = lists:map(
              fun(InfoItem) ->
                      {InfoItem, i(InfoItem, State)}
              end,
              InfoItems),
    From ! {info_reply, Infos},
    State;
handle_other(Other, _State) ->
    %% internal error -> something worth dying for
    exit({unexpected_message, Other}).

switch_callback(State, Callback, Length) ->
    State#v1{callback = Callback, recv_len = Length}.

terminate(Reason, State) when ?IS_RUNNING(State) ->
    {normal, handle_exception(State, 0,
                              error_frame(?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                                          "Connection forced: ~tp", [Reason]))};
terminate(_Reason, State) ->
    {force, State}.

%%--------------------------------------------------------------------------
%% error handling / termination

close(Error, State = #v1{sock = Sock,
                         connection = #v1_connection{timeout_sec = TimeoutSec}}) ->
    Self = self(),

    %% Client properties will be emitted in the connection_closed event by rabbit_reader.
    ClientProperties = i(client_properties, State),
    put(client_properties, ClientProperties),

    Seconds = if TimeoutSec > 0 andalso
                 TimeoutSec < ?CLOSING_TIMEOUT ->
                     TimeoutSec;
                 true ->
                     ?CLOSING_TIMEOUT
              end,
    Millis = Seconds * 1000,
    _TRef = erlang:send_after(Millis, Self, terminate_connection),

    ok = send_on_channel0(Sock, #'v1_0.close'{error = Error}),
    State#v1{connection_state = closed}.

handle_dependent_exit(ChPid, Reason, State) ->
    case {ChPid, termination_kind(Reason)} of
        {_Channel, controlled} ->
            maybe_close(State);
        {Channel, uncontrolled} ->
            {RealReason, Trace} = Reason,
            R = error_frame(?V_1_0_AMQP_ERROR_INTERNAL_ERROR, "Session error: ~tp~n~tp", [RealReason, Trace]),
            maybe_close(handle_exception(State, Channel, R))
    end.

termination_kind(normal) ->
    controlled;
termination_kind(shutdown) ->
    controlled;
termination_kind({shutdown, _}) ->
    controlled;
termination_kind(_) ->
    uncontrolled.

maybe_close(State = #v1{connection_state = closing}) ->
    close(undefined, State);
maybe_close(State) ->
    State.

error_frame(Condition, Fmt, Args) ->
    #'v1_0.error'{condition = Condition,
                  description = {utf8, list_to_binary(
                                         rabbit_misc:format(Fmt, Args))}}.

handle_exception(State = #v1{connection_state = closed}, Channel,
                 #'v1_0.error'{description = {utf8, Desc}}) ->
    rabbit_log_connection:error(
      "Error on AMQP 1.0 connection ~tp (~tp), channel ~tp:~n~tp",
      [self(), closed, Channel, Desc]),
    State;
handle_exception(State = #v1{connection_state = CS}, Channel,
                 Error = #'v1_0.error'{description = {utf8, Desc}})
  when ?IS_RUNNING(State) orelse CS =:= closing ->
    rabbit_log_connection:error(
      "Error on AMQP 1.0 connection ~tp (~tp), channel ~tp:~n~tp",
      [self(), CS, Channel, Desc]),
    %% TODO: session errors shouldn't force the connection to close
    close(Error, State);
handle_exception(State, Channel, Error) ->
    %% We don't trust the client at this point - force them to wait
    %% for a bit so they can't DOS us with repeated failed logins etc.
    timer:sleep(?SILENT_CLOSE_DELAY * 1000),
    throw({handshake_error, State#v1.connection_state, Channel, Error}).

%%--------------------------------------------------------------------------

%% Begin 1-0

%% ----------------------------------------
%% AMQP 1.0 frame handlers

is_connection_frame(#'v1_0.open'{})  -> true;
is_connection_frame(#'v1_0.close'{}) -> true;
is_connection_frame(_)               -> false.

%% TODO Handle depending on connection state
%% TODO It'd be nice to only decode up to the descriptor

handle_1_0_frame(Mode, Channel, Payload, State) ->
    try
        handle_1_0_frame0(Mode, Channel, Payload, State)
    catch
        _:#'v1_0.error'{} = Reason ->
            handle_exception(State, 0, Reason);
        _:{error, {not_allowed, Username}} ->
            %% section 2.8.15 in http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-complete-v1.0-os.pdf
            handle_exception(State, 0, error_frame(
                                         ?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS,
                                         "Access for user '~ts' was refused: insufficient permissions",
                                         [Username]));
        _:Reason:Trace ->
            handle_exception(State, 0, error_frame(
                                         ?V_1_0_AMQP_ERROR_INTERNAL_ERROR,
                                         "Reader error: ~tp~n~tp",
                                         [Reason, Trace]))
    end.

%% Nothing specifies that connection methods have to be on a
%% particular channel.
handle_1_0_frame0(_Mode, Channel, Payload,
                 State = #v1{ connection_state = CS}) when
      CS =:= closing; CS =:= closed ->
    Sections = parse_1_0_frame(Payload, Channel),
    case is_connection_frame(Sections) of
        true  -> handle_1_0_connection_frame(Sections, State);
        false -> State
    end;
handle_1_0_frame0(Mode, Channel, Payload, State) ->
    Sections = parse_1_0_frame(Payload, Channel),
    case {Mode, is_connection_frame(Sections)} of
        {amqp, true}  -> handle_1_0_connection_frame(Sections, State);
        {amqp, false} -> handle_1_0_session_frame(Channel, Sections, State);
        {sasl, false} -> handle_1_0_sasl_frame(Sections, State)
    end.

parse_1_0_frame(Payload, _Channel) ->
    {PerfDesc, Rest} = amqp10_binary_parser:parse(Payload),
    Perf = amqp10_framing:decode(PerfDesc),
    ?DEBUG("Channel ~tp ->~n~tp~n~ts~n",
           [_Channel, amqp10_framing:pprint(Perf),
            case Rest of
                <<>> -> <<>>;
                _    -> rabbit_misc:format(
                          " followed by ~tp bytes of content", [size(Rest)])
            end]),
    case Rest of
        <<>> -> Perf;
        _    -> {Perf, Rest}
    end.

handle_1_0_connection_frame(
  #'v1_0.open'{max_frame_size = ClientMaxFrame,
               channel_max = ClientChannelMax,
               idle_time_out = IdleTimeout,
               hostname = Hostname,
               properties = Properties},
  #v1{connection_state = starting,
      connection = Connection,
      helper_sup = HelperSupPid,
      sock = Sock} = State0) ->
    ClientHeartbeatSec = case IdleTimeout of
                             undefined -> 0;
                             {uint, Interval} -> Interval div 1000
                         end,
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
    {ok, HeartbeatSec} = application:get_env(rabbit, heartbeat),
    SendFun = fun() ->
                      Frame = amqp10_binary_generator:build_heartbeat_frame(),
                      catch rabbit_net:send(Sock, Frame)
              end,
    Self = self(),
    Parent = Self,
    ReceiveFun = fun() ->
                         Parent ! heartbeat_timeout
                 end,
    %% [2.4.5] the value in idle-time-out SHOULD be half the peer's
    %%         actual timeout threshold
    ReceiverHeartbeatSec = lists:min([HeartbeatSec * 2, 4294967]),
    %% TODO: only start heartbeat receive timer at next next frame
    Heartbeater = rabbit_heartbeat:start(
                    HelperSupPid, Sock,
                    ClientHeartbeatSec, SendFun,
                    ReceiverHeartbeatSec, ReceiveFun),
    Vhost = vhost(Hostname),
    {ok, IncomingMaxFrameSize} = application:get_env(rabbit, frame_max),
    State1 = State0#v1{connection_state = running,
                       connection = Connection#v1_connection{
                                      vhost = Vhost,
                                      incoming_max_frame_size = IncomingMaxFrameSize,
                                      outgoing_max_frame_size = OutgoingMaxFrameSize,
                                      hostname = Hostname,
                                      properties = Properties},
                       heartbeater = Heartbeater},
    State = start_writer(State1),
    HostnameVal = case Hostname of
                      undefined -> undefined;
                      null -> undefined;
                      {utf8, Val} -> Val
                  end,
    rabbit_log:debug("AMQP 1.0 connection.open frame: hostname = ~ts, "
                     "extracted vhost = ~ts, idle_timeout = ~tp" ,
                     [HostnameVal, Vhost, HeartbeatSec * 1000]),
    %% TODO enforce channel_max
    ok = send_on_channel0(
           Sock,
           #'v1_0.open'{channel_max    = ClientChannelMax,
                        max_frame_size = {uint, IncomingMaxFrameSize},
                        idle_time_out  = {uint, HeartbeatSec * 1000},
                        container_id   = {utf8, rabbit_nodes:cluster_name()},
                        properties     = server_properties()}),

    Infos = infos(?CONNECTION_EVENT_KEYS, State),
    ok = rabbit_core_metrics:connection_created(
           proplists:get_value(pid, Infos),
           Infos),
    ok = rabbit_event:notify(connection_created, Infos),
    ok = rabbit_amqp1_0:register_connection(Self),
    State;
handle_1_0_connection_frame(#'v1_0.close'{}, State0) ->
    State = State0#v1{connection_state = closing},
    close(undefined, State).

start_writer(#v1{helper_sup = SupPid,
                 sock = Sock,
                 connection = #v1_connection{outgoing_max_frame_size = MaxFrame}} = State) ->
    ChildSpec = #{id => writer,
                  start => {rabbit_amqp1_0_writer, start_link,
                            [Sock, MaxFrame, self()]},
                  restart => transient,
                  significant => true,
                  shutdown => ?WORKER_WAIT,
                  type => worker,
                  modules => [rabbit_amqp1_0_writer]
                 },
    {ok, Pid} = supervisor:start_child(SupPid, ChildSpec),
    State#v1{writer = Pid}.

handle_1_0_session_frame(Channel, Frame, State) ->
    case maps:get(Channel, State#v1.tracked_channels, undefined) of
        undefined ->
            case ?IS_RUNNING(State) of
                true ->
                    send_to_new_1_0_session(Channel, Frame, State);
                false ->
                    throw({channel_frame_while_starting,
                           Channel, State#v1.connection_state,
                           Frame})
            end;
        SessionPid ->
            ok = rabbit_amqp1_0_session:process_frame(SessionPid, Frame),
            case Frame of
                #'v1_0.end'{} ->
                    untrack_channel(Channel, State);
                _ ->
                    State
            end
    end.

%% TODO: write a proper ANONYMOUS plugin and unify with STOMP
handle_1_0_sasl_frame(#'v1_0.sasl_init'{mechanism = {symbol, <<"ANONYMOUS">>},
                                        hostname = _Hostname},
                      State = #v1{connection_state = starting,
                                  sock             = Sock}) ->
    case application:get_env(rabbitmq_amqp1_0, default_user) of
        {ok, none} ->
            %% No need to do anything, we will blow up in start_connection
            ok;
        {ok, _} ->
            %% We only need to send the frame, again start_connection
            %% will set up the default user.
            Outcome = #'v1_0.sasl_outcome'{code = {ubyte, 0}},
            ok = send_on_channel0(Sock, Outcome, rabbit_amqp1_0_sasl),
            switch_callback(State#v1{connection_state = waiting_amqp0100},
                            handshake, 8)
    end;
handle_1_0_sasl_frame(#'v1_0.sasl_init'{mechanism        = {symbol, Mechanism},
                                        initial_response = {binary, Response},
                                        hostname         = _Hostname},
                      State0 = #v1{connection_state = starting,
                                   connection       = Connection,
                                   sock             = Sock}) ->
    AuthMechanism = auth_mechanism_to_module(Mechanism, Sock),
    State = State0#v1{connection       =
                          Connection#v1_connection{
                            auth_mechanism    = {Mechanism, AuthMechanism},
                            auth_state        = AuthMechanism:init(Sock)},
                      connection_state = securing},
    auth_phase_1_0(Response, State);
handle_1_0_sasl_frame(#'v1_0.sasl_response'{response = {binary, Response}},
                      State = #v1{connection_state = securing}) ->
    auth_phase_1_0(Response, State);
handle_1_0_sasl_frame(Frame, State) ->
    throw({unexpected_1_0_sasl_frame, Frame, State}).

%% We need to handle restarts...
handle_input(handshake, <<"AMQP", 0, 1, 0, 0>>, State) ->
    start_1_0_connection(amqp, State);

%% 3 stands for "SASL" (keeping this here for when we do TLS)
handle_input(handshake, <<"AMQP", 3, 1, 0, 0>>, State) ->
    start_1_0_connection(sasl, State);

handle_input({frame_header_1_0, Mode},
             Header = <<Size:32, DOff:8, Type:8, Channel:16>>,
             State) when DOff >= 2 ->
    case {Mode, Type} of
        {amqp, 0} -> ok;
        {sasl, 1} -> ok;
        _         -> throw({bad_1_0_header_type, Header, Mode})
    end,
    MaxFrameSize = State#v1.connection#v1_connection.incoming_max_frame_size,
    if Size =:= 8 ->
           %% heartbeat
           State;
       Size > MaxFrameSize ->
           handle_exception(
             State, Channel, error_frame(
                               ?V_1_0_CONNECTION_ERROR_FRAMING_ERROR,
                               "frame size (~b bytes) > maximum frame size (~b bytes)",
                               [Size, MaxFrameSize]));
       true ->
           switch_callback(State, {frame_payload_1_0, Mode, DOff, Channel}, Size - 8)
    end;
handle_input({frame_header_1_0, _Mode}, Malformed, _State) ->
    throw({bad_1_0_header, Malformed});
handle_input({frame_payload_1_0, Mode, DOff, Channel},
            FrameBin, State) ->
    SkipBits = (DOff * 32 - 64), % DOff = 4-byte words, we've read 8 already
    <<Skip:SkipBits, FramePayload/binary>> = FrameBin,
    Skip = Skip, %% hide warning when debug is off
    handle_1_0_frame(Mode, Channel, FramePayload,
                     switch_callback(State, {frame_header_1_0, Mode}, 8));

handle_input(Callback, Data, _State) ->
    throw({bad_input, Callback, Data}).

init(Mode, PackedState) ->
    %% By invoking recvloop here we become 1.0.
    recvloop(sys:debug_options([]),
             start_1_0_connection(Mode, unpack_from_0_9_1(PackedState))).

start_1_0_connection(sasl, State = #v1{sock = Sock}) ->
    send_1_0_handshake(Sock, <<"AMQP",3,1,0,0>>),
    Ms = {array, symbol,
          case application:get_env(rabbitmq_amqp1_0, default_user)  of
              {ok, none} -> [];
              {ok, _}    -> [{symbol, <<"ANONYMOUS">>}]
          end ++
              [{symbol, list_to_binary(atom_to_list(M))} || M <- auth_mechanisms(Sock)]},
    Mechanisms = #'v1_0.sasl_mechanisms'{sasl_server_mechanisms = Ms},
    ok = send_on_channel0(Sock, Mechanisms, rabbit_amqp1_0_sasl),
    start_1_0_connection0(sasl, State);

start_1_0_connection(amqp,
                     State = #v1{sock       = Sock,
                                 connection = C = #v1_connection{user = User}}) ->
    {ok, NoAuthUsername} = application:get_env(rabbitmq_amqp1_0, default_user),
    case {User, NoAuthUsername} of
        {none, none} ->
            send_1_0_handshake(Sock, <<"AMQP",3,1,0,0>>),
            throw(banned_unauthenticated_connection);
        {none, Username} ->
            case rabbit_access_control:check_user_login(
                   list_to_binary(Username), []) of
                {ok, NoAuthUser} ->
                    State1 = State#v1{
                               connection = C#v1_connection{user = NoAuthUser}},
                    send_1_0_handshake(Sock, <<"AMQP",0,1,0,0>>),
                    start_1_0_connection0(amqp, State1);
                _ ->
                    send_1_0_handshake(Sock, <<"AMQP",3,1,0,0>>),
                    throw(default_user_missing)
            end;
        _ ->
            send_1_0_handshake(Sock, <<"AMQP",0,1,0,0>>),
            start_1_0_connection0(amqp, State)
    end.

start_1_0_connection0(Mode, State = #v1{connection = Connection,
                                        helper_sup = HelperSup}) ->
    SessionSup = case Mode of
                     sasl ->
                         undefined;
                     amqp ->
                         ChildSpec = #{id => session_sup,
                                       start => {rabbit_amqp1_0_session_sup, start_link, [self()]},
                                       restart => transient,
                                       significant => true,
                                       shutdown => infinity,
                                       type => supervisor,
                                       modules => [rabbit_amqp1_0_session_sup]},
                         {ok, Pid} = supervisor:start_child(HelperSup, ChildSpec),
                         Pid
                 end,
    switch_callback(State#v1{connection = Connection#v1_connection{
                                            timeout_sec = ?NORMAL_TIMEOUT},
                             session_sup = SessionSup,
                             connection_state = starting},
                    {frame_header_1_0, Mode}, 8).

send_1_0_handshake(Sock, Handshake) ->
    ok = inet_op(fun () -> rabbit_net:send(Sock, Handshake) end).

send_on_channel0(Sock, Method) ->
    send_on_channel0(Sock, Method, amqp10_framing).

send_on_channel0(Sock, Method, Framing) ->
    ok = rabbit_amqp1_0_writer:internal_send_command(Sock, Method, Framing).

%% End 1-0

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

auth_mechanisms(Sock) ->
    {ok, Configured} = application:get_env(rabbit, auth_mechanisms),
    [Name || {Name, Module} <- rabbit_registry:lookup_all(auth_mechanism),
             Module:should_offer(Sock), lists:member(Name, Configured)].

%% Begin 1-0

auth_phase_1_0(Response,
               State = #v1{connection = Connection =
                               #v1_connection{auth_mechanism = {Name, AuthMechanism},
                                              auth_state     = AuthState},
                       sock = Sock}) ->
    case AuthMechanism:handle_response(Response, AuthState) of
        {refused, Username, Msg, Args} ->
            %% We don't trust the client at this point - force them to wait
            %% for a bit before sending the sasl outcome frame
            %% so they can't DOS us with repeated failed logins etc.
            auth_fail(Username, State),
            timer:sleep(?SILENT_CLOSE_DELAY * 1000),
            Outcome = #'v1_0.sasl_outcome'{code = {ubyte, 1}},
            ok = send_on_channel0(Sock, Outcome, rabbit_amqp1_0_sasl),
            protocol_error(
              ?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS, "~ts login refused: ~ts",
              [Name, io_lib:format(Msg, Args)]);
        {protocol_error, Msg, Args} ->
            auth_fail(none, State),
            protocol_error(?V_1_0_AMQP_ERROR_DECODE_ERROR, Msg, Args);
        {challenge, Challenge, AuthState1} ->
            rabbit_core_metrics:auth_attempt_succeeded(<<>>, <<>>, amqp10),
            Secure = #'v1_0.sasl_challenge'{challenge = {binary, Challenge}},
            ok = send_on_channel0(Sock, Secure, rabbit_amqp1_0_sasl),
            State#v1{connection = Connection#v1_connection{auth_state = AuthState1}};
        {ok, User = #user{username = Username}} ->
            case rabbit_access_control:check_user_loopback(Username, Sock) of
                ok ->
                    rabbit_log_connection:info(
                      "AMQP 1.0 connection: user '~ts' authenticated", [Username]),
                    rabbit_core_metrics:auth_attempt_succeeded(<<>>, Username, amqp10),
                    notify_auth(user_authentication_success, Username, State);
                not_allowed ->
                    auth_fail(Username, State),
                    protocol_error(
                      ?V_1_0_AMQP_ERROR_UNAUTHORIZED_ACCESS,
                      "user '~ts' can only connect via localhost",
                      [Username])
            end,
            Outcome = #'v1_0.sasl_outcome'{code = {ubyte, 0}},
            ok = send_on_channel0(Sock, Outcome, rabbit_amqp1_0_sasl),
            State1 = State#v1{connection_state = waiting_amqp0100,
                              connection = Connection#v1_connection{user = User}},
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

track_channel(ChannelNum, SessionPid, State) ->
    rabbit_log:debug("AMQP 1.0 created session process ~p for channel number ~b",
                     [SessionPid, ChannelNum]),
    State#v1{tracked_channels = maps:put(ChannelNum, SessionPid, State#v1.tracked_channels)}.

untrack_channel(Channel, State) ->
    case maps:take(Channel, State#v1.tracked_channels) of
        {Value, NewMap} ->
            rabbit_log:debug("AMQP 1.0 closed channel = ~tp ", [{Channel, Value}]),
            State#v1{tracked_channels = NewMap};
        error -> State
    end.

send_to_new_1_0_session(
  ChannelNum, BeginFrame,
  #v1{session_sup = SessionSup,
      connection = #v1_connection{outgoing_max_frame_size = MaxFrame,
                                  hostname = Hostname,
                                  user = User},
      writer = WriterPid} = State0) ->
    %% Subtract fixed frame header size.
    OutgoingMaxFrameSize = case MaxFrame of
                               unlimited -> unlimited;
                               _ -> MaxFrame - 8
                           end,
    ChildArgs = [WriterPid,
                 ChannelNum,
                 OutgoingMaxFrameSize,
                 User,
                 vhost(Hostname),
                 BeginFrame],
    case rabbit_amqp1_0_session_sup:start_session(SessionSup, ChildArgs) of
        {ok, SessionPid} ->
            erlang:monitor(process, SessionPid),
            State = track_channel(ChannelNum, SessionPid, State0),
            rabbit_log_connection:info(
              "AMQP 1.0 connection: user '~ts' authenticated and granted access to vhost '~ts'",
              [User#user.username, vhost(Hostname)]),
            State;
        {error, {not_allowed, _}} ->
            rabbit_log:error("AMQP 1.0: user '~ts' is not allowed to access virtual host '~ts'",
                [User#user.username, vhost(Hostname)]),
            %% Let's skip the supervisor trace, this is an expected error
            throw({error, {not_allowed, User#user.username}});
        {error, _} = E ->
            throw(E)
    end.

vhost({utf8, <<"vhost:", VHost/binary>>}) ->
    VHost;
vhost(_) ->
    application:get_env(rabbitmq_amqp1_0, default_vhost,
                        application:get_env(rabbit, default_vhost, <<"/">>)).

%% End 1-0

info(Pid, InfoItems) ->
    case InfoItems -- ?INFO_ITEMS of
        [] ->
            Ref = erlang:monitor(process, Pid),
            Pid ! {info, InfoItems, self()},
            receive
                {info_reply, Items} ->
                    erlang:demonitor(Ref),
                    Items;
                {'DOWN', _, process, Pid, _} ->
                    []
            end;
        UnknownItems -> throw({bad_argument, UnknownItems})
    end.

infos(Items, State) ->
    [{Item, i(Item, State)} || Item <- Items].

i(pid, #v1{}) ->
    self();
i(type, #v1{}) ->
    network;
i(protocol, #v1{}) ->
    {'AMQP', {1, 0}};
i(connection, #v1{connection = Val}) ->
    Val;
i(node, #v1{}) ->
    node();
i(auth_mechanism, #v1{connection = #v1_connection{auth_mechanism = none}}) ->
    none;
i(auth_mechanism, #v1{connection = #v1_connection{auth_mechanism = {Name, _Mod}}}) ->
    Name;
i(frame_max, #v1{connection = #v1_connection{outgoing_max_frame_size = Val}}) ->
    Val;
i(timeout, #v1{connection = #v1_connection{timeout_sec = Val}}) ->
    Val;
i(user,
  #v1{connection = #v1_connection{user = #user{username = Val}}}) ->
    Val;
i(user,
  #v1{connection = #v1_connection{user = none}}) ->
    '';
i(connection_state, #v1{connection_state = Val}) ->
    Val;
i(connected_at, #v1{connection = #v1_connection{connected_at = Val}}) ->
    Val;
i(name, #v1{connection = #v1_connection{name = Val}}) ->
    Val;
i(vhost, #v1{connection = #v1_connection{vhost = Val}}) ->
    Val;
% i(host, #v1{connection = #v1_connection{hostname = {utf8, Val}}}) ->
%     Val;
% i(host, #v1{connection = #v1_connection{hostname = Val}}) ->
%     Val;
i(host, #v1{connection = #v1_connection{host = Val}}) ->
    Val;
i(port, #v1{connection = #v1_connection{port = Val}}) ->
    Val;
i(peer_host, #v1{connection = #v1_connection{peer_host = Val}}) ->
    Val;
i(peer_port, #v1{connection = #v1_connection{peer_port = Val}}) ->
    Val;
i(SockStat, S) when SockStat =:= recv_oct;
                    SockStat =:= recv_cnt;
                    SockStat =:= send_oct;
                    SockStat =:= send_cnt;
                    SockStat =:= send_pend ->
    socket_info(fun (Sock) -> rabbit_net:getstat(Sock, [SockStat]) end,
                fun ([{_, I}]) -> I end, S);
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
            [mc_amqpl:to_091(Key, TypeVal) ||
             {{symbol, Key}, TypeVal} <- Fields]
    end.

%% From rabbit_reader
socket_info(Get, Select, #v1{sock = Sock}) ->
    case Get(Sock) of
        {ok,    T} -> Select(T);
        {error, _} -> ''
    end.
