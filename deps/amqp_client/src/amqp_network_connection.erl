%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

%% @private
-module(amqp_network_connection).

-include("amqp_client.hrl").

-behaviour(amqp_gen_connection).

-export([init/1, terminate/2, connect/4, do/2, open_channel_args/1, i/2,
         info_keys/0, handle_message/2, closing/3, channels_terminated/1]).

-define(RABBIT_TCP_OPTS, [binary, {packet, 0}, {active,false}, {nodelay, true}]).
-define(SOCKET_CLOSING_TIMEOUT, 1000).
-define(HANDSHAKE_RECEIVE_TIMEOUT, 60000).

-record(state, {sock,
                heartbeat,
                writer0,
                frame_max,
                closing_reason, %% undefined | Reason
                waiting_socket_close = false}).

-define(INFO_KEYS, [type, heartbeat, frame_max, sock]).

%%---------------------------------------------------------------------------

init([]) ->
    {ok, #state{}}.

open_channel_args(#state{sock = Sock}) ->
    [Sock].

do(#'connection.close_ok'{} = CloseOk, State) ->
    erlang:send_after(?SOCKET_CLOSING_TIMEOUT, self(), socket_closing_timeout),
    do2(CloseOk, State);
do(Method, State) ->
    do2(Method, State).

do2(Method, #state{writer0 = Writer}) ->
    %% Catching because it expects the {channel_exit, _} message on error
    catch rabbit_writer:send_command_sync(Writer, Method).

handle_message(socket_closing_timeout,
               State = #state{closing_reason = Reason}) ->
    {stop, {socket_closing_timeout, Reason}, State};
handle_message(socket_closed, State = #state{waiting_socket_close = true,
                                             closing_reason = Reason}) ->
    {stop, {shutdown, Reason}, State};
handle_message(socket_closed, State = #state{waiting_socket_close = false}) ->
    {stop, socket_closed_unexpectedly, State};
handle_message({socket_error, _} = SocketError, State) ->
    {stop, SocketError, State};
handle_message({channel_exit, Reason}, State) ->
    {stop, {channel0_died, Reason}, State};
handle_message(heartbeat_timeout, State) ->
    {stop, heartbeat_timeout, State}.

closing(_ChannelCloseType, Reason, State) ->
    {ok, State#state{closing_reason = Reason}}.

channels_terminated(State = #state{closing_reason =
                                     {server_initiated_close, _, _}}) ->
    {ok, State#state{waiting_socket_close = true}};
channels_terminated(State) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

i(type,     _State) -> network;
i(heartbeat, State) -> State#state.heartbeat;
i(frame_max, State) -> State#state.frame_max;
i(sock,      State) -> State#state.sock;
i(Item,     _State) -> throw({bad_argument, Item}).

info_keys() ->
    ?INFO_KEYS.

%%---------------------------------------------------------------------------
%% Handshake
%%---------------------------------------------------------------------------

connect(AmqpParams = #amqp_params_network{host = Host}, SIF, ChMgr, State) ->
    case gethostaddr(Host) of
        []     -> {error, unknown_host};
        [AF|_] -> do_connect(AF, AmqpParams, SIF, ChMgr, State)
    end.

do_connect({Addr, Family},
           AmqpParams = #amqp_params_network{ssl_options        = none,
                                             port               = Port,
                                             connection_timeout = Timeout,
                                             socket_options     = ExtraOpts},
           SIF, ChMgr, State) ->
    case gen_tcp:connect(Addr, Port,
                         [Family | ?RABBIT_TCP_OPTS] ++ ExtraOpts,
                         Timeout) of
        {ok, Sock}     -> try_handshake(AmqpParams, SIF, ChMgr,
                                        State#state{sock = Sock});
        {error, _} = E -> E
    end;
do_connect({Addr, Family},
           AmqpParams = #amqp_params_network{ssl_options        = SslOpts,
                                             port               = Port,
                                             connection_timeout = Timeout,
                                             socket_options     = ExtraOpts},
           SIF, ChMgr, State) ->
    rabbit_misc:start_applications([crypto, public_key, ssl]),
    case gen_tcp:connect(Addr, Port,
                         [Family | ?RABBIT_TCP_OPTS] ++ ExtraOpts,
                         Timeout) of
        {ok, Sock} ->
            case ssl:connect(Sock, SslOpts) of
                {ok, SslSock} ->
                    RabbitSslSock = #ssl_socket{ssl = SslSock, tcp = Sock},
                    try_handshake(AmqpParams, SIF, ChMgr,
                                  State#state{sock = RabbitSslSock});
                {error, _} = E ->
                    E
            end;
        {error, _} = E ->
            E
    end.

gethostaddr(Host) ->
    Lookups = [{Family, inet:getaddr(Host, Family)}
               || Family <- [inet, inet6]],
    [{IP, Family} || {Family, {ok, IP}} <- Lookups].

try_handshake(AmqpParams, SIF, ChMgr, State) ->
    try handshake(AmqpParams, SIF, ChMgr, State) of
        Return -> Return
    catch exit:Reason -> {error, Reason}
    end.

handshake(AmqpParams, SIF, ChMgr, State0 = #state{sock = Sock}) ->
    ok = rabbit_net:send(Sock, ?PROTOCOL_HEADER),
    {SHF, State1} = start_infrastructure(SIF, ChMgr, State0),
    network_handshake(AmqpParams, SHF, State1).

start_infrastructure(SIF, ChMgr, State = #state{sock = Sock}) ->
    {ok, {_MainReader, _AState, Writer, SHF}} = SIF(Sock, ChMgr),
    {SHF, State#state{writer0 = Writer}}.

network_handshake(AmqpParams = #amqp_params_network{virtual_host = VHost},
                  SHF, State0) ->
    Start = #'connection.start'{server_properties = ServerProperties,
                                mechanisms = Mechanisms} =
        handshake_recv('connection.start'),
    ok = check_version(Start),
    Tune = login(AmqpParams, Mechanisms, State0),
    {TuneOk, ChannelMax, State1} = tune(Tune, AmqpParams, SHF, State0),
    do2(TuneOk, State1),
    do2(#'connection.open'{virtual_host = VHost}, State1),
    Params = {ServerProperties, ChannelMax, State1},
    case handshake_recv('connection.open_ok') of
        #'connection.open_ok'{}                     -> {ok, Params};
        {closing, #amqp_error{} = AmqpError, Error} -> {closing, Params,
                                                        AmqpError, Error}
    end.

check_version(#'connection.start'{version_major = ?PROTOCOL_VERSION_MAJOR,
                                  version_minor = ?PROTOCOL_VERSION_MINOR}) ->
    ok;
check_version(#'connection.start'{version_major = 8,
                                  version_minor = 0}) ->
    exit({protocol_version_mismatch, 0, 8});
check_version(#'connection.start'{version_major = Major,
                                  version_minor = Minor}) ->
    exit({protocol_version_mismatch, Major, Minor}).

tune(#'connection.tune'{channel_max = ServerChannelMax,
                        frame_max   = ServerFrameMax,
                        heartbeat   = ServerHeartbeat},
     #amqp_params_network{channel_max = ClientChannelMax,
                          frame_max   = ClientFrameMax,
                          heartbeat   = ClientHeartbeat}, SHF, State) ->
    [ChannelMax, Heartbeat, FrameMax] =
        lists:zipwith(fun (Client, Server) when Client =:= 0; Server =:= 0 ->
                              lists:max([Client, Server]);
                          (Client, Server) ->
                              lists:min([Client, Server])
                      end, [ClientChannelMax, ClientHeartbeat, ClientFrameMax],
                           [ServerChannelMax, ServerHeartbeat, ServerFrameMax]),
    NewState = State#state{heartbeat = Heartbeat, frame_max = FrameMax},
    start_heartbeat(SHF, NewState),
    {#'connection.tune_ok'{channel_max = ChannelMax,
                           frame_max   = FrameMax,
                           heartbeat   = Heartbeat}, ChannelMax, NewState}.

start_heartbeat(SHF, #state{sock = Sock, heartbeat = Heartbeat}) ->
    Frame = rabbit_binary_generator:build_heartbeat_frame(),
    SendFun = fun () -> catch rabbit_net:send(Sock, Frame) end,
    Connection = self(),
    ReceiveFun = fun () -> Connection ! heartbeat_timeout end,
    SHF(Sock, Heartbeat, SendFun, Heartbeat, ReceiveFun).

login(Params = #amqp_params_network{auth_mechanisms = ClientMechanisms,
                                    client_properties = UserProps},
      ServerMechanismsStr, State) ->
    ServerMechanisms = string:tokens(binary_to_list(ServerMechanismsStr), " "),
    case [{N, S, F} || F <- ClientMechanisms,
                       {N, S} <- [F(none, Params, init)],
                       lists:member(binary_to_list(N), ServerMechanisms)] of
        [{Name, MState0, Mech}|_] ->
            {Resp, MState1} = Mech(none, Params, MState0),
            StartOk = #'connection.start_ok'{
              client_properties = client_properties(UserProps),
              mechanism = Name,
              response = Resp},
            do2(StartOk, State),
            login_loop(Mech, MState1, Params, State);
        [] ->
            exit({no_suitable_auth_mechanism, ServerMechanisms})
    end.

login_loop(Mech, MState0, Params, State) ->
    case handshake_recv('connection.tune') of
        Tune = #'connection.tune'{} ->
            Tune;
        #'connection.secure'{challenge = Challenge} ->
            {Resp, MState1} = Mech(Challenge, Params, MState0),
            do2(#'connection.secure_ok'{response = Resp}, State),
            login_loop(Mech, MState1, Params, State)
    end.

client_properties(UserProperties) ->
    {ok, Vsn} = application:get_key(amqp_client, vsn),
    Default = [{<<"product">>,   longstr, <<"RabbitMQ">>},
               {<<"version">>,   longstr, list_to_binary(Vsn)},
               {<<"platform">>,  longstr, <<"Erlang">>},
               {<<"copyright">>, longstr,
                <<"Copyright (c) 2007-2012 VMware, Inc.">>},
               {<<"information">>, longstr,
                <<"Licensed under the MPL.  "
                  "See http://www.rabbitmq.com/">>},
               {<<"capabilities">>, table, ?CLIENT_CAPABILITIES}],
    lists:foldl(fun({K, _, _} = Tuple, Acc) ->
                    lists:keystore(K, 1, Acc, Tuple)
                end, Default, UserProperties).

handshake_recv(Expecting) ->
    receive
        {'$gen_cast', {method, Method, none, noflow}} ->
            case {Expecting, element(1, Method)} of
                {E, M} when E =:= M ->
                    Method;
                {'connection.open_ok', _} ->
                    {closing,
                     #amqp_error{name        = command_invalid,
                                 explanation = "was expecting "
                                               "connection.open_ok"},
                     {error, {unexpected_method, Method,
                              {expecting, Expecting}}}};
                _ ->
                    throw({unexpected_method, Method,
                          {expecting, Expecting}})
            end;
        socket_closed ->
            case Expecting of
                'connection.tune'    -> exit(auth_failure);
                'connection.open_ok' -> exit(access_refused);
                _                    -> exit({socket_closed_unexpectedly,
                                              Expecting})
            end;
        {socket_error, _} = SocketError ->
            exit({SocketError, {expecting, Expecting}});
        heartbeat_timeout ->
            exit(heartbeat_timeout);
        Other ->
            throw({handshake_recv_unexpected_message, Other})
    after ?HANDSHAKE_RECEIVE_TIMEOUT ->
        case Expecting of
            'connection.open_ok' ->
                {closing,
                 #amqp_error{name        = internal_error,
                             explanation = "handshake timed out waiting "
                                           "connection.open_ok"},
                 {error, handshake_receive_timed_out}};
            _ ->
                exit(handshake_receive_timed_out)
        end
    end.
