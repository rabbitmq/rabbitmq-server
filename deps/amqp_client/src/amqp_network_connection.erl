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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
%%

%% @private
-module(amqp_network_connection).

-include("amqp_client_internal.hrl").

-behaviour(amqp_gen_connection).
-export([init/0, terminate/2, connect/4, do/2, open_channel_args/1, i/2,
         info_keys/0, handle_message/2, closing/3, channels_terminated/1]).

-define(RABBIT_TCP_OPTS, [binary, {packet, 0}, {active,false}, {nodelay, true}]).
-define(SOCKET_CLOSING_TIMEOUT, 1000).
-define(HANDSHAKE_RECEIVE_TIMEOUT, 60000).
-define(TCP_MAX_PACKET_SIZE, (16#4000000 + ?EMPTY_FRAME_SIZE - 1)).

-record(state, {sock,
                name,
                heartbeat,
                writer0,
                frame_max,
                type_sup,
                closing_reason, %% undefined | Reason
                waiting_socket_close = false}).

-define(INFO_KEYS, [type, heartbeat, frame_max, sock, name]).

%%---------------------------------------------------------------------------

init() ->
    {ok, #state{}}.

open_channel_args(#state{sock = Sock, frame_max = FrameMax}) ->
    [Sock, FrameMax].

do(#'connection.close_ok'{} = CloseOk, State) ->
    erlang:send_after(?SOCKET_CLOSING_TIMEOUT, self(), socket_closing_timeout),
    do2(CloseOk, State);
do(Method, State) ->
    do2(Method, State).

do2(Method, #state{writer0 = Writer}) ->
    %% Catching because it expects the {channel_exit, _, _} message on error
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
handle_message({channel_exit, 0, Reason}, State) ->
    {stop, {channel0_died, Reason}, State};
handle_message(heartbeat_timeout, State) ->
    {stop, heartbeat_timeout, State};
handle_message(closing_timeout, State = #state{closing_reason = Reason}) ->
    {stop, Reason, State};
%% see http://erlang.org/pipermail/erlang-bugs/2012-June/002933.html
handle_message({Ref, {error, Reason}},
               State = #state{waiting_socket_close = Waiting,
                              closing_reason       = CloseReason})
  when is_reference(Ref) ->
    {stop, case {Reason, Waiting} of
               {closed,  true} -> {shutdown, CloseReason};
               {closed, false} -> socket_closed_unexpectedly;
               {_,          _} -> {socket_error, Reason}
           end, State}.

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
i(name,      State) -> State#state.name;
i(Item,     _State) -> throw({bad_argument, Item}).

info_keys() ->
    ?INFO_KEYS.

%%---------------------------------------------------------------------------
%% Handshake
%%---------------------------------------------------------------------------

connect(AmqpParams = #amqp_params_network{host = Host}, SIF, TypeSup, State) ->
    case gethostaddr(Host) of
        []     -> {error, unknown_host};
        [AF|_] -> do_connect(
                    AF, AmqpParams, SIF, State#state{type_sup = TypeSup})
    end.

do_connect({Addr, Family},
           AmqpParams = #amqp_params_network{ssl_options        = none,
                                             port               = Port,
                                             connection_timeout = Timeout,
                                             socket_options     = ExtraOpts},
           SIF, State) ->
    obtain(),
    case gen_tcp:connect(Addr, Port,
                         [Family | ?RABBIT_TCP_OPTS] ++ ExtraOpts,
                         Timeout) of
        {ok, Sock}     -> try_handshake(AmqpParams, SIF,
                                        State#state{sock = Sock});
        {error, _} = E -> E
    end;
do_connect({Addr, Family},
           AmqpParams = #amqp_params_network{ssl_options        = SslOpts0,
                                             port               = Port,
                                             connection_timeout = Timeout,
                                             socket_options     = ExtraOpts},
           SIF, State) ->
    {ok, GlobalSslOpts} = application:get_env(amqp_client, ssl_options),
    app_utils:start_applications([asn1, crypto, public_key, ssl]),
    obtain(),
    case gen_tcp:connect(Addr, Port,
                         [Family | ?RABBIT_TCP_OPTS] ++ ExtraOpts,
                         Timeout) of
        {ok, Sock} ->
            SslOpts = rabbit_networking:fix_ssl_options(
                        orddict:to_list(
                          orddict:merge(fun (_, _A, B) -> B end,
                                        orddict:from_list(GlobalSslOpts),
                                        orddict:from_list(SslOpts0)))),
            case ssl:connect(Sock, SslOpts) of
                {ok, SslSock} ->
                    RabbitSslSock = #ssl_socket{ssl = SslSock, tcp = Sock},
                    try_handshake(AmqpParams, SIF,
                                  State#state{sock = RabbitSslSock});
                {error, _} = E ->
                    E
            end;
        {error, _} = E ->
            E
    end.

inet_address_preference() ->
    case application:get_env(amqp_client, prefer_ipv6) of
        {ok, true}  -> [inet6, inet];
        {ok, false} -> [inet, inet6]
    end.

gethostaddr(Host) ->
    Lookups = [{Family, inet:getaddr(Host, Family)}
               || Family <- inet_address_preference()],
    [{IP, Family} || {Family, {ok, IP}} <- Lookups].

try_handshake(AmqpParams, SIF, State = #state{sock = Sock}) ->
    Name = case rabbit_net:connection_string(Sock, outbound) of
               {ok, Str}  -> list_to_binary(Str);
               {error, _} -> <<"unknown">>
           end,
    try handshake(AmqpParams, SIF,
                  State#state{name = <<"client ", Name/binary>>}) of
        Return -> Return
    catch exit:Reason -> {error, Reason}
    end.

handshake(AmqpParams, SIF, State0 = #state{sock = Sock}) ->
    ok = rabbit_net:send(Sock, ?PROTOCOL_HEADER),
    network_handshake(AmqpParams, start_infrastructure(SIF, State0)).

start_infrastructure(SIF, State = #state{sock = Sock, name = Name}) ->
    {ok, ChMgr, Writer} = SIF(Sock, Name),
    {ChMgr, State#state{writer0 = Writer}}.

network_handshake(AmqpParams = #amqp_params_network{virtual_host = VHost},
                  {ChMgr, State0}) ->
    Start = #'connection.start'{server_properties = ServerProperties,
                                mechanisms = Mechanisms} =
        handshake_recv('connection.start'),
    ok = check_version(Start),
    case login(AmqpParams, Mechanisms, State0) of
        {closing, #amqp_error{}, _Error} = Err ->
            do(#'connection.close_ok'{}, State0),
            Err;
        Tune ->
            {TuneOk, ChannelMax, State1} = tune(Tune, AmqpParams, State0),
            do2(TuneOk, State1),
            do2(#'connection.open'{virtual_host = VHost}, State1),
            Params = {ServerProperties, ChannelMax, ChMgr, State1},
            case handshake_recv('connection.open_ok') of
                #'connection.open_ok'{}                     -> {ok, Params};
                {closing, #amqp_error{} = AmqpError, Error} -> {closing, Params,
                                                                AmqpError, Error}
            end
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
                          heartbeat   = ClientHeartbeat}, State) ->
    [ChannelMax, Heartbeat, FrameMax] =
        lists:zipwith(fun (Client, Server) when Client =:= 0; Server =:= 0 ->
                              lists:max([Client, Server]);
                          (Client, Server) ->
                              lists:min([Client, Server])
                      end,
                      [ClientChannelMax, ClientHeartbeat, ClientFrameMax],
                      [ServerChannelMax, ServerHeartbeat, ServerFrameMax]),
    %% If we attempt to recv > 64Mb, inet_drv will return enomem, so
    %% we cap the max negotiated frame size accordingly. Note that
    %% since we receive the frame header separately, we can actually
    %% cope with frame sizes of 64M + ?EMPTY_FRAME_SIZE - 1.
    CappedFrameMax = case FrameMax of
                         0 -> ?TCP_MAX_PACKET_SIZE;
                         _ -> lists:min([FrameMax, ?TCP_MAX_PACKET_SIZE])
                     end,
    NewState = State#state{heartbeat = Heartbeat, frame_max = CappedFrameMax},
    start_heartbeat(NewState),
    {#'connection.tune_ok'{channel_max = ChannelMax,
                           frame_max   = CappedFrameMax,
                           heartbeat   = Heartbeat}, ChannelMax, NewState}.

start_heartbeat(#state{sock      = Sock,
                       name      = Name,
                       heartbeat = Heartbeat,
                       type_sup  = Sup}) ->
    Frame = rabbit_binary_generator:build_heartbeat_frame(),
    SendFun = fun () -> catch rabbit_net:send(Sock, Frame) end,
    Connection = self(),
    ReceiveFun = fun () -> Connection ! heartbeat_timeout end,
    rabbit_heartbeat:start(
      Sup, Sock, Name, Heartbeat, SendFun, Heartbeat, ReceiveFun).

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
            login_loop(Mech, MState1, Params, State);
        #'connection.close'{reply_code = ?ACCESS_REFUSED,
                            reply_text = ExplanationBin} ->
            Explanation = binary_to_list(ExplanationBin),
            {closing,
             #amqp_error{name        = access_refused,
                         explanation = Explanation},
             {error, {auth_failure, Explanation}}}
    end.

client_properties(UserProperties) ->
    {ok, Vsn} = application:get_key(amqp_client, vsn),
    Default = [{<<"product">>,   longstr, <<"RabbitMQ">>},
               {<<"version">>,   longstr, list_to_binary(Vsn)},
               {<<"platform">>,  longstr, <<"Erlang">>},
               {<<"copyright">>, longstr,
                <<"Copyright (c) 2007-2014 GoPivotal, Inc.">>},
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
                {'connection.tune', 'connection.secure'} ->
                    Method;
                {'connection.tune', 'connection.close'} ->
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
                'connection.tune'    -> exit({auth_failure, "Disconnected"});
                'connection.open_ok' -> exit(access_refused);
                _                    -> exit({socket_closed_unexpectedly,
                                              Expecting})
            end;
        {socket_error, _} = SocketError ->
            exit({SocketError, {expecting, Expecting}});
        {refused, Version} ->
            exit({server_refused_connection, Version});
        {malformed_header, All} ->
            exit({server_sent_malformed_header, All});
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

obtain() ->
    case code:is_loaded(file_handle_cache) of
        false -> ok;
        _     -> file_handle_cache:obtain()
    end.
