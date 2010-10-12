%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is the RabbitMQ Erlang Client.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C)
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): Ben Hood <0x6e6562@gmail.com>.

%% @private
-module(amqp_network_connection).

-include("amqp_client.hrl").

-behaviour(amqp_gen_connection).

-export([init/1, terminate/2, connect/3, do/2, open_channel_args/1, i/2,
         info_keys/0, handle_message/2, closing_state_set/3,
         channels_terminated/1]).

-define(RABBIT_TCP_OPTS, [binary, {packet, 0}, {active,false}, {nodelay, true}]).
-define(SOCKET_CLOSING_TIMEOUT, 1000).
-define(HANDSHAKE_RECEIVE_TIMEOUT, 60000).

-record(state, {sock,
                heartbeat,
                writer0,
                closing_reason = false, %% false | Reason
                waiting_socket_close = false}).

-define(INFO_KEYS, [type, heartbeat, sock]).

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
    %% Catching because it expects the {channel_exit, _, _} message on error
    catch rabbit_writer:send_command_sync(Writer, Method).

handle_message(timeout_waiting_for_close_ok,
               State = #state{closing_reason = Reason}) ->
    {stop, {timeout_waiting_for_close_ok, Reason}, State};
handle_message(socket_closing_timeout,
               State = #state{closing_reason = Reason}) ->
    {stop, {socket_closing_timeout, Reason}, State};
handle_message(socket_closed, State = #state{waiting_socket_close = true,
                                             closing_reason = Reason}) ->
    {stop, Reason, State};
handle_message(socket_closed, State = #state{waiting_socket_close = false}) ->
    {stop, socket_closed_unexpectedly, State};
handle_message({socket_error, _} = SocketError, State) ->
    {stop, SocketError, State};
handle_message({channel_exit, _, Reason}, State) ->
    {stop, {channel0_died, Reason}, State};
handle_message(timeout, State) ->
    {stop, heartbeat_timeout, State}.

closing_state_set(_ChannelCloseType, Reason, State) ->
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
i(sock,      State) -> State#state.sock;
i(Item,     _State) -> throw({bad_argument, Item}).

info_keys() ->
    ?INFO_KEYS.

%%---------------------------------------------------------------------------
%% Handshake
%%---------------------------------------------------------------------------

connect(AmqpParams = #amqp_params{host        = Host,
                                  port        = Port,
                                  ssl_options = none}, SIF, State) ->
    case gen_tcp:connect(Host, Port, ?RABBIT_TCP_OPTS) of
        {ok, Sock}     -> try_handshake(AmqpParams, SIF,
                                        State#state{sock = Sock});
        {error, _} = E -> E
    end;
connect(AmqpParams = #amqp_params{host        = Host,
                                  port        = Port,
                                  ssl_options = SslOpts}, SIF, State) ->
    rabbit_misc:start_applications([crypto, public_key, ssl]),
    case gen_tcp:connect(Host, Port, ?RABBIT_TCP_OPTS) of
        {ok, Sock} ->
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

try_handshake(AmqpParams, SIF, State) ->
    try handshake(AmqpParams, SIF, State) of
        Return -> Return
    catch
        exit:socket_closed_unexpectedly = Reason ->
            {error, {auth_failure_likely, Reason}};
        _:Reason ->
            {error, Reason}
    end.

handshake(AmqpParams, SIF, State0 = #state{sock = Sock}) ->
    ok = rabbit_net:send(Sock, ?PROTOCOL_HEADER),
    {SHF, ChMgr, State1} = start_infrastructure(SIF, State0),
    {ServerProperties, ChannelMax, State2} =
        network_handshake(AmqpParams, ChMgr, State1),
    start_heartbeat(SHF, State2),
    {ok, ServerProperties, ChannelMax, ChMgr, State2}.

start_infrastructure(SIF, State = #state{sock = Sock}) ->
    {ok, {ChMgr, _MainReader, _Framing, Writer, SHF}} = SIF(Sock),
    {SHF, ChMgr, State#state{writer0 = Writer}}.

network_handshake(AmqpParams, ChMgr, State) ->
    Start = handshake_recv(),
    #'connection.start'{server_properties = ServerProperties} = Start,
    ok = check_version(Start),
    do2(start_ok(AmqpParams), State),
    Tune = handshake_recv(),
    TuneOk = negotiate_values(Tune, AmqpParams),
    do2(TuneOk, State),
    ConnectionOpen =
        #'connection.open'{virtual_host = AmqpParams#amqp_params.virtual_host},
    do2(ConnectionOpen, State),
    #'connection.open_ok'{} = handshake_recv(),
    #'connection.tune_ok'{channel_max = ChannelMax,
                          frame_max   = FrameMax,
                          heartbeat   = Heartbeat} = TuneOk,
    %% TODO: remove this message and add info items for these instead
    ?LOG_INFO("Negotiated maximums: (Channel = ~p, Frame = ~p, "
              "Heartbeat = ~p)~n",
              [ChannelMax, FrameMax, Heartbeat]),
    if ChannelMax =/= 0 -> amqp_channels_manager:set_channel_max(ChMgr,
                                                                 ChannelMax);
       true             -> ok
    end,
    {ServerProperties, ChannelMax, State#state{heartbeat = Heartbeat}}.

start_heartbeat(SHF, #state{sock = Sock, heartbeat = Heartbeat}) ->
    SHF(Sock, Heartbeat).

check_version(#'connection.start'{version_major = ?PROTOCOL_VERSION_MAJOR,
                                  version_minor = ?PROTOCOL_VERSION_MINOR}) ->
    ok;
check_version(#'connection.start'{version_major = 8,
                                  version_minor = 0}) ->
    exit({protocol_version_mismatch, 0, 8});
check_version(#'connection.start'{version_major = Major,
                                  version_minor = Minor}) ->
    exit({protocol_version_mismatch, Major, Minor}).

negotiate_values(#'connection.tune'{channel_max = ServerChannelMax,
                                    frame_max   = ServerFrameMax,
                                    heartbeat   = ServerHeartbeat},
                 #amqp_params{channel_max = ClientChannelMax,
                              frame_max   = ClientFrameMax,
                              heartbeat   = ClientHeartbeat}) ->
    #'connection.tune_ok'{
        channel_max = negotiate_max_value(ClientChannelMax, ServerChannelMax),
        frame_max   = negotiate_max_value(ClientFrameMax, ServerFrameMax),
        heartbeat   = negotiate_max_value(ClientHeartbeat, ServerHeartbeat)}.

negotiate_max_value(Client, Server) when Client =:= 0; Server =:= 0 ->
    lists:max([Client, Server]);
negotiate_max_value(Client, Server) ->
    lists:min([Client, Server]).

start_ok(#amqp_params{username          = Username,
                      password          = Password,
                      client_properties = UserProps}) ->
    LoginTable = [{<<"LOGIN">>, longstr, Username},
                  {<<"PASSWORD">>, longstr, Password}],
    #'connection.start_ok'{
        client_properties = client_properties(UserProps),
        mechanism = <<"AMQPLAIN">>,
        response = rabbit_binary_generator:generate_table(LoginTable)}.

client_properties(UserProperties) ->
    {ok, Vsn} = application:get_key(amqp_client, vsn),
    Default = [{<<"product">>,   longstr, <<"RabbitMQ">>},
               {<<"version">>,   longstr, list_to_binary(Vsn)},
               {<<"platform">>,  longstr, <<"Erlang">>},
               {<<"copyright">>, longstr,
                <<"Copyright (C) 2007-2009 LShift Ltd., "
                  "Cohesive Financial Technologies LLC., "
                  "and Rabbit Technologies Ltd.">>},
               {<<"information">>, longstr,
                <<"Licensed under the MPL.  "
                  "See http://www.rabbitmq.com/">>}],
    lists:foldl(fun({K, _, _} = Tuple, Acc) ->
                    lists:keystore(K, 1, Acc, Tuple)
                end, Default, UserProperties).

handshake_recv() ->
    receive
        {'$gen_cast', {method, Method, none}} ->
            Method;
        socket_closed ->
            exit(socket_closed_unexpectedly);
        {socket_error, _} = SocketError ->
            exit(SocketError);
        Other ->
            exit({handshake_recv_unexpected_message, Other})
    after ?HANDSHAKE_RECEIVE_TIMEOUT ->
        exit(handshake_receive_timed_out)
    end.
