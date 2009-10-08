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
%%

%% @private
-module(amqp_network_driver).

-include("amqp_client.hrl").

-define(RABBIT_TCP_OPTS, [binary, {packet, 0},{active,false}, {nodelay, true}]).

-export([handshake/1, open_channel/2, close_channel/3]).
-export([close_connection/2, close_connection/3]).
-export([start_main_reader/2, start_writer/2]).
-export([do/3]).
-export([handle_broker_close/1]).

-define(SOCKET_CLOSING_TIMEOUT, 1000).
-define(CLIENT_CLOSE_TIMEOUT, 5000).
-define(HANDSHAKE_RECEIVE_TIMEOUT, 60000).

%---------------------------------------------------------------------------
% Driver API Methods
%---------------------------------------------------------------------------

handshake(State = #connection_state{serverhost = Host, port = Port,
                                    ssl_options = undefined}) ->
    case gen_tcp:connect(Host, Port, ?RABBIT_TCP_OPTS) of
        {ok, Sock} ->
            do_handshake(Sock, State);
        {error, Reason} ->
            ?LOG_INFO("Could not start the network driver: ~p~n",[Reason]),
            exit(Reason)
    end;

handshake(State = #connection_state{serverhost = Host, port = Port,
                                    ssl_options = SslOpts}) ->
    rabbit_misc:start_applications([crypto, ssl]),

    case gen_tcp:connect(Host, Port, ?RABBIT_TCP_OPTS) of
        {ok, Sock} ->
            case ssl:connect(Sock, SslOpts) of
                {ok, SslSock} ->
                    RabbitSslSock = #ssl_socket{ssl = SslSock, tcp = Sock},
                    do_handshake(RabbitSslSock, State);
                {error, Reason} ->
                    ?LOG_INFO("Could not upgrade the network driver to ssl: "
                              "~p~n", [Reason]),
                    exit(Reason)
            end;
        {error, Reason} ->
            ?LOG_WARN("Could not start the network driver: ~p~n", [Reason]),
            exit(Reason)
    end.



%% The reader runs unaware of the channel number that it is bound to
%% because this will be parsed out of the frames received off the socket.
%% Hence, you have tell the main reader which Pids are intended to
%% process messages for a particular channel
open_channel(ChannelNumber,
             #connection_state{main_reader_pid = MainReaderPid, sock = Sock}) ->
    FramingPid = start_framing_channel(),
    MainReaderPid ! {register_framing_channel, ChannelNumber, FramingPid},
    WriterPid = start_writer(Sock, ChannelNumber),
    {WriterPid, FramingPid}.

close_channel(_Reason, WriterPid, ReaderPid) ->
    rabbit_writer:shutdown(WriterPid),
    rabbit_framing_channel:shutdown(ReaderPid),
    ok.

close_connection(Close = #'connection.close'{}, State) ->
    close_connection(Close, none, State).

%% This closes the writer down, waits for the confirmation from
%% the channel and then returns the ack to the user
close_connection(Close = #'connection.close'{}, From,
                 #connection_state{channel0_writer_pid = WriterPid,
                                   channel0_reader_pid = FramingPid}) ->
    do(WriterPid, Close, none),
    rabbit_writer:shutdown(WriterPid),
    receive
        {'$gen_cast', {method, {'connection.close_ok'}, none }} ->
            case From of
                none -> ok;
                _    -> gen_server:reply(From, #'connection.close_ok'{})
            end
    after
        ?CLIENT_CLOSE_TIMEOUT -> exit(timeout_on_exit)
    end,
    rabbit_framing_channel:shutdown(FramingPid),
    ok.

do(Writer, Method, Content) ->
    case Content of
        none ->
            rabbit_writer:send_command_and_signal_back(Writer, Method, self());
        _ ->
            rabbit_writer:send_command_and_signal_back(Writer, Method, Content,
                                                       self())
    end,
    receive_writer_send_command_signal(Writer).

receive_writer_send_command_signal(Writer) ->
    receive
        rabbit_writer_send_command_signal -> ok;
        WriterExitMsg = {'EXIT', Writer, _} -> self() ! WriterExitMsg
    end.

handle_broker_close(#connection_state{channel0_writer_pid = Writer,
                                      main_reader_pid = MainReader}) ->
    do(Writer, #'connection.close_ok'{}, none),
    rabbit_writer:shutdown(Writer),
    erlang:send_after(?SOCKET_CLOSING_TIMEOUT, MainReader,
                      socket_closing_timeout).

%---------------------------------------------------------------------------
% AMQP message sending and receiving
%---------------------------------------------------------------------------

send_frame(Channel, Frame) ->
    {framing_pid, FramingPid} = resolve_framing_channel({channel, Channel}),
    rabbit_framing_channel:process(FramingPid, Frame).

recv(#connection_state{main_reader_pid = MainReaderPid}) ->
    receive
        {'$gen_cast', {method, Method, _Content}} ->
            Method;
        {'EXIT', MainReaderPid, Reason} ->
            exit({reader_exited, Reason})
    after ?HANDSHAKE_RECEIVE_TIMEOUT ->
        exit(awaiting_response_from_server_timed_out)
    end.

%---------------------------------------------------------------------------
% Internal plumbing
%---------------------------------------------------------------------------

do_handshake(Sock, State) ->
    ok = rabbit_net:send(Sock, ?PROTOCOL_HEADER),
    FramingPid = start_framing_channel(),
    MainReaderPid = spawn_link(?MODULE, start_main_reader, [Sock, FramingPid]),
    WriterPid = start_writer(Sock, 0),
    State1 = State#connection_state{channel0_writer_pid = WriterPid,
                                    channel0_reader_pid = FramingPid,
                                    main_reader_pid = MainReaderPid,
                                    sock = Sock},
    State2 = network_handshake(WriterPid, State1),
    #connection_state{heartbeat = Heartbeat} = State2,
    MainReaderPid ! {heartbeat, Heartbeat},
    State2.

network_handshake(Writer,
                  State = #connection_state{vhostpath = VHostPath}) ->
    #'connection.start'{} = recv(State),
    do(Writer, start_ok(State), none),
    #'connection.tune'{channel_max = ChannelMax,
                       frame_max = FrameMax,
                       heartbeat = Heartbeat} = recv(State),
    TuneOk = #'connection.tune_ok'{channel_max = ChannelMax,
                                   frame_max = FrameMax,
                                   heartbeat = Heartbeat},
    do(Writer, TuneOk, none),

    %% This is something where I don't understand the protocol,
    %% What happens if the following command reaches the server
    %% before the tune ok?
    %% Or doesn't get sent at all?
    ConnectionOpen = #'connection.open'{virtual_host = VHostPath},
    do(Writer, ConnectionOpen, none),
    #'connection.open_ok'{} = recv(State),
    %% TODO What should I do with the KnownHosts?
    State#connection_state{channel_max = ChannelMax, heartbeat = Heartbeat}.

start_ok(#connection_state{username = Username, password = Password}) ->
    %% TODO This eagerly starts the amqp_client application in order to
    %% to get the version from the app descriptor, which may be
    %% overkill - maybe there is a more suitable point to boot the app
    rabbit_misc:start_applications([amqp_client]),
    {ok, Vsn} = application:get_key(amqp_client, vsn),
    LoginTable = [ {<<"LOGIN">>, longstr, Username },
                   {<<"PASSWORD">>, longstr, Password }],
    #'connection.start_ok'{
           client_properties = [
                            {<<"product">>,   longstr, <<"RabbitMQ">>},
                            {<<"version">>,   longstr, list_to_binary(Vsn)},
                            {<<"platform">>,  longstr, <<"Erlang">>},
                            {<<"copyright">>, longstr,
                             <<"Copyright (C) 2007-2009 LShift Ltd., "
                               "Cohesive Financial Technologies LLC., "
                               "and Rabbit Technologies Ltd.">>},
                            {<<"information">>, longstr,
                             <<"Licensed under the MPL.  "
                               "See http://www.rabbitmq.com/">>}
                           ],
           mechanism = <<"AMQPLAIN">>,
           response = rabbit_binary_generator:generate_table(LoginTable)}.

start_writer(Sock, Channel) ->
    rabbit_writer:start_link(Sock, Channel, ?FRAME_MIN_SIZE).

start_main_reader(Sock, FramingPid) ->
    register_framing_channel(0, FramingPid),
    {ok, _Ref} = rabbit_net:async_recv(Sock, 7, infinity),
    ok = main_reader_loop(Sock, undefined, undefined, undefined),
    rabbit_net:close(Sock).

main_reader_loop(Sock, Type, Channel, Length) ->
    receive
        {inet_async, Sock, _, {ok, <<Payload:Length/binary, ?FRAME_END>>} } ->
            case handle_frame(Type, Channel, Payload) of
                closed_ok ->
                    ok;
                _ ->
                    {ok, _Ref} = rabbit_net:async_recv(Sock, 7, infinity),
                    main_reader_loop(Sock, undefined, undefined, undefined)
            end;
        {inet_async, Sock, _, {ok, <<NewType:8,NewChannel:16,NewLength:32>>}} ->
            {ok, _Ref} = rabbit_net:async_recv(Sock, NewLength + 1, infinity),
            main_reader_loop(Sock, NewType, NewChannel, NewLength);
        {inet_async, Sock, _Ref, {error, closed}} ->
            exit(socket_closed);
        {inet_async, Sock, _Ref, {error, Reason}} ->
            ?LOG_WARN("Socket error: ~p~n", [Reason]),
            exit({socket_error, Reason});
        {heartbeat, Heartbeat} ->
            rabbit_heartbeat:start_heartbeat(Sock, Heartbeat),
            main_reader_loop(Sock, Type, Channel, Length);
        {register_framing_channel, ChannelNumber, FramingPid} ->
            register_framing_channel(ChannelNumber, FramingPid),
            main_reader_loop(Sock, Type, Channel, Length);
        timeout ->
            ?LOG_WARN("Reader (~p) received timeout from heartbeat, "
                      "exiting ~n", [self()]),
            exit(connection_timeout);
        socket_closing_timeout ->
            ?LOG_WARN("Reader (~p) received socket_closing_timeout, exiting ~n",
                      [self()]),
            exit(socket_closing_timeout);
        close ->
            ?LOG_WARN("Reader (~p) received close command, "
                      "exiting ~n", [self()]),
            ok;
        {'DOWN', _MonitorRef, process, Pid, Info} ->
            case unregister_framing_channel({framing_pid, Pid}) of
                {channel, _} ->
                    ok;
                undefined ->
                    ?LOG_WARN("Reader received unexpected DOWN signal from "
                              "(~p). Info: ~p~n", [Pid, Info]),
                    exit({unexpected_down_signal, Pid, Info})
            end,
            main_reader_loop(Sock, Type, Channel, Length);
        Other ->
            ?LOG_WARN("Unknown message type: ~p~n", [Other]),
            exit({unknown_message_type, Other})
    end.

handle_frame(Type, Channel, Payload) ->
    case rabbit_reader:analyze_frame(Type, Payload) of
        heartbeat when Channel /= 0 ->
            rabbit_misc:die(frame_error);
        trace when Channel /= 0 ->
            rabbit_misc:die(frame_error);
        %% Match heartbeats and trace frames, but don't do anything with them
        heartbeat ->
            heartbeat;
        trace ->
            trace;
        {method, Method = 'connection.close_ok', none} ->
            send_frame(Channel, {method, Method}),
            closed_ok;
        AnalyzedFrame ->
            send_frame(Channel, AnalyzedFrame)
    end.

start_framing_channel() ->
    rabbit_framing_channel:start_link(fun(X) -> link(X), X end, [self()]).

register_framing_channel(ChannelNumber, FramingPid) ->
    put({channel, ChannelNumber}, {framing_pid, FramingPid}),
    put({framing_pid, FramingPid}, {channel, ChannelNumber}),
    erlang:monitor(process, FramingPid).

%% Unregister framing channel by passing either {channel, ChannelNumber} or
%% {framing_pid, FramingPid}
unregister_framing_channel(Channel) ->
    case erase(Channel) of
        Val = {channel, _}     -> erase(Val), Val;
        Val = {framing_pid, _} -> erase(Val), Val;
        undefined              -> undefined
    end.

%% Resolve framing channel by passing either {channel, ChannelNumber} or
%% {framing_pid, FramingPid}
resolve_framing_channel(Channel) ->
    get(Channel).
