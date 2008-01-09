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

-module(amqp_network_driver).

-include_lib("rabbitmq_server/include/rabbit_framing.hrl").
-include_lib("rabbitmq_server/include/rabbit.hrl").
-include("amqp_client.hrl").

-export([handshake/2, open_channel/3, close_connection/3]).
-export([start_reader/2, start_writer/2]).
-export([do/2,do/3]).

%---------------------------------------------------------------------------
% Driver API Methods
%---------------------------------------------------------------------------

handshake(ConnectionPid, ConnectionState = #connection_state{serverhost = Host}) ->
    case gen_tcp:connect(Host, 5672, [binary, {packet, 0},{active,false}]) of
        {ok, Sock} ->
            ok = gen_tcp:send(Sock, amqp_util:protocol_header()),
            ReaderPid = spawn_link(?MODULE, start_reader, [Sock, ConnectionPid]),
            WriterPid = start_writer(Sock, 0),
            ConnectionState1 = ConnectionState#connection_state{writer_pid = WriterPid,
                                                                reader_pid = ReaderPid,
                                                                sock = Sock},
            ConnectionState2 = network_handshake(WriterPid, ConnectionState1),

            #connection_state{heartbeat = Heartbeat} = ConnectionState2,
            rabbit_heartbeat:start_heartbeat(Sock, Heartbeat),
            ConnectionState2;
        {error, Reason} ->
            io:format("Could not start the network driver: ~p~n",[Reason]),
            exit(Reason)
    end.

%% The reader runs unaware of the channel number that it is bound to
%% because this will be parsed out of the frames received off the socket.
%% Hence, you have tell the singelton reader which Pids are intended to
%% process messages for a particular channel
open_channel({ChannelNumber, OutOfBand}, ChannelPid,
             State = #connection_state{reader_pid = ReaderPid,
                                       sock = Sock}) ->
    ReaderPid ! {ChannelPid, ChannelNumber},
    WriterPid = start_writer(Sock, ChannelNumber),
    amqp_channel:register_direct_peer(ChannelPid, WriterPid ).

close_connection(Close = #'connection.close'{}, From, #connection_state{writer_pid = Writer}) ->
    rabbit_writer:send_command_and_shutdown(Writer, Close),
    receive
        {frame, Channel, {method,'connection.close_ok',<<>>}, ReaderPid } ->
            gen_server:reply(From, #'connection.close_ok'{}),
            Writer ! stop
            %% Don't need to stop the reader because it stops itself by magic
            %% This is because it is looping over a socket read and it would
            %% be difficult to slip it a poison pill from this process to get it to stop
    after
        5000 ->
            exit(timeout_on_exit)
    end.

do(Writer, Method) -> rabbit_writer:send_command(Writer, Method).
do(Writer, Method, Content) -> rabbit_writer:send_command(Writer, Method, Content).

%---------------------------------------------------------------------------
% AMQP message sending and receiving
%---------------------------------------------------------------------------

send_frame(Channel, Method) ->
    ChPid = resolve_receiver(Channel),
    ChPid ! {frame, Channel, Method, self() }.

recv() ->
    receive
        {frame, Channel, {method, Method, Content}, ReaderPid } ->
            ReaderPid ! ack,
            amqp_util:decode_method(Method, Content )
    end.

%---------------------------------------------------------------------------
% Internal plumbing
%---------------------------------------------------------------------------

network_handshake(Writer, State = #connection_state{ vhostpath = VHostPath }) ->
    #'connection.start'{version_major = MajorVersion,
                        version_minor = MinorVersion,
                        server_properties = Properties,
                        mechanisms = Mechansims,
                        locales = Locales } = recv(),
    do(Writer, start_ok(State)),
    #'connection.tune'{channel_max = ChannelMax,
                       frame_max = FrameMax,
                       heartbeat = Heartbeat} = recv(),
    TuneOk = #'connection.tune_ok'{channel_max = ChannelMax,
                                   frame_max = FrameMax,
                                   heartbeat = Heartbeat},
    do(Writer, TuneOk),

    %% This is something where I don't understand the protocol,
    %% What happens if the following command reaches the server before the tune ok?
    %% Or doesn't get sent at all?
    ConnectionOpen = #'connection.open'{virtual_host = VHostPath,
                                        capabilities = <<"">>,
                                        insist = false },
    do(Writer, ConnectionOpen),
    #'connection.open_ok'{known_hosts = KnownHosts} = recv(),
    %% TODO What should I do with the KnownHosts?
    State#connection_state{channel_max = ChannelMax, heartbeat = Heartbeat}.

start_ok(#connection_state{username = Username, password = Password}) ->
    LoginTable = [ {<<"LOGIN">>, longstr, Username },
                   {<<"PASSWORD">>, longstr, Password }],
    #'connection.start_ok'{
           client_properties = [
                            {<<"product">>, longstr, <<"Erlang-AMQC">>},
                            {<<"version">>, longstr, <<"0.1">>},
                            {<<"platform">>,longstr, <<"Erlang">>}
                           ],
           mechanism = <<"AMQPLAIN">>,
           response = rabbit_binary_generator:generate_table(LoginTable),
           locale = <<"en_US">>}.

start_reader(Sock, ConnectionPid) ->
    put({channel, 0},{chpid, ConnectionPid}),
    reader_loop(Sock, 7).

start_writer(Sock, Channel) ->
    Connection = #connection{frame_max = ?FRAME_MIN_SIZE},
    rabbit_writer:start(Sock, Channel, Connection).

reader_loop(Sock, Length) ->
    case gen_tcp:recv(Sock, Length, -1) of
    {ok, <<Type:8,Channel:16,PayloadSize:32>>} ->
        case gen_tcp:recv(Sock, PayloadSize + 1, -1) of
            {ok, <<Payload:PayloadSize/binary, ?FRAME_END>>} ->
                handle_frame(Type, Channel, Payload),
                reader_loop(Sock, 7);
            %% TODO This needs better handling
            R -> exit(R)
        end;
    R ->
        %% TODO This needs better handling
        exit(R)
    end,
    gen_tcp:close(Sock).

handle_frame(Type, Channel, Payload) ->
    case rabbit_reader:analyze_frame(Type, Payload) of
    heartbeat when Channel /= 0 ->
       rabbit_misc:die(frame_error);
    heartbeat ->
       heartbeat;
    trace when Channel /= 0 ->
       rabbit_misc:die(frame_error);
    trace ->
       trace;
    %% This terminates the reading process but still passes on the response to the user
    %% To understand the other half, look at close_connection/3
    {method,'connection.close_ok',<<>>} ->
        send_frame(Channel, {method,'connection.close_ok',<<>>}),
        io:format("Socket Reader exiting normally ~n"),
        exit(normal);
    AnalyzedFrame ->
        send_frame(Channel, AnalyzedFrame)
    end.

resolve_receiver(Channel) ->
   case get({channel, Channel}) of
       {chpid, ChPid} ->
           ChPid;
       undefined ->
            receive
                {Sender,Channel} ->
                    put({channel, Channel},{chpid, Sender}),
                    Sender
            after 1000 ->
               io:format("Could not resolve receiver from channel ~p~n",[Channel]),
               exit(unknown_channel)
            end
   end.
