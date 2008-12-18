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

-export([handshake/1, open_channel/3, close_channel/1, close_connection/3]).
-export([start_reader/2, start_writer/2]).
-export([do/2,do/3]).

%---------------------------------------------------------------------------
% Driver API Methods
%---------------------------------------------------------------------------

handshake(ConnectionState = #connection_state{serverhost = Host}) ->
    case gen_tcp:connect(Host, 5672, [binary, {packet, 0},{active,false}]) of
        {ok, Sock} ->
            ok = rabbit_net:send(Sock, amqp_util:protocol_header()),
            Parent = self(),
            FramingPid = rabbit_framing_channel:start_link(fun(X) -> X end, [Parent]),
            ReaderPid = spawn_link(?MODULE, start_reader, [Sock, FramingPid]),
            WriterPid = start_writer(Sock, 0),
            ConnectionState1 = ConnectionState#connection_state{channel0_writer_pid = WriterPid,
                                                                reader_pid = ReaderPid,
                                                                sock = Sock},
            ConnectionState2 = network_handshake(WriterPid, ConnectionState1),
            #connection_state{heartbeat = Heartbeat} = ConnectionState2,
            ReaderPid ! {heartbeat, Heartbeat},
            ConnectionState2;
        {error, Reason} ->
            io:format("Could not start the network driver: ~p~n",[Reason]),
            exit(Reason)
    end.

%% The reader runs unaware of the channel number that it is bound to
%% because this will be parsed out of the frames received off the socket.
%% Hence, you have tell the singelton reader which Pids are intended to
%% process messages for a particular channel
open_channel({ChannelNumber, _OutOfBand}, ChannelPid,
             #connection_state{reader_pid = ReaderPid,
                               sock = Sock}) ->
    ReaderPid ! {ChannelPid, ChannelNumber},
    WriterPid = start_writer(Sock, ChannelNumber),
    amqp_channel:register_direct_peer(ChannelPid, WriterPid ).

close_channel(WriterPid) ->
    %io:format("Shutting the channel writer ~p down~n",[WriterPid]),
    rabbit_writer:shutdown(WriterPid).

%% This closes the writer down, waits for the confirmation from the
%% the channel and then returns the ack to the user
close_connection(Close = #'connection.close'{}, From,
                 #connection_state{channel0_writer_pid = Writer, reader_pid = Reader}) ->
    rabbit_writer:send_command(Writer, Close),
    rabbit_writer:shutdown(Writer),
    receive
        {method, {'connection.close_ok'}, none } ->
            gen_server:reply(From, #'connection.close_ok'{})
    after
        5000 ->
            exit(timeout_on_exit)
    end,
    Reader ! close.

do(Writer, Method) -> rabbit_writer:send_command(Writer, Method).
do(Writer, Method, Content) -> rabbit_writer:send_command(Writer, Method, Content).

%---------------------------------------------------------------------------
% AMQP message sending and receiving
%---------------------------------------------------------------------------

send_frame(Channel, Frame) ->
    ChPid = resolve_receiver(Channel),
    rabbit_framing_channel:process(ChPid, Frame).

recv() ->
    receive
        {method, Method, _Content} ->
            Method
    end.

%---------------------------------------------------------------------------
% Internal plumbing
%---------------------------------------------------------------------------

network_handshake(Writer, State = #connection_state{ vhostpath = VHostPath }) ->
    #'connection.start'{} = recv(),
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
    #'connection.open_ok'{} = recv(),
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

start_reader(Sock, FramingPid) ->
    process_flag(trap_exit, true),
    put({channel, 0},{chpid, FramingPid}),
    {ok, _Ref} = rabbit_net:async_recv(Sock, 7, -1),
    reader_loop(Sock, undefined, undefined, undefined),
    rabbit_net:close(Sock).

start_writer(Sock, Channel) ->
    rabbit_writer:start(Sock, Channel, ?FRAME_MIN_SIZE).

reader_loop(Sock, Type, Channel, Length) ->
    receive
        {inet_async, Sock, _, {ok, <<Payload:Length/binary,?FRAME_END>>} } ->
            case handle_frame(Type, Channel, Payload) of
                closed_ok ->
                    ok;
                _ ->
                    {ok, _Ref} = rabbit_net:async_recv(Sock, 7, -1),
                    reader_loop(Sock, undefined, undefined, undefined)
            end;
        {inet_async, Sock, _, {ok, <<_Type:8,_Channel:16,PayloadSize:32>>}} ->
            {ok, _Ref} = rabbit_net:async_recv(Sock, PayloadSize + 1, -1),
            reader_loop(Sock, _Type, _Channel, PayloadSize);
        {inet_async, Sock, _Ref, {error, Reason}} ->
            io:format("Socket error: ~p~n", [Reason]),
            exit({socket_error, Reason});
        {heartbeat, Heartbeat} ->
            rabbit_heartbeat:start_heartbeat(Sock, Heartbeat),
            reader_loop(Sock, Type, Channel, Length);
        {ChannelPid, ChannelNumber} ->
            start_framing_channel(ChannelPid, ChannelNumber),
            reader_loop(Sock, Type, Channel, Length);
        timeout ->
            io:format("Reader (~p) received timeout from heartbeat, exiting ~n",[self()]);
        close ->
            io:format("Reader (~p) received close command, exiting ~n",[self()]);
        {'EXIT', Pid, _Reason} ->
            [H|_] = get_keys({chpid,Pid}),
            erase(H),
            reader_loop(Sock, Type, Channel, Length);
        Other ->
            io:format("Unknown message type: ~p~n", [Other]),
            exit({unknown_message_type, Other})
    end.

start_framing_channel(ChannelPid, ChannelNumber) ->
    FramingPid = rabbit_framing_channel:start_link(fun(X) -> link(X), X end, [ChannelPid]),
    put({channel, ChannelNumber},{chpid, FramingPid}).

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
        {method,'connection.close_ok',Content} ->
            send_frame(Channel, {method,'connection.close_ok',Content}),
            closed_ok;
        AnalyzedFrame ->
            send_frame(Channel, AnalyzedFrame)
    end.

resolve_receiver(Channel) ->
   case get({channel, Channel}) of
       {chpid, ChPid} ->
           ChPid;
       undefined ->
            exit(unknown_channel)
   end.
