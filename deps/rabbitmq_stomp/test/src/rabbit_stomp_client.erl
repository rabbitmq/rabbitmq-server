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
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developers of the Original Code are Rabbit Technologies Ltd.
%%
%%   Copyright (C) 2011 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

%% The stupidest client imaginable, just for testing.

-module(rabbit_stomp_client).

-export([connect/1, connect/2, connect/4, connect/5, disconnect/1, send/2, send/3, send/4, recv/1]).

-include("rabbit_stomp_frame.hrl").

-define(TIMEOUT, 1000). % milliseconds

connect(Port)  -> connect0([], "guest", "guest", Port, []).
connect(V, Port) -> connect0([{"accept-version", V}], "guest", "guest", Port, []).
connect(V, Login, Pass, Port) -> connect0([{"accept-version", V}], Login, Pass, Port, []).
connect(V, Login, Pass, Port, Headers) -> connect0([{"accept-version", V}], Login, Pass, Port, Headers).

connect0(Version, Login, Pass, Port, Headers) ->
    %% The default port is 61613 but it's in the middle of the ephemeral
    %% ports range on many operating systems. Therefore, there is a
    %% chance this port is already in use. Let's use a port close to the
    %% AMQP default port.
    {ok, Sock} = gen_tcp:connect(localhost, Port, [{active, false}, binary]),
    Client0 = recv_state(Sock),
    send(Client0, "CONNECT", [{"login", Login},
                              {"passcode", Pass} | Version] ++ Headers),
    {#stomp_frame{command = "CONNECTED"}, Client1} = recv(Client0),
    {ok, Client1}.

disconnect(Client = {Sock, _}) ->
    send(Client, "DISCONNECT"),
    gen_tcp:close(Sock).

send(Client, Command) ->
    send(Client, Command, []).

send(Client, Command, Headers) ->
    send(Client, Command, Headers, []).

send({Sock, _}, Command, Headers, Body) ->
    Frame = rabbit_stomp_frame:serialize(
              #stomp_frame{command     = list_to_binary(Command),
                           headers     = Headers,
                           body_iolist = Body}),
    gen_tcp:send(Sock, Frame).

recv_state(Sock) ->
    {Sock, []}.

recv({_Sock, []} = Client) ->
    recv(Client, rabbit_stomp_frame:initial_state(), 0);
recv({Sock, [Frame | Frames]}) ->
    {Frame, {Sock, Frames}}.

recv(Client = {Sock, _}, FrameState, Length) ->
    {ok, Payload} = gen_tcp:recv(Sock, Length, ?TIMEOUT),
    parse(Payload, Client, FrameState, Length).

parse(Payload, Client = {Sock, FramesRev}, FrameState, Length) ->
    case rabbit_stomp_frame:parse(Payload, FrameState) of
        {ok, Frame, <<>>} ->
            recv({Sock, lists:reverse([Frame | FramesRev])});
        {ok, Frame, <<"\n">>} ->
            recv({Sock, lists:reverse([Frame | FramesRev])});
        {ok, Frame, Rest} ->
            parse(Rest, {Sock, [Frame | FramesRev]},
                  rabbit_stomp_frame:initial_state(), Length);
        {more, NewState} ->
            recv(Client, NewState, 0)
    end.
