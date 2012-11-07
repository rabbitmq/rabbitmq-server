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

-export([connect/0, connect/1, disconnect/1, send/2, send/3, send/4, recv/1]).

-include("rabbit_stomp_frame.hrl").

-define(TIMEOUT, 1000). % milliseconds

connect()  -> connect0([]).
connect(V) -> connect0([{"accept-version", V}]).

connect0(Version) ->
    {ok, Sock} = gen_tcp:connect(localhost, 61613, [{active, false}, binary]),
    Client0 = recv_state(Sock),
    send(Client0, "CONNECT", [{"login", "guest"},
                              {"passcode", "guest"} | Version]),
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
        {ok, Frame, Rest} ->
            parse(Rest, {Sock, [Frame | FramesRev]},
                  rabbit_stomp_frame:initial_state(), Length);
        {more, NewState} ->
            recv(Client, NewState, 0)
    end.
