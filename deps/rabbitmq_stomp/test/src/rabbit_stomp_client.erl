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

-export([connect/0, disconnect/1, send/2, send/3, send/4, recv/1]).

-include("rabbit_stomp_frame.hrl").

connect() ->
    {ok, Sock} = gen_tcp:connect(localhost, 61613, [{active, false}, binary]),
    send(Sock, "CONNECT"),
    #stomp_frame{command = "CONNECTED"} = recv(Sock),
    {ok, Sock}.

disconnect(Sock) ->
    send(Sock, "DISCONNECT").

send(Sock, Command) ->
    send(Sock, Command, []).

send(Sock, Command, Headers) ->
    send(Sock, Command, Headers, []).

send(Sock, Command, Headers, Body) ->
    Frame = rabbit_stomp_frame:serialize(
              #stomp_frame{command     = list_to_binary(Command),
                           headers     = Headers,
                           body_iolist = Body}),
    gen_tcp:send(Sock, Frame).

recv(Sock) ->
    {ok, Payload} = gen_tcp:recv(Sock, 0),
    {ok, Frame, _Rest} =
        rabbit_stomp_frame:parse(Payload,
                                 rabbit_stomp_frame:initial_state()),
    Frame.

