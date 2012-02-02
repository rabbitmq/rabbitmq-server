%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_stomp_amqqueue_test).
-export([all_tests/0]).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_stomp_frame.hrl").

-define(QUEUE, <<"TestQueue">>).
-define(DESTINATION, "/amq/queue/TestQueue").

all_tests() ->
    [ok = run_test(TestFun) || TestFun <- [fun test_subscribe_error/2,
                                           fun test_subscribe/2,
                                           fun test_send/2]],
    ok.

run_test(TestFun) ->
    {ok, Connection} = amqp_connection:start(#amqp_params_direct{}),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    {ok, Sock} = stomp_connect(),

    Result = (catch TestFun(Channel, Sock)),

    stomp_disconnect(Sock),
    amqp_channel:close(Channel),
    amqp_connection:close(Connection),
    Result.

test_subscribe_error(_Channel, Sock) ->
    %% SUBSCRIBE to missing queue
    stomp_send(Sock, "SUBSCRIBE", [{"destination", ?DESTINATION}]),
    #stomp_frame{command = "ERROR", headers = Hdrs} = stomp_recv(Sock),
    "not_found" = proplists:get_value("message", Hdrs),
    ok.

test_subscribe(Channel, Sock) ->
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue       = ?QUEUE,
                                                    auto_delete = true}),

    %% subscribe and wait for receipt
    stomp_send(Sock, "SUBSCRIBE", [{"destination", ?DESTINATION},
                                   {"receipt", "foo"}]),
    #stomp_frame{command = "RECEIPT"} = stomp_recv(Sock),


    %% send from amqp
    Method = #'basic.publish'{
      exchange    = <<"">>,
      routing_key = ?QUEUE},

    amqp_channel:call(Channel, Method, #amqp_msg{props = #'P_basic'{},
                                                 payload = <<"hello">>}),

    #stomp_frame{command     = "MESSAGE",
                 body_iolist = [<<"hello">>]} = stomp_recv(Sock),

    ok.

test_send(Channel, Sock) ->
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue       = ?QUEUE,
                                                    auto_delete = true}),

    %% subscribe and wait for receipt
    stomp_send(Sock, "SUBSCRIBE", [{"destination", ?DESTINATION},
                                   {"receipt", "foo"}]),
    #stomp_frame{command = "RECEIPT"} = stomp_recv(Sock),


    %% send from stomp
    stomp_send(Sock, "SEND", [{"destination", ?DESTINATION}], ["hello"]),

    #stomp_frame{command     = "MESSAGE",
                 body_iolist = [<<"hello">>]} = stomp_recv(Sock),

    ok.

stomp_connect() ->
    {ok, Sock} = gen_tcp:connect(localhost, 61613, [{active, false}, binary]),
    stomp_send(Sock, "CONNECT"),
    #stomp_frame{command = "CONNECTED"} = stomp_recv(Sock),
    {ok, Sock}.

stomp_disconnect(Sock) ->
    stomp_send(Sock, "DISCONNECT").

stomp_send(Sock, Command) ->
    stomp_send(Sock, Command, []).

stomp_send(Sock, Command, Headers) ->
    stomp_send(Sock, Command, Headers, []).

stomp_send(Sock, Command, Headers, Body) ->
    gen_tcp:send(Sock, rabbit_stomp_frame:serialize(
                         #stomp_frame{command     = list_to_binary(Command),
                                      headers     = Headers,
                                      body_iolist = Body})).

stomp_recv(Sock) ->
    {ok, Payload} = gen_tcp:recv(Sock, 0),
    {ok, Frame, _Rest} =
        rabbit_stomp_frame:parse(Payload,
                                 rabbit_stomp_frame:initial_state()),
    Frame.

