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
    {ok, Client} = rabbit_stomp_client:connect(),

    Result = (catch TestFun(Channel, Client)),

    rabbit_stomp_client:disconnect(Client),
    amqp_channel:close(Channel),
    amqp_connection:close(Connection),
    Result.

test_subscribe_error(_Channel, Client) ->
    %% SUBSCRIBE to missing queue
    rabbit_stomp_client:send(
      Client, "SUBSCRIBE", [{"destination", ?DESTINATION}]),
    {#stomp_frame{command = "ERROR",
                  headers = Hdrs}, _} = rabbit_stomp_client:recv(Client),
    "not_found" = proplists:get_value("message", Hdrs),
    ok.

test_subscribe(Channel, Client) ->
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue       = ?QUEUE,
                                                    auto_delete = true}),

    %% subscribe and wait for receipt
    rabbit_stomp_client:send(Client, "SUBSCRIBE",
                             [{"destination", ?DESTINATION},
                              {"receipt", "foo"}]),
    {#stomp_frame{command = "RECEIPT"}, Client1} =
        rabbit_stomp_client:recv(Client),

    %% send from amqp
    Method = #'basic.publish'{
      exchange    = <<"">>,
      routing_key = ?QUEUE},

    amqp_channel:call(Channel, Method, #amqp_msg{props = #'P_basic'{},
                                                 payload = <<"hello">>}),

    {#stomp_frame{command     = "MESSAGE",
                  body_iolist = [<<"hello">>]}, Client2} =
        rabbit_stomp_client:recv(Client),
    ok.

test_send(Channel, Client) ->
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue       = ?QUEUE,
                                                    auto_delete = true}),

    %% subscribe and wait for receipt
    rabbit_stomp_client:send(
      Client, "SUBSCRIBE", [{"destination", ?DESTINATION}, {"receipt", "foo"}]),
    {#stomp_frame{command = "RECEIPT"}, Client1} =
        rabbit_stomp_client:recv(Client),

    %% send from stomp
    rabbit_stomp_client:send(
      Client1, "SEND", [{"destination", ?DESTINATION}], ["hello"]),

    {#stomp_frame{command     = "MESSAGE",
                  body_iolist = [<<"hello">>]}, _Client2} =
        rabbit_stomp_client:recv(Client1),
    ok.
