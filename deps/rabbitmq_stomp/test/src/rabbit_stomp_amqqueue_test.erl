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

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_stomp_frame.hrl").

-define(QUEUE, <<"TestQueue">>).
-define(DESTINATION, "/amq/queue/TestQueue").

all_tests() ->
    [ok = run_test(TestFun) || TestFun <- [fun test_subscribe_error/2,
                                           fun test_subscribe/2,
                                           fun test_unsubscribe_ack/2,
                                           fun test_send/2,
                                           fun test_delete_queue_subscribe/2]],
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
    {ok, _Client1, Hdrs, _} = stomp_receive(Client, "ERROR"),
    "not_found" = proplists:get_value("message", Hdrs),
    ok.

test_subscribe(Channel, Client) ->
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue       = ?QUEUE,
                                                    auto_delete = true}),

    %% subscribe and wait for receipt
    rabbit_stomp_client:send(
      Client, "SUBSCRIBE", [{"destination", ?DESTINATION}, {"receipt", "foo"}]),
    {ok, Client1, _, _} = stomp_receive(Client, "RECEIPT"),

    %% send from amqp
    Method = #'basic.publish'{exchange = <<"">>, routing_key = ?QUEUE},

    amqp_channel:call(Channel, Method, #amqp_msg{props = #'P_basic'{},
                                                 payload = <<"hello">>}),

    {ok, _Client2, _, [<<"hello">>]} = stomp_receive(Client1, "MESSAGE"),
    ok.

test_unsubscribe_ack(Channel, Client) ->
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue       = ?QUEUE,
                                                    auto_delete = true}),
    %% subscribe and wait for receipt
    rabbit_stomp_client:send(
      Client, "SUBSCRIBE", [{"destination", ?DESTINATION},
                            {"receipt", "rcpt1"},
                            {"ack", "client"},
                            {"id", "subscription-id"}]),
    {ok, Client1, _, _} = stomp_receive(Client, "RECEIPT"),

    %% send from amqp
    Method = #'basic.publish'{exchange = <<"">>, routing_key = ?QUEUE},

    amqp_channel:call(Channel, Method, #amqp_msg{props = #'P_basic'{},
                                                 payload = <<"hello">>}),

    {ok, Client2, Hdrs1, [<<"hello">>]} = stomp_receive(Client1, "MESSAGE"),

    rabbit_stomp_client:send(
      Client2, "UNSUBSCRIBE", [{"destination", ?DESTINATION},
                              {"id", "subscription-id"}]),

    rabbit_stomp_client:send(
      Client2, "ACK", [{"message-id", proplists:get_value("message-id", Hdrs1)},
                       {"receipt", "rcpt2"}]),

    {ok, Client3, Hdrs2, Body2} = stomp_receive(Client2, "ERROR"),
    ?assertEqual("Subscription not found",
                 proplists:get_value("message", Hdrs2)),
    ok.

test_send(Channel, Client) ->
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue       = ?QUEUE,
                                                    auto_delete = true}),

    %% subscribe and wait for receipt
    rabbit_stomp_client:send(
      Client, "SUBSCRIBE", [{"destination", ?DESTINATION}, {"receipt", "foo"}]),
    {ok, Client1, _, _} = stomp_receive(Client, "RECEIPT"),

    %% send from stomp
    rabbit_stomp_client:send(
      Client1, "SEND", [{"destination", ?DESTINATION}], ["hello"]),

    {ok, _Client2, _, [<<"hello">>]} = stomp_receive(Client1, "MESSAGE"),
    ok.

test_delete_queue_subscribe(Channel, Client) ->
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue       = ?QUEUE,
                                                    auto_delete = true}),

    %% subscribe and wait for receipt
    rabbit_stomp_client:send(
      Client, "SUBSCRIBE", [{"destination", ?DESTINATION}, {"receipt", "bah"}]),
    {ok, Client1, _, _} = stomp_receive(Client, "RECEIPT"),

    %% delete queue while subscribed
    #'queue.delete_ok'{} =
        amqp_channel:call(Channel, #'queue.delete'{queue = ?QUEUE}),

    {ok, _Client2, Headers, _} = stomp_receive(Client1, "ERROR"),

    ?DESTINATION = proplists:get_value("subscription", Headers),

    % server closes connection
    ok.

stomp_receive(Client, Command) ->
    {#stomp_frame{command     = Command,
                  headers     = Hdrs,
                  body_iolist = Body},   Client1} =
    rabbit_stomp_client:recv(Client),
    {ok, Client1, Hdrs, Body}.

