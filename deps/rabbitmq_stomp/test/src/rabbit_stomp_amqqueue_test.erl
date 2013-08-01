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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_stomp_amqqueue_test).
-export([all_tests/0]).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_stomp.hrl").
-include("rabbit_stomp_frame.hrl").
-include("rabbit_stomp_headers.hrl").

-define(QUEUE, <<"TestQueue">>).
-define(DESTINATION, "/amq/queue/TestQueue").

all_tests() ->
    [[ok = run_test(TestFun, Version)
      || TestFun <- [fun test_subscribe_error/3,
                     fun test_subscribe/3,
                     fun test_unsubscribe_ack/3,
                     fun test_subscribe_ack/3,
                     fun test_send/3,
                     fun test_delete_queue_subscribe/3,
                     fun test_temp_destination_queue/3,
                     fun test_temp_destination_in_send/3]]
     || Version <- ?SUPPORTED_VERSIONS],
    ok.

run_test(TestFun, Version) ->
    {ok, Connection} = amqp_connection:start(#amqp_params_direct{}),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    {ok, Client} = rabbit_stomp_client:connect(Version),

    Result = (catch TestFun(Channel, Client, Version)),

    rabbit_stomp_client:disconnect(Client),
    amqp_channel:close(Channel),
    amqp_connection:close(Connection),
    Result.

test_subscribe_error(_Channel, Client, _Version) ->
    %% SUBSCRIBE to missing queue
    rabbit_stomp_client:send(
      Client, "SUBSCRIBE", [{"destination", ?DESTINATION}]),
    {ok, _Client1, Hdrs, _} = stomp_receive(Client, "ERROR"),
    "not_found" = proplists:get_value("message", Hdrs),
    ok.

test_subscribe(Channel, Client, _Version) ->
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

test_unsubscribe_ack(Channel, Client, Version) ->
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
      Client2, "ACK", [{rabbit_stomp_util:ack_header_name(Version),
                        proplists:get_value(
                          rabbit_stomp_util:msg_header_name(Version), Hdrs1)},
                       {"receipt", "rcpt2"}]),

    {ok, Client3, Hdrs2, Body2} = stomp_receive(Client2, "ERROR"),
    ?assertEqual("Subscription not found",
                 proplists:get_value("message", Hdrs2)),
    ok.

test_subscribe_ack(Channel, Client, Version) ->
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue       = ?QUEUE,
                                                    auto_delete = true}),

    %% subscribe and wait for receipt
    rabbit_stomp_client:send(
      Client, "SUBSCRIBE", [{"destination", ?DESTINATION},
                            {"receipt",     "foo"},
                            {"ack",         "client"}]),
    {ok, Client1, _, _} = stomp_receive(Client, "RECEIPT"),

    %% send from amqp
    Method = #'basic.publish'{exchange = <<"">>, routing_key = ?QUEUE},

    amqp_channel:call(Channel, Method, #amqp_msg{props = #'P_basic'{},
                                                 payload = <<"hello">>}),

    {ok, _Client2, Headers, [<<"hello">>]} = stomp_receive(Client1, "MESSAGE"),
    false = (Version == "1.2") xor proplists:is_defined(?HEADER_ACK, Headers),

    MsgHeader = rabbit_stomp_util:msg_header_name(Version),
    AckValue  = proplists:get_value(MsgHeader, Headers),
    AckHeader = rabbit_stomp_util:ack_header_name(Version),

    rabbit_stomp_client:send(Client, "ACK", [{AckHeader, AckValue}]),
    #'basic.get_empty'{} =
        amqp_channel:call(Channel, #'basic.get'{queue = ?QUEUE}),
    ok.

test_send(Channel, Client, _Version) ->
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

test_delete_queue_subscribe(Channel, Client, _Version) ->
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

test_temp_destination_queue(Channel, Client, _Version) ->
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue       = ?QUEUE,
                                                    auto_delete = true}),
    rabbit_stomp_client:send( Client, "SEND", [{"destination", ?DESTINATION},
                                               {"reply-to", "/temp-queue/foo"}],
                                              ["ping"]),
    amqp_channel:call(Channel,#'basic.consume'{queue  = ?QUEUE, no_ack = true}),
    receive #'basic.consume_ok'{consumer_tag = Tag} -> ok end,
    receive {#'basic.deliver'{delivery_tag = DTag},
             #'amqp_msg'{payload = <<"ping">>,
                         props   = #'P_basic'{reply_to = ReplyTo}}} -> ok
    end,
    ok = amqp_channel:call(Channel,
                           #'basic.publish'{routing_key = ReplyTo},
                           #amqp_msg{payload = <<"pong">>}),
    {ok, _Client1, _, [<<"pong">>]} = stomp_receive(Client, "MESSAGE"),
    ok.

test_temp_destination_in_send(Channel, Client, _Version) ->
    rabbit_stomp_client:send( Client, "SEND", [{"destination", "/temp-queue/foo"}],
                                              ["poing"]),
    {ok, _Client1, Hdrs, _} = stomp_receive(Client, "ERROR"),
    "Invalid destination" = proplists:get_value("message", Hdrs),
    ok.

stomp_receive(Client, Command) ->
    {#stomp_frame{command     = Command,
                  headers     = Hdrs,
                  body_iolist = Body},   Client1} =
    rabbit_stomp_client:recv(Client),
    {ok, Client1, Hdrs, Body}.

