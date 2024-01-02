%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(system_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_stomp.hrl").
-include("rabbit_stomp_frame.hrl").
-include("rabbit_stomp_headers.hrl").

-define(QUEUE, <<"TestQueue">>).
-define(DESTINATION, "/amq/queue/TestQueue").

all() ->
    [{group, version_to_group_name(V)} || V <- ?SUPPORTED_VERSIONS].

groups() ->
    Tests = [
        publish_no_dest_error,
        publish_unauthorized_error,
        subscribe_error,
        subscribe,
        unsubscribe_ack,
        subscribe_ack,
        send,
        delete_queue_subscribe,
        temp_destination_queue,
        temp_destination_in_send,
        blank_destination_in_send,
        stream_filtering
    ],

    [{version_to_group_name(V), [sequence], Tests}
     || V <- ?SUPPORTED_VERSIONS].

version_to_group_name(V) ->
    list_to_atom(re:replace("version_" ++ V,
                            "\\.",
                            "_",
                            [global, {return, list}])).

init_per_suite(Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodename_suffix, ?MODULE}]),
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(Group, Config) ->
    Suffix = string:sub_string(atom_to_list(Group), 9),
    Version = re:replace(Suffix, "_", ".", [global, {return, list}]),
    rabbit_ct_helpers:set_config(Config, [{version, Version}]).

end_per_group(_Group, Config) -> Config.

init_per_testcase(TestCase, Config) ->
    Version = ?config(version, Config),
    StompPort = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp),
    {ok, Connection} = amqp_connection:start(#amqp_params_direct{
        node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename)
    }),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    {ok, Client} = rabbit_stomp_client:connect(Version, StompPort),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {amqp_connection, Connection},
        {amqp_channel, Channel},
        {stomp_client, Client}
      ]),
    init_per_testcase0(TestCase, Config1).

end_per_testcase(TestCase, Config) ->
    Connection = ?config(amqp_connection, Config),
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
    rabbit_stomp_client:disconnect(Client),
    amqp_channel:close(Channel),
    amqp_connection:close(Connection),
    end_per_testcase0(TestCase, Config).

init_per_testcase0(publish_unauthorized_error, Config) ->
    Channel = ?config(amqp_channel, Config),
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue       = <<"RestrictedQueue">>,
                                                    auto_delete = true}),

    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_auth_backend_internal, add_user,
                                 [<<"user">>, <<"pass">>, <<"acting-user">>]),
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_auth_backend_internal, set_permissions, [
        <<"user">>, <<"/">>, <<"nothing">>, <<"nothing">>, <<"nothing">>, <<"acting-user">>]),
    Version = ?config(version, Config),
    StompPort = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_stomp),
    {ok, ClientFoo} = rabbit_stomp_client:connect(Version, "user", "pass", StompPort),
    rabbit_ct_helpers:set_config(Config, [{client_foo, ClientFoo}]);
init_per_testcase0(stream_filtering, Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "mixed version clusters are not supported for stream filtering"};
        _ ->
            Config
    end;
init_per_testcase0(_, Config) ->
    Config.

end_per_testcase0(publish_unauthorized_error, Config) ->
    ClientFoo = ?config(client_foo, Config),
    rabbit_stomp_client:disconnect(ClientFoo),
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_auth_backend_internal, delete_user,
                                 [<<"user">>, <<"acting-user">>]),
    Config;
end_per_testcase0(_, Config) ->
    Config.

publish_no_dest_error(Config) ->
    Client = ?config(stomp_client, Config),
    rabbit_stomp_client:send(
      Client, "SEND", [{"destination", "/exchange/non-existent"}], ["hello"]),
    {ok, _Client1, Hdrs, _} = stomp_receive(Client, "ERROR"),
    "not_found" = proplists:get_value("message", Hdrs),
    ok.

publish_unauthorized_error(Config) ->
    ClientFoo = ?config(client_foo, Config),
    rabbit_stomp_client:send(
      ClientFoo, "SEND", [{"destination", "/amq/queue/RestrictedQueue"}], ["hello"]),
    {ok, _Client1, Hdrs, _} = stomp_receive(ClientFoo, "ERROR"),
    "access_refused" = proplists:get_value("message", Hdrs),
    ok.

subscribe_error(Config) ->
    Client = ?config(stomp_client, Config),
    %% SUBSCRIBE to missing queue
    rabbit_stomp_client:send(
      Client, "SUBSCRIBE", [{"destination", ?DESTINATION}]),
    {ok, _Client1, Hdrs, _} = stomp_receive(Client, "ERROR"),
    "not_found" = proplists:get_value("message", Hdrs),
    ok.

subscribe(Config) ->
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
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

unsubscribe_ack(Config) ->
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
    Version = ?config(version, Config),
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

    {ok, _Client3, Hdrs2, _Body2} = stomp_receive(Client2, "ERROR"),
    ?assertEqual("Subscription not found",
                 proplists:get_value("message", Hdrs2)),
    ok.

subscribe_ack(Config) ->
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
    Version = ?config(version, Config),
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

send(Config) ->
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
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

delete_queue_subscribe(Config) ->
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
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

temp_destination_queue(Config) ->
    Channel = ?config(amqp_channel, Config),
    Client = ?config(stomp_client, Config),
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue       = ?QUEUE,
                                                    auto_delete = true}),
    rabbit_stomp_client:send( Client, "SEND", [{"destination", ?DESTINATION},
                                               {"reply-to", "/temp-queue/foo"}],
                                              ["ping"]),
    amqp_channel:call(Channel,#'basic.consume'{queue  = ?QUEUE, no_ack = true}),
    receive #'basic.consume_ok'{consumer_tag = _Tag} -> ok end,
    ReplyTo = receive {#'basic.deliver'{delivery_tag = _DTag},
             #'amqp_msg'{payload = <<"ping">>,
                         props   = #'P_basic'{reply_to = RT}}} -> RT
    end,
    ok = amqp_channel:call(Channel,
                           #'basic.publish'{routing_key = ReplyTo},
                           #amqp_msg{payload = <<"pong">>}),
    {ok, _Client1, _, [<<"pong">>]} = stomp_receive(Client, "MESSAGE"),
    ok.

temp_destination_in_send(Config) ->
    Client = ?config(stomp_client, Config),
    rabbit_stomp_client:send( Client, "SEND", [{"destination", "/temp-queue/foo"}],
                                              ["poing"]),
    {ok, _Client1, Hdrs, _} = stomp_receive(Client, "ERROR"),
    "Invalid destination" = proplists:get_value("message", Hdrs),
    ok.

blank_destination_in_send(Config) ->
    Client = ?config(stomp_client, Config),
    rabbit_stomp_client:send( Client, "SEND", [{"destination", ""}],
                                              ["poing"]),
    {ok, _Client1, Hdrs, _} = stomp_receive(Client, "ERROR"),
    "Invalid destination" = proplists:get_value("message", Hdrs),
    ok.

stream_filtering(Config) ->
    Version = ?config(version, Config),
    Client = ?config(stomp_client, Config),
    Stream = atom_to_list(?FUNCTION_NAME) ++ "-" ++ integer_to_list(rand:uniform(10000)),
    %% subscription just to create the stream from STOMP
    SubDestination = "/topic/stream-queue-test",
    rabbit_stomp_client:send(
      Client, "SUBSCRIBE",
      [{"destination", SubDestination},
       {"receipt", "foo"},
       {"x-queue-name", Stream},
       {"x-queue-type", "stream"},
       {?HEADER_X_STREAM_FILTER_SIZE_BYTES, "32"},
       {"durable", "true"},
       {"auto-delete", "false"},
       {"id", "1234"},
       {"prefetch-count", "1"},
       {"ack", "client"}]),
    {ok, Client1, _, _} = stomp_receive(Client, "RECEIPT"),
    rabbit_stomp_client:send(
      Client1, "UNSUBSCRIBE", [{"destination", SubDestination},
                               {"id", "1234"},
                               {"receipt", "bar"}]),
    {ok, Client2, _, _} = stomp_receive(Client1, "RECEIPT"),

    %% we are going to publish several waves of messages with and without filter values.
    %% we will then create subscriptions with various filter options
    %% and make sure we receive only what we asked for and not all the messages.

    StreamDestination = "/amq/queue/" ++ Stream,
    %% logic to publish a wave of messages with or without a filter value
    WaveCount = 1000,
    Publish =
    fun(C, FilterValue) ->
            lists:foldl(fun(Seq, C0) ->
                                Headers0 = [{"destination", StreamDestination},
                                            {"receipt", integer_to_list(Seq)}],
                                Headers = case FilterValue of
                                              undefined ->
                                                  Headers0;
                                              _ ->
                                                  [{"x-stream-filter-value", FilterValue}] ++ Headers0
                                          end,
                                rabbit_stomp_client:send(
                                  C0, "SEND", Headers, ["hello"]),
                                {ok, C1, _, _} = stomp_receive(C0, "RECEIPT"),
                                C1
                        end, C, lists:seq(1, WaveCount))
    end,
    %% publishing messages with the "apple" filter value
    Client3 = Publish(Client2, "apple"),
    %% publishing messages with no filter value
    Client4 = Publish(Client3, undefined),
    %% publishing messages with the "orange" filter value
    Client5 = Publish(Client4, "orange"),

    %% filtering on "apple"
    rabbit_stomp_client:send(
      Client5, "SUBSCRIBE",
      [{"destination", StreamDestination},
       {"id", "0"},
       {"ack", "client"},
       {"prefetch-count", "1"},
       {"x-stream-filter", "apple"},
       {"x-stream-offset", "first"}]),
    {Client6, AppleMessages} = stomp_receive_messages(Client5, Version),
    %% we should get less than all the waves combined
    ?assert(length(AppleMessages) < WaveCount * 3),
    %% client-side filtering
    AppleFilteredMessages =
    lists:filter(fun(H) ->
                         proplists:get_value("x-stream-filter-value", H) =:= "apple"
                 end, AppleMessages),
    %% we should have only the "apple" messages
    ?assert(length(AppleFilteredMessages) =:= WaveCount),
    rabbit_stomp_client:send(
      Client6, "UNSUBSCRIBE", [{"destination", StreamDestination},
                               {"id", "0"},
                               {"receipt", "bar"}]),
    {ok, Client7, _, _} = stomp_receive(Client6, "RECEIPT"),

    %% filtering on "apple" and "orange"
    rabbit_stomp_client:send(
      Client7, "SUBSCRIBE",
      [{"destination", StreamDestination},
       {"id", "0"},
       {"ack", "client"},
       {"prefetch-count", "1"},
       {"x-stream-filter", "apple,orange"},
       {"x-stream-offset", "first"}]),
    {Client8, AppleOrangeMessages} = stomp_receive_messages(Client7, Version),
    %% we should get less than all the waves combined
    ?assert(length(AppleOrangeMessages) < WaveCount * 3),
    %% client-side filtering
    AppleOrangeFilteredMessages =
    lists:filter(fun(H) ->
                         proplists:get_value("x-stream-filter-value", H) =:= "apple" orelse
                         proplists:get_value("x-stream-filter-value", H) =:= "orange"
                 end, AppleOrangeMessages),
    %% we should have only the "apple" and "orange" messages
    ?assert(length(AppleOrangeFilteredMessages) =:= WaveCount * 2),
    rabbit_stomp_client:send(
      Client8, "UNSUBSCRIBE", [{"destination", StreamDestination},
                               {"id", "0"},
                               {"receipt", "bar"}]),
    {ok, Client9, _, _} = stomp_receive(Client8, "RECEIPT"),

    %% filtering on "apple" and messages without a filter value
    rabbit_stomp_client:send(
      Client9, "SUBSCRIBE",
      [{"destination", StreamDestination},
       {"id", "0"},
       {"ack", "client"},
       {"prefetch-count", "1"},
       {"x-stream-filter", "apple"},
       {"x-stream-match-unfiltered", "true"},
       {"x-stream-offset", "first"}]),
    {Client10, AppleUnfilteredMessages} = stomp_receive_messages(Client9, Version),
    %% we should get less than all the waves combined
    ?assert(length(AppleUnfilteredMessages) < WaveCount * 3),
    %% client-side filtering
    AppleUnfilteredFilteredMessages =
    lists:filter(fun(H) ->
                         proplists:get_value("x-stream-filter-value", H) =:= "apple" orelse
                         proplists:get_value("x-stream-filter-value", H) =:= undefined
                 end, AppleUnfilteredMessages),
    %% we should have only the "apple" messages and messages without a filter value
    ?assert(length(AppleUnfilteredFilteredMessages) =:= WaveCount * 2),
    rabbit_stomp_client:send(
      Client10, "UNSUBSCRIBE", [{"destination", StreamDestination},
                               {"id", "0"},
                               {"receipt", "bar"}]),
    {ok, _, _, _} = stomp_receive(Client10, "RECEIPT"),

    Channel = ?config(amqp_channel, Config),
    #'queue.delete_ok'{} = amqp_channel:call(Channel,
                                             #'queue.delete'{queue = list_to_binary(Stream)}),
    ok.

stomp_receive_messages(Client, Version) ->
    stomp_receive_messages(Client, [], Version).

stomp_receive_messages(Client, Acc, Version) ->
    try rabbit_stomp_client:recv(Client) of
        {#stomp_frame{command = "MESSAGE",
                      headers = Headers}, Client1} ->
        MsgHeader = rabbit_stomp_util:msg_header_name(Version),
        AckValue  = proplists:get_value(MsgHeader, Headers),
        AckHeader = rabbit_stomp_util:ack_header_name(Version),
        rabbit_stomp_client:send(Client1, "ACK", [{AckHeader, AckValue}]),
        stomp_receive_messages(Client1, [Headers] ++ Acc, Version)
    catch
      error:{badmatch, {error, timeout}} ->
            {Client, Acc}
    end.

stomp_receive(Client, Command) ->
    {#stomp_frame{command     = Command,
                  headers     = Hdrs,
                  body_iolist = Body},   Client1} =
    rabbit_stomp_client:recv(Client),
    {ok, Client1, Hdrs, Body}.

