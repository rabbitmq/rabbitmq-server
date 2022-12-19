%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(system_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_mgmt_test.hrl").

-compile([export_all, nowarn_export_all]).

-import(rabbit_ct_broker_helpers,
        [rpc/5]).
-import(rabbit_ct_helpers,
        [eventually/1,
         eventually/3]).
-import(rabbit_mgmt_test_util,
        [http_get/2,
         http_delete/3]).

-define(MANAGEMENT_PLUGIN_TESTS,
        [management_plugin_connection,
         management_plugin_enable]).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
     {non_parallel_tests, [],
      [connection
       , pubsub_shared_connection
       , pubsub_separate_connections
       , last_will_enabled_disconnect
       , last_will_enabled_no_disconnect
       , disconnect
       , keepalive
       , maintenance
       , client_no_supported_protocol
       , client_not_support_mqtt
       , unacceptable_data_type
       , duplicate_id
       , handle_invalid_packets
      ] ++ ?MANAGEMENT_PLUGIN_TESTS
     }
    ].

suite() ->
    [{timetrap, {minutes, 5}}].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE},
        {protocol, "ws"}
      ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    maybe_start_inets(Testcase),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    maybe_stop_inets(Testcase),
    rabbit_ct_client_helpers:close_channels_and_connection(Config, 0),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

maybe_start_inets(Testcase) ->
    case lists:member(Testcase, ?MANAGEMENT_PLUGIN_TESTS) of
        true ->
            ok = inets:start();
        false ->
            ok
    end.

maybe_stop_inets(Testcase) ->
    case lists:member(Testcase, ?MANAGEMENT_PLUGIN_TESTS) of
        true ->
            ok = inets:stop();
        false ->
            ok
    end.

connection(Config) ->
    C = ws_connect(?FUNCTION_NAME, Config),
    ok = emqtt:disconnect(C).

pubsub_shared_connection(Config) ->
    C = ws_connect(?FUNCTION_NAME, Config),

    Topic = <<"/topic/test-web-mqtt">>,
    {ok, _, [1]} = emqtt:subscribe(C, Topic, qos1),

    Payload = <<"a\x00a">>,
    ?assertMatch({ok, #{packet_id := _,
                        reason_code := 0,
                        reason_code_name := success
                       }},
                 emqtt:publish(C, Topic, Payload, [{qos, 1}])),
    ok = expect_publishes(C, Topic, [Payload]),
    ok = emqtt:disconnect(C).

pubsub_separate_connections(Config) ->
    Publisher = ws_connect(<<(atom_to_binary(?FUNCTION_NAME))/binary, "_publisher">>, Config),
    Consumer = ws_connect(<<(atom_to_binary(?FUNCTION_NAME))/binary, "_consumer">>, Config),

    Topic = <<"/topic/test-web-mqtt">>,
    {ok, _, [1]} = emqtt:subscribe(Consumer, Topic, qos1),

    Payload = <<"a\x00a">>,
    ?assertMatch({ok, #{packet_id := _,
                        reason_code := 0,
                        reason_code_name := success
                       }},
                 emqtt:publish(Publisher, Topic, Payload, [{qos, 1}])),
    ok = expect_publishes(Consumer, Topic, [Payload]),
    ok = emqtt:disconnect(Publisher),
    ok = emqtt:disconnect(Consumer).

last_will_enabled_disconnect(Config) ->
    LastWillTopic = <<"/topic/web-mqtt-tests-ws1-last-will">>,
    LastWillMsg = <<"a last will and testament message">>,
    PubOpts = [{will_topic, LastWillTopic},
               {will_payload, LastWillMsg},
               {will_qos, 1}],
    Publisher = ws_connect(<<(atom_to_binary(?FUNCTION_NAME))/binary, "_publisher">>, Config, PubOpts),
    Consumer = ws_connect(<<(atom_to_binary(?FUNCTION_NAME))/binary, "_consumer">>, Config),
    {ok, _, [1]} = emqtt:subscribe(Consumer, LastWillTopic, qos1),

    %% Client sends DISCONNECT packet. Therefore, will message should not be sent.
    ok = emqtt:disconnect(Publisher),
    ?assertEqual({publish_not_received, LastWillMsg},
                 expect_publishes(Consumer, LastWillTopic, [LastWillMsg])),

    ok = emqtt:disconnect(Consumer).

last_will_enabled_no_disconnect(Config) ->
    LastWillTopic = <<"/topic/web-mqtt-tests-ws1-last-will">>,
    LastWillMsg = <<"a last will and testament message">>,
    PubOpts = [{will_topic, LastWillTopic},
               {will_payload, LastWillMsg},
               {will_qos, 1}],
    _Publisher = ws_connect(<<(atom_to_binary(?FUNCTION_NAME))/binary, "_publisher">>, Config, PubOpts),
    timer:sleep(100),
    [ServerPublisherPid] = rpc(Config, 0, rabbit_mqtt, local_connection_pids, []),
    Consumer = ws_connect(<<(atom_to_binary(?FUNCTION_NAME))/binary, "_consumer">>, Config),
    {ok, _, [1]} = emqtt:subscribe(Consumer, LastWillTopic, qos1),

    %% Client does not send DISCONNECT packet. Therefore, will message should be sent.
    erlang:exit(ServerPublisherPid, test_will),
    ?assertEqual(ok, expect_publishes(Consumer, LastWillTopic, [LastWillMsg])),

    ok = emqtt:disconnect(Consumer).

disconnect(Config) ->
    C = ws_connect(?FUNCTION_NAME, Config),
    process_flag(trap_exit, true),
    eventually(?_assertEqual(1, num_mqtt_connections(Config, 0))),
    ok = emqtt:disconnect(C),
    receive
        {'EXIT', C, normal} ->
            ok
    after 5000 ->
              ct:fail("disconnect didn't terminate client")
    end,
    eventually(?_assertEqual(0, num_mqtt_connections(Config, 0))),
    ok.

keepalive(Config) ->
    KeepaliveSecs = 1,
    KeepaliveMs = timer:seconds(KeepaliveSecs),
    C = ws_connect(?FUNCTION_NAME, Config, [{keepalive, KeepaliveSecs}]),

    %% Connection should stay up when client sends PING requests.
    timer:sleep(KeepaliveMs),

    %% Mock the server socket to not have received any bytes.
    rabbit_ct_broker_helpers:setup_meck(Config),
    Mod = rabbit_net,
    ok = rpc(Config, 0, meck, new, [Mod, [no_link, passthrough]]),
    ok = rpc(Config, 0, meck, expect, [Mod, getstat, 2, {ok, [{recv_oct, 999}]} ]),

    process_flag(trap_exit, true),
    receive
        {'EXIT', C, _Reason} ->
            ok
    after
        ceil(3 * 0.75 * KeepaliveMs) ->
            ct:fail("server did not respect keepalive")
    end,

    true = rpc(Config, 0, meck, validate, [Mod]),
    ok = rpc(Config, 0, meck, unload, [Mod]).

maintenance(Config) ->
    C = ws_connect(?FUNCTION_NAME, Config),
    true = unlink(C),
    eventually(?_assertEqual(1, num_mqtt_connections(Config, 0))),
    ok = rabbit_ct_broker_helpers:drain_node(Config, 0),
    eventually(?_assertEqual(0, num_mqtt_connections(Config, 0))),
    ok = rabbit_ct_broker_helpers:revive_node(Config, 0).

client_no_supported_protocol(Config) ->
    client_protocol_test(Config, []).

client_not_support_mqtt(Config) ->
    client_protocol_test(Config, ["not-mqtt-protocol"]).

client_protocol_test(Config, Protocol) ->
    PortStr = rabbit_ws_test_util:get_web_mqtt_port_str(Config),
    WS = rfc6455_client:new("ws://localhost:" ++ PortStr ++ "/ws", self(), undefined, Protocol),
    {_, [{http_response, Res}]} = rfc6455_client:open(WS),
    {'HTTP/1.1', 400, <<"Bad Request">>, _} = cow_http:parse_status_line(rabbit_data_coercion:to_binary(Res)),
    rfc6455_client:send_binary(WS, rabbit_ws_test_util:mqtt_3_1_1_connect_packet()),
    {close, _} = rfc6455_client:recv(WS, timer:seconds(1)).

unacceptable_data_type(Config) ->
    PortStr = rabbit_ws_test_util:get_web_mqtt_port_str(Config),
    WS = rfc6455_client:new("ws://localhost:" ++ PortStr ++ "/ws", self(), undefined, ["mqtt"]),
    {ok, _} = rfc6455_client:open(WS),
    rfc6455_client:send(WS, "not-binary-data"),
    {close, {1003, _}} = rfc6455_client:recv(WS, timer:seconds(1)).

duplicate_id(Config) ->
    C1 = ws_connect(?FUNCTION_NAME, Config),
    eventually(?_assertEqual(1, num_mqtt_connections(Config, 0))),
    process_flag(trap_exit, true),
    C2 = ws_connect(?FUNCTION_NAME, Config),
    receive
        {'EXIT', C1, _Reason} ->
            ok
    after 5000 ->
            ct:fail("server did not disconnect a client with duplicate ID")
    end,
    eventually(?_assertEqual(1, num_mqtt_connections(Config, 0))),
    ok = emqtt:disconnect(C2).

handle_invalid_packets(Config) ->
    PortStr = rabbit_ws_test_util:get_web_mqtt_port_str(Config),
    WS = rfc6455_client:new("ws://localhost:" ++ PortStr ++ "/ws", self(), undefined, ["mqtt"]),
    {ok, _} = rfc6455_client:open(WS),
    Bin = <<"GET / HTTP/1.1\r\nHost: www.rabbitmq.com\r\nUser-Agent: curl/7.43.0\r\nAccept: */*">>,
    rfc6455_client:send_binary(WS, Bin),
    {close, {1002, _}} = rfc6455_client:recv(WS, timer:seconds(1)).

%% Test that Web MQTT connection can be listed and closed via the rabbitmq_management plugin.
management_plugin_connection(Config) ->
    KeepaliveSecs = 99,
    ClientId = atom_to_binary(?FUNCTION_NAME),
    Node = atom_to_binary(rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename)),
    C = ws_connect(ClientId, Config, [{keepalive, KeepaliveSecs}]),

    eventually(?_assertEqual(1, length(http_get(Config, "/connections"))), 1000, 10),
    [#{client_properties := #{client_id := ClientId},
       timeout := KeepaliveSecs,
       node := Node,
       name := ConnectionName}] = http_get(Config, "/connections"),

    process_flag(trap_exit, true),
    http_delete(Config,
                "/connections/" ++ binary_to_list(uri_string:quote((ConnectionName))),
                ?NO_CONTENT),
    receive
        {'EXIT', C, _} ->
            ok
    after 5000 ->
              ct:fail("server did not close connection")
    end,
    ?assertEqual([], http_get(Config, "/connections")),
    ?assertEqual(0, num_mqtt_connections(Config, 0)).

management_plugin_enable(Config) ->
    ?assertEqual(0, length(http_get(Config, "/connections"))),
    ok = rabbit_ct_broker_helpers:disable_plugin(Config, 0, rabbitmq_management),
    ok = rabbit_ct_broker_helpers:disable_plugin(Config, 0, rabbitmq_management_agent),

    %% If the Web MQTT connection is established **before** the management plugin is enabled,
    %% the management plugin should still list the Web MQTT connection.
    C = ws_connect(?FUNCTION_NAME, Config),
    ok = rabbit_ct_broker_helpers:enable_plugin(Config, 0, rabbitmq_management_agent),
    ok = rabbit_ct_broker_helpers:enable_plugin(Config, 0, rabbitmq_management),
    eventually(?_assertEqual(1, length(http_get(Config, "/connections"))), 1000, 10),

    ok = emqtt:disconnect(C).

%% Web mqtt connections are tracked together with mqtt connections
num_mqtt_connections(Config, Node) ->
    length(rpc(Config, Node, rabbit_mqtt, local_connection_pids, [])).

ws_connect(ClientId, Config) ->
    ws_connect(ClientId, Config, []).
ws_connect(ClientId, Config, AdditionalOpts) ->
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_mqtt),
    Options = [{host, "localhost"},
               {username, "guest"},
               {password, "guest"},
               {ws_path, "/ws"},
               {port, P},
               {clientid, rabbit_data_coercion:to_binary(ClientId)},
               {proto_ver, v4}
              ] ++ AdditionalOpts,
    {ok, C} = emqtt:start_link(Options),
    {ok, _Properties} = emqtt:ws_connect(C),
    C.

expect_publishes(_ClientPid, _Topic, []) ->
    ok;
expect_publishes(ClientPid, Topic, [Payload|Rest]) ->
    receive
        {publish, #{client_pid := ClientPid,
                    topic := Topic,
                    payload := Payload}} ->
            expect_publishes(ClientPid, Topic, Rest)
    after 1000 ->
              {publish_not_received, Payload}
    end.
