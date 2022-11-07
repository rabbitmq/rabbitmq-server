%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(system_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile([export_all, nowarn_export_all]).

-import(rabbit_ct_broker_helpers, [rpc/5]).

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
        , last_will_enabled
        , disconnect
        , keepalive
        , maintenance
        ]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE}
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
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

connection(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_mqtt),
    {ok, C} = emqtt:start_link([{host, "127.0.0.1"},
                                {username, "guest"},
                                {password, "guest"},
                                {ws_path, "/ws"},
                                {port, Port}]),
    {ok, _} = emqtt:ws_connect(C),
    ok = emqtt:disconnect(C).

pubsub_shared_connection(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_mqtt),
    {ok, C} = emqtt:start_link([{host, "127.0.0.1"},
                                {username, "guest"},
                                {password, "guest"},
                                {ws_path, "/ws"},
                                {clientid, atom_to_binary(?FUNCTION_NAME)},
                                {clean_start, true},
                                {port, Port}]),
    {ok, _} = emqtt:ws_connect(C),
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
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_mqtt),
    {ok, Publisher} = emqtt:start_link([{host, "127.0.0.1"},
                                        {username, "guest"},
                                        {password, "guest"},
                                        {ws_path, "/ws"},
                                        {clientid, <<(atom_to_binary(?FUNCTION_NAME))/binary, "_publisher">>},
                                        {clean_start, true},
                                        {port, Port}]),
    {ok, _} = emqtt:ws_connect(Publisher),
    {ok, Consumer} = emqtt:start_link([{host, "127.0.0.1"},
                                       {username, "guest"},
                                       {password, "guest"},
                                       {ws_path, "/ws"},
                                       {clientid, <<(atom_to_binary(?FUNCTION_NAME))/binary, "_consumer">>},
                                       {clean_start, true},
                                       {port, Port}]),
    {ok, _} = emqtt:ws_connect(Consumer),

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

last_will_enabled(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_mqtt),
    LastWillTopic = <<"/topic/web-mqtt-tests-ws1-last-will">>,
    LastWillMsg = <<"a last will and testament message">>,
    {ok, Publisher} = emqtt:start_link([{host, "127.0.0.1"},
                                        {username, "guest"},
                                        {password, "guest"},
                                        {ws_path, "/ws"},
                                        {clientid, <<(atom_to_binary(?FUNCTION_NAME))/binary, "_publisher">>},
                                        {clean_start, true},
                                        {port, Port},
                                        {will_topic, LastWillTopic},
                                        {will_payload, LastWillMsg},
                                        {will_qos, 1}
                                       ]),
    {ok, _} = emqtt:ws_connect(Publisher),
    {ok, Consumer} = emqtt:start_link([{host, "127.0.0.1"},
                                       {username, "guest"},
                                       {password, "guest"},
                                       {ws_path, "/ws"},
                                       {clientid, <<(atom_to_binary(?FUNCTION_NAME))/binary, "_consumer">>},
                                       {clean_start, true},
                                       {port, Port}
                                      ]),
    {ok, _} = emqtt:ws_connect(Consumer),
    {ok, _, [1]} = emqtt:subscribe(Consumer, LastWillTopic, qos1),
    ok = emqtt:disconnect(Publisher),
    ok = expect_publishes(Consumer, LastWillTopic, [LastWillMsg]),
    ok = emqtt:disconnect(Consumer).

disconnect(Config) ->
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_mqtt),
    {ok, C} = emqtt:start_link([{host, "127.0.0.1"},
                                {username, "guest"},
                                {password, "guest"},
                                {ws_path, "/ws"},
                                {clientid, atom_to_binary(?FUNCTION_NAME)},
                                {clean_start, true},
                                {port, Port}]),
    process_flag(trap_exit, true),
    {ok, _} = emqtt:ws_connect(C),
    ?assertEqual(1, num_mqtt_connections(Config, 0)),
    ok = emqtt:disconnect(C),
    receive
        {'EXIT', C, normal} ->
            ok
    after 5000 ->
              ct:fail("disconnect didn't terminate client")
    end,
    ?assertEqual(0, num_mqtt_connections(Config, 0)).

keepalive(Config) ->
    KeepaliveSecs = 1,
    KeepaliveMs = timer:seconds(KeepaliveSecs),
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_mqtt),
    {ok, C} = emqtt:start_link([{keepalive, KeepaliveSecs},
                                {host, "127.0.0.1"},
                                {username, "guest"},
                                {password, "guest"},
                                {ws_path, "/ws"},
                                {clientid, atom_to_binary(?FUNCTION_NAME)},
                                {clean_start, true},
                                {port, Port}]),
    {ok, _} = emqtt:ws_connect(C),

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
    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_web_mqtt),
    {ok, C} = emqtt:start_link([{host, "127.0.0.1"},
                                {username, "guest"},
                                {password, "guest"},
                                {ws_path, "/ws"},
                                {clientid, atom_to_binary(?FUNCTION_NAME)},
                                {clean_start, true},
                                {port, Port}]),
    {ok, _} = emqtt:ws_connect(C),
    unlink(C),

    ?assertEqual(1, num_mqtt_connections(Config, 0)),
    ok = rabbit_ct_broker_helpers:drain_node(Config, 0),
    ?assertEqual(0, num_mqtt_connections(Config, 0)),
    ok = rabbit_ct_broker_helpers:revive_node(Config, 0).

%% Web mqtt connections are tracked together with mqtt connections
num_mqtt_connections(Config, Node) ->
    length(rpc(Config, Node, rabbit_mqtt, local_connection_pids, [])).

expect_publishes(_ClientPid, _Topic, []) ->
    ok;
expect_publishes(ClientPid, Topic, [Payload|Rest]) ->
    receive
        {publish, #{client_pid := ClientPid,
                    topic := Topic,
                    payload := Payload}} ->
            expect_publishes(ClientPid, Topic, Rest)
    after 5000 ->
              throw({publish_not_received, Payload})
    end.
