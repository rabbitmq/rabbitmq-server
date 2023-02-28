%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.

%% This test suite covers MQTT 5.0 features.
-module(v5_SUITE).

-compile([export_all,
          nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-import(util,
        [
         start_client/4,
         connect/2, connect/3, connect/4
        ]).

all() ->
    [{group, mqtt},
     {group, web_mqtt}].

groups() ->
    [
     {mqtt, [],
      [{cluster_size_1, [shuffle], cluster_size_1_tests()},
       {cluster_size_3, [shuffle], cluster_size_3_tests()}]},
     {web_mqtt, [],
      [{cluster_size_1, [shuffle], cluster_size_1_tests()},
       {cluster_size_3, [shuffle], cluster_size_3_tests()}]}
    ].

cluster_size_1_tests() ->
    [
     client_set_max_packet_size_publish,
     client_set_max_packet_size_connack,
     client_set_max_packet_size_invalid
    ].

cluster_size_3_tests() ->
    [
     satisfy_bazel
    ].

suite() ->
    [{timetrap, {minutes, 1}}].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(mqtt, Config) ->
    rabbit_ct_helpers:set_config(Config, {websocket, false});
init_per_group(web_mqtt, Config) ->
    rabbit_ct_helpers:set_config(Config, {websocket, true});

init_per_group(Group, Config0) ->
    Nodes = case Group of
                cluster_size_1 -> 1;
                cluster_size_3 -> 3
            end,
    Suffix = rabbit_ct_helpers:testcase_absname(Config0, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(
                Config0,
                [{mqtt_version, v5},
                 {rmq_nodes_count, Nodes},
                 {rmq_nodename_suffix, Suffix},
                 {rmq_extra_tcp_ports, [tcp_port_mqtt_extra,
                                        tcp_port_mqtt_tls_extra]}]),
    Config2 = rabbit_ct_helpers:merge_app_env(
                Config1,
                {rabbit, [{classic_queue_default_version, 2}]}),
    Config = rabbit_ct_helpers:run_steps(
               Config2,
               rabbit_ct_broker_helpers:setup_steps() ++
               rabbit_ct_client_helpers:setup_steps()),
    util:maybe_skip_v5(Config).

end_per_group(G, Config)
  when G =:= cluster_size_1;
       G =:= cluster_size_3 ->
    rabbit_ct_helpers:run_teardown_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps());
end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testsuite cases
%% -------------------------------------------------------------------

client_set_max_packet_size_publish(Config) ->
    Topic = ClientId = atom_to_binary(?FUNCTION_NAME),
    MaxPacketSize = 500,
    C = connect(ClientId, Config, [{properties, #{'Maximum-Packet-Size' => MaxPacketSize}}]),
    {ok, _, [1]} = emqtt:subscribe(C, Topic, qos1),
    PayloadTooLarge = binary:copy(<<"x">>, MaxPacketSize + 1),
    %% We expect the PUBLISH from client to server to succeed.
    ?assertMatch({ok, _}, emqtt:publish(C, Topic, PayloadTooLarge, [{qos, 1}])),
    %% We expect the server to drop the PUBLISH packet prior to sending to the client
    %% because the packet is larger than what the client is able to receive.
    receive Unexpected -> ct:fail("Unexpected message: ~p", [Unexpected])
    after 500 -> ok
    end,
    Counters = rabbit_ct_broker_helpers:rpc(Config, rabbit_global_counters, overview, []),
    M = maps:get([{queue_type, rabbit_classic_queue}, {dead_letter_strategy, disabled}], Counters),
    ?assertEqual(1, maps:get(messages_dead_lettered_rejected_total, M)),
    ok = emqtt:disconnect(C).

client_set_max_packet_size_connack(Config) ->
    {C, Connect} = start_client(?FUNCTION_NAME, Config, 0,
                                [{properties, #{'Maximum-Packet-Size' => 2}},
                                 {connect_timeout, 1}]),
    %% We expect the server to drop the CONNACK packet because it's larger than 2 bytes.
    ?assertEqual({error, connack_timeout}, Connect(C)).

%% "It is a Protocol Error to include the Receive Maximum
%% value more than once or for it to have the value 0."
client_set_max_packet_size_invalid(Config) ->
    {C, Connect} = start_client(?FUNCTION_NAME, Config, 0,
                                [{properties, #{'Maximum-Packet-Size' => 0}}]),
    unlink(C),
    ?assertMatch({error, _}, Connect(C)).

satisfy_bazel(_Config) ->
    ok.
