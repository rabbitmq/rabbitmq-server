%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.


-module(command_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_mqtt.hrl").
-import(util, [connect/3]).

-define(COMMAND, 'Elixir.RabbitMQ.CLI.Ctl.Commands.ListMqttConnectionsCommand').

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
                                merge_defaults,
                                run
                               ]}
    ].

suite() ->
    [
      {timetrap, {minutes, 3}}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE},
        {rmq_extra_tcp_ports, [tcp_port_mqtt_extra,
                               tcp_port_mqtt_tls_extra]},
        {rmq_nodes_clustered, true},
        {rmq_nodes_count, 3}
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

merge_defaults(_Config) ->
    {[<<"client_id">>, <<"conn_name">>], #{verbose := false}} =
        ?COMMAND:merge_defaults([], #{}),

    {[<<"other_key">>], #{verbose := true}} =
        ?COMMAND:merge_defaults([<<"other_key">>], #{verbose => true}),

    {[<<"other_key">>], #{verbose := false}} =
        ?COMMAND:merge_defaults([<<"other_key">>], #{verbose => false}).


run(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Opts = #{node => Node, timeout => 10_000, verbose => false},

    %% No connections
    [] = 'Elixir.Enum':to_list(?COMMAND:run([], Opts)),

    C1 = connect(<<"simpleClient">>, Config, [{ack_timeout, 1}]),

    timer:sleep(100),

    [[{client_id, <<"simpleClient">>}]] =
        'Elixir.Enum':to_list(?COMMAND:run([<<"client_id">>], Opts)),

    C2 = connect(<<"simpleClient1">>, Config, [{ack_timeout, 1}]),
    timer:sleep(200),

    [[{client_id, <<"simpleClient">>}, {user, <<"guest">>}],
     [{client_id, <<"simpleClient1">>}, {user, <<"guest">>}]] =
        lists:sort(
            'Elixir.Enum':to_list(?COMMAND:run([<<"client_id">>, <<"user">>],
                                               Opts))),

    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    start_amqp_connection(network, Node, Port),

    %% There are still just two MQTT connections
    [[{client_id, <<"simpleClient">>}],
     [{client_id, <<"simpleClient1">>}]] =
        lists:sort('Elixir.Enum':to_list(?COMMAND:run([<<"client_id">>], Opts))),

    start_amqp_connection(direct, Node, Port),
    timer:sleep(200),

    %% Still two MQTT connections
    ?assertEqual(
       [[{client_id, <<"simpleClient">>}],
        [{client_id, <<"simpleClient1">>}]],
       lists:sort('Elixir.Enum':to_list(?COMMAND:run([<<"client_id">>], Opts)))),

    %% Verbose returns all keys
    AllKeys = lists:map(fun(I) -> atom_to_binary(I) end, ?INFO_ITEMS),
    [AllInfos1Con1, _AllInfos1Con2] = 'Elixir.Enum':to_list(?COMMAND:run(AllKeys, Opts)),
    [AllInfos2Con1, _AllInfos2Con2] = 'Elixir.Enum':to_list(?COMMAND:run([], Opts#{verbose => true})),

    %% Keys are INFO_ITEMS
    InfoItemsSorted = lists:sort(?INFO_ITEMS),
    ?assertEqual(InfoItemsSorted, lists:sort(proplists:get_keys(AllInfos1Con1))),
    ?assertEqual(InfoItemsSorted, lists:sort(proplists:get_keys(AllInfos2Con1))),

    ok = emqtt:disconnect(C1),
    ok = emqtt:disconnect(C2).

start_amqp_connection(Type, Node, Port) ->
    amqp_connection:start(amqp_params(Type, Node, Port)).

amqp_params(network, _, Port) ->
    #amqp_params_network{port = Port};
amqp_params(direct, Node, _) ->
    #amqp_params_direct{node = Node}.
