%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.


-module(web_mqtt_command_SUITE).
-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_mqtt/include/rabbit_mqtt.hrl").

-import(rabbit_web_mqtt_test_util,
        [connect/3, connect/4]).

-define(COMMAND, 'Elixir.RabbitMQ.CLI.Ctl.Commands.ListWebMqttConnectionsCommand').
-define(MQTT_COMMAND, 'Elixir.RabbitMQ.CLI.Ctl.Commands.ListMqttConnectionsCommand').

all() ->
    [
     {group, unit},
     {group, v5}
    ].

groups() ->
    [
     {unit, [], [merge_defaults]},
     {v5, [], [run,
               user_property]}
    ].

suite() ->
    [
      {timetrap, {minutes, 10}}
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

init_per_group(unit, Config) ->
    Config;
init_per_group(v5 = V5, Config) ->
    rabbit_ct_helpers:set_config(Config, {mqtt_version, V5}).

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


run(BaseConfig) ->
    Node = rabbit_ct_broker_helpers:get_node_config(BaseConfig, 0, nodename),
    Config = [{websocket, true} | BaseConfig],
    Opts = #{node => Node, timeout => 10_000, verbose => false},
    %% No connections
    [] = 'Elixir.Enum':to_list(?COMMAND:run([], Opts)),

    %% Open an MQTT connection
    C1 = connect(<<"simpleMqttClient">>, BaseConfig, [{ack_timeout, 1}]),
    timer:sleep(200),

    %% No connections for MQTT-over-WebSockets, C1 is an MQTT connection
    [] = 'Elixir.Enum':to_list(?COMMAND:run([<<"client_id">>], Opts)),

    %% Open a WebMQTT connection
    C2 = connect(<<"simpleWebMqttClient">>, Config, [{ack_timeout, 1}]),
    timer:sleep(200),

    %% WebMQTT CLI should list only WebMQTT connection.
    [[{client_id, <<"simpleWebMqttClient">>}]] =
    'Elixir.Enum':to_list(?COMMAND:run([<<"client_id">>], Opts)),

    %% MQTT CLI should list only MQTT connection.
    [[{client_id, <<"simpleMqttClient">>}]] =
    'Elixir.Enum':to_list(?MQTT_COMMAND:run([<<"client_id">>], Opts)),

    C3 = connect(<<"simpleWebMqttClient1">>, Config, [{ack_timeout, 1}]),
    timer:sleep(200),

    [[{client_id, <<"simpleWebMqttClient">>}, {user, <<"guest">>}],
     [{client_id, <<"simpleWebMqttClient1">>}, {user, <<"guest">>}]] =
        lists:sort(
            'Elixir.Enum':to_list(
                ?COMMAND:run([<<"client_id">>, <<"user">>], Opts))),

    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    start_amqp_connection(network, Node, Port),

    %% There are still just two Web MQTT connections
    [[{client_id, <<"simpleWebMqttClient">>}],
     [{client_id, <<"simpleWebMqttClient1">>}]] =
        lists:sort('Elixir.Enum':to_list(?COMMAND:run([<<"client_id">>], Opts))),

    start_amqp_connection(direct, Node, Port),
    timer:sleep(200),

    %% Still two Web MQTT connections
   [[{client_id, <<"simpleWebMqttClient">>}],
    [{client_id, <<"simpleWebMqttClient1">>}]] =
       lists:sort('Elixir.Enum':to_list(?COMMAND:run([<<"client_id">>], Opts))),

    %% Verbose returns all keys
    AllKeys = lists:map(fun(I) -> atom_to_binary(I) end, ?INFO_ITEMS),
    [AllInfos1Con1, _AllInfos1Con2] =
        'Elixir.Enum':to_list(?COMMAND:run(AllKeys, Opts)),
    [AllInfos2Con1, _AllInfos2Con2] = 'Elixir.Enum':to_list(?COMMAND:run([], Opts#{verbose => true})),

    %% Keys are INFO_ITEMS
    InfoItemsSorted = lists:sort(?INFO_ITEMS),
    ?assertEqual(InfoItemsSorted, lists:sort(proplists:get_keys(AllInfos1Con1))),
    ?assertEqual(InfoItemsSorted, lists:sort(proplists:get_keys(AllInfos2Con1))),

    %% List Web MQTT connections from all nodes
    C4 = connect(<<"simpleWebMqttClient2">>, Config, 1, [{ack_timeout, 1}]),
    rabbit_ct_helpers:eventually(
      ?_assertEqual(
         [[{client_id, <<"simpleWebMqttClient">>}],
          [{client_id, <<"simpleWebMqttClient1">>}],
          [{client_id, <<"simpleWebMqttClient2">>}]],
         lists:sort('Elixir.Enum':to_list(?COMMAND:run([<<"client_id">>], Opts))))),

    ok = emqtt:disconnect(C1),
    ok = emqtt:disconnect(C2),
    ok = emqtt:disconnect(C3),
    ok = emqtt:disconnect(C4).

user_property(BaseConfig) ->
    Node = rabbit_ct_broker_helpers:get_node_config(BaseConfig, 0, nodename),
    Config = [{websocket, true} | BaseConfig],
    Opts = #{node => Node, timeout => 10_000, verbose => false},
    ClientId = <<"my-client">>,
    UserProp = [{<<"name 1">>, <<"value 1">>},
                {<<"name 2">>, <<"value 2">>},
                %% "The same name is allowed to appear more than once." [v5 3.1.2.11.8]
                {<<"name 2">>, <<"value 3">>}],
    C = connect(ClientId, Config, 1, [{properties, #{'User-Property' => UserProp}}]),
    rabbit_ct_helpers:eventually(
      ?_assertEqual(
         [[{client_id, ClientId},
           {user_property, UserProp}]],
         'Elixir.Enum':to_list(?COMMAND:run([<<"client_id">>, <<"user_property">>], Opts)))),
    ok = emqtt:disconnect(C).

start_amqp_connection(Type, Node, Port) ->
    amqp_connection:start(amqp_params(Type, Node, Port)).

amqp_params(network, _, Port) ->
    #amqp_params_network{port = Port};
amqp_params(direct, Node, _) ->
    #amqp_params_direct{node = Node}.
