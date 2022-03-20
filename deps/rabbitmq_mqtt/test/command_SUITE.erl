%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.


-module(command_SUITE).
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_mqtt.hrl").


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
    Opts = #{node => Node, timeout => 10000, verbose => false},

    %% No connections
    [] = 'Elixir.Enum':to_list(?COMMAND:run([], Opts)),

    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_mqtt),
    {ok, _} = emqttc:start_link([{host, "localhost"},
                                 {port, P},
                                 {client_id, <<"simpleClient">>},
                                 {proto_ver, 3},
                                 {logger, info},
                                 {puback_timeout, 1}]),
    ct:sleep(100),

    [[{client_id, <<"simpleClient">>}]] =
            'Elixir.Enum':to_list(?COMMAND:run([<<"client_id">>], Opts)),


    {ok, _} = emqttc:start_link([{host, "localhost"},
                                  {port, P},
                                  {client_id, <<"simpleClient1">>},
                                  {proto_ver, 3},
                                  {logger, info},
                                  {username, <<"guest">>},
                                  {password, <<"guest">>},
                                  {puback_timeout, 1}]),
    ct:sleep(200),

    [[{client_id, <<"simpleClient">>}, {user, <<"guest">>}],
     [{client_id, <<"simpleClient1">>}, {user, <<"guest">>}]] =
        lists:sort(
            'Elixir.Enum':to_list(?COMMAND:run([<<"client_id">>, <<"user">>],
                                               Opts))),

    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    start_amqp_connection(network, Node, Port),

    %% There are still just two connections
    [[{client_id, <<"simpleClient">>}],
     [{client_id, <<"simpleClient1">>}]] =
        lists:sort('Elixir.Enum':to_list(?COMMAND:run([<<"client_id">>], Opts))),

    start_amqp_connection(direct, Node, Port),
    ct:sleep(200),

    %% Still two MQTT connections, one direct AMQP 0-9-1 connection
    [[{client_id, <<"simpleClient">>}],
     [{client_id, <<"simpleClient1">>}]] =
        lists:sort('Elixir.Enum':to_list(?COMMAND:run([<<"client_id">>], Opts))),

    %% Verbose returns all keys
    Infos = lists:map(fun(El) -> atom_to_binary(El, utf8) end, ?INFO_ITEMS),
    AllKeys1 = 'Elixir.Enum':to_list(?COMMAND:run(Infos, Opts)),
    AllKeys2 = 'Elixir.Enum':to_list(?COMMAND:run([], Opts#{verbose => true})),

    %% There are two connections
    [FirstPL, _]  = AllKeys1,
    [SecondPL, _] = AllKeys2,

    First         = maps:from_list(lists:usort(FirstPL)),
    Second        = maps:from_list(lists:usort(SecondPL)),

    %% Keys are INFO_ITEMS
    KeysCount = length(?INFO_ITEMS),
    ?assert(KeysCount =:= maps:size(First)),
    ?assert(KeysCount =:= maps:size(Second)),

    Keys = maps:keys(First),

    [] = Keys -- ?INFO_ITEMS,
    [] = ?INFO_ITEMS -- Keys.


start_amqp_connection(Type, Node, Port) ->
    amqp_connection:start(amqp_params(Type, Node, Port)).

amqp_params(network, _, Port) ->
    #amqp_params_network{port = Port};
amqp_params(direct, Node, _) ->
    #amqp_params_direct{node = Node}.



