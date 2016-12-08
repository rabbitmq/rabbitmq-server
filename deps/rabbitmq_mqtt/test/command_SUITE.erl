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
%% Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.


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
                               tcp_port_mqtt_tls_extra]}
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
    ct:sleep(100),

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

    %% Still two MQTT connections, one direct AMQP 0-9-1 connection
    [[{client_id, <<"simpleClient">>}],
     [{client_id, <<"simpleClient1">>}]] =
        lists:sort('Elixir.Enum':to_list(?COMMAND:run([<<"client_id">>], Opts))),

    %% Verbose returns all keys
    Infos = lists:map(fun(El) -> atom_to_binary(El, utf8) end, ?INFO_ITEMS),
    AllKeys = 'Elixir.Enum':to_list(?COMMAND:run(Infos, Opts)),
    AllKeys = 'Elixir.Enum':to_list(?COMMAND:run([], Opts#{verbose => true})),

    %% There are two connections
    [First, _Second] = AllKeys,

    %% Keys are INFO_ITEMS
    KeysCount = length(?INFO_ITEMS),
    KeysCount = length(First),

    {Keys, _} = lists:unzip(First),

    [] = Keys -- ?INFO_ITEMS,
    [] = ?INFO_ITEMS -- Keys.


start_amqp_connection(Type, Node, Port) ->
    Params = amqp_params(Type, Node, Port),
    {ok, Connection} = amqp_connection:start(Params).

amqp_params(network, _, Port) ->
    #amqp_params_network{port = Port};
amqp_params(direct, Node, _) ->
    #amqp_params_direct{node = Node}.



