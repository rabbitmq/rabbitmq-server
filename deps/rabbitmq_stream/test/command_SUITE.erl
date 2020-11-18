%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(command_SUITE).
-compile([export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_stream.hrl").


-define(COMMAND, 'Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamConnectionsCommand').

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
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodename_suffix, ?MODULE}]),
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
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
    {[<<"conn_name">>], #{verbose := false}} =
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

    StreamPort = rabbit_stream_SUITE:get_stream_port(Config),

    S1 = start_stream_connection(StreamPort),
    ct:sleep(100),

    [[{conn_name, _}]] =
            'Elixir.Enum':to_list(?COMMAND:run([<<"conn_name">>], Opts)),

    S2 = start_stream_connection(StreamPort),
    ct:sleep(100),

    [[{conn_name, _}], [{conn_name, _}]] =
        'Elixir.Enum':to_list(?COMMAND:run([<<"conn_name">>], Opts)),

    Port = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    start_amqp_connection(network, Node, Port),

    %% There are still just two connections
    [[{conn_name, _}], [{conn_name, _}]] =
        'Elixir.Enum':to_list(?COMMAND:run([<<"conn_name">>], Opts)),

    start_amqp_connection(direct, Node, Port),

    %% Still two MQTT connections, one direct AMQP 0-9-1 connection
    [[{conn_name, _}], [{conn_name, _}]] =
        'Elixir.Enum':to_list(?COMMAND:run([<<"conn_name">>], Opts)),

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
    [] = ?INFO_ITEMS -- Keys,

    rabbit_stream_SUITE:test_close(S1),
    rabbit_stream_SUITE:test_close(S2),
    ok.

start_stream_connection(Port) ->
    {ok, S} = gen_tcp:connect("localhost", Port, [{active, false},
        {mode, binary}]),
    rabbit_stream_SUITE:test_peer_properties(S),
    rabbit_stream_SUITE:test_authenticate(S),
    S.

start_amqp_connection(Type, Node, Port) ->
    Params = amqp_params(Type, Node, Port),
    {ok, _Connection} = amqp_connection:start(Params).

amqp_params(network, _, Port) ->
    #amqp_params_network{port = Port};
amqp_params(direct, Node, _) ->
    #amqp_params_direct{node = Node}.
