%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(commands_SUITE).

-compile(nowarn_export_all).
-compile([export_all]).

% -include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-include("rabbit_stream.hrl").

-define(COMMAND_LIST_CONNECTIONS,
        'Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamConnectionsCommand').
-define(COMMAND_LIST_CONSUMERS,
        'Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamConsumersCommand').
-define(COMMAND_LIST_PUBLISHERS,
        'Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamPublishersCommand').

all() ->
    [{group, list_connections}, {group, list_consumers},
     {group, list_publishers}].

groups() ->
    [{list_connections, [],
      [list_connections_merge_defaults, list_connections_run]},
     {list_consumers, [],
      [list_consumers_merge_defaults, list_consumers_run]},
     {list_publishers, [],
      [list_publishers_merge_defaults, list_publishers_run]}].

init_per_suite(Config) ->
    Config1 =
        rabbit_ct_helpers:set_config(Config,
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

list_connections_merge_defaults(_Config) ->
    {[<<"conn_name">>], #{verbose := false}} =
        ?COMMAND_LIST_CONNECTIONS:merge_defaults([], #{}),

    {[<<"other_key">>], #{verbose := true}} =
        ?COMMAND_LIST_CONNECTIONS:merge_defaults([<<"other_key">>],
                                                 #{verbose => true}),

    {[<<"other_key">>], #{verbose := false}} =
        ?COMMAND_LIST_CONNECTIONS:merge_defaults([<<"other_key">>],
                                                 #{verbose => false}).

list_connections_run(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Opts =
        #{node => Node,
          timeout => 10000,
          verbose => false},

    %% No connections
    [] = to_list(?COMMAND_LIST_CONNECTIONS:run([], Opts)),

    StreamPort = rabbit_stream_SUITE:get_stream_port(Config),

    {S1, C1} = start_stream_connection(StreamPort),
    wait_until_connection_count(Config, 1),

    [[{conn_name, _}]] =
        to_list(?COMMAND_LIST_CONNECTIONS:run([<<"conn_name">>], Opts)),

    {S2, C2} = start_stream_connection(StreamPort),
    wait_until_connection_count(Config, 2),

    [[{conn_name, _}], [{conn_name, _}]] =
        to_list(?COMMAND_LIST_CONNECTIONS:run([<<"conn_name">>], Opts)),

    Port =
        rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    start_amqp_connection(network, Node, Port),

    %% There are still just two connections
    [[{conn_name, _}], [{conn_name, _}]] =
        to_list(?COMMAND_LIST_CONNECTIONS:run([<<"conn_name">>], Opts)),

    start_amqp_connection(direct, Node, Port),

    %% Still two stream connections, one direct AMQP 0-9-1 connection
    [[{conn_name, _}], [{conn_name, _}]] =
        to_list(?COMMAND_LIST_CONNECTIONS:run([<<"conn_name">>], Opts)),

    %% Verbose returns all keys
    Infos =
        lists:map(fun(El) -> atom_to_binary(El, utf8) end, ?INFO_ITEMS),
    AllKeys = to_list(?COMMAND_LIST_CONNECTIONS:run(Infos, Opts)),
    Verbose =
        to_list(?COMMAND_LIST_CONNECTIONS:run([], Opts#{verbose => true})),
    ?assertEqual(AllKeys, Verbose),

    %% There are two connections
    [First, _Second] = AllKeys,

    %% Keys are INFO_ITEMS
    ?assertEqual(length(?INFO_ITEMS), length(First)),

    {Keys, _} = lists:unzip(First),

    ?assertEqual([], Keys -- ?INFO_ITEMS),
    ?assertEqual([], ?INFO_ITEMS -- Keys),

    rabbit_stream_SUITE:test_close(S1, C1),
    rabbit_stream_SUITE:test_close(S2, C2),
    ok.

list_consumers_merge_defaults(_Config) ->
    DefaultItems =
        [rabbit_data_coercion:to_binary(Item)
         || Item <- ?CONSUMER_INFO_ITEMS],
    {DefaultItems, #{verbose := false}} =
        ?COMMAND_LIST_CONSUMERS:merge_defaults([], #{}),

    {[<<"other_key">>], #{verbose := true}} =
        ?COMMAND_LIST_CONSUMERS:merge_defaults([<<"other_key">>],
                                               #{verbose => true}),

    {[<<"other_key">>], #{verbose := false}} =
        ?COMMAND_LIST_CONSUMERS:merge_defaults([<<"other_key">>],
                                               #{verbose => false}).

list_consumers_run(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Opts =
        #{node => Node,
          timeout => 10000,
          verbose => false,
          vhost => <<"/">>},

    %% No connections, no consumers
    [] = to_list(?COMMAND_LIST_CONSUMERS:run([], Opts)),

    StreamPort = rabbit_stream_SUITE:get_stream_port(Config),
    {S1, C1} = start_stream_connection(StreamPort),
    wait_until_connection_count(Config, 1),

    Stream = <<"list_consumers_run">>,
    C1_1 = create_stream(S1, Stream, C1),
    SubId = 42,
    C1_2 = subscribe(S1, SubId, Stream, C1_1),

    wait_until_consumer_count(Config, 1),

    {S2, C2} = start_stream_connection(StreamPort),
    wait_until_connection_count(Config, 2),
    C2_1 = subscribe(S2, SubId, Stream, C2),

    wait_until_consumer_count(Config, 2),

    %% Verbose returns all keys
    InfoItems = ?CONSUMER_INFO_ITEMS,
    Infos = lists:map(fun(El) -> atom_to_binary(El, utf8) end, InfoItems),
    AllKeys = to_list(?COMMAND_LIST_CONSUMERS:run(Infos, Opts)),
    Verbose =
        to_list(?COMMAND_LIST_CONSUMERS:run([], Opts#{verbose => true})),
    ?assertEqual(AllKeys, Verbose),
    %% There are two consumers
    [[First], [_Second]] = AllKeys,

    %% Keys are info items
    ?assertEqual(length(InfoItems), length(First)),

    {Keys, _} = lists:unzip(First),

    ?assertEqual([], Keys -- InfoItems),
    ?assertEqual([], InfoItems -- Keys),

    C1_3 = delete_stream(S1, Stream, C1_2),
    % metadata_update_stream_deleted(S1, Stream),
    metadata_update_stream_deleted(S2, Stream, C2_1),
    close(S1, C1_3),
    close(S2, C2_1),
    wait_until_consumer_count(Config, 0),
    ok.

list_publishers_merge_defaults(_Config) ->
    DefaultItems =
        [rabbit_data_coercion:to_binary(Item)
         || Item <- ?PUBLISHER_INFO_ITEMS],
    {DefaultItems, #{verbose := false}} =
        ?COMMAND_LIST_PUBLISHERS:merge_defaults([], #{}),

    {[<<"other_key">>], #{verbose := true}} =
        ?COMMAND_LIST_PUBLISHERS:merge_defaults([<<"other_key">>],
                                                #{verbose => true}),

    {[<<"other_key">>], #{verbose := false}} =
        ?COMMAND_LIST_PUBLISHERS:merge_defaults([<<"other_key">>],
                                                #{verbose => false}).

list_publishers_run(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Opts =
        #{node => Node,
          timeout => 10000,
          verbose => false,
          vhost => <<"/">>},

    %% No connections, no publishers
    [] = to_list(?COMMAND_LIST_PUBLISHERS:run([], Opts)),

    StreamPort = rabbit_stream_SUITE:get_stream_port(Config),
    {S1, C1} = start_stream_connection(StreamPort),
    wait_until_connection_count(Config, 1),

    Stream = <<"list_publishers_run">>,
    C1_1 = create_stream(S1, Stream, C1),
    PubId = 42,
    C1_2 = declare_publisher(S1, PubId, Stream, C1_1),

    wait_until_publisher_count(Config, 1),

    {S2, C2} = start_stream_connection(StreamPort),
    wait_until_connection_count(Config, 2),
    C2_1 = declare_publisher(S2, PubId, Stream, C2),

    wait_until_publisher_count(Config, 2),

    %% Verbose returns all keys
    InfoItems = ?PUBLISHER_INFO_ITEMS,
    Infos = lists:map(fun(El) -> atom_to_binary(El, utf8) end, InfoItems),
    AllKeys = to_list(?COMMAND_LIST_PUBLISHERS:run(Infos, Opts)),
    Verbose =
        to_list(?COMMAND_LIST_PUBLISHERS:run([], Opts#{verbose => true})),
    ?assertEqual(AllKeys, Verbose),
    %% There are two consumers
    [[First], [_Second]] = AllKeys,

    %% Keys are info items
    ?assertEqual(length(InfoItems), length(First)),

    {Keys, _} = lists:unzip(First),

    ?assertEqual([], Keys -- InfoItems),
    ?assertEqual([], InfoItems -- Keys),

    C1_3 = delete_stream(S1, Stream, C1_2),
    % metadata_update_stream_deleted(S1, Stream),
    C2_2 = metadata_update_stream_deleted(S2, Stream, C2_1),
    close(S1, C1_3),
    close(S2, C2_2),
    wait_until_publisher_count(Config, 0),
    ok.

create_stream(S, Stream, C0) ->
    rabbit_stream_SUITE:test_create_stream(S, Stream, C0).

subscribe(S, SubId, Stream, C) ->
    rabbit_stream_SUITE:test_subscribe(S, SubId, Stream, C).

declare_publisher(S, PubId, Stream, C) ->
    rabbit_stream_SUITE:test_declare_publisher(S, PubId, Stream, C).

delete_stream(S, Stream, C) ->
    rabbit_stream_SUITE:test_delete_stream(S, Stream, C).

metadata_update_stream_deleted(S, Stream, C) ->
    rabbit_stream_SUITE:test_metadata_update_stream_deleted(S, Stream, C).

close(S, C) ->
    rabbit_stream_SUITE:test_close(S, C).

options(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Opts =
        #{node => Node,
          timeout => 10000,
          verbose => false,
          vhost => <<"/">>}, %% just for list_consumers and list_publishers
    Opts.

to_list(CommandRun) ->
    'Elixir.Enum':to_list(CommandRun).

command_result_count(CommandRun) ->
    length(to_list(CommandRun)).

connection_count(Config) ->
    command_result_count(?COMMAND_LIST_CONNECTIONS:run([<<"conn_name">>],
                                                       options(Config))).

wait_until_connection_count(Config, Expected) ->
    ok =
        test_utils:wait_until(fun() -> connection_count(Config) == Expected
                              end).

consumer_count(Config) ->
    command_result_count(?COMMAND_LIST_CONSUMERS:run([<<"stream">>],
                                                     options(Config))).

wait_until_consumer_count(Config, Expected) ->
    ok =
        test_utils:wait_until(fun() -> consumer_count(Config) == Expected
                              end).

publisher_count(Config) ->
    command_result_count(?COMMAND_LIST_PUBLISHERS:run([<<"stream">>],
                                                      options(Config))).

wait_until_publisher_count(Config, Expected) ->
    ok =
        test_utils:wait_until(fun() -> publisher_count(Config) == Expected
                              end).

start_stream_connection(Port) ->
    {ok, S} =
        gen_tcp:connect("localhost", Port, [{active, false}, {mode, binary}]),
    C0 = rabbit_stream_core:init(0),
    C1 = rabbit_stream_SUITE:test_peer_properties(S, C0),
    C = rabbit_stream_SUITE:test_authenticate(S, C1),
    {S, C}.

start_amqp_connection(Type, Node, Port) ->
    Params = amqp_params(Type, Node, Port),
    {ok, _Connection} = amqp_connection:start(Params).

amqp_params(network, _, Port) ->
    #amqp_params_network{port = Port};
amqp_params(direct, Node, _) ->
    #amqp_params_direct{node = Node}.
