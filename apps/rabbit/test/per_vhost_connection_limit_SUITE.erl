%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(per_vhost_connection_limit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile(export_all).

all() ->
    [
     {group, cluster_size_1_network},
     {group, cluster_size_2_network},
     {group, cluster_size_1_direct},
     {group, cluster_size_2_direct}
    ].

groups() ->
    ClusterSize1Tests = [
        most_basic_single_node_connection_count,
        single_node_single_vhost_connection_count,
        single_node_multiple_vhosts_connection_count,
        single_node_list_in_vhost,
        single_node_single_vhost_limit,
        single_node_single_vhost_zero_limit,
        single_node_multiple_vhosts_limit,
        single_node_multiple_vhosts_zero_limit
    ],
    ClusterSize2Tests = [
        most_basic_cluster_connection_count,
        cluster_single_vhost_connection_count,
        cluster_multiple_vhosts_connection_count,
        cluster_node_restart_connection_count,
        cluster_node_list_on_node,
        cluster_single_vhost_limit,
        cluster_single_vhost_limit2,
        cluster_single_vhost_zero_limit,
        cluster_multiple_vhosts_zero_limit
    ],
    [
      {cluster_size_1_network, [], ClusterSize1Tests},
      {cluster_size_2_network, [], ClusterSize2Tests},
      {cluster_size_1_direct, [], ClusterSize1Tests},
      {cluster_size_2_direct, [], ClusterSize2Tests},
      {cluster_rename, [], [
          vhost_limit_after_node_renamed
        ]}
    ].

suite() ->
    [
      %% If a test hangs, no need to wait for 30 minutes.
      {timetrap, {minutes, 8}}
    ].

%% see partitions_SUITE
-define(DELAY, 9000).
-define(AWAIT, 1000).
-define(INTERVAL, 250).

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(cluster_size_1_network, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [{connection_type, network}]),
    init_per_multinode_group(cluster_size_1_network, Config1, 1);
init_per_group(cluster_size_2_network, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [{connection_type, network}]),
    init_per_multinode_group(cluster_size_2_network, Config1, 2);
init_per_group(cluster_size_1_direct, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [{connection_type, direct}]),
    init_per_multinode_group(cluster_size_1_direct, Config1, 1);
init_per_group(cluster_size_2_direct, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [{connection_type, direct}]),
    init_per_multinode_group(cluster_size_2_direct, Config1, 2);

init_per_group(cluster_rename, Config) ->
    init_per_multinode_group(cluster_rename, Config, 2).

init_per_multinode_group(Group, Config, NodeCount) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodes_count, NodeCount},
                                                    {rmq_nodename_suffix, Suffix}
      ]),
    case Group of
        cluster_rename ->
            % The broker is managed by {init,end}_per_testcase().
            Config1;
        _ ->
            rabbit_ct_helpers:run_steps(Config1,
              rabbit_ct_broker_helpers:setup_steps() ++
              rabbit_ct_client_helpers:setup_steps())
    end.

end_per_group(cluster_rename, Config) ->
    % The broker is managed by {init,end}_per_testcase().
    Config;
end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(vhost_limit_after_node_renamed = Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps());
init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    clear_all_connection_tracking_tables(Config),
    Config.

end_per_testcase(vhost_limit_after_node_renamed = Testcase, Config) ->
    Config1 = ?config(save_config, Config),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase);
end_per_testcase(Testcase, Config) ->
    clear_all_connection_tracking_tables(Config),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

clear_all_connection_tracking_tables(Config) ->
    rabbit_ct_broker_helpers:rpc_all(
      Config,
      rabbit_connection_tracking,
      clear_tracked_connection_tables_for_this_node,
      []).

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

most_basic_single_node_connection_count(Config) ->
    VHost = <<"/">>,
    ?assertEqual(0, count_connections_in(Config, VHost)),
    [Conn] = open_connections(Config, [0]),
    ?awaitMatch(1, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),
    close_connections([Conn]),
    ?awaitMatch(0, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL).

single_node_single_vhost_connection_count(Config) ->
    VHost = <<"/">>,
    ?assertEqual(0, count_connections_in(Config, VHost)),

    [Conn1] = open_connections(Config, [0]),
    ?awaitMatch(1, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),
    close_connections([Conn1]),
    ?awaitMatch(0, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    [Conn2] = open_connections(Config, [0]),
    ?awaitMatch(1, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    [Conn3] = open_connections(Config, [0]),
    ?awaitMatch(2, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    [Conn4] = open_connections(Config, [0]),
    ?awaitMatch(3, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    kill_connections([Conn4]),
    ?awaitMatch(2, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    [Conn5] = open_connections(Config, [0]),
    ?awaitMatch(3, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    close_connections([Conn2, Conn3, Conn5]),
    ?awaitMatch(0, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL).

single_node_multiple_vhosts_connection_count(Config) ->
    VHost1 = <<"vhost1">>,
    VHost2 = <<"vhost2">>,

    set_up_vhost(Config, VHost1),
    set_up_vhost(Config, VHost2),

    ?assertEqual(0, count_connections_in(Config, VHost1)),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

    [Conn1] = open_connections(Config, [{0, VHost1}]),
    ?awaitMatch(1, count_connections_in(Config, VHost1), ?AWAIT, ?INTERVAL),
    close_connections([Conn1]),
    ?awaitMatch(0, count_connections_in(Config, VHost1), ?AWAIT, ?INTERVAL),

    [Conn2] = open_connections(Config, [{0, VHost2}]),
    ?awaitMatch(1, count_connections_in(Config, VHost2), ?AWAIT, ?INTERVAL),

    [Conn3] = open_connections(Config, [{0, VHost1}]),
    ?awaitMatch(1, count_connections_in(Config, VHost1), ?AWAIT, ?INTERVAL),
    ?awaitMatch(1, count_connections_in(Config, VHost2), ?AWAIT, ?INTERVAL),

    [Conn4] = open_connections(Config, [{0, VHost1}]),
    ?awaitMatch(2, count_connections_in(Config, VHost1), ?AWAIT, ?INTERVAL),

    kill_connections([Conn4]),
    ?awaitMatch(1, count_connections_in(Config, VHost1), ?AWAIT, ?INTERVAL),

    [Conn5] = open_connections(Config, [{0, VHost2}]),
    ?awaitMatch(2, count_connections_in(Config, VHost2), ?AWAIT, ?INTERVAL),

    [Conn6] = open_connections(Config, [{0, VHost2}]),
    ?awaitMatch(3, count_connections_in(Config, VHost2), ?AWAIT, ?INTERVAL),

    close_connections([Conn2, Conn3, Conn5, Conn6]),
    ?awaitMatch(0, count_connections_in(Config, VHost1), ?AWAIT, ?INTERVAL),
    ?awaitMatch(0, count_connections_in(Config, VHost2), ?AWAIT, ?INTERVAL),

    rabbit_ct_broker_helpers:delete_vhost(Config, VHost1),
    rabbit_ct_broker_helpers:delete_vhost(Config, VHost2).

single_node_list_in_vhost(Config) ->
    VHost1 = <<"vhost1">>,
    VHost2 = <<"vhost2">>,

    set_up_vhost(Config, VHost1),
    set_up_vhost(Config, VHost2),

    ?assertEqual(0, length(connections_in(Config, VHost1))),
    ?assertEqual(0, length(connections_in(Config, VHost2))),

    [Conn1] = open_connections(Config, [{0, VHost1}]),
    ?awaitMatch([#tracked_connection{vhost = VHost1}],
                connections_in(Config, VHost1),
                ?AWAIT, ?INTERVAL),
    close_connections([Conn1]),
    ?awaitMatch(Connections when length(Connections) == 0,
                                 connections_in(Config, VHost1),
                                 ?AWAIT, ?INTERVAL),

    [Conn2] = open_connections(Config, [{0, VHost2}]),
    ?awaitMatch([#tracked_connection{vhost = VHost2}],
                connections_in(Config, VHost2),
                ?AWAIT, ?INTERVAL),

    [Conn3] = open_connections(Config, [{0, VHost1}]),
    ?awaitMatch([#tracked_connection{vhost = VHost1}],
                connections_in(Config, VHost1),
                ?AWAIT, ?INTERVAL),

    [Conn4] = open_connections(Config, [{0, VHost1}]),
    ?awaitMatch([#tracked_connection{vhost = VHost1},
                 #tracked_connection{vhost = VHost1}],
                connections_in(Config, VHost1),
                ?AWAIT, ?INTERVAL),
    kill_connections([Conn4]),
    ?awaitMatch([#tracked_connection{vhost = VHost1}],
                connections_in(Config, VHost1),
                ?AWAIT, ?INTERVAL),

    [Conn5, Conn6] = open_connections(Config, [{0, VHost2}, {0, VHost2}]),
    ?awaitMatch([<<"vhost1">>, <<"vhost2">>],
                lists:usort(lists:map(fun (#tracked_connection{vhost = V}) -> V end,
                                      all_connections(Config))),
                ?AWAIT, ?INTERVAL),

    close_connections([Conn2, Conn3, Conn5, Conn6]),
    ?awaitMatch(0, length(all_connections(Config)), ?AWAIT, ?INTERVAL),

    rabbit_ct_broker_helpers:delete_vhost(Config, VHost1),
    rabbit_ct_broker_helpers:delete_vhost(Config, VHost2).

most_basic_cluster_connection_count(Config) ->
    VHost = <<"/">>,
    ?assertEqual(0, count_connections_in(Config, VHost)),
    [Conn1] = open_connections(Config, [0]),
    ?awaitMatch(1, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    [Conn2] = open_connections(Config, [1]),
    ?awaitMatch(2, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    [Conn3] = open_connections(Config, [1]),
    ?awaitMatch(3, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    close_connections([Conn1, Conn2, Conn3]),
    ?awaitMatch(0, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL).

cluster_single_vhost_connection_count(Config) ->
    VHost = <<"/">>,
    ?assertEqual(0, count_connections_in(Config, VHost)),

    [Conn1] = open_connections(Config, [0]),
    ?awaitMatch(1, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),
    close_connections([Conn1]),
    ?awaitMatch(0, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    [Conn2] = open_connections(Config, [1]),
    ?awaitMatch(1, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    [Conn3] = open_connections(Config, [0]),
    ?awaitMatch(2, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    [Conn4] = open_connections(Config, [1]),
    ?awaitMatch(3, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    kill_connections([Conn4]),
    ?awaitMatch(2, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    [Conn5] = open_connections(Config, [1]),
    ?awaitMatch(3, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    close_connections([Conn2, Conn3, Conn5]),
    ?awaitMatch(0, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL).

cluster_multiple_vhosts_connection_count(Config) ->
    VHost1 = <<"vhost1">>,
    VHost2 = <<"vhost2">>,

    set_up_vhost(Config, VHost1),
    set_up_vhost(Config, VHost2),

    ?assertEqual(0, count_connections_in(Config, VHost1)),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

    [Conn1] = open_connections(Config, [{0, VHost1}]),
    ?awaitMatch(1, count_connections_in(Config, VHost1), ?AWAIT, ?INTERVAL),
    close_connections([Conn1]),
    ?awaitMatch(0, count_connections_in(Config, VHost1), ?AWAIT, ?INTERVAL),

    [Conn2] = open_connections(Config, [{1, VHost2}]),
    ?awaitMatch(1, count_connections_in(Config, VHost2), ?AWAIT, ?INTERVAL),

    [Conn3] = open_connections(Config, [{1, VHost1}]),
    ?awaitMatch(1, count_connections_in(Config, VHost1), ?AWAIT, ?INTERVAL),
    ?awaitMatch(1, count_connections_in(Config, VHost2), ?AWAIT, ?INTERVAL),

    [Conn4] = open_connections(Config, [{0, VHost1}]),
    ?awaitMatch(2, count_connections_in(Config, VHost1), ?AWAIT, ?INTERVAL),

    kill_connections([Conn4]),
    ?awaitMatch(1, count_connections_in(Config, VHost1), ?AWAIT, ?INTERVAL),

    [Conn5] = open_connections(Config, [{1, VHost2}]),
    ?awaitMatch(2, count_connections_in(Config, VHost2), ?AWAIT, ?INTERVAL),

    [Conn6] = open_connections(Config, [{0, VHost2}]),
    ?awaitMatch(3, count_connections_in(Config, VHost2), ?AWAIT, ?INTERVAL),

    close_connections([Conn2, Conn3, Conn5, Conn6]),
    ?awaitMatch(0, count_connections_in(Config, VHost1), ?AWAIT, ?INTERVAL),
    ?awaitMatch(0, count_connections_in(Config, VHost2), ?AWAIT, ?INTERVAL),

    rabbit_ct_broker_helpers:delete_vhost(Config, VHost1),
    rabbit_ct_broker_helpers:delete_vhost(Config, VHost2).

cluster_node_restart_connection_count(Config) ->
    VHost = <<"/">>,
    ?assertEqual(0, count_connections_in(Config, VHost)),

    [Conn1] = open_connections(Config, [0]),
    ?awaitMatch(1, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),
    close_connections([Conn1]),
    ?awaitMatch(0, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    [Conn2] = open_connections(Config, [1]),
    ?awaitMatch(1, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    [Conn3] = open_connections(Config, [0]),
    ?awaitMatch(2, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    [Conn4] = open_connections(Config, [1]),
    ?awaitMatch(3, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    [Conn5] = open_connections(Config, [1]),
    ?awaitMatch(4, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    rabbit_ct_broker_helpers:restart_broker(Config, 1),
    ?awaitMatch(1, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    close_connections([Conn2, Conn3, Conn4, Conn5]),
    ?awaitMatch(0, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL).

cluster_node_list_on_node(Config) ->
    [A, B] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ?assertEqual(0, length(all_connections(Config))),
    ?assertEqual(0, length(connections_on_node(Config, 0))),

    [Conn1] = open_connections(Config, [0]),
    ?awaitMatch([#tracked_connection{node = A}],
                connections_on_node(Config, 0),
                ?AWAIT, ?INTERVAL),
    close_connections([Conn1]),
    ?awaitMatch(0, length(connections_on_node(Config, 0)), ?AWAIT, ?INTERVAL),

    [_Conn2] = open_connections(Config, [1]),
    ?awaitMatch([#tracked_connection{node = B}],
                connections_on_node(Config, 1),
                ?AWAIT, ?INTERVAL),

    [Conn3] = open_connections(Config, [0]),
    ?awaitMatch(1, length(connections_on_node(Config, 0)), ?AWAIT, ?INTERVAL),

    [Conn4] = open_connections(Config, [1]),
    ?awaitMatch(2, length(connections_on_node(Config, 1)), ?AWAIT, ?INTERVAL),

    kill_connections([Conn4]),
    ?awaitMatch(1, length(connections_on_node(Config, 1)), ?AWAIT, ?INTERVAL),

    [Conn5] = open_connections(Config, [0]),
    ?awaitMatch(2, length(connections_on_node(Config, 0)), ?AWAIT, ?INTERVAL),

    rabbit_ct_broker_helpers:stop_broker(Config, 1),

    ?awaitMatch(2, length(all_connections(Config)), 1000),
    ?assertEqual(0, length(connections_on_node(Config, 0, B))),

    close_connections([Conn3, Conn5]),
    ?awaitMatch(0, length(all_connections(Config, 0)), ?AWAIT, ?INTERVAL),

    rabbit_ct_broker_helpers:start_broker(Config, 1).

single_node_single_vhost_limit(Config) ->
    single_node_single_vhost_limit_with(Config, 5),
    single_node_single_vhost_limit_with(Config, -1).

single_node_single_vhost_limit_with(Config, WatermarkLimit) ->
    VHost = <<"/">>,
    set_vhost_connection_limit(Config, VHost, 3),

    ?assertEqual(0, count_connections_in(Config, VHost)),

    [Conn1, Conn2, Conn3] = open_connections(Config, [0, 0, 0]),

    %% we've crossed the limit
    expect_that_client_connection_is_rejected(Config, 0),
    expect_that_client_connection_is_rejected(Config, 0),
    expect_that_client_connection_is_rejected(Config, 0),

    set_vhost_connection_limit(Config, VHost, WatermarkLimit),
    [Conn4, Conn5] = open_connections(Config, [0, 0]),
    ?awaitMatch(5, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    close_connections([Conn1, Conn2, Conn3, Conn4, Conn5]),
    ?awaitMatch(0, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    set_vhost_connection_limit(Config, VHost,  -1).

single_node_single_vhost_zero_limit(Config) ->
    VHost = <<"/">>,
    set_vhost_connection_limit(Config, VHost, 0),

    ?assertEqual(0, count_connections_in(Config, VHost)),

    %% with limit = 0 no connections are allowed
    expect_that_client_connection_is_rejected(Config),
    expect_that_client_connection_is_rejected(Config),
    expect_that_client_connection_is_rejected(Config),

    set_vhost_connection_limit(Config, VHost, -1),
    [Conn1, Conn2] = open_connections(Config, [0, 0]),
    ?awaitMatch(2, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    close_connections([Conn1, Conn2]),
    ?awaitMatch(0, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL).


single_node_multiple_vhosts_limit(Config) ->
    VHost1 = <<"vhost1">>,
    VHost2 = <<"vhost2">>,

    set_up_vhost(Config, VHost1),
    set_up_vhost(Config, VHost2),

    set_vhost_connection_limit(Config, VHost1, 2),
    set_vhost_connection_limit(Config, VHost2, 2),

    ?awaitMatch(0, count_connections_in(Config, VHost1), ?AWAIT, ?INTERVAL),
    ?awaitMatch(0, count_connections_in(Config, VHost2), ?AWAIT, ?INTERVAL),

    [Conn1, Conn2, Conn3, Conn4] = open_connections(Config, [
        {0, VHost1},
        {0, VHost1},
        {0, VHost2},
        {0, VHost2}]),

    %% we've crossed the limit
    expect_that_client_connection_is_rejected(Config, 0, VHost1),
    expect_that_client_connection_is_rejected(Config, 0, VHost2),

    [Conn5] = open_connections(Config, [0]),
    ?awaitMatch(Conns when length(Conns) == 5,
                           connections_on_node(Config, 0),
                           ?AWAIT, ?INTERVAL),

    set_vhost_connection_limit(Config, VHost1, 5),
    set_vhost_connection_limit(Config, VHost2, -10),

    [Conn6, Conn7, Conn8, Conn9, Conn10] = open_connections(Config, [
        {0, VHost1},
        {0, VHost1},
        {0, VHost1},
        {0, VHost2},
        {0, VHost2}]),
    ?awaitMatch(Conns when length(Conns) == 10,
                           connections_on_node(Config, 0),
                           ?AWAIT, ?INTERVAL),

    close_connections([Conn1, Conn2, Conn3, Conn4, Conn5,
                       Conn6, Conn7, Conn8, Conn9, Conn10]),
    ?awaitMatch(0, count_connections_in(Config, VHost1), ?AWAIT, ?INTERVAL),
    ?awaitMatch(0, count_connections_in(Config, VHost2), ?AWAIT, ?INTERVAL),

    set_vhost_connection_limit(Config, VHost1, -1),
    set_vhost_connection_limit(Config, VHost2, -1),

    rabbit_ct_broker_helpers:delete_vhost(Config, VHost1),
    rabbit_ct_broker_helpers:delete_vhost(Config, VHost2).


single_node_multiple_vhosts_zero_limit(Config) ->
    VHost1 = <<"vhost1">>,
    VHost2 = <<"vhost2">>,

    set_up_vhost(Config, VHost1),
    set_up_vhost(Config, VHost2),

    set_vhost_connection_limit(Config, VHost1, 0),
    set_vhost_connection_limit(Config, VHost2, 0),

    ?awaitMatch(0, count_connections_in(Config, VHost1), ?AWAIT, ?INTERVAL),
    ?awaitMatch(0, count_connections_in(Config, VHost2), ?AWAIT, ?INTERVAL),

    %% with limit = 0 no connections are allowed
    expect_that_client_connection_is_rejected(Config, 0, VHost1),
    expect_that_client_connection_is_rejected(Config, 0, VHost2),
    expect_that_client_connection_is_rejected(Config, 0, VHost1),

    set_vhost_connection_limit(Config, VHost1, -1),
    [Conn1, Conn2] = open_connections(Config, [{0, VHost1}, {0, VHost1}]),
    ?awaitMatch(2, count_connections_in(Config, VHost1), ?AWAIT, ?INTERVAL),

    close_connections([Conn1, Conn2]),
    ?awaitMatch(0, count_connections_in(Config, VHost1), ?AWAIT, ?INTERVAL),
    ?awaitMatch(0, count_connections_in(Config, VHost2), ?AWAIT, ?INTERVAL),

    set_vhost_connection_limit(Config, VHost1, -1),
    set_vhost_connection_limit(Config, VHost2, -1).


cluster_single_vhost_limit(Config) ->
    VHost = <<"/">>,
    set_vhost_connection_limit(Config, VHost, 2),

    ?assertEqual(0, count_connections_in(Config, VHost)),

    %% here connections are opened to different nodes
    [Conn1, Conn2] = open_connections(Config, [{0, VHost}, {1, VHost}]),
    ?awaitMatch(2, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    %% we've crossed the limit
    expect_that_client_connection_is_rejected(Config, 0, VHost),
    expect_that_client_connection_is_rejected(Config, 1, VHost),

    set_vhost_connection_limit(Config, VHost, 5),

    [Conn3, Conn4] = open_connections(Config, [{0, VHost}, {0, VHost}]),
    ?awaitMatch(4, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    close_connections([Conn1, Conn2, Conn3, Conn4]),
    ?awaitMatch(0, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    set_vhost_connection_limit(Config, VHost,  -1).

cluster_single_vhost_limit2(Config) ->
    VHost = <<"/">>,
    set_vhost_connection_limit(Config, VHost, 2),

    ?assertEqual(0, count_connections_in(Config, VHost)),

    %% here a limit is reached on one node first
    [Conn1, Conn2] = open_connections(Config, [{0, VHost}, {0, VHost}]),
    ?awaitMatch(2, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    %% we've crossed the limit
    expect_that_client_connection_is_rejected(Config, 0, VHost),
    expect_that_client_connection_is_rejected(Config, 1, VHost),

    set_vhost_connection_limit(Config, VHost, 5),

    [Conn3, Conn4, Conn5, {error, not_allowed}] = open_connections(Config, [
        {1, VHost},
        {1, VHost},
        {1, VHost},
        {1, VHost}]),
    ?awaitMatch(5, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    close_connections([Conn1, Conn2, Conn3, Conn4, Conn5]),
    ?awaitMatch(0, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    set_vhost_connection_limit(Config, VHost,  -1).


cluster_single_vhost_zero_limit(Config) ->
    VHost = <<"/">>,
    set_vhost_connection_limit(Config, VHost, 0),

    ?assertEqual(0, count_connections_in(Config, VHost)),

    %% with limit = 0 no connections are allowed
    expect_that_client_connection_is_rejected(Config, 0),
    expect_that_client_connection_is_rejected(Config, 1),
    expect_that_client_connection_is_rejected(Config, 0),

    set_vhost_connection_limit(Config, VHost, -1),
    [Conn1, Conn2, Conn3, Conn4] = open_connections(Config, [0, 1, 0, 1]),
    ?awaitMatch(4, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    close_connections([Conn1, Conn2, Conn3, Conn4]),
    ?awaitMatch(0, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),

    set_vhost_connection_limit(Config, VHost, -1).


cluster_multiple_vhosts_zero_limit(Config) ->
    VHost1 = <<"vhost1">>,
    VHost2 = <<"vhost2">>,

    set_up_vhost(Config, VHost1),
    set_up_vhost(Config, VHost2),

    set_vhost_connection_limit(Config, VHost1, 0),
    set_vhost_connection_limit(Config, VHost2, 0),

    ?assertEqual(0, count_connections_in(Config, VHost1)),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

    %% with limit = 0 no connections are allowed
    expect_that_client_connection_is_rejected(Config, 0, VHost1),
    expect_that_client_connection_is_rejected(Config, 0, VHost2),
    expect_that_client_connection_is_rejected(Config, 1, VHost1),
    expect_that_client_connection_is_rejected(Config, 1, VHost2),

    set_vhost_connection_limit(Config, VHost1, -1),
    set_vhost_connection_limit(Config, VHost2, -1),

    [Conn1, Conn2, Conn3, Conn4] = open_connections(Config, [
        {0, VHost1},
        {0, VHost2},
        {1, VHost1},
        {1, VHost2}]),
    ?awaitMatch(2, count_connections_in(Config, VHost1), ?AWAIT, ?INTERVAL),
    ?awaitMatch(2, count_connections_in(Config, VHost2), ?AWAIT, ?INTERVAL),

    close_connections([Conn1, Conn2, Conn3, Conn4]),
    ?awaitMatch(0, count_connections_in(Config, VHost1), ?AWAIT, ?INTERVAL),
    ?awaitMatch(0, count_connections_in(Config, VHost2), ?AWAIT, ?INTERVAL),

    set_vhost_connection_limit(Config, VHost1, -1),
    set_vhost_connection_limit(Config, VHost2, -1).

vhost_limit_after_node_renamed(Config) ->
    A = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    VHost = <<"/renaming_node">>,
    set_up_vhost(Config, VHost),
    set_vhost_connection_limit(Config, VHost, 2),

    ?assertEqual(0, count_connections_in(Config, VHost)),

    [Conn1, Conn2, {error, not_allowed}] = open_connections(Config,
      [{0, VHost}, {1, VHost}, {0, VHost}]),
    ?awaitMatch(2, count_connections_in(Config, VHost), ?AWAIT, ?INTERVAL),
    close_connections([Conn1, Conn2]),

    Config1 = cluster_rename_SUITE:stop_rename_start(Config, A, [A, 'new-A']),

    ?awaitMatch(0, count_connections_in(Config1, VHost), ?AWAIT, ?INTERVAL),

    [Conn3, Conn4, {error, not_allowed}] = open_connections(Config1,
      [{0, VHost}, {1, VHost}, {0, VHost}]),
    ?awaitMatch(2, count_connections_in(Config1, VHost), ?AWAIT, ?INTERVAL),
    close_connections([Conn3, Conn4]),

    set_vhost_connection_limit(Config1, VHost,  -1),
    {save_config, Config1}.

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

open_connections(Config, NodesAndVHosts) ->
    % Randomly select connection type
    OpenConnectionFun = case ?config(connection_type, Config) of
        network -> open_unmanaged_connection;
        direct  -> open_unmanaged_connection_direct
    end,
    Conns = lists:map(fun
      ({Node, VHost}) ->
          rabbit_ct_client_helpers:OpenConnectionFun(Config, Node,
            VHost);
      (Node) ->
          rabbit_ct_client_helpers:OpenConnectionFun(Config, Node)
      end, NodesAndVHosts),
    Conns.

close_connections(Conns) ->
    lists:foreach(fun
      (Conn) ->
          rabbit_ct_client_helpers:close_connection(Conn)
      end, Conns).

kill_connections(Conns) ->
    lists:foreach(fun
      (Conn) ->
          (catch exit(Conn, please_terminate))
      end, Conns).

count_connections_in(Config, VHost) ->
    count_connections_in(Config, VHost, 0).
count_connections_in(Config, VHost, NodeIndex) ->
    timer:sleep(200),
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                 rabbit_connection_tracking,
                                 count_tracked_items_in, [{vhost, VHost}]).

connections_in(Config, VHost) ->
    connections_in(Config, 0, VHost).
connections_in(Config, NodeIndex, VHost) ->
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                 rabbit_connection_tracking,
                                 list, [VHost]).

connections_on_node(Config) ->
    connections_on_node(Config, 0).
connections_on_node(Config, NodeIndex) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, NodeIndex, nodename),
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                 rabbit_connection_tracking,
                                 list_on_node, [Node]).
connections_on_node(Config, NodeIndex, NodeForListing) ->
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                 rabbit_connection_tracking,
                                 list_on_node, [NodeForListing]).

all_connections(Config) ->
    all_connections(Config, 0).
all_connections(Config, NodeIndex) ->
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                 rabbit_connection_tracking,
                                 list, []).

set_up_vhost(Config, VHost) ->
    rabbit_ct_broker_helpers:add_vhost(Config, VHost),
    rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VHost),
    set_vhost_connection_limit(Config, VHost, -1).

set_vhost_connection_limit(Config, VHost, Count) ->
    set_vhost_connection_limit(Config, 0, VHost, Count).

set_vhost_connection_limit(Config, NodeIndex, VHost, Count) ->
    Node  = rabbit_ct_broker_helpers:get_node_config(
              Config, NodeIndex, nodename),
    ok = rabbit_ct_broker_helpers:control_action(
      set_vhost_limits, Node,
      ["{\"max-connections\": " ++ integer_to_list(Count) ++ "}"],
      [{"-p", binary_to_list(VHost)}]).

expect_that_client_connection_is_rejected(Config) ->
    expect_that_client_connection_is_rejected(Config, 0).

expect_that_client_connection_is_rejected(Config, NodeIndex) ->
    {error, not_allowed} =
      rabbit_ct_client_helpers:open_unmanaged_connection(Config, NodeIndex).

expect_that_client_connection_is_rejected(Config, NodeIndex, VHost) ->
    {error, not_allowed} =
      rabbit_ct_client_helpers:open_unmanaged_connection(Config, NodeIndex, VHost).
