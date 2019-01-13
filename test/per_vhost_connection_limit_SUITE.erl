%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(per_vhost_connection_limit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

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

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, [
                                               fun rabbit_ct_broker_helpers:enable_dist_proxy_manager/1
                                              ]).

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
    [rabbit_ct_broker_helpers:rpc(Config,
        N,
        rabbit_connection_tracking,
        clear_tracked_connection_tables_for_this_node,
        []) || N <- rabbit_ct_broker_helpers:get_node_configs(Config, nodename)].

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

most_basic_single_node_connection_count(Config) ->
    VHost = <<"/">>,
    ?assertEqual(0, count_connections_in(Config, VHost)),
    [Conn] = open_connections(Config, [0]),
    ?assertEqual(1, count_connections_in(Config, VHost)),
    close_connections([Conn]),
    ?assertEqual(0, count_connections_in(Config, VHost)).

single_node_single_vhost_connection_count(Config) ->
    VHost = <<"/">>,
    ?assertEqual(0, count_connections_in(Config, VHost)),

    [Conn1] = open_connections(Config, [0]),
    ?assertEqual(1, count_connections_in(Config, VHost)),
    close_connections([Conn1]),
    ?assertEqual(0, count_connections_in(Config, VHost)),

    [Conn2] = open_connections(Config, [0]),
    ?assertEqual(1, count_connections_in(Config, VHost)),

    [Conn3] = open_connections(Config, [0]),
    ?assertEqual(2, count_connections_in(Config, VHost)),

    [Conn4] = open_connections(Config, [0]),
    ?assertEqual(3, count_connections_in(Config, VHost)),

    kill_connections([Conn4]),
    ?assertEqual(2, count_connections_in(Config, VHost)),

    [Conn5] = open_connections(Config, [0]),
    ?assertEqual(3, count_connections_in(Config, VHost)),

    close_connections([Conn2, Conn3, Conn5]),
    ?assertEqual(0, count_connections_in(Config, VHost)).

single_node_multiple_vhosts_connection_count(Config) ->
    VHost1 = <<"vhost1">>,
    VHost2 = <<"vhost2">>,

    set_up_vhost(Config, VHost1),
    set_up_vhost(Config, VHost2),

    ?assertEqual(0, count_connections_in(Config, VHost1)),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

    [Conn1] = open_connections(Config, [{0, VHost1}]),
    ?assertEqual(1, count_connections_in(Config, VHost1)),
    close_connections([Conn1]),
    ?assertEqual(0, count_connections_in(Config, VHost1)),

    [Conn2] = open_connections(Config, [{0, VHost2}]),
    ?assertEqual(1, count_connections_in(Config, VHost2)),

    [Conn3] = open_connections(Config, [{0, VHost1}]),
    ?assertEqual(1, count_connections_in(Config, VHost1)),
    ?assertEqual(1, count_connections_in(Config, VHost2)),

    [Conn4] = open_connections(Config, [{0, VHost1}]),
    ?assertEqual(2, count_connections_in(Config, VHost1)),

    kill_connections([Conn4]),
    ?assertEqual(1, count_connections_in(Config, VHost1)),

    [Conn5] = open_connections(Config, [{0, VHost2}]),
    ?assertEqual(2, count_connections_in(Config, VHost2)),

    [Conn6] = open_connections(Config, [{0, VHost2}]),
    ?assertEqual(3, count_connections_in(Config, VHost2)),

    close_connections([Conn2, Conn3, Conn5, Conn6]),
    ?assertEqual(0, count_connections_in(Config, VHost1)),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

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
    [#tracked_connection{vhost = VHost1}] = connections_in(Config, VHost1),
    close_connections([Conn1]),
    ?assertEqual(0, length(connections_in(Config, VHost1))),

    [Conn2] = open_connections(Config, [{0, VHost2}]),
    [#tracked_connection{vhost = VHost2}] = connections_in(Config, VHost2),

    [Conn3] = open_connections(Config, [{0, VHost1}]),
    [#tracked_connection{vhost = VHost1}] = connections_in(Config, VHost1),

    [Conn4] = open_connections(Config, [{0, VHost1}]),
    kill_connections([Conn4]),
    [#tracked_connection{vhost = VHost1}] = connections_in(Config, VHost1),

    [Conn5, Conn6] = open_connections(Config, [{0, VHost2}, {0, VHost2}]),
    [<<"vhost1">>, <<"vhost2">>] =
      lists:usort(lists:map(fun (#tracked_connection{vhost = V}) -> V end,
                     all_connections(Config))),

    close_connections([Conn2, Conn3, Conn5, Conn6]),
    ?assertEqual(0, length(all_connections(Config))),

    rabbit_ct_broker_helpers:delete_vhost(Config, VHost1),
    rabbit_ct_broker_helpers:delete_vhost(Config, VHost2).

most_basic_cluster_connection_count(Config) ->
    VHost = <<"/">>,
    ?assertEqual(0, count_connections_in(Config, VHost)),
    [Conn1] = open_connections(Config, [0]),
    ?assertEqual(1, count_connections_in(Config, VHost)),

    [Conn2] = open_connections(Config, [1]),
    ?assertEqual(2, count_connections_in(Config, VHost)),

    [Conn3] = open_connections(Config, [1]),
    ?assertEqual(3, count_connections_in(Config, VHost)),

    close_connections([Conn1, Conn2, Conn3]),
    ?assertEqual(0, count_connections_in(Config, VHost)).

cluster_single_vhost_connection_count(Config) ->
    VHost = <<"/">>,
    ?assertEqual(0, count_connections_in(Config, VHost)),

    [Conn1] = open_connections(Config, [0]),
    ?assertEqual(1, count_connections_in(Config, VHost)),
    close_connections([Conn1]),
    ?assertEqual(0, count_connections_in(Config, VHost)),

    [Conn2] = open_connections(Config, [1]),
    ?assertEqual(1, count_connections_in(Config, VHost)),

    [Conn3] = open_connections(Config, [0]),
    ?assertEqual(2, count_connections_in(Config, VHost)),

    [Conn4] = open_connections(Config, [1]),
    ?assertEqual(3, count_connections_in(Config, VHost)),

    kill_connections([Conn4]),
    ?assertEqual(2, count_connections_in(Config, VHost)),

    [Conn5] = open_connections(Config, [1]),
    ?assertEqual(3, count_connections_in(Config, VHost)),

    close_connections([Conn2, Conn3, Conn5]),
    ?assertEqual(0, count_connections_in(Config, VHost)).

cluster_multiple_vhosts_connection_count(Config) ->
    VHost1 = <<"vhost1">>,
    VHost2 = <<"vhost2">>,

    set_up_vhost(Config, VHost1),
    set_up_vhost(Config, VHost2),

    ?assertEqual(0, count_connections_in(Config, VHost1)),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

    [Conn1] = open_connections(Config, [{0, VHost1}]),
    ?assertEqual(1, count_connections_in(Config, VHost1)),
    close_connections([Conn1]),
    ?assertEqual(0, count_connections_in(Config, VHost1)),

    [Conn2] = open_connections(Config, [{1, VHost2}]),
    ?assertEqual(1, count_connections_in(Config, VHost2)),

    [Conn3] = open_connections(Config, [{1, VHost1}]),
    ?assertEqual(1, count_connections_in(Config, VHost1)),
    ?assertEqual(1, count_connections_in(Config, VHost2)),

    [Conn4] = open_connections(Config, [{0, VHost1}]),
    ?assertEqual(2, count_connections_in(Config, VHost1)),

    kill_connections([Conn4]),
    ?assertEqual(1, count_connections_in(Config, VHost1)),

    [Conn5] = open_connections(Config, [{1, VHost2}]),
    ?assertEqual(2, count_connections_in(Config, VHost2)),

    [Conn6] = open_connections(Config, [{0, VHost2}]),
    ?assertEqual(3, count_connections_in(Config, VHost2)),

    close_connections([Conn2, Conn3, Conn5, Conn6]),
    ?assertEqual(0, count_connections_in(Config, VHost1)),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

    rabbit_ct_broker_helpers:delete_vhost(Config, VHost1),
    rabbit_ct_broker_helpers:delete_vhost(Config, VHost2).

cluster_node_restart_connection_count(Config) ->
    VHost = <<"/">>,
    ?assertEqual(0, count_connections_in(Config, VHost)),

    [Conn1] = open_connections(Config, [0]),
    ?assertEqual(1, count_connections_in(Config, VHost)),
    close_connections([Conn1]),
    ?assertEqual(0, count_connections_in(Config, VHost)),

    [Conn2] = open_connections(Config, [1]),
    ?assertEqual(1, count_connections_in(Config, VHost)),

    [Conn3] = open_connections(Config, [0]),
    ?assertEqual(2, count_connections_in(Config, VHost)),

    [Conn4] = open_connections(Config, [1]),
    ?assertEqual(3, count_connections_in(Config, VHost)),

    [Conn5] = open_connections(Config, [1]),
    ?assertEqual(4, count_connections_in(Config, VHost)),

    rabbit_ct_broker_helpers:restart_broker(Config, 1),
    ?assertEqual(1, count_connections_in(Config, VHost)),

    close_connections([Conn2, Conn3, Conn4, Conn5]),
    ?assertEqual(0, count_connections_in(Config, VHost)).

cluster_node_list_on_node(Config) ->
    [A, B] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ?assertEqual(0, length(all_connections(Config))),
    ?assertEqual(0, length(connections_on_node(Config, 0))),

    [Conn1] = open_connections(Config, [0]),
    [#tracked_connection{node = A}] = connections_on_node(Config, 0),
    close_connections([Conn1]),
    ?assertEqual(0, length(connections_on_node(Config, 0))),

    [_Conn2] = open_connections(Config, [1]),
    [#tracked_connection{node = B}] = connections_on_node(Config, 1),

    [Conn3] = open_connections(Config, [0]),
    ?assertEqual(1, length(connections_on_node(Config, 0))),

    [Conn4] = open_connections(Config, [1]),
    ?assertEqual(2, length(connections_on_node(Config, 1))),

    kill_connections([Conn4]),
    ?assertEqual(1, length(connections_on_node(Config, 1))),

    [Conn5] = open_connections(Config, [0]),
    ?assertEqual(2, length(connections_on_node(Config, 0))),

    rabbit_ct_broker_helpers:stop_broker(Config, 1),
    await_running_node_refresh(Config, 0),

    ?assertEqual(2, length(all_connections(Config))),
    ?assertEqual(0, length(connections_on_node(Config, 0, B))),

    close_connections([Conn3, Conn5]),
    ?assertEqual(0, length(all_connections(Config, 0))),

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

    close_connections([Conn1, Conn2, Conn3, Conn4, Conn5]),
    ?assertEqual(0, count_connections_in(Config, VHost)),

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

    close_connections([Conn1, Conn2]),
    ?assertEqual(0, count_connections_in(Config, VHost)).


single_node_multiple_vhosts_limit(Config) ->
    VHost1 = <<"vhost1">>,
    VHost2 = <<"vhost2">>,

    set_up_vhost(Config, VHost1),
    set_up_vhost(Config, VHost2),

    set_vhost_connection_limit(Config, VHost1, 2),
    set_vhost_connection_limit(Config, VHost2, 2),

    ?assertEqual(0, count_connections_in(Config, VHost1)),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

    [Conn1, Conn2, Conn3, Conn4] = open_connections(Config, [
        {0, VHost1},
        {0, VHost1},
        {0, VHost2},
        {0, VHost2}]),

    %% we've crossed the limit
    expect_that_client_connection_is_rejected(Config, 0, VHost1),
    expect_that_client_connection_is_rejected(Config, 0, VHost2),

    [Conn5] = open_connections(Config, [0]),

    set_vhost_connection_limit(Config, VHost1, 5),
    set_vhost_connection_limit(Config, VHost2, -10),

    [Conn6, Conn7, Conn8, Conn9, Conn10] = open_connections(Config, [
        {0, VHost1},
        {0, VHost1},
        {0, VHost1},
        {0, VHost2},
        {0, VHost2}]),

    close_connections([Conn1, Conn2, Conn3, Conn4, Conn5,
                       Conn6, Conn7, Conn8, Conn9, Conn10]),
    ?assertEqual(0, count_connections_in(Config, VHost1)),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

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

    ?assertEqual(0, count_connections_in(Config, VHost1)),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

    %% with limit = 0 no connections are allowed
    expect_that_client_connection_is_rejected(Config, 0, VHost1),
    expect_that_client_connection_is_rejected(Config, 0, VHost2),
    expect_that_client_connection_is_rejected(Config, 0, VHost1),

    set_vhost_connection_limit(Config, VHost1, -1),
    [Conn1, Conn2] = open_connections(Config, [{0, VHost1}, {0, VHost1}]),

    close_connections([Conn1, Conn2]),
    ?assertEqual(0, count_connections_in(Config, VHost1)),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

    set_vhost_connection_limit(Config, VHost1, -1),
    set_vhost_connection_limit(Config, VHost2, -1).


cluster_single_vhost_limit(Config) ->
    VHost = <<"/">>,
    set_vhost_connection_limit(Config, VHost, 2),

    ?assertEqual(0, count_connections_in(Config, VHost)),

    %% here connections are opened to different nodes
    [Conn1, Conn2] = open_connections(Config, [{0, VHost}, {1, VHost}]),

    %% we've crossed the limit
    expect_that_client_connection_is_rejected(Config, 0, VHost),
    expect_that_client_connection_is_rejected(Config, 1, VHost),

    set_vhost_connection_limit(Config, VHost, 5),

    [Conn3, Conn4] = open_connections(Config, [{0, VHost}, {0, VHost}]),

    close_connections([Conn1, Conn2, Conn3, Conn4]),
    ?assertEqual(0, count_connections_in(Config, VHost)),

    set_vhost_connection_limit(Config, VHost,  -1).

cluster_single_vhost_limit2(Config) ->
    VHost = <<"/">>,
    set_vhost_connection_limit(Config, VHost, 2),

    ?assertEqual(0, count_connections_in(Config, VHost)),

    %% here a limit is reached on one node first
    [Conn1, Conn2] = open_connections(Config, [{0, VHost}, {0, VHost}]),

    %% we've crossed the limit
    expect_that_client_connection_is_rejected(Config, 0, VHost),
    expect_that_client_connection_is_rejected(Config, 1, VHost),

    set_vhost_connection_limit(Config, VHost, 5),

    [Conn3, Conn4, Conn5, {error, not_allowed}] = open_connections(Config, [
        {1, VHost},
        {1, VHost},
        {1, VHost},
        {1, VHost}]),

    close_connections([Conn1, Conn2, Conn3, Conn4, Conn5]),
    ?assertEqual(0, count_connections_in(Config, VHost)),

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

    close_connections([Conn1, Conn2, Conn3, Conn4]),
    ?assertEqual(0, count_connections_in(Config, VHost)),

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

    close_connections([Conn1, Conn2, Conn3, Conn4]),
    ?assertEqual(0, count_connections_in(Config, VHost1)),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

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
    ?assertEqual(2, count_connections_in(Config, VHost)),
    close_connections([Conn1, Conn2]),

    Config1 = cluster_rename_SUITE:stop_rename_start(Config, A, [A, 'new-A']),

    ?assertEqual(0, count_connections_in(Config1, VHost)),

    [Conn3, Conn4, {error, not_allowed}] = open_connections(Config,
      [{0, VHost}, {1, VHost}, {0, VHost}]),
    ?assertEqual(2, count_connections_in(Config1, VHost)),
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
    timer:sleep(500),
    Conns.

close_connections(Conns) ->
    lists:foreach(fun
      (Conn) ->
          rabbit_ct_client_helpers:close_connection(Conn)
      end, Conns),
    timer:sleep(500).

kill_connections(Conns) ->
    lists:foreach(fun
      (Conn) ->
          (catch exit(Conn, please_terminate))
      end, Conns),
    timer:sleep(500).

count_connections_in(Config, VHost) ->
    count_connections_in(Config, VHost, 0).
count_connections_in(Config, VHost, NodeIndex) ->
    timer:sleep(200),
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                 rabbit_connection_tracking,
                                 count_connections_in, [VHost]).

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

await_running_node_refresh(_Config, _NodeIndex) ->
    timer:sleep(250).

expect_that_client_connection_is_rejected(Config) ->
    expect_that_client_connection_is_rejected(Config, 0).

expect_that_client_connection_is_rejected(Config, NodeIndex) ->
    {error, not_allowed} =
      rabbit_ct_client_helpers:open_unmanaged_connection(Config, NodeIndex).

expect_that_client_connection_is_rejected(Config, NodeIndex, VHost) ->
    {error, not_allowed} =
      rabbit_ct_client_helpers:open_unmanaged_connection(Config, NodeIndex, VHost).
