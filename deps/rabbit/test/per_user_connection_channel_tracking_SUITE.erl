%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(per_user_connection_channel_tracking_SUITE).

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
        single_node_user_connection_channel_tracking,
        single_node_user_deletion,
        single_node_vhost_down_mimic,
        single_node_vhost_deletion
    ],
    ClusterSize2Tests = [
        cluster_user_deletion,
        cluster_vhost_down_mimic,
        cluster_vhost_deletion,
        cluster_node_removed
    ],
    [
      {cluster_size_1_network, [], ClusterSize1Tests},
      {cluster_size_2_network, [], ClusterSize2Tests},
      {cluster_size_1_direct, [], ClusterSize1Tests},
      {cluster_size_2_direct, [], ClusterSize2Tests}
    ].

suite() ->
    [
      %% If a test hangs, no need to wait for 30 minutes.
      {timetrap, {minutes, 8}}
    ].

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
    case rabbit_ct_helpers:is_mixed_versions() of
        false ->
            Config1 = rabbit_ct_helpers:set_config(Config, [{connection_type, network}]),
            init_per_multinode_group(cluster_size_2_network, Config1, 2);
        _ ->
            %% In a mixed 3.8/3.9 cluster, changes to rabbit_core_ff.erl imply that some
            %% feature flag related migrations cannot occur, and therefore user_limits
            %% cannot be enabled in a 3.8/3.9 mixed cluster
            {skip, "cluster_size_2_network is not mixed version compatible"}
    end;
init_per_group(cluster_size_1_direct, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [{connection_type, direct}]),
    init_per_multinode_group(cluster_size_1_direct, Config1, 1);
init_per_group(cluster_size_2_direct, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [{connection_type, direct}]),
    init_per_multinode_group(cluster_size_2_direct, Config1, 2).

init_per_multinode_group(Group, Config, NodeCount) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodes_count, NodeCount},
                                                    {rmq_nodename_suffix, Suffix}
      ]),
    Config2 = rabbit_ct_helpers:run_steps(
                Config1, rabbit_ct_broker_helpers:setup_steps() ++
                rabbit_ct_client_helpers:setup_steps()),
    EnableFF = rabbit_ct_broker_helpers:enable_feature_flag(
                 Config2, user_limits),
    case EnableFF of
        ok ->
            Config2;
        {skip, _} = Skip ->
            end_per_group(Group, Config2),
            Skip;
        Other ->
            end_per_group(Group, Config2),
            {skip, Other}
    end.

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    clear_all_connection_tracking_tables(Config),
    clear_all_channel_tracking_tables(Config),
    Config.

end_per_testcase(Testcase, Config) ->
    clear_all_connection_tracking_tables(Config),
    clear_all_channel_tracking_tables(Config),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

clear_all_connection_tracking_tables(Config) ->
    [rabbit_ct_broker_helpers:rpc(Config,
        N,
        rabbit_connection_tracking,
        clear_tracking_tables,
        []) || N <- rabbit_ct_broker_helpers:get_node_configs(Config, nodename)].

clear_all_channel_tracking_tables(Config) ->
    [rabbit_ct_broker_helpers:rpc(Config,
        N,
        rabbit_channel_tracking,
        clear_tracking_tables,
        []) || N <- rabbit_ct_broker_helpers:get_node_configs(Config, nodename)].

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------
single_node_user_connection_channel_tracking(Config) ->
    Username = proplists:get_value(rmq_username, Config),
    Username2 = <<"guest2">>,

    Vhost = proplists:get_value(rmq_vhost, Config),

    rabbit_ct_broker_helpers:add_user(Config, Username2),
    rabbit_ct_broker_helpers:set_full_permissions(Config, Username2, Vhost),

    ?assertEqual(0, count_connections_in(Config, Username)),
    ?assertEqual(0, count_connections_in(Config, Username2)),
    ?assertEqual(0, count_channels_in(Config, Username)),
    ?assertEqual(0, count_channels_in(Config, Username2)),
    ?assertEqual(0, tracked_user_connection_count(Config, Username)),
    ?assertEqual(0, tracked_user_connection_count(Config, Username2)),
    ?assertEqual(0, tracked_user_channel_count(Config, Username)),
    ?assertEqual(0, tracked_user_channel_count(Config, Username2)),

    [Conn1] = open_connections(Config, [0]),
    [Chan1] = open_channels(Conn1, 1),
    [#tracked_connection{username = Username}] = connections_in(Config, Username),
    [#tracked_channel{username = Username}] = channels_in(Config, Username),
    ?assertEqual(true, is_process_alive(Conn1)),
    ?assertEqual(true, is_process_alive(Chan1)),
    close_channels([Chan1]),
    ?awaitMatch(0, count_channels_in(Config, Username), 20000),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username), 20000),
    ?awaitMatch(false, is_process_alive(Chan1), 20000),
    close_connections([Conn1]),
    ?awaitMatch(0, length(connections_in(Config, Username)), 20000),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username), 20000),
    ?awaitMatch(false, is_process_alive(Conn1), 20000),

    [Conn2] = open_connections(Config, [{0, Username2}]),
    Chans2  = [_|_] = open_channels(Conn2, 5),
    timer:sleep(100),
    [#tracked_connection{username = Username2}] = connections_in(Config, Username2),
    ?assertEqual(5, count_channels_in(Config, Username2)),
    ?assertEqual(1, tracked_user_connection_count(Config, Username2)),
    ?assertEqual(5, tracked_user_channel_count(Config, Username2)),
    ?assertEqual(true, is_process_alive(Conn2)),
    [?assertEqual(true, is_process_alive(Ch)) || Ch <- Chans2],

    [Conn3] = open_connections(Config, [0]),
    Chans3 = [_|_] = open_channels(Conn3, 5),
    [#tracked_connection{username = Username}] = connections_in(Config, Username),
    ?assertEqual(5, count_channels_in(Config, Username)),
    ?assertEqual(1, tracked_user_connection_count(Config, Username)),
    ?assertEqual(5, tracked_user_channel_count(Config, Username)),
    ?assertEqual(true, is_process_alive(Conn3)),
    [?assertEqual(true, is_process_alive(Ch)) || Ch <- Chans3],

    [Conn4] = open_connections(Config, [0]),
    Chans4 = [_|_] = open_channels(Conn4, 5),
    ?assertEqual(2, tracked_user_connection_count(Config, Username)),
    ?assertEqual(10, tracked_user_channel_count(Config, Username)),
    ?assertEqual(true, is_process_alive(Conn4)),
    [?assertEqual(true, is_process_alive(Ch)) || Ch <- Chans4],
    kill_connections([Conn4]),
    [#tracked_connection{username = Username}] = connections_in(Config, Username),
    ?awaitMatch(5, count_channels_in(Config, Username), 20000),
    ?awaitMatch(1, tracked_user_connection_count(Config, Username), 20000),
    ?awaitMatch(5, tracked_user_channel_count(Config, Username), 20000),
    ?assertEqual(false, is_process_alive(Conn4)),
    [?assertEqual(false, is_process_alive(Ch)) || Ch <- Chans4],

    [Conn5] = open_connections(Config, [0]),
    Chans5  = [_|_] = open_channels(Conn5, 7),
    [Username, Username] =
        lists:map(fun (#tracked_connection{username = U}) -> U end,
                  connections_in(Config, Username)),
    ?assertEqual(12, count_channels_in(Config, Username)),
    ?assertEqual(12, tracked_user_channel_count(Config, Username)),
    ?assertEqual(2, tracked_user_connection_count(Config, Username)),
    ?assertEqual(true, is_process_alive(Conn5)),
    [?assertEqual(true, is_process_alive(Ch)) || Ch <- Chans5],

    close_channels(Chans2 ++ Chans3 ++ Chans5),
    ?awaitMatch(0, length(all_channels(Config)), 20000),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username), 20000),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username2), 20000),

    close_connections([Conn2, Conn3, Conn5]),
    rabbit_ct_broker_helpers:delete_user(Config, Username2),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username), 20000),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username2), 20000),
    ?awaitMatch(0, length(all_connections(Config)), 20000).

single_node_user_deletion(Config) ->
    set_tracking_execution_timeout(Config, 100),

    Username = proplists:get_value(rmq_username, Config),
    Username2 = <<"guest2">>,

    Vhost = proplists:get_value(rmq_vhost, Config),

    rabbit_ct_broker_helpers:add_user(Config, Username2),
    rabbit_ct_broker_helpers:set_full_permissions(Config, Username2, Vhost),

    ?assertEqual(100, get_tracking_execution_timeout(Config)),

    ?assertEqual(0, count_connections_in(Config, Username)),
    ?assertEqual(0, count_connections_in(Config, Username2)),
    ?assertEqual(0, count_channels_in(Config, Username)),
    ?assertEqual(0, count_channels_in(Config, Username2)),
    ?assertEqual(0, tracked_user_connection_count(Config, Username)),
    ?assertEqual(0, tracked_user_connection_count(Config, Username2)),
    ?assertEqual(0, tracked_user_channel_count(Config, Username)),
    ?assertEqual(0, tracked_user_channel_count(Config, Username2)),

    [Conn1] = open_connections(Config, [0]),
    Chans1 = [_|_] = open_channels(Conn1, 5),
    ?assertEqual(1, count_connections_in(Config, Username)),
    ?assertEqual(5, count_channels_in(Config, Username)),
    ?assertEqual(1, tracked_user_connection_count(Config, Username)),
    ?assertEqual(5, tracked_user_channel_count(Config, Username)),
    ?assertEqual(true, is_process_alive(Conn1)),
    [?assertEqual(true, is_process_alive(Ch)) || Ch <- Chans1],

    [Conn2] = open_connections(Config, [{0, Username2}]),
    Chans2 = [_|_] = open_channels(Conn2, 5),
    ?assertEqual(1, count_connections_in(Config, Username2)),
    ?assertEqual(5, count_channels_in(Config, Username2)),
    ?assertEqual(1, tracked_user_connection_count(Config, Username2)),
    ?assertEqual(5, tracked_user_channel_count(Config, Username2)),
    ?assertEqual(true, is_process_alive(Conn2)),
    [?assertEqual(true, is_process_alive(Ch)) || Ch <- Chans2],

    ?assertEqual(true, exists_in_tracked_connection_per_user_table(Config, Username2)),
    ?assertEqual(true, exists_in_tracked_channel_per_user_table(Config, Username2)),

    rabbit_ct_broker_helpers:delete_user(Config, Username2),
    timer:sleep(100),
    ?assertEqual(0, count_connections_in(Config, Username2)),
    ?assertEqual(0, count_channels_in(Config, Username2)),
    ?assertEqual(0, tracked_user_connection_count(Config, Username2)),
    ?assertEqual(0, tracked_user_channel_count(Config, Username2)),
    ?assertEqual(false, is_process_alive(Conn2)),
    [?assertEqual(false, is_process_alive(Ch)) || Ch <- Chans2],

    %% ensure vhost entry is cleared after 'tracking_execution_timeout'
    ?awaitMatch(false, exists_in_tracked_connection_per_user_table(Config, Username2), 20000),
    ?awaitMatch(false, exists_in_tracked_channel_per_user_table(Config, Username2), 20000),

    ?assertEqual(1, count_connections_in(Config, Username)),
    ?assertEqual(5, count_channels_in(Config, Username)),
    ?assertEqual(1, tracked_user_connection_count(Config, Username)),
    ?assertEqual(5, tracked_user_channel_count(Config, Username)),
    ?assertEqual(true, is_process_alive(Conn1)),
    [?assertEqual(true, is_process_alive(Ch)) || Ch <- Chans1],

    close_channels(Chans1),
    ?awaitMatch(0, count_channels_in(Config, Username), 20000),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username), 20000),

    close_connections([Conn1]),
    ?awaitMatch(0, count_connections_in(Config, Username), 20000),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username), 20000).

single_node_vhost_deletion(Config) ->
    set_tracking_execution_timeout(Config, 100),

    Username = proplists:get_value(rmq_username, Config),
    Username2 = <<"guest2">>,

    Vhost = proplists:get_value(rmq_vhost, Config),

    rabbit_ct_broker_helpers:add_user(Config, Username2),
    rabbit_ct_broker_helpers:set_full_permissions(Config, Username2, Vhost),

    ?assertEqual(100, get_tracking_execution_timeout(Config)),

    ?assertEqual(0, count_connections_in(Config, Username)),
    ?assertEqual(0, count_connections_in(Config, Username2)),
    ?assertEqual(0, count_channels_in(Config, Username)),
    ?assertEqual(0, count_channels_in(Config, Username2)),
    ?assertEqual(0, tracked_user_connection_count(Config, Username)),
    ?assertEqual(0, tracked_user_connection_count(Config, Username2)),
    ?assertEqual(0, tracked_user_channel_count(Config, Username)),
    ?assertEqual(0, tracked_user_channel_count(Config, Username2)),

    [Conn1] = open_connections(Config, [0]),
    Chans1 = [_|_] = open_channels(Conn1, 5),
    ?assertEqual(1, count_connections_in(Config, Username)),
    ?assertEqual(5, count_channels_in(Config, Username)),
    ?assertEqual(1, tracked_user_connection_count(Config, Username)),
    ?assertEqual(5, tracked_user_channel_count(Config, Username)),
    ?assertEqual(true, is_process_alive(Conn1)),
    [?assertEqual(true, is_process_alive(Ch)) || Ch <- Chans1],

    [Conn2] = open_connections(Config, [{0, Username2}]),
    Chans2 = [_|_] = open_channels(Conn2, 5),
    ?assertEqual(1, count_connections_in(Config, Username2)),
    ?assertEqual(5, count_channels_in(Config, Username2)),
    ?assertEqual(1, tracked_user_connection_count(Config, Username2)),
    ?assertEqual(5, tracked_user_channel_count(Config, Username2)),
    ?assertEqual(true, is_process_alive(Conn2)),
    [?assertEqual(true, is_process_alive(Ch)) || Ch <- Chans2],

    ?assertEqual(true, exists_in_tracked_connection_per_vhost_table(Config, Vhost)),

    rabbit_ct_broker_helpers:delete_vhost(Config, Vhost),
    timer:sleep(200),
    ?assertEqual(0, count_connections_in(Config, Username2)),
    ?assertEqual(0, count_channels_in(Config, Username2)),
    ?assertEqual(0, tracked_user_connection_count(Config, Username2)),
    ?assertEqual(0, tracked_user_channel_count(Config, Username2)),
    ?assertEqual(false, is_process_alive(Conn2)),
    [?assertEqual(false, is_process_alive(Ch)) || Ch <- Chans2],

    ?assertEqual(0, count_connections_in(Config, Username)),
    ?assertEqual(0, count_channels_in(Config, Username)),
    ?assertEqual(0, tracked_user_connection_count(Config, Username)),
    ?assertEqual(0, tracked_user_channel_count(Config, Username)),
    ?assertEqual(false, is_process_alive(Conn1)),
    [?assertEqual(false, is_process_alive(Ch)) || Ch <- Chans1],

    %% ensure vhost entry is cleared after 'tracking_execution_timeout'
    ?assertEqual(false, exists_in_tracked_connection_per_vhost_table(Config, Vhost)),

    rabbit_ct_broker_helpers:add_vhost(Config, Vhost).

single_node_vhost_down_mimic(Config) ->
    Username = proplists:get_value(rmq_username, Config),
    Username2 = <<"guest2">>,

    Vhost = proplists:get_value(rmq_vhost, Config),

    rabbit_ct_broker_helpers:add_user(Config, Username2),
    rabbit_ct_broker_helpers:set_full_permissions(Config, Username2, Vhost),

    ?assertEqual(0, count_connections_in(Config, Username)),
    ?assertEqual(0, count_connections_in(Config, Username2)),
    ?assertEqual(0, count_channels_in(Config, Username)),
    ?assertEqual(0, count_channels_in(Config, Username2)),
    ?assertEqual(0, tracked_user_connection_count(Config, Username)),
    ?assertEqual(0, tracked_user_connection_count(Config, Username2)),
    ?assertEqual(0, tracked_user_channel_count(Config, Username)),
    ?assertEqual(0, tracked_user_channel_count(Config, Username2)),

    [Conn1] = open_connections(Config, [0]),
    Chans1 = [_|_] = open_channels(Conn1, 5),
    ?assertEqual(1, count_connections_in(Config, Username)),
    ?assertEqual(5, count_channels_in(Config, Username)),
    ?assertEqual(1, tracked_user_connection_count(Config, Username)),
    ?assertEqual(5, tracked_user_channel_count(Config, Username)),
    ?assertEqual(true, is_process_alive(Conn1)),
    [?assertEqual(true, is_process_alive(Ch)) || Ch <- Chans1],

    [Conn2] = open_connections(Config, [{0, Username2}]),
    Chans2 = [_|_] = open_channels(Conn2, 5),
    ?assertEqual(1, count_connections_in(Config, Username2)),
    ?assertEqual(5, count_channels_in(Config, Username2)),
    ?assertEqual(1, tracked_user_connection_count(Config, Username2)),
    ?assertEqual(5, tracked_user_channel_count(Config, Username2)),
    ?assertEqual(true, is_process_alive(Conn2)),
    [?assertEqual(true, is_process_alive(Ch)) || Ch <- Chans2],

    %% mimic vhost down event, while connections exist
    mimic_vhost_down(Config, 0, Vhost),
    timer:sleep(200),
    ?assertEqual(0, count_connections_in(Config, Username2)),
    ?assertEqual(0, count_channels_in(Config, Username2)),
    ?assertEqual(0, tracked_user_connection_count(Config, Username2)),
    ?assertEqual(0, tracked_user_channel_count(Config, Username2)),
    ?assertEqual(false, is_process_alive(Conn2)),
    [?assertEqual(false, is_process_alive(Ch)) || Ch <- Chans2],

    ?assertEqual(0, count_connections_in(Config, Username)),
    ?assertEqual(0, count_channels_in(Config, Username)),
    ?assertEqual(0, tracked_user_connection_count(Config, Username)),
    ?assertEqual(0, tracked_user_channel_count(Config, Username)),
    ?assertEqual(false, is_process_alive(Conn1)),
    [?assertEqual(false, is_process_alive(Ch)) || Ch <- Chans1].

cluster_user_deletion(Config) ->
    set_tracking_execution_timeout(Config, 0, 100),
    set_tracking_execution_timeout(Config, 1, 100),
    Username = proplists:get_value(rmq_username, Config),
    Username2 = <<"guest2">>,

    Vhost = proplists:get_value(rmq_vhost, Config),

    rabbit_ct_broker_helpers:add_user(Config, Username2),
    rabbit_ct_broker_helpers:set_full_permissions(Config, Username2, Vhost),

    ?assertEqual(100, get_tracking_execution_timeout(Config, 0)),
    ?assertEqual(100, get_tracking_execution_timeout(Config, 1)),

    ?assertEqual(0, count_connections_in(Config, Username)),
    ?assertEqual(0, count_connections_in(Config, Username2)),
    ?assertEqual(0, count_channels_in(Config, Username)),
    ?assertEqual(0, count_channels_in(Config, Username2)),
    ?assertEqual(0, tracked_user_connection_count(Config, Username)),
    ?assertEqual(0, tracked_user_connection_count(Config, Username2)),
    ?assertEqual(0, tracked_user_channel_count(Config, Username)),
    ?assertEqual(0, tracked_user_channel_count(Config, Username2)),

    [Conn1] = open_connections(Config, [0]),
    Chans1 = [_|_] = open_channels(Conn1, 5),
    ?assertEqual(1, count_connections_in(Config, Username)),
    ?assertEqual(5, count_channels_in(Config, Username)),
    ?assertEqual(1, tracked_user_connection_count(Config, Username)),
    ?assertEqual(5, tracked_user_channel_count(Config, Username)),
    ?assertEqual(true, is_process_alive(Conn1)),
    [?assertEqual(true, is_process_alive(Ch)) || Ch <- Chans1],

    [Conn2] = open_connections(Config, [{1, Username2}]),
    Chans2 = [_|_] = open_channels(Conn2, 5),
    ?assertEqual(1, count_connections_in(Config, Username2)),
    ?assertEqual(5, count_channels_in(Config, Username2)),
    ?assertEqual(1, tracked_user_connection_count(Config, Username2)),
    ?assertEqual(5, tracked_user_channel_count(Config, Username2)),
    ?assertEqual(true, is_process_alive(Conn2)),
    [?assertEqual(true, is_process_alive(Ch)) || Ch <- Chans2],

    ?assertEqual(true, exists_in_tracked_connection_per_user_table(Config, 1, Username2)),
    ?assertEqual(true, exists_in_tracked_channel_per_user_table(Config, 1, Username2)),

    rabbit_ct_broker_helpers:delete_user(Config, Username2),
    timer:sleep(200),
    ?assertEqual(0, count_connections_in(Config, Username2)),
    ?assertEqual(0, count_channels_in(Config, Username2)),
    ?assertEqual(0, tracked_user_connection_count(Config, Username2)),
    ?assertEqual(0, tracked_user_channel_count(Config, Username2)),
    ?assertEqual(false, is_process_alive(Conn2)),
    [?assertEqual(false, is_process_alive(Ch)) || Ch <- Chans2],

    %% ensure user entry is cleared after 'tracking_execution_timeout'
    ?assertEqual(false, exists_in_tracked_connection_per_user_table(Config, 1, Username2)),
    ?assertEqual(false, exists_in_tracked_channel_per_user_table(Config, 1, Username2)),

    close_channels(Chans1),
    ?awaitMatch(0, count_channels_in(Config, Username), 20000),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username), 20000),

    close_connections([Conn1]),
    ?awaitMatch(0, count_connections_in(Config, Username), 20000),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username), 20000).

cluster_vhost_deletion(Config) ->
    set_tracking_execution_timeout(Config, 0, 100),
    set_tracking_execution_timeout(Config, 1, 100),
    Username = proplists:get_value(rmq_username, Config),
    Username2 = <<"guest2">>,

    Vhost = proplists:get_value(rmq_vhost, Config),

    rabbit_ct_broker_helpers:add_user(Config, Username2),
    rabbit_ct_broker_helpers:set_full_permissions(Config, Username2, Vhost),

    ?assertEqual(100, get_tracking_execution_timeout(Config, 0)),
    ?assertEqual(100, get_tracking_execution_timeout(Config, 1)),

    ?assertEqual(0, count_connections_in(Config, Username)),
    ?assertEqual(0, count_connections_in(Config, Username2)),
    ?assertEqual(0, count_channels_in(Config, Username)),
    ?assertEqual(0, count_channels_in(Config, Username2)),
    ?assertEqual(0, tracked_user_connection_count(Config, Username)),
    ?assertEqual(0, tracked_user_connection_count(Config, Username2)),
    ?assertEqual(0, tracked_user_channel_count(Config, Username)),
    ?assertEqual(0, tracked_user_channel_count(Config, Username2)),

    [Conn1] = open_connections(Config, [{0, Username}]),
    Chans1 = [_|_] = open_channels(Conn1, 5),
    ?assertEqual(1, count_connections_in(Config, Username)),
    ?assertEqual(5, count_channels_in(Config, Username)),
    ?assertEqual(1, tracked_user_connection_count(Config, Username)),
    ?assertEqual(5, tracked_user_channel_count(Config, Username)),
    ?assertEqual(true, is_process_alive(Conn1)),
    [?assertEqual(true, is_process_alive(Ch)) || Ch <- Chans1],

    [Conn2] = open_connections(Config, [{1, Username2}]),
    Chans2 = [_|_] = open_channels(Conn2, 5),
    ?assertEqual(1, count_connections_in(Config, Username2)),
    ?assertEqual(5, count_channels_in(Config, Username2)),
    ?assertEqual(1, tracked_user_connection_count(Config, Username2)),
    ?assertEqual(5, tracked_user_channel_count(Config, Username2)),
    ?assertEqual(true, is_process_alive(Conn2)),
    [?assertEqual(true, is_process_alive(Ch)) || Ch <- Chans2],

    ?assertEqual(true, exists_in_tracked_connection_per_vhost_table(Config, 0, Vhost)),
    ?assertEqual(true, exists_in_tracked_connection_per_vhost_table(Config, 1, Vhost)),

    rabbit_ct_broker_helpers:delete_vhost(Config, Vhost),
    timer:sleep(200),
    ?assertEqual(0, count_connections_in(Config, Username2)),
    ?assertEqual(0, count_channels_in(Config, Username2)),
    ?assertEqual(0, tracked_user_connection_count(Config, Username2)),
    ?assertEqual(0, tracked_user_channel_count(Config, Username2)),
    ?assertEqual(false, is_process_alive(Conn2)),
    [?assertEqual(false, is_process_alive(Ch)) || Ch <- Chans2],

    ?assertEqual(0, count_connections_in(Config, Username)),
    ?assertEqual(0, count_channels_in(Config, Username)),
    ?assertEqual(0, tracked_user_connection_count(Config, Username)),
    ?assertEqual(0, tracked_user_channel_count(Config, Username)),
    ?assertEqual(false, is_process_alive(Conn1)),
    [?assertEqual(false, is_process_alive(Ch)) || Ch <- Chans1],

    %% ensure vhost entry is cleared after 'tracking_execution_timeout'
    ?assertEqual(false, exists_in_tracked_connection_per_vhost_table(Config, 0, Vhost)),
    ?assertEqual(false, exists_in_tracked_connection_per_vhost_table(Config, 1, Vhost)),

    rabbit_ct_broker_helpers:add_vhost(Config, Vhost),
    rabbit_ct_broker_helpers:add_user(Config, Username),
    rabbit_ct_broker_helpers:set_full_permissions(Config, Username, Vhost).

cluster_vhost_down_mimic(Config) ->
    Username = proplists:get_value(rmq_username, Config),
    Username2 = <<"guest2">>,

    Vhost = proplists:get_value(rmq_vhost, Config),

    rabbit_ct_broker_helpers:add_user(Config, Username2),
    rabbit_ct_broker_helpers:set_full_permissions(Config, Username2, Vhost),

    ?assertEqual(0, count_connections_in(Config, Username)),
    ?assertEqual(0, count_connections_in(Config, Username2)),
    ?assertEqual(0, count_channels_in(Config, Username)),
    ?assertEqual(0, count_channels_in(Config, Username2)),
    ?assertEqual(0, tracked_user_connection_count(Config, Username)),
    ?assertEqual(0, tracked_user_connection_count(Config, Username2)),
    ?assertEqual(0, tracked_user_channel_count(Config, Username)),
    ?assertEqual(0, tracked_user_channel_count(Config, Username2)),

    [Conn1] = open_connections(Config, [{0, Username}]),
    Chans1 = [_|_] = open_channels(Conn1, 5),
    ?assertEqual(1, count_connections_in(Config, Username)),
    ?assertEqual(5, count_channels_in(Config, Username)),
    ?assertEqual(1, tracked_user_connection_count(Config, Username)),
    ?assertEqual(5, tracked_user_channel_count(Config, Username)),
    ?assertEqual(true, is_process_alive(Conn1)),
    [?assertEqual(true, is_process_alive(Ch)) || Ch <- Chans1],

    [Conn2] = open_connections(Config, [{1, Username2}]),
    Chans2 = [_|_] = open_channels(Conn2, 5),
    ?assertEqual(1, count_connections_in(Config, Username2)),
    ?assertEqual(5, count_channels_in(Config, Username2)),
    ?assertEqual(1, tracked_user_connection_count(Config, Username2)),
    ?assertEqual(5, tracked_user_channel_count(Config, Username2)),
    ?assertEqual(true, is_process_alive(Conn2)),
    [?assertEqual(true, is_process_alive(Ch)) || Ch <- Chans2],

    mimic_vhost_down(Config, 1, Vhost),
    timer:sleep(100),
    ?assertEqual(0, count_connections_in(Config, Username2)),
    ?assertEqual(0, count_channels_in(Config, Username2)),
    ?assertEqual(0, tracked_user_connection_count(Config, Username2)),
    ?assertEqual(0, tracked_user_channel_count(Config, Username2)),
    ?assertEqual(false, is_process_alive(Conn2)),
    [?assertEqual(false, is_process_alive(Ch)) || Ch <- Chans2],

    %% gen_event notifies local handlers. remote connections still active
    ?assertEqual(1, count_connections_in(Config, Username)),
    ?assertEqual(5, count_channels_in(Config, Username)),
    ?assertEqual(1, tracked_user_connection_count(Config, Username)),
    ?assertEqual(5, tracked_user_channel_count(Config, Username)),
    ?assertEqual(true, is_process_alive(Conn1)),
    [?assertEqual(true, is_process_alive(Ch)) || Ch <- Chans1],

    mimic_vhost_down(Config, 0, Vhost),
    timer:sleep(100),
    ?assertEqual(0, count_connections_in(Config, Username)),
    ?assertEqual(0, count_channels_in(Config, Username)),
    ?assertEqual(0, tracked_user_connection_count(Config, Username)),
    ?assertEqual(0, tracked_user_channel_count(Config, Username)),
    ?assertEqual(false, is_process_alive(Conn1)),
    [?assertEqual(false, is_process_alive(Ch)) || Ch <- Chans1].

cluster_node_removed(Config) ->
    Username = proplists:get_value(rmq_username, Config),
    Username2 = <<"guest2">>,

    Vhost = proplists:get_value(rmq_vhost, Config),

    rabbit_ct_broker_helpers:add_user(Config, Username2),
    rabbit_ct_broker_helpers:set_full_permissions(Config, Username2, Vhost),

    ?assertEqual(0, count_connections_in(Config, Username)),
    ?assertEqual(0, count_connections_in(Config, Username2)),
    ?assertEqual(0, count_channels_in(Config, Username)),
    ?assertEqual(0, count_channels_in(Config, Username2)),
    ?assertEqual(0, tracked_user_connection_count(Config, Username)),
    ?assertEqual(0, tracked_user_connection_count(Config, Username2)),
    ?assertEqual(0, tracked_user_channel_count(Config, Username)),
    ?assertEqual(0, tracked_user_channel_count(Config, Username2)),

    [Conn1] = open_connections(Config, [{0, Username}]),
    Chans1 = [_|_] = open_channels(Conn1, 5),
    ?assertEqual(1, count_connections_in(Config, Username)),
    ?assertEqual(5, count_channels_in(Config, Username)),
    ?assertEqual(1, tracked_user_connection_count(Config, Username)),
    ?assertEqual(5, tracked_user_channel_count(Config, Username)),
    ?assertEqual(true, is_process_alive(Conn1)),
    [?assertEqual(true, is_process_alive(Ch)) || Ch <- Chans1],

    [Conn2] = open_connections(Config, [{1, Username2}]),
    Chans2 = [_|_] = open_channels(Conn2, 5),
    ?assertEqual(1, count_connections_in(Config, Username2)),
    ?assertEqual(5, count_channels_in(Config, Username2)),
    ?assertEqual(1, tracked_user_connection_count(Config, Username2)),
    ?assertEqual(5, tracked_user_channel_count(Config, Username2)),
    ?assertEqual(true, is_process_alive(Conn2)),
    [?assertEqual(true, is_process_alive(Ch)) || Ch <- Chans2],

    rabbit_ct_broker_helpers:stop_broker(Config, 1),
    timer:sleep(200),
    ?assertEqual(1, count_connections_in(Config, Username)),
    ?assertEqual(5, count_channels_in(Config, Username)),
    ?assertEqual(1, tracked_user_connection_count(Config, Username)),
    ?assertEqual(5, tracked_user_channel_count(Config, Username)),
    ?assertEqual(true, is_process_alive(Conn1)),
    [?assertEqual(true, is_process_alive(Ch)) || Ch <- Chans1],

    rabbit_ct_broker_helpers:forget_cluster_node(Config, 0, 1),
    timer:sleep(200),
    NodeName = rabbit_ct_broker_helpers:get_node_config(Config, 1, nodename),

    DroppedConnTrackingTables =
        rabbit_connection_tracking:get_all_tracked_connection_table_names_for_node(NodeName),
    [?assertEqual(
        {'EXIT', {aborted, {no_exists, Tab, all}}},
        catch mnesia:table_info(Tab, all)) || Tab <- DroppedConnTrackingTables],

    DroppedChTrackingTables =
        rabbit_channel_tracking:get_all_tracked_channel_table_names_for_node(NodeName),
    [?assertEqual(
        {'EXIT', {aborted, {no_exists, Tab, all}}},
        catch mnesia:table_info(Tab, all)) || Tab <- DroppedChTrackingTables],

    ?assertEqual(false, is_process_alive(Conn2)),
    [?assertEqual(false, is_process_alive(Ch)) || Ch <- Chans2],

    ?assertEqual(1, count_connections_in(Config, Username)),
    ?assertEqual(5, count_channels_in(Config, Username)),
    ?assertEqual(1, tracked_user_connection_count(Config, Username)),
    ?assertEqual(5, tracked_user_channel_count(Config, Username)),
    ?assertEqual(true, is_process_alive(Conn1)),
    [?assertEqual(true, is_process_alive(Ch)) || Ch <- Chans1],

    close_channels(Chans1),
    ?awaitMatch(0, count_channels_in(Config, Username), 20000),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username), 20000),

    close_connections([Conn1]),
    ?awaitMatch(0, count_connections_in(Config, Username), 20000),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username), 20000).

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

open_connections(Config, NodesAndUsers) ->
    % Randomly select connection type
    OpenConnectionFun = case ?config(connection_type, Config) of
        network -> open_unmanaged_connection;
        direct  -> open_unmanaged_connection_direct
    end,
    Conns = lists:map(fun
      ({Node, User}) ->
          rabbit_ct_client_helpers:OpenConnectionFun(Config, Node,
                                                     User, User);
      (Node) ->
          rabbit_ct_client_helpers:OpenConnectionFun(Config, Node)
      end, NodesAndUsers),
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

open_channels(Conn, N) ->
    [begin
        {ok, Ch} = amqp_connection:open_channel(Conn),
        Ch
     end || _ <- lists:seq(1, N)].

close_channels(Channels = [_|_]) ->
    [rabbit_ct_client_helpers:close_channel(Ch) || Ch <- Channels].

count_connections_in(Config, Username) ->
    length(connections_in(Config, Username)).

connections_in(Config, Username) ->
    connections_in(Config, 0, Username).
connections_in(Config, NodeIndex, Username) ->
    tracked_list_of_user(Config, NodeIndex, rabbit_connection_tracking, Username).

count_channels_in(Config, Username) ->
    Channels = channels_in(Config, Username),
    length([Ch || Ch = #tracked_channel{username = Username0} <- Channels,
                  Username =:= Username0]).

channels_in(Config, Username) ->
    channels_in(Config, 0, Username).
channels_in(Config, NodeIndex, Username) ->
    tracked_list_of_user(Config, NodeIndex, rabbit_channel_tracking, Username).

tracked_list_of_user(Config, NodeIndex, TrackingMod, Username) ->
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                 TrackingMod,
                                 list_of_user, [Username]).

tracked_user_connection_count(Config, Username) ->
    tracked_user_connection_count(Config, 0, Username).
tracked_user_connection_count(Config, NodeIndex, Username) ->
    count_user_tracked_items(Config, NodeIndex, rabbit_connection_tracking, Username).

tracked_user_channel_count(Config, Username) ->
    tracked_user_channel_count(Config, 0, Username).
tracked_user_channel_count(Config, NodeIndex, Username) ->
    count_user_tracked_items(Config, NodeIndex, rabbit_channel_tracking, Username).

count_user_tracked_items(Config, NodeIndex, TrackingMod, Username) ->
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                 TrackingMod,
                                 count_tracked_items_in, [{user, Username}]).

exists_in_tracked_connection_per_vhost_table(Config, VHost) ->
    exists_in_tracked_connection_per_vhost_table(Config, 0, VHost).
exists_in_tracked_connection_per_vhost_table(Config, NodeIndex, VHost) ->
    exists_in_tracking_table(Config, NodeIndex,
        fun rabbit_connection_tracking:tracked_connection_per_vhost_table_name_for/1,
        VHost).

exists_in_tracked_connection_per_user_table(Config, Username) ->
    exists_in_tracked_connection_per_user_table(Config, 0, Username).
exists_in_tracked_connection_per_user_table(Config, NodeIndex, Username) ->
    exists_in_tracking_table(Config, NodeIndex,
        fun rabbit_connection_tracking:tracked_connection_per_user_table_name_for/1,
        Username).

exists_in_tracked_channel_per_user_table(Config, Username) ->
    exists_in_tracked_channel_per_user_table(Config, 0, Username).
exists_in_tracked_channel_per_user_table(Config, NodeIndex, Username) ->
    exists_in_tracking_table(Config, NodeIndex,
        fun rabbit_channel_tracking:tracked_channel_per_user_table_name_for/1,
        Username).

exists_in_tracking_table(Config, NodeIndex, TableNameFun, Key) ->
    Node = rabbit_ct_broker_helpers:get_node_config(
                Config, NodeIndex, nodename),
    Tab = TableNameFun(Node),
    AllKeys = rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                           mnesia,
                                           dirty_all_keys, [Tab]),
    lists:member(Key, AllKeys).

mimic_vhost_down(Config, NodeIndex, VHost) ->
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                 rabbit_vhost, vhost_down, [VHost]).

all_connections(Config) ->
    all_connections(Config, 0).
all_connections(Config, NodeIndex) ->
    all_tracked_items(Config, NodeIndex, rabbit_connection_tracking).

all_channels(Config) ->
    all_channels(Config, 0).
all_channels(Config, NodeIndex) ->
    all_tracked_items(Config, NodeIndex, rabbit_channel_tracking).

all_tracked_items(Config, NodeIndex, TrackingMod) ->
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                 TrackingMod,
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

set_tracking_execution_timeout(Config, Timeout) ->
    set_tracking_execution_timeout(Config, 0, Timeout).
set_tracking_execution_timeout(Config, NodeIndex, Timeout) ->
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                 application, set_env,
                                 [rabbit, tracking_execution_timeout, Timeout]).

get_tracking_execution_timeout(Config) ->
    get_tracking_execution_timeout(Config, 0).
get_tracking_execution_timeout(Config, NodeIndex) ->
    {ok, Timeout} = rabbit_ct_broker_helpers:rpc(
                                    Config, NodeIndex,
                                    application, get_env,
                                    [rabbit, tracking_execution_timeout]),
    Timeout.

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
