%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(per_user_connection_channel_tracking_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile(nowarn_export_all).
-compile(export_all).

-define(A_TOUT, 20000).

all() ->
    [
     {group, tests}
    ].

groups() ->
    ClusterSize1Tests = [
        single_node_user_connection_channel_tracking,
        single_node_user_deletion,
        single_node_vhost_down_mimic,
        single_node_vhost_deletion
    ],
    ClusterSize3Tests = [
        cluster_user_deletion,
        cluster_vhost_down_mimic,
        cluster_vhost_deletion,
        cluster_node_removed
    ],
    [
     {tests, [], [
                  {cluster_size_1_network, [], ClusterSize1Tests},
                  {cluster_size_3_network, [], ClusterSize3Tests},
                  {cluster_size_1_direct, [], ClusterSize1Tests},
                  {cluster_size_3_direct, [], ClusterSize3Tests}
                 ]}
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
init_per_group(cluster_size_3_network, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [{connection_type, network}]),
    init_per_multinode_group(cluster_size_3_network, Config1, 3);
init_per_group(cluster_size_1_direct, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [{connection_type, direct}]),
    init_per_multinode_group(cluster_size_1_direct, Config1, 1);
init_per_group(cluster_size_3_direct, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [{connection_type, direct}]),
    init_per_multinode_group(cluster_size_3_direct, Config1, 3);
init_per_group(_Group, Config) ->
    Config.

init_per_multinode_group(_Group, Config, NodeCount) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodes_count, NodeCount},
                                                    {rmq_nodename_suffix, Suffix}
      ]),
    rabbit_ct_helpers:run_steps(
      Config1, rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(tests, Config) ->
    % The broker is managed by {init,end}_per_testcase().
    Config;
end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config.

end_per_testcase(Testcase, Config) ->
    Vhost = proplists:get_value(rmq_vhost, Config),
    Username = proplists:get_value(rmq_username, Config),
    rabbit_ct_broker_helpers:add_vhost(Config, Vhost),
    rabbit_ct_broker_helpers:add_user(Config, Username),
    rabbit_ct_broker_helpers:set_full_permissions(Config, Username, Vhost),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------
single_node_user_connection_channel_tracking(Config) ->
    Username = proplists:get_value(rmq_username, Config),
    Username2 = <<"guest2">>,

    Vhost = proplists:get_value(rmq_vhost, Config),

    rabbit_ct_broker_helpers:add_user(Config, Username2),
    rabbit_ct_broker_helpers:set_full_permissions(Config, Username2, Vhost),

    ?awaitMatch(0, count_connections_in(Config, Username), ?A_TOUT),
    ?awaitMatch(0, count_connections_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, count_channels_in(Config, Username), ?A_TOUT),
    ?awaitMatch(0, count_channels_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username), ?A_TOUT),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username), ?A_TOUT),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username2), ?A_TOUT),

    [Conn1] = open_connections(Config, [0]),
    [Chan1] = open_channels(Conn1, 1),
    ?awaitMatch(1, count_connections_in(Config, Username), ?A_TOUT),
    [#tracked_connection{username = Username}] = connections_in(Config, Username),
    ?awaitMatch(1, count_channels_in(Config, Username), ?A_TOUT),
    [#tracked_channel{username = Username}] = channels_in(Config, Username),
    ?awaitMatch(true, is_process_alive(Conn1), ?A_TOUT),
    ?awaitMatch(true, is_process_alive(Chan1), ?A_TOUT),
    close_channels([Chan1]),
    ?awaitMatch(0, count_channels_in(Config, Username), ?A_TOUT),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username), ?A_TOUT),
    ?awaitMatch(false, is_process_alive(Chan1), ?A_TOUT),
    close_connections([Conn1]),
    ?awaitMatch(0, length(connections_in(Config, Username)), ?A_TOUT),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username), ?A_TOUT),
    ?awaitMatch(false, is_process_alive(Conn1), ?A_TOUT),

    [Conn2] = open_connections(Config, [{0, Username2}]),
    Chans2  = [_|_] = open_channels(Conn2, 5),
    ?awaitMatch(1, count_connections_in(Config, Username2), ?A_TOUT),
    [#tracked_connection{username = Username2}] = connections_in(Config, Username2),
    ?awaitMatch(5, count_channels_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(1, tracked_user_connection_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(5, tracked_user_channel_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(true, is_process_alive(Conn2), ?A_TOUT),
    [?awaitMatch(true, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans2],

    [Conn3] = open_connections(Config, [0]),
    Chans3 = [_|_] = open_channels(Conn3, 5),
    ?awaitMatch(1, count_connections_in(Config, Username), ?A_TOUT),
    [#tracked_connection{username = Username}] = connections_in(Config, Username),
    ?awaitMatch(5, count_channels_in(Config, Username), ?A_TOUT),
    ?awaitMatch(1, tracked_user_connection_count(Config, Username), ?A_TOUT),
    ?awaitMatch(5, tracked_user_channel_count(Config, Username), ?A_TOUT),
    ?awaitMatch(true, is_process_alive(Conn3), ?A_TOUT),
    [?awaitMatch(true, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans3],

    [Conn4] = open_connections(Config, [0]),
    Chans4 = [_|_] = open_channels(Conn4, 5),
    ?awaitMatch(2, tracked_user_connection_count(Config, Username), ?A_TOUT),
    ?awaitMatch(10, tracked_user_channel_count(Config, Username), ?A_TOUT),
    ?awaitMatch(true, is_process_alive(Conn4), ?A_TOUT),
    [?awaitMatch(true, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans4],
    kill_connections([Conn4]),
    ?awaitMatch(1, count_connections_in(Config, Username), ?A_TOUT),
    [#tracked_connection{username = Username}] = connections_in(Config, Username),
    ?awaitMatch(5, count_channels_in(Config, Username), ?A_TOUT),
    ?awaitMatch(1, tracked_user_connection_count(Config, Username), ?A_TOUT),
    ?awaitMatch(5, tracked_user_channel_count(Config, Username), ?A_TOUT),
    ?awaitMatch(false, is_process_alive(Conn4), ?A_TOUT),
    [?awaitMatch(false, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans4],

    [Conn5] = open_connections(Config, [0]),
    Chans5  = [_|_] = open_channels(Conn5, 7),
    ?awaitMatch(2, count_connections_in(Config, Username), ?A_TOUT),
    [Username, Username] =
        lists:map(fun (#tracked_connection{username = U}) -> U end,
                  connections_in(Config, Username)),
    ?awaitMatch(12, count_channels_in(Config, Username), ?A_TOUT),
    ?awaitMatch(12, tracked_user_channel_count(Config, Username), ?A_TOUT),
    ?awaitMatch(2, tracked_user_connection_count(Config, Username), ?A_TOUT),
    ?awaitMatch(true, is_process_alive(Conn5), ?A_TOUT),
    [?awaitMatch(true, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans5],

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

    ?awaitMatch(100, get_tracking_execution_timeout(Config), ?A_TOUT),

    ?awaitMatch(0, count_connections_in(Config, Username), ?A_TOUT),
    ?awaitMatch(0, count_connections_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, count_channels_in(Config, Username), ?A_TOUT),
    ?awaitMatch(0, count_channels_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username), ?A_TOUT),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username), ?A_TOUT),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username2), ?A_TOUT),

    [Conn1] = open_connections(Config, [0]),
    Chans1 = [_|_] = open_channels(Conn1, 5),
    ?awaitMatch(1, count_connections_in(Config, Username), ?A_TOUT),
    ?awaitMatch(5, count_channels_in(Config, Username), ?A_TOUT),
    ?awaitMatch(1, tracked_user_connection_count(Config, Username), ?A_TOUT),
    ?awaitMatch(5, tracked_user_channel_count(Config, Username), ?A_TOUT),
    ?awaitMatch(true, is_process_alive(Conn1), ?A_TOUT),
    [?awaitMatch(true, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans1],

    [Conn2] = open_connections(Config, [{0, Username2}]),
    Chans2 = [_|_] = open_channels(Conn2, 5),
    ?awaitMatch(1, count_connections_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(5, count_channels_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(1, tracked_user_connection_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(5, tracked_user_channel_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(true, is_process_alive(Conn2), ?A_TOUT),
    [?awaitMatch(true, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans2],

    ?awaitMatch(true, exists_in_tracked_connection_per_user_table(Config, Username2), ?A_TOUT),
    ?awaitMatch(true, exists_in_tracked_channel_per_user_table(Config, Username2), ?A_TOUT),

    rabbit_ct_broker_helpers:delete_user(Config, Username2),
    ?awaitMatch(0, count_connections_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, count_channels_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(false, is_process_alive(Conn2), ?A_TOUT),
    [?awaitMatch(false, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans2],

    %% ensure vhost entry is cleared after 'tracking_execution_timeout'
    ?awaitMatch(false, exists_in_tracked_connection_per_user_table(Config, Username2), 20000),
    ?awaitMatch(false, exists_in_tracked_channel_per_user_table(Config, Username2), 20000),

    ?awaitMatch(1, count_connections_in(Config, Username), ?A_TOUT),
    ?awaitMatch(5, count_channels_in(Config, Username), ?A_TOUT),
    ?awaitMatch(1, tracked_user_connection_count(Config, Username), ?A_TOUT),
    ?awaitMatch(5, tracked_user_channel_count(Config, Username), ?A_TOUT),
    ?awaitMatch(true, is_process_alive(Conn1), ?A_TOUT),
    [?awaitMatch(true, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans1],

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

    ?awaitMatch(100, get_tracking_execution_timeout(Config), ?A_TOUT),

    ?awaitMatch(0, count_connections_in(Config, Username), ?A_TOUT),
    ?awaitMatch(0, count_connections_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, count_channels_in(Config, Username), ?A_TOUT),
    ?awaitMatch(0, count_channels_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username), ?A_TOUT),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username), ?A_TOUT),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username2), ?A_TOUT),

    [Conn1] = open_connections(Config, [0]),
    Chans1 = [_|_] = open_channels(Conn1, 5),
    ?awaitMatch(1, count_connections_in(Config, Username), ?A_TOUT),
    ?awaitMatch(5, count_channels_in(Config, Username), ?A_TOUT),
    ?awaitMatch(1, tracked_user_connection_count(Config, Username), ?A_TOUT),
    ?awaitMatch(5, tracked_user_channel_count(Config, Username), ?A_TOUT),
    ?awaitMatch(true, is_process_alive(Conn1), ?A_TOUT),
    [?awaitMatch(true, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans1],

    [Conn2] = open_connections(Config, [{0, Username2}]),
    Chans2 = [_|_] = open_channels(Conn2, 5),
    ?awaitMatch(1, count_connections_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(5, count_channels_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(1, tracked_user_connection_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(5, tracked_user_channel_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(true, is_process_alive(Conn2), ?A_TOUT),
    [?awaitMatch(true, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans2],

    ?awaitMatch(true, exists_in_tracked_connection_per_vhost_table(Config, Vhost), ?A_TOUT),

    rabbit_ct_broker_helpers:delete_vhost(Config, Vhost),
    ?awaitMatch(0, count_connections_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, count_channels_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(false, is_process_alive(Conn2), ?A_TOUT),
    [?awaitMatch(false, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans2],

    ?awaitMatch(0, count_connections_in(Config, Username), ?A_TOUT),
    ?awaitMatch(0, count_channels_in(Config, Username), ?A_TOUT),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username), ?A_TOUT),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username), ?A_TOUT),
    ?awaitMatch(false, is_process_alive(Conn1), ?A_TOUT),
    [?awaitMatch(false, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans1],

    %% ensure vhost entry is cleared after 'tracking_execution_timeout'
    ?awaitMatch(false, exists_in_tracked_connection_per_vhost_table(Config, Vhost), 20000),

    rabbit_ct_broker_helpers:add_vhost(Config, Vhost).

single_node_vhost_down_mimic(Config) ->
    Username = proplists:get_value(rmq_username, Config),
    Username2 = <<"guest2">>,

    Vhost = proplists:get_value(rmq_vhost, Config),

    rabbit_ct_broker_helpers:add_user(Config, Username2),
    rabbit_ct_broker_helpers:set_full_permissions(Config, Username2, Vhost),

    ?awaitMatch(0, count_connections_in(Config, Username), ?A_TOUT),
    ?awaitMatch(0, count_connections_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, count_channels_in(Config, Username), ?A_TOUT),
    ?awaitMatch(0, count_channels_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username), ?A_TOUT),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username), ?A_TOUT),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username2), ?A_TOUT),

    [Conn1] = open_connections(Config, [0]),
    Chans1 = [_|_] = open_channels(Conn1, 5),
    ?awaitMatch(1, count_connections_in(Config, Username), ?A_TOUT),
    ?awaitMatch(5, count_channels_in(Config, Username), ?A_TOUT),
    ?awaitMatch(1, tracked_user_connection_count(Config, Username), ?A_TOUT),
    ?awaitMatch(5, tracked_user_channel_count(Config, Username), ?A_TOUT),
    ?awaitMatch(true, is_process_alive(Conn1), ?A_TOUT),
    [?awaitMatch(true, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans1],

    [Conn2] = open_connections(Config, [{0, Username2}]),
    Chans2 = [_|_] = open_channels(Conn2, 5),
    ?awaitMatch(1, count_connections_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(5, count_channels_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(1, tracked_user_connection_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(5, tracked_user_channel_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(true, is_process_alive(Conn2), ?A_TOUT),
    [?awaitMatch(true, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans2],

    %% mimic vhost down event, while connections exist
    mimic_vhost_down(Config, 0, Vhost),
    ?awaitMatch(0, count_connections_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, count_channels_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(false, is_process_alive(Conn2), ?A_TOUT),
    [?awaitMatch(false, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans2],

    ?awaitMatch(0, count_connections_in(Config, Username), ?A_TOUT),
    ?awaitMatch(0, count_channels_in(Config, Username), ?A_TOUT),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username), ?A_TOUT),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username), ?A_TOUT),
    ?awaitMatch(false, is_process_alive(Conn1), ?A_TOUT),
    [?awaitMatch(false, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans1].

cluster_user_deletion(Config) ->
    set_tracking_execution_timeout(Config, 0, 100),
    set_tracking_execution_timeout(Config, 1, 100),
    Username = proplists:get_value(rmq_username, Config),
    Username2 = <<"guest2">>,

    Vhost = proplists:get_value(rmq_vhost, Config),

    rabbit_ct_broker_helpers:add_user(Config, Username2),
    rabbit_ct_broker_helpers:set_full_permissions(Config, Username2, Vhost),

    ?awaitMatch(100, get_tracking_execution_timeout(Config, 0), ?A_TOUT),
    ?awaitMatch(100, get_tracking_execution_timeout(Config, 1), ?A_TOUT),

    ?awaitMatch(0, count_connections_in(Config, Username), ?A_TOUT),
    ?awaitMatch(0, count_connections_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, count_channels_in(Config, Username), ?A_TOUT),
    ?awaitMatch(0, count_channels_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username), ?A_TOUT),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username), ?A_TOUT),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username2), ?A_TOUT),

    [Conn1] = open_connections(Config, [0]),
    Chans1 = [_|_] = open_channels(Conn1, 5),
    ?awaitMatch(1, count_connections_in(Config, Username), ?A_TOUT),
    ?awaitMatch(5, count_channels_in(Config, Username), ?A_TOUT),
    ?awaitMatch(1, tracked_user_connection_count(Config, Username), ?A_TOUT),
    ?awaitMatch(5, tracked_user_channel_count(Config, Username), ?A_TOUT),
    ?awaitMatch(true, is_process_alive(Conn1), ?A_TOUT),
    [?awaitMatch(true, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans1],

    [Conn2] = open_connections(Config, [{1, Username2}]),
    Chans2 = [_|_] = open_channels(Conn2, 5),
    ?awaitMatch(1, count_connections_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(5, count_channels_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(1, tracked_user_connection_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(5, tracked_user_channel_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(true, is_process_alive(Conn2), ?A_TOUT),
    [?awaitMatch(true, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans2],

    ?awaitMatch(true, exists_in_tracked_connection_per_user_table(Config, 1, Username2), ?A_TOUT),
    ?awaitMatch(true, exists_in_tracked_channel_per_user_table(Config, 1, Username2), ?A_TOUT),

    rabbit_ct_broker_helpers:delete_user(Config, Username2),
    ?awaitMatch(0, count_connections_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, count_channels_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(false, is_process_alive(Conn2), ?A_TOUT),
    [?awaitMatch(false, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans2],

    %% ensure user entry is cleared after 'tracking_execution_timeout'
    ?awaitMatch(false, exists_in_tracked_connection_per_user_table(Config, 1, Username2), ?A_TOUT),
    ?awaitMatch(false, exists_in_tracked_channel_per_user_table(Config, 1, Username2), ?A_TOUT),

    close_channels(Chans1),
    ?awaitMatch(0, count_channels_in(Config, Username), ?A_TOUT),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username), ?A_TOUT),

    close_connections([Conn1]),
    ?awaitMatch(0, count_connections_in(Config, Username), ?A_TOUT),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username), ?A_TOUT).

cluster_vhost_deletion(Config) ->
    set_tracking_execution_timeout(Config, 0, 100),
    set_tracking_execution_timeout(Config, 1, 100),
    Username = proplists:get_value(rmq_username, Config),
    Username2 = <<"guest2">>,

    Vhost = proplists:get_value(rmq_vhost, Config),

    rabbit_ct_broker_helpers:add_user(Config, Username2),
    rabbit_ct_broker_helpers:set_full_permissions(Config, Username2, Vhost),

    ?awaitMatch(100, get_tracking_execution_timeout(Config, 0), ?A_TOUT),
    ?awaitMatch(100, get_tracking_execution_timeout(Config, 1), ?A_TOUT),

    ?awaitMatch(0, count_connections_in(Config, Username), ?A_TOUT),
    ?awaitMatch(0, count_connections_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, count_channels_in(Config, Username), ?A_TOUT),
    ?awaitMatch(0, count_channels_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username), ?A_TOUT),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username), ?A_TOUT),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username2), ?A_TOUT),

    [Conn1] = open_connections(Config, [{0, Username}]),
    Chans1 = [_|_] = open_channels(Conn1, 5),
    ?awaitMatch(1, count_connections_in(Config, Username), ?A_TOUT),
    ?awaitMatch(5, count_channels_in(Config, Username), ?A_TOUT),
    ?awaitMatch(1, tracked_user_connection_count(Config, Username), ?A_TOUT),
    ?awaitMatch(5, tracked_user_channel_count(Config, Username), ?A_TOUT),
    ?awaitMatch(true, is_process_alive(Conn1), ?A_TOUT),
    [?awaitMatch(true, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans1],

    [Conn2] = open_connections(Config, [{1, Username2}]),
    Chans2 = [_|_] = open_channels(Conn2, 5),
    ?awaitMatch(1, count_connections_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(5, count_channels_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(1, tracked_user_connection_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(5, tracked_user_channel_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(true, is_process_alive(Conn2), ?A_TOUT),
    [?awaitMatch(true, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans2],

    ?awaitMatch(true, exists_in_tracked_connection_per_vhost_table(Config, 0, Vhost), ?A_TOUT),
    ?awaitMatch(true, exists_in_tracked_connection_per_vhost_table(Config, 1, Vhost), ?A_TOUT),

    rabbit_ct_broker_helpers:delete_vhost(Config, Vhost),
    ?awaitMatch(0, count_connections_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, count_channels_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(false, is_process_alive(Conn2), ?A_TOUT),
    [?awaitMatch(false, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans2],

    ?awaitMatch(0, count_connections_in(Config, Username), ?A_TOUT),
    ?awaitMatch(0, count_channels_in(Config, Username), ?A_TOUT),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username), ?A_TOUT),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username), ?A_TOUT),
    ?awaitMatch(false, is_process_alive(Conn1), ?A_TOUT),
    [?awaitMatch(false, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans1],

    %% ensure vhost entry is cleared after 'tracking_execution_timeout'

    ?awaitMatch(false, exists_in_tracked_connection_per_vhost_table(Config, 0, Vhost), ?A_TOUT),
    ?awaitMatch(false, exists_in_tracked_connection_per_vhost_table(Config, 1, Vhost), ?A_TOUT).

cluster_vhost_down_mimic(Config) ->
    Username = proplists:get_value(rmq_username, Config),
    Username2 = <<"guest2">>,

    Vhost = proplists:get_value(rmq_vhost, Config),

    rabbit_ct_broker_helpers:add_user(Config, Username2),
    rabbit_ct_broker_helpers:set_full_permissions(Config, Username2, Vhost),

    ?awaitMatch(0, count_connections_in(Config, Username), ?A_TOUT),
    ?awaitMatch(0, count_connections_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, count_channels_in(Config, Username), ?A_TOUT),
    ?awaitMatch(0, count_channels_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username), ?A_TOUT),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username), ?A_TOUT),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username2), ?A_TOUT),

    [Conn1] = open_connections(Config, [{0, Username}]),
    Chans1 = [_|_] = open_channels(Conn1, 5),
    ?awaitMatch(1, count_connections_in(Config, Username), ?A_TOUT),
    ?awaitMatch(5, count_channels_in(Config, Username), ?A_TOUT),
    ?awaitMatch(1, tracked_user_connection_count(Config, Username), ?A_TOUT),
    ?awaitMatch(5, tracked_user_channel_count(Config, Username), ?A_TOUT),
    ?awaitMatch(true, is_process_alive(Conn1), ?A_TOUT),
    [?awaitMatch(true, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans1],

    [Conn2] = open_connections(Config, [{1, Username2}]),
    Chans2 = [_|_] = open_channels(Conn2, 5),
    ?awaitMatch(1, count_connections_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(5, count_channels_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(1, tracked_user_connection_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(5, tracked_user_channel_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(true, is_process_alive(Conn2), ?A_TOUT),
    [?awaitMatch(true, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans2],

    mimic_vhost_down(Config, 1, Vhost),
    ?awaitMatch(0, count_connections_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, count_channels_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(false, is_process_alive(Conn2), ?A_TOUT),
    [?awaitMatch(false, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans2],

    %% gen_event notifies local handlers. remote connections still active
    ?awaitMatch(1, count_connections_in(Config, Username), ?A_TOUT),
    ?awaitMatch(5, count_channels_in(Config, Username), ?A_TOUT),
    ?awaitMatch(1, tracked_user_connection_count(Config, Username), ?A_TOUT),
    ?awaitMatch(5, tracked_user_channel_count(Config, Username), ?A_TOUT),
    ?awaitMatch(true, is_process_alive(Conn1), ?A_TOUT),
    [?awaitMatch(true, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans1],

    mimic_vhost_down(Config, 0, Vhost),
    ?awaitMatch(0, count_connections_in(Config, Username), ?A_TOUT),
    ?awaitMatch(0, count_channels_in(Config, Username), ?A_TOUT),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username), ?A_TOUT),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username), ?A_TOUT),
    ?awaitMatch(false, is_process_alive(Conn1), ?A_TOUT),
    [?awaitMatch(false, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans1].

cluster_node_removed(Config) ->
    Username = proplists:get_value(rmq_username, Config),
    Username2 = <<"guest2">>,

    Vhost = proplists:get_value(rmq_vhost, Config),

    rabbit_ct_broker_helpers:add_user(Config, Username2),
    rabbit_ct_broker_helpers:set_full_permissions(Config, Username2, Vhost),

    ?awaitMatch(0, count_connections_in(Config, Username), ?A_TOUT),
    ?awaitMatch(0, count_connections_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, count_channels_in(Config, Username), ?A_TOUT),
    ?awaitMatch(0, count_channels_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username), ?A_TOUT),
    ?awaitMatch(0, tracked_user_connection_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username), ?A_TOUT),
    ?awaitMatch(0, tracked_user_channel_count(Config, Username2), ?A_TOUT),

    [Conn1] = open_connections(Config, [{0, Username}]),
    Chans1 = [_|_] = open_channels(Conn1, 5),
    ?awaitMatch(1, count_connections_in(Config, Username), ?A_TOUT),
    ?awaitMatch(5, count_channels_in(Config, Username), ?A_TOUT),
    ?awaitMatch(1, tracked_user_connection_count(Config, Username), ?A_TOUT),
    ?awaitMatch(5, tracked_user_channel_count(Config, Username), ?A_TOUT),
    ?awaitMatch(true, is_process_alive(Conn1), ?A_TOUT),
    [?awaitMatch(true, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans1],

    [Conn2] = open_connections(Config, [{1, Username2}]),
    Chans2 = [_|_] = open_channels(Conn2, 5),
    ?awaitMatch(1, count_connections_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(5, count_channels_in(Config, Username2), ?A_TOUT),
    ?awaitMatch(1, tracked_user_connection_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(5, tracked_user_channel_count(Config, Username2), ?A_TOUT),
    ?awaitMatch(true, is_process_alive(Conn2), ?A_TOUT),
    [?awaitMatch(true, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans2],

    rabbit_ct_broker_helpers:stop_broker(Config, 1),
    ?awaitMatch(1, count_connections_in(Config, Username), ?A_TOUT),
    ?awaitMatch(5, count_channels_in(Config, Username), ?A_TOUT),
    ?awaitMatch(1, tracked_user_connection_count(Config, Username), ?A_TOUT),
    ?awaitMatch(5, tracked_user_channel_count(Config, Username), ?A_TOUT),
    ?awaitMatch(true, is_process_alive(Conn1), ?A_TOUT),
    [?awaitMatch(true, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans1],

    rabbit_ct_broker_helpers:forget_cluster_node(Config, 0, 1),

    ?awaitMatch(false, is_process_alive(Conn2), ?A_TOUT),
    [?awaitMatch(false, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans2],

    ?awaitMatch(1, count_connections_in(Config, Username), ?A_TOUT),
    ?awaitMatch(5, count_channels_in(Config, Username), ?A_TOUT),
    ?awaitMatch(1, tracked_user_connection_count(Config, Username), ?A_TOUT),
    ?awaitMatch(5, tracked_user_channel_count(Config, Username), ?A_TOUT),
    ?awaitMatch(true, is_process_alive(Conn1), ?A_TOUT),
    [?awaitMatch(true, is_process_alive(Ch), ?A_TOUT) || Ch <- Chans1],

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
        tracked_connection_per_vhost,
        VHost).

exists_in_tracked_connection_per_user_table(Config, Username) ->
    exists_in_tracked_connection_per_user_table(Config, 0, Username).
exists_in_tracked_connection_per_user_table(Config, NodeIndex, Username) ->
    exists_in_tracking_table(Config, NodeIndex,
        tracked_connection_per_user,
        Username).

exists_in_tracked_channel_per_user_table(Config, Username) ->
    exists_in_tracked_channel_per_user_table(Config, 0, Username).
exists_in_tracked_channel_per_user_table(Config, NodeIndex, Username) ->
    exists_in_tracking_table(Config, NodeIndex,
        tracked_channel_per_user,
        Username).

exists_in_tracking_table(Config, NodeIndex, Table, Key) ->
    All = rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                       ets, lookup, [Table, Key]),
    lists:keymember(Key, 1, All).

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

expect_that_client_connection_is_rejected(Config) ->
    expect_that_client_connection_is_rejected(Config, 0).

expect_that_client_connection_is_rejected(Config, NodeIndex) ->
    {error, not_allowed} =
      rabbit_ct_client_helpers:open_unmanaged_connection(Config, NodeIndex).

expect_that_client_connection_is_rejected(Config, NodeIndex, VHost) ->
    {error, not_allowed} =
      rabbit_ct_client_helpers:open_unmanaged_connection(Config, NodeIndex, VHost).
