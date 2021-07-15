%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(per_vhost_connection_limit_partitions_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile(export_all).

-import(rabbit_ct_client_helpers, [open_unmanaged_connection/2,
                                   open_unmanaged_connection/3]).


all() ->
    [
     {group, net_ticktime_1}
    ].

groups() ->
    [
     {net_ticktime_1, [], [
          cluster_full_partition_with_autoheal
     ]}
    ].

suite() ->
    [
      %% If a test hangs, no need to wait for 30 minutes.
      {timetrap, {minutes, 8}}
    ].

%% see partitions_SUITE
-define(DELAY, 12000).

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, [
      fun rabbit_ct_broker_helpers:configure_dist_proxy/1
    ]).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(net_ticktime_1 = Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [{net_ticktime, 1}]),
    init_per_multinode_group(Group, Config1, 3).

init_per_multinode_group(_Group, Config, NodeCount) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodes_count, NodeCount},
                                                    {rmq_nodename_suffix, Suffix}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

cluster_full_partition_with_autoheal(Config) ->
    VHost = <<"/">>,
    rabbit_ct_broker_helpers:set_partition_handling_mode_globally(Config, autoheal),

    ?assertEqual(0, count_connections_in(Config, VHost)),
    [A, B, C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    %% 6 connections, 2 per node
    Conn1 = open_unmanaged_connection(Config, A),
    Conn2 = open_unmanaged_connection(Config, A),
    Conn3 = open_unmanaged_connection(Config, B),
    Conn4 = open_unmanaged_connection(Config, B),
    Conn5 = open_unmanaged_connection(Config, C),
    Conn6 = open_unmanaged_connection(Config, C),
    ?awaitMatch(Connections when length(Connections) == 6,
                                 connections_in(Config, VHost),
                                 60000, 3000),

    %% B drops off the network, non-reachable by either A or C
    rabbit_ct_broker_helpers:block_traffic_between(A, B),
    rabbit_ct_broker_helpers:block_traffic_between(B, C),
    timer:sleep(?DELAY),

    %% A and C are still connected, so 4 connections are tracked
    ?awaitMatch(Connections when length(Connections) == 4,
                                 connections_in(Config, VHost),
                                 60000, 3000),

    rabbit_ct_broker_helpers:allow_traffic_between(A, B),
    rabbit_ct_broker_helpers:allow_traffic_between(B, C),
    timer:sleep(?DELAY),

    %% during autoheal B's connections were dropped
    ?awaitMatch(Connections when length(Connections) == 4,
                                 connections_in(Config, VHost),
                                 60000, 3000),

    lists:foreach(fun (Conn) ->
                          (catch rabbit_ct_client_helpers:close_connection(Conn))
                  end, [Conn1, Conn2, Conn3, Conn4,
                        Conn5, Conn6]),

    passed.

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

count_connections_in(Config, VHost) ->
    count_connections_in(Config, VHost, 0).
count_connections_in(Config, VHost, NodeIndex) ->
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                 rabbit_connection_tracking,
                                 count_tracked_items_in, [{vhost, VHost}]).

connections_in(Config, VHost) ->
    connections_in(Config, 0, VHost).
connections_in(Config, NodeIndex, VHost) ->
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                 rabbit_connection_tracking,
                                 list, [VHost]).
