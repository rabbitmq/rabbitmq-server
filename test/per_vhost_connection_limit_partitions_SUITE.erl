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

-module(per_vhost_connection_limit_partitions_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

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
                                               fun rabbit_ct_broker_helpers:enable_dist_proxy_manager/1
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
                                                    {rmq_nodename_suffix, Suffix},
                                                    {rmq_nodes_clustered, false}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps() ++ [
        fun rabbit_ct_broker_helpers:enable_dist_proxy/1,
        fun rabbit_ct_broker_helpers:cluster_nodes/1
      ]).

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
    wait_for_count_connections_in(Config, VHost, 6, 60000),

    %% B drops off the network, non-reachable by either A or C
    rabbit_ct_broker_helpers:block_traffic_between(A, B),
    rabbit_ct_broker_helpers:block_traffic_between(B, C),
    timer:sleep(?DELAY),

    %% A and C are still connected, so 4 connections are tracked
    wait_for_count_connections_in(Config, VHost, 4, 60000),

    rabbit_ct_broker_helpers:allow_traffic_between(A, B),
    rabbit_ct_broker_helpers:allow_traffic_between(B, C),
    timer:sleep(?DELAY),

    %% during autoheal B's connections were dropped
    wait_for_count_connections_in(Config, VHost, 4, 60000),

    lists:foreach(fun (Conn) ->
                          (catch rabbit_ct_client_helpers:close_connection(Conn))
                  end, [Conn1, Conn2, Conn3, Conn4,
                        Conn5, Conn6]),

    passed.

%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

wait_for_count_connections_in(Config, VHost, Expected, Time) when Time =< 0 ->
    ?assertEqual(Expected, count_connections_in(Config, VHost));
wait_for_count_connections_in(Config, VHost, Expected, Time) ->
    case count_connections_in(Config, VHost) of
        Expected ->
            ok;
        _ ->
            Sleep = 3000,
            timer:sleep(Sleep),
            wait_for_count_connections_in(Config, VHost, Expected, Time - Sleep)
    end.

count_connections_in(Config, VHost) ->
    count_connections_in(Config, VHost, 0).
count_connections_in(Config, VHost, NodeIndex) ->
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
