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
%% Copyright (c) 2011-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(per_vhost_connection_limit_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-import(rabbit_ct_client_helpers, [open_unmanaged_connection/2,
                                   open_unmanaged_connection/3]).


all() ->
    [
     {group, cluster_size_1},
     {group, cluster_size_2}
    ].

groups() ->
    [
      {cluster_size_1, [], [
          most_basic_single_node_connection_count_test,
          single_node_single_vhost_connection_count_test,
          single_node_multiple_vhost_connection_count_test,
          single_node_list_in_vhost_test,
          single_node_connection_reregistration_idempotency_test,
          single_node_single_vhost_limit_test,
          single_node_multiple_vhost_limit_test,
          single_node_vhost_deletion_forces_connection_closure_test
        ]},
      {cluster_size_2, [], [
          most_basic_cluster_connection_count_test,
          cluster_single_vhost_connection_count_test,
          cluster_multiple_vhost_connection_count_test,
          cluster_node_restart_connection_count_test,
          cluster_node_list_on_node_test,
          cluster_connection_reregistration_idempotency_test,
          cluster_single_vhost_limit_test
        ]}
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

init_per_group(cluster_size_1, Config) ->
    Suffix = rabbit_ct_helpers:testcase_absname(Config, "", "-"),
    Config1 = rabbit_ct_helpers:set_config(Config, [
                                                    {rmq_nodes_count, 1},
                                                    {rmq_nodename_suffix, Suffix}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps());
init_per_group(cluster_size_2, Config) ->
    init_per_multinode_group(cluster_size_2, Config, 2);
init_per_group(partition_handling, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [{net_ticktime, 1}]),
    init_per_multinode_group(partition_handling, Config1, 3).

init_per_multinode_group(_GroupName, Config, NodeCount) ->
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
    rabbit_ct_client_helpers:setup_steps(),
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_client_helpers:teardown_steps(),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Test cases.
%% -------------------------------------------------------------------

most_basic_single_node_connection_count_test(Config) ->
    VHost = <<"/">>,
    ?assertEqual(0, count_connections_in(Config, VHost)),
    Conn = open_unmanaged_connection(Config, 0),
    ?assertEqual(1, count_connections_in(Config, VHost)),
    rabbit_ct_client_helpers:close_connection(Conn),
    ?assertEqual(0, count_connections_in(Config, VHost)),

    passed.

single_node_single_vhost_connection_count_test(Config) ->
    VHost = <<"/">>,
    ?assertEqual(0, count_connections_in(Config, VHost)),

    Conn1 = open_unmanaged_connection(Config, 0),
    ?assertEqual(1, count_connections_in(Config, VHost)),
    rabbit_ct_client_helpers:close_connection(Conn1),
    ?assertEqual(0, count_connections_in(Config, VHost)),

    Conn2 = open_unmanaged_connection(Config, 0),
    ?assertEqual(1, count_connections_in(Config, VHost)),

    Conn3 = open_unmanaged_connection(Config, 0),
    ?assertEqual(2, count_connections_in(Config, VHost)),

    Conn4 = open_unmanaged_connection(Config, 0),
    ?assertEqual(3, count_connections_in(Config, VHost)),

    (catch exit(Conn4, please_terminate)),
    ?assertEqual(2, count_connections_in(Config, VHost)),

    Conn5 = open_unmanaged_connection(Config, 0),
    ?assertEqual(3, count_connections_in(Config, VHost)),

    lists:foreach(fun (C) ->
                          rabbit_ct_client_helpers:close_connection(C)
                  end, [Conn2, Conn3, Conn5]),

    ?assertEqual(0, count_connections_in(Config, VHost)),

    passed.

single_node_multiple_vhost_connection_count_test(Config) ->
    VHost1 = <<"vhost1">>,
    VHost2 = <<"vhost2">>,

    set_up_vhost(Config, VHost1),
    set_up_vhost(Config, VHost2),

    ?assertEqual(0, count_connections_in(Config, VHost1)),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

    Conn1 = open_unmanaged_connection(Config, 0, VHost1),
    ?assertEqual(1, count_connections_in(Config, VHost1)),
    rabbit_ct_client_helpers:close_connection(Conn1),
    ?assertEqual(0, count_connections_in(Config, VHost1)),

    Conn2 = open_unmanaged_connection(Config, 0, VHost2),
    ?assertEqual(1, count_connections_in(Config, VHost2)),

    Conn3 = open_unmanaged_connection(Config, 0, VHost1),
    ?assertEqual(1, count_connections_in(Config, VHost1)),
    ?assertEqual(1, count_connections_in(Config, VHost2)),

    Conn4 = open_unmanaged_connection(Config, 0, VHost1),
    ?assertEqual(2, count_connections_in(Config, VHost1)),

    (catch exit(Conn4, please_terminate)),
    ?assertEqual(1, count_connections_in(Config, VHost1)),

    Conn5 = open_unmanaged_connection(Config, 0, VHost2),
    ?assertEqual(2, count_connections_in(Config, VHost2)),

    Conn6 = open_unmanaged_connection(Config, 0, VHost2),
    ?assertEqual(3, count_connections_in(Config, VHost2)),

    lists:foreach(fun (C) ->
                          rabbit_ct_client_helpers:close_connection(C)
                  end, [Conn2, Conn3, Conn5, Conn6]),

    ?assertEqual(0, count_connections_in(Config, VHost1)),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

    rabbit_ct_broker_helpers:delete_vhost(Config, VHost1),
    rabbit_ct_broker_helpers:delete_vhost(Config, VHost2),

    passed.

single_node_list_in_vhost_test(Config) ->
    VHost1 = <<"vhost1">>,
    VHost2 = <<"vhost2">>,

    set_up_vhost(Config, VHost1),
    set_up_vhost(Config, VHost2),

    ?assertEqual(0, length(connections_in(Config, VHost1))),
    ?assertEqual(0, length(connections_in(Config, VHost2))),

    Conn1 = open_unmanaged_connection(Config, 0, VHost1),
    [#tracked_connection{vhost = VHost1}] = connections_in(Config, VHost1),
    rabbit_ct_client_helpers:close_connection(Conn1),
    ?assertEqual(0, length(connections_in(Config, VHost1))),

    Conn2 = open_unmanaged_connection(Config, 0, VHost2),
    [#tracked_connection{vhost = VHost2}] = connections_in(Config, VHost2),

    Conn3 = open_unmanaged_connection(Config, 0, VHost1),
    [#tracked_connection{vhost = VHost1}] = connections_in(Config, VHost1),

    Conn4 = open_unmanaged_connection(Config, 0, VHost1),
    (catch exit(Conn4, please_terminate)),
    [#tracked_connection{vhost = VHost1}] = connections_in(Config, VHost1),

    Conn5 = open_unmanaged_connection(Config, 0, VHost2),
    Conn6 = open_unmanaged_connection(Config, 0, VHost2),
    [<<"vhost1">>, <<"vhost2">>] =
      lists:usort(lists:map(fun (#tracked_connection{vhost = V}) -> V end,
                     all_connections(Config))),

    lists:foreach(fun (C) ->
                          rabbit_ct_client_helpers:close_connection(C)
                  end, [Conn2, Conn3, Conn5, Conn6]),

    ?assertEqual(0, length(all_connections(Config))),

    rabbit_ct_broker_helpers:delete_vhost(Config, VHost1),
    rabbit_ct_broker_helpers:delete_vhost(Config, VHost2),

    passed.

most_basic_cluster_connection_count_test(Config) ->
    VHost = <<"/">>,
    ?assertEqual(0, count_connections_in(Config, VHost)),
    Conn1 = open_unmanaged_connection(Config, 0),
    ?assertEqual(1, count_connections_in(Config, VHost)),

    Conn2 = open_unmanaged_connection(Config, 1),
    ?assertEqual(2, count_connections_in(Config, VHost)),

    Conn3 = open_unmanaged_connection(Config, 1),
    ?assertEqual(3, count_connections_in(Config, VHost)),

    lists:foreach(fun (C) ->
                          rabbit_ct_client_helpers:close_connection(C)
                  end, [Conn1, Conn2, Conn3]),

    ?assertEqual(0, count_connections_in(Config, VHost)),

    passed.

cluster_single_vhost_connection_count_test(Config) ->
    VHost = <<"/">>,
    ?assertEqual(0, count_connections_in(Config, VHost)),

    Conn1 = open_unmanaged_connection(Config, 0),
    ?assertEqual(1, count_connections_in(Config, VHost)),
    rabbit_ct_client_helpers:close_connection(Conn1),
    ?assertEqual(0, count_connections_in(Config, VHost)),

    Conn2 = open_unmanaged_connection(Config, 1),
    ?assertEqual(1, count_connections_in(Config, VHost)),

    Conn3 = open_unmanaged_connection(Config, 0),
    ?assertEqual(2, count_connections_in(Config, VHost)),

    Conn4 = open_unmanaged_connection(Config, 1),
    ?assertEqual(3, count_connections_in(Config, VHost)),

    (catch exit(Conn4, please_terminate)),
    ?assertEqual(2, count_connections_in(Config, VHost)),

    Conn5 = open_unmanaged_connection(Config, 1),
    ?assertEqual(3, count_connections_in(Config, VHost)),

    lists:foreach(fun (C) ->
                          rabbit_ct_client_helpers:close_connection(C)
                  end, [Conn2, Conn3, Conn5]),

    ?assertEqual(0, count_connections_in(Config, VHost)),

    passed.

cluster_multiple_vhost_connection_count_test(Config) ->
    VHost1 = <<"vhost1">>,
    VHost2 = <<"vhost2">>,

    set_up_vhost(Config, VHost1),
    set_up_vhost(Config, VHost2),

    ?assertEqual(0, count_connections_in(Config, VHost1)),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

    Conn1 = open_unmanaged_connection(Config, 0, VHost1),
    ?assertEqual(1, count_connections_in(Config, VHost1)),
    rabbit_ct_client_helpers:close_connection(Conn1),
    ?assertEqual(0, count_connections_in(Config, VHost1)),

    Conn2 = open_unmanaged_connection(Config, 1, VHost2),
    ?assertEqual(1, count_connections_in(Config, VHost2)),

    Conn3 = open_unmanaged_connection(Config, 1, VHost1),
    ?assertEqual(1, count_connections_in(Config, VHost1)),
    ?assertEqual(1, count_connections_in(Config, VHost2)),

    Conn4 = open_unmanaged_connection(Config, 0, VHost1),
    ?assertEqual(2, count_connections_in(Config, VHost1)),

    (catch exit(Conn4, please_terminate)),
    ?assertEqual(1, count_connections_in(Config, VHost1)),

    Conn5 = open_unmanaged_connection(Config, 1, VHost2),
    ?assertEqual(2, count_connections_in(Config, VHost2)),

    Conn6 = open_unmanaged_connection(Config, 0, VHost2),
    ?assertEqual(3, count_connections_in(Config, VHost2)),

    lists:foreach(fun (C) ->
                          rabbit_ct_client_helpers:close_connection(C)
                  end, [Conn2, Conn3, Conn5, Conn6]),

    ?assertEqual(0, count_connections_in(Config, VHost1)),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

    rabbit_ct_broker_helpers:delete_vhost(Config, VHost1),
    rabbit_ct_broker_helpers:delete_vhost(Config, VHost2),

    passed.

cluster_node_restart_connection_count_test(Config) ->
    VHost = <<"/">>,
    ?assertEqual(0, count_connections_in(Config, VHost)),

    Conn1 = open_unmanaged_connection(Config, 0),
    ?assertEqual(1, count_connections_in(Config, VHost)),
    rabbit_ct_client_helpers:close_connection(Conn1),
    ?assertEqual(0, count_connections_in(Config, VHost)),

    Conn2 = open_unmanaged_connection(Config, 1),
    ?assertEqual(1, count_connections_in(Config, VHost)),

    Conn3 = open_unmanaged_connection(Config, 0),
    ?assertEqual(2, count_connections_in(Config, VHost)),

    Conn4 = open_unmanaged_connection(Config, 1),
    ?assertEqual(3, count_connections_in(Config, VHost)),

    Conn5 = open_unmanaged_connection(Config, 1),
    ?assertEqual(4, count_connections_in(Config, VHost)),

    rabbit_ct_broker_helpers:restart_broker(Config, 1),
    ?assertEqual(1, count_connections_in(Config, VHost)),

    lists:foreach(fun (C) ->
                          (catch rabbit_ct_client_helpers:close_connection(C))
                  end, [Conn2, Conn3, Conn4, Conn5]),

    ?assertEqual(0, count_connections_in(Config, VHost)),

    passed.

cluster_node_list_on_node_test(Config) ->
    [A, B] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ?assertEqual(0, length(all_connections(Config))),
    ?assertEqual(0, length(connections_on_node(Config, 0))),

    Conn1 = open_unmanaged_connection(Config, 0),
    [#tracked_connection{node = A}] = connections_on_node(Config, 0),
    rabbit_ct_client_helpers:close_connection(Conn1),
    ?assertEqual(0, length(connections_on_node(Config, 0))),

    _Conn2 = open_unmanaged_connection(Config, 1),
    [#tracked_connection{node = B}] = connections_on_node(Config, 1),

    Conn3 = open_unmanaged_connection(Config, 0),
    ?assertEqual(1, length(connections_on_node(Config, 0))),

    Conn4 = open_unmanaged_connection(Config, 1),
    ?assertEqual(2, length(connections_on_node(Config, 1))),

    (catch exit(Conn4, please_terminate)),
    ?assertEqual(1, length(connections_on_node(Config, 1))),

    Conn5 = open_unmanaged_connection(Config, 0),
    ?assertEqual(2, length(connections_on_node(Config, 0))),

    rabbit_ct_broker_helpers:stop_broker(Config, 1),
    ?assertEqual(2, length(all_connections(Config))),
    ?assertEqual(0, length(connections_on_node(Config, 0, B))),

    lists:foreach(fun (C) ->
                          rabbit_ct_client_helpers:close_connection(C)
                  end, [Conn3, Conn5]),

    timer:sleep(100),
    ?assertEqual(0, length(all_connections(Config, 0))),

    rabbit_ct_broker_helpers:start_broker(Config, 1),

    passed.

single_node_connection_reregistration_idempotency_test(Config) ->
    VHost = <<"/">>,
    ?assertEqual(0, count_connections_in(Config, VHost)),

    Conn1 = open_unmanaged_connection(Config, 0),
    Conn2 = open_unmanaged_connection(Config, 0),
    Conn3 = open_unmanaged_connection(Config, 0),
    Conn4 = open_unmanaged_connection(Config, 0),
    Conn5 = open_unmanaged_connection(Config, 0),

    ?assertEqual(5, count_connections_in(Config, VHost)),

    reregister_connections_on(Config, 0),
    timer:sleep(100),

    ?assertEqual(5, count_connections_in(Config, VHost)),

    lists:foreach(fun (C) ->
                          rabbit_ct_client_helpers:close_connection(C)
                  end, [Conn1, Conn2, Conn3, Conn4, Conn5]),

    ?assertEqual(0, count_connections_in(Config, VHost)),

    passed.

cluster_connection_reregistration_idempotency_test(Config) ->
    VHost = <<"/">>,

    ?assertEqual(0, count_connections_in(Config, VHost)),

    Conn1 = open_unmanaged_connection(Config, 0),
    Conn2 = open_unmanaged_connection(Config, 1),
    Conn3 = open_unmanaged_connection(Config, 0),
    Conn4 = open_unmanaged_connection(Config, 1),
    Conn5 = open_unmanaged_connection(Config, 1),

    ?assertEqual(5, count_connections_in(Config, VHost)),

    reregister_connections_on(Config, 0),
    reregister_connections_on(Config, 1),
    timer:sleep(100),

    ?assertEqual(5, count_connections_in(Config, VHost)),

    lists:foreach(fun (C) ->
                          rabbit_ct_client_helpers:close_connection(C)
                  end, [Conn1, Conn2, Conn3, Conn4, Conn5]),

    ?assertEqual(0, count_connections_in(Config, VHost)),

    passed.

single_node_single_vhost_limit_test(Config) ->
    VHost = <<"/">>,
    set_vhost_connection_limit(Config, VHost, 3),

    ?assertEqual(0, count_connections_in(Config, VHost)),

    Conn1 = open_unmanaged_connection(Config, 0),
    Conn2 = open_unmanaged_connection(Config, 0),
    Conn3 = open_unmanaged_connection(Config, 0),

    %% we've crossed the limit
    {error, not_allowed} = open_unmanaged_connection(Config, 0),
    {error, not_allowed} = open_unmanaged_connection(Config, 0),
    {error, not_allowed} = open_unmanaged_connection(Config, 0),

    set_vhost_connection_limit(Config, VHost, 5),
    Conn4 = open_unmanaged_connection(Config, 0),
    Conn5 = open_unmanaged_connection(Config, 0),

    lists:foreach(fun (C) ->
                          rabbit_ct_client_helpers:close_connection(C)
                  end, [Conn1, Conn2, Conn3, Conn4, Conn5]),

    ?assertEqual(0, count_connections_in(Config, VHost)),
    set_vhost_connection_limit(Config, VHost, 0),

    passed.

single_node_multiple_vhost_limit_test(Config) ->
    VHost1 = <<"vhost1">>,
    VHost2 = <<"vhost2">>,

    set_up_vhost(Config, VHost1),
    set_up_vhost(Config, VHost2),

    set_vhost_connection_limit(Config, VHost1, 2),
    set_vhost_connection_limit(Config, VHost2, 2),

    ?assertEqual(0, count_connections_in(Config, VHost1)),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

    Conn1 = open_unmanaged_connection(Config, 0, VHost1),
    Conn2 = open_unmanaged_connection(Config, 0, VHost1),
    Conn3 = open_unmanaged_connection(Config, 0, VHost2),
    Conn4 = open_unmanaged_connection(Config, 0, VHost2),

    %% we've crossed the limit
    {error, not_allowed} = open_unmanaged_connection(Config, 0, VHost1),
    {error, not_allowed} = open_unmanaged_connection(Config, 0, VHost2),

    Conn5 = open_unmanaged_connection(Config, 0),

    set_vhost_connection_limit(Config, VHost1, 5),
    set_vhost_connection_limit(Config, VHost2, 5),

    Conn6 = open_unmanaged_connection(Config, 0, VHost1),
    Conn7 = open_unmanaged_connection(Config, 0, VHost1),
    Conn8 = open_unmanaged_connection(Config, 0, VHost1),
    Conn9 = open_unmanaged_connection(Config, 0, VHost2),
    Conn10 = open_unmanaged_connection(Config, 0, VHost2),

    lists:foreach(fun (C) ->
                          rabbit_ct_client_helpers:close_connection(C)
                  end, [Conn1, Conn2, Conn3, Conn4, Conn5,
                        Conn6, Conn7, Conn8, Conn9, Conn10]),

    ?assertEqual(0, count_connections_in(Config, VHost1)),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

    set_vhost_connection_limit(Config, VHost1, 100000000),
    set_vhost_connection_limit(Config, VHost2, 100000000),

    rabbit_ct_broker_helpers:delete_vhost(Config, VHost1),
    rabbit_ct_broker_helpers:delete_vhost(Config, VHost2),

    passed.

cluster_single_vhost_limit_test(Config) ->
    VHost = <<"/">>,
    set_vhost_connection_limit(Config, VHost, 2),

    ?assertEqual(0, count_connections_in(Config, VHost)),

    Conn1 = open_unmanaged_connection(Config, 0, VHost),
    Conn2 = open_unmanaged_connection(Config, 1, VHost),

    %% we've crossed the limit
    {error, not_allowed} = open_unmanaged_connection(Config, 0, VHost),
    {error, not_allowed} = open_unmanaged_connection(Config, 1, VHost),

    set_vhost_connection_limit(Config, VHost, 5),

    Conn3 = open_unmanaged_connection(Config, 0, VHost),
    Conn4 = open_unmanaged_connection(Config, 0, VHost),

    lists:foreach(fun (C) ->
                          rabbit_ct_client_helpers:close_connection(C)
                  end, [Conn1, Conn2, Conn3, Conn4]),

    ?assertEqual(0, count_connections_in(Config, VHost)),

    set_vhost_connection_limit(Config, VHost, 0),

    passed.

single_node_vhost_deletion_forces_connection_closure_test(Config) ->
    VHost1 = <<"vhost1">>,
    VHost2 = <<"vhost2">>,

    set_up_vhost(Config, VHost1),
    set_up_vhost(Config, VHost2),

    ?assertEqual(0, count_connections_in(Config, VHost1)),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

    Conn1 = open_unmanaged_connection(Config, 0, VHost1),
    ?assertEqual(1, count_connections_in(Config, VHost1)),

    _Conn2 = open_unmanaged_connection(Config, 0, VHost2),
    ?assertEqual(1, count_connections_in(Config, VHost2)),

    rabbit_ct_broker_helpers:delete_vhost(Config, VHost2),
    timer:sleep(200),
    ?assertEqual(0, count_connections_in(Config, VHost2)),

    rabbit_ct_client_helpers:close_connection(Conn1),
    ?assertEqual(0, count_connections_in(Config, VHost1)),

    rabbit_ct_broker_helpers:delete_vhost(Config, VHost1),

    passed.


%% -------------------------------------------------------------------
%% Helpers
%% -------------------------------------------------------------------

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

reregister_connections_on(Config, NodeIndex) ->
    Node  = rabbit_ct_broker_helpers:get_node_config(
              Config, NodeIndex, nodename),
    rabbit_ct_broker_helpers:rpc(Config, NodeIndex,
                                rabbit_connection_tracker,
                                reregister,
                                [Node]).

set_up_vhost(Config, VHost) ->
    rabbit_ct_broker_helpers:add_vhost(Config, VHost),
    rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VHost).

set_vhost_connection_limit(Config, VHost, Count) ->
    set_vhost_connection_limit(Config, 0, VHost, Count).

set_vhost_connection_limit(Config, NodeIndex, VHost, Count) ->
    Node  = rabbit_ct_broker_helpers:get_node_config(
              Config, NodeIndex, nodename),
    rabbit_ct_broker_helpers:control_action(
      set_vhost_limits, Node,
      ["{\"max-connections\": " ++ integer_to_list(Count) ++ "}"],
      [{"-p", binary_to_list(VHost)}]).
