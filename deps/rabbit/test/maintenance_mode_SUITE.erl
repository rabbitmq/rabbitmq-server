%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(maintenance_mode_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile(export_all).

all() ->
    [
      {group, cluster_size_3},
      {group, quorum_queues}
    ].

groups() ->
    [
      {cluster_size_3, [], [
          maintenance_mode_status,
          listener_suspension_status,
          client_connection_closure
        ]},
      {quorum_queues, [], [
          quorum_queue_leadership_transfer
      ]}
    ].

%% -------------------------------------------------------------------
%% Setup and teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(quorum_queues, Config) ->
    case rabbit_ct_helpers:is_mixed_versions(Config) of
        true ->
            %% In a mixed 3.8/3.9 cluster, unless the 3.8 node is the
            %% one in maintenance mode, a quorum won't be available
            %% due to mixed ra major versions
            {skip, "test not supported in mixed version mode"};
        _ ->
            rabbit_ct_helpers:set_config(Config,
                                         [{rmq_nodes_count, 3}])
    end;
init_per_group(_Group, Config) ->
    rabbit_ct_helpers:set_config(Config,
                                 [{rmq_nodes_count, 3}]).

end_per_group(_, Config) ->
    Config.

init_per_testcase(quorum_queue_leadership_transfer = Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    ClusterSize = ?config(rmq_nodes_count, Config),
    TestNumber = rabbit_ct_helpers:testcase_number(Config, ?MODULE, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_clustered, true},
        {rmq_nodename_suffix, Testcase},
        {tcp_ports_base, {skip_n_nodes, TestNumber * ClusterSize}}
      ]),
    Config2 = rabbit_ct_helpers:run_steps(
                Config1,
                rabbit_ct_broker_helpers:setup_steps() ++
                rabbit_ct_client_helpers:setup_steps()),
    MaintenanceModeFFEnabled = rabbit_ct_broker_helpers:enable_feature_flag(
                                Config2, maintenance_mode_status),
    QuorumQueueFFEnabled = rabbit_ct_broker_helpers:enable_feature_flag(
                                Config2, quorum_queue),
    case MaintenanceModeFFEnabled of
        ok ->
            case QuorumQueueFFEnabled of
                ok ->
                    Config2;
                Skip ->
                    end_per_testcase(Testcase, Config2),
                    Skip
            end;
        Skip ->
            end_per_testcase(Testcase, Config2),
            Skip
    end;
init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    ClusterSize = ?config(rmq_nodes_count, Config),
    TestNumber = rabbit_ct_helpers:testcase_number(Config, ?MODULE, Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_clustered, true},
        {rmq_nodename_suffix, Testcase},
        {tcp_ports_base, {skip_n_nodes, TestNumber * ClusterSize}}
      ]),
    Config2 = rabbit_ct_helpers:run_steps(
                Config1,
                rabbit_ct_broker_helpers:setup_steps() ++
                rabbit_ct_client_helpers:setup_steps() ++
                [fun rabbit_ct_broker_helpers:set_ha_policy_all/1]),
    MaintenanceModeFFEnabled = rabbit_ct_broker_helpers:enable_feature_flag(
                                Config2,
                                maintenance_mode_status),
    case MaintenanceModeFFEnabled of
        ok ->
            Config2;
        Skip ->
            end_per_testcase(Testcase, Config2),
            Skip
    end.

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Test Cases
%% -------------------------------------------------------------------

maintenance_mode_status(Config) ->
    Nodes = [A, B, C] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    [begin
         ?assertNot(rabbit_ct_broker_helpers:is_being_drained_local_read(Config, Node)),
         ?assertNot(rabbit_ct_broker_helpers:is_being_drained_consistent_read(Config, Node))
     end || Node <- Nodes],

    [begin
        [begin
             ?assertNot(rabbit_ct_broker_helpers:is_being_drained_consistent_read(Config, TargetNode, NodeToCheck))
        end || NodeToCheck <- Nodes]
     end || TargetNode <- Nodes],

    rabbit_ct_broker_helpers:mark_as_being_drained(Config, B),
    rabbit_ct_helpers:await_condition(
        fun () -> rabbit_ct_broker_helpers:is_being_drained_local_read(Config, B) end,
        10000),

    [begin
         ?assert(rabbit_ct_broker_helpers:is_being_drained_consistent_read(Config, TargetNode, B))
     end || TargetNode <- Nodes],

    ?assertEqual(
        lists:usort([A, C]),
        lists:usort(rabbit_ct_broker_helpers:rpc(Config, B,
                        rabbit_maintenance, primary_replica_transfer_candidate_nodes, []))),

    rabbit_ct_broker_helpers:unmark_as_being_drained(Config, B),
    rabbit_ct_helpers:await_condition(
        fun () -> not rabbit_ct_broker_helpers:is_being_drained_local_read(Config, B) end,
        10000),

    [begin
         ?assertNot(rabbit_ct_broker_helpers:is_being_drained_local_read(Config, TargetNode, B)),
         ?assertNot(rabbit_ct_broker_helpers:is_being_drained_consistent_read(Config, TargetNode, B))
     end || TargetNode <- Nodes],

    ?assertEqual(
        lists:usort([A, C]),
        lists:usort(rabbit_ct_broker_helpers:rpc(Config, B,
                        rabbit_maintenance, primary_replica_transfer_candidate_nodes, []))),

    ok.


listener_suspension_status(Config) ->
    Nodes = [A | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ct:pal("Picked node ~s for maintenance tests...", [A]),

    rabbit_ct_helpers:await_condition(
        fun () -> not rabbit_ct_broker_helpers:is_being_drained_local_read(Config, A) end, 10000),

    [begin
         ?assertNot(rabbit_ct_broker_helpers:is_being_drained_consistent_read(Config, Node))
     end || Node <- Nodes],

    Conn1 = rabbit_ct_client_helpers:open_connection(Config, A),
    ?assert(is_pid(Conn1)),
    rabbit_ct_client_helpers:close_connection(Conn1),

    rabbit_ct_broker_helpers:drain_node(Config, A),
    rabbit_ct_helpers:await_condition(
        fun () -> rabbit_ct_broker_helpers:is_being_drained_local_read(Config, A) end, 10000),

    ?assertEqual({error, econnrefused}, rabbit_ct_client_helpers:open_unmanaged_connection(Config, A)),

    rabbit_ct_broker_helpers:revive_node(Config, A),
    rabbit_ct_helpers:await_condition(
        fun () -> not rabbit_ct_broker_helpers:is_being_drained_local_read(Config, A) end, 10000),

    Conn3 = rabbit_ct_client_helpers:open_connection(Config, A),
    ?assert(is_pid(Conn3)),
    rabbit_ct_client_helpers:close_connection(Conn3),

    ok.


client_connection_closure(Config) ->
    [A | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    ct:pal("Picked node ~s for maintenance tests...", [A]),

    rabbit_ct_helpers:await_condition(
        fun () -> not rabbit_ct_broker_helpers:is_being_drained_local_read(Config, A) end, 10000),

    Conn1 = rabbit_ct_client_helpers:open_connection(Config, A),
    ?assert(is_pid(Conn1)),
    ?assertEqual(1, length(rabbit_ct_broker_helpers:rpc(Config, A, rabbit_networking, local_connections, []))),

    rabbit_ct_broker_helpers:drain_node(Config, A),
    ?assertEqual(0, length(rabbit_ct_broker_helpers:rpc(Config, A, rabbit_networking, local_connections, []))),

    rabbit_ct_broker_helpers:revive_node(Config, A).


quorum_queue_leadership_transfer(Config) ->
    [A | _] = Nodenames = rabbit_ct_broker_helpers:get_node_configs(
                            Config, nodename),
    ct:pal("Picked node ~s for maintenance tests...", [A]),

    rabbit_ct_helpers:await_condition(
        fun () -> not rabbit_ct_broker_helpers:is_being_drained_local_read(Config, A) end, 10000),

    Conn = rabbit_ct_client_helpers:open_connection(Config, A),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    QName = <<"qq.1">>,
    amqp_channel:call(Ch, #'queue.declare'{queue = QName, durable = true, arguments = [
        {<<"x-queue-type">>, longstr, <<"quorum">>}
    ]}),

    %% we cannot assert on the number of local leaders here: declaring a QQ on node A
    %% does not guarantee that the leader will be hosted on node A

    rabbit_ct_broker_helpers:drain_node(Config, A),
    rabbit_ct_helpers:await_condition(
        fun () -> rabbit_ct_broker_helpers:is_being_drained_local_read(Config, A) end, 10000),

    %% quorum queue leader election is asynchronous
    AllTheSame = quorum_queue_utils:fifo_machines_use_same_version(
                   Config, Nodenames),
    case AllTheSame of
        true ->
            ?awaitMatch(
               LocalLeaders when length(LocalLeaders) == 0,
                                 rabbit_ct_broker_helpers:rpc(
                                   Config, A,
                                   rabbit_amqqueue,
                                   list_local_leaders,
                                   []),
                                 20000);
        false ->
            ct:pal(
              ?LOW_IMPORTANCE,
              "Skip leader election check because rabbit_fifo machines "
              "have different versions", [])
    end,

    rabbit_ct_broker_helpers:revive_node(Config, A).
