%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(metadata_store_clustering_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile([nowarn_export_all, export_all]).

suite() ->
    [{timetrap, 5 * 60_000}].

all() ->
    [
      {group, unclustered}
    ].

groups() ->
    [
     {unclustered, [], [{cluster_size_2, [], cluster_size_2_tests()},
                        {cluster_size_3, [], cluster_size_3_tests()}]}
    ].

cluster_size_2_tests() ->
    [
     join_khepri_khepri_cluster,
     join_mnesia_khepri_cluster,
     join_mnesia_khepri_cluster_reverse,
     join_khepri_mnesia_cluster,
     join_khepri_mnesia_cluster_reverse
    ].

cluster_size_3_tests() ->
    [
     join_khepri_khepri_khepri_cluster,
     join_mnesia_khepri_khepri_cluster,
     join_mnesia_khepri_khepri_cluster_reverse,
     join_khepri_mnesia_khepri_cluster,
     join_khepri_mnesia_khepri_cluster_reverse,
     join_khepri_khepri_mnesia_cluster,
     join_khepri_khepri_mnesia_cluster_reverse,
     join_mnesia_mnesia_khepri_cluster,
     join_mnesia_mnesia_khepri_cluster_reverse,
     join_mnesia_khepri_mnesia_cluster,
     join_mnesia_khepri_mnesia_cluster_reverse,
     join_khepri_mnesia_mnesia_cluster,
     join_khepri_mnesia_mnesia_cluster_reverse
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, []).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(unclustered, Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip, "Test suite requires Khepri"};
        false ->
            rabbit_ct_helpers:set_config(Config, [{metadata_store, mnesia},
                                                  {rmq_nodes_clustered, false},
                                                  {tcp_ports_base},
                                                  {net_ticktime, 10}])
    end;
init_per_group(cluster_size_2, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 2}]);
init_per_group(cluster_size_3, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 3}]).

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    Q = rabbit_data_coercion:to_binary(Testcase),
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodename_suffix, Testcase},
                                            {queue_name, Q}
                                           ]),
    Config2 = rabbit_ct_helpers:testcase_started(Config1, Testcase),
    rabbit_ct_helpers:run_steps(Config2,
                                rabbit_ct_broker_helpers:setup_steps() ++
                                    rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
                                          rabbit_ct_client_helpers:teardown_steps() ++
                                              rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

join_khepri_khepri_cluster(Config) ->
    Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, Servers, raft_based_metadata_store_phase1),
    
    join_size_2_cluster(Config, Servers).

join_khepri_mnesia_cluster(Config) ->
    [Server0, _] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server0], raft_based_metadata_store_phase1),

    join_size_2_cluster(Config, Servers).

join_khepri_mnesia_cluster_reverse(Config) ->
    [Server0, _] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server0], raft_based_metadata_store_phase1),

    join_size_2_cluster(Config, lists:reverse(Servers)).

join_mnesia_khepri_cluster(Config) ->
    [_, Server1] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server1], raft_based_metadata_store_phase1),

    join_size_2_cluster(Config, Servers).

join_mnesia_khepri_cluster_reverse(Config) ->
    [_, Server1] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server1], raft_based_metadata_store_phase1),

    join_size_2_cluster(Config, lists:reverse(Servers)).

join_khepri_khepri_khepri_cluster(Config) ->
    Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    
    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, Servers, raft_based_metadata_store_phase1),
    
    join_size_3_cluster(Config, Servers).
    
join_mnesia_khepri_khepri_cluster(Config) ->
    [_, Server1, Server2] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server1, Server2], raft_based_metadata_store_phase1),

    join_size_3_cluster(Config, Servers).

join_mnesia_khepri_khepri_cluster_reverse(Config) ->
    [_, Server1, Server2] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server1, Server2], raft_based_metadata_store_phase1),

    join_size_3_cluster(Config, lists:reverse(Servers)).

join_khepri_mnesia_khepri_cluster(Config) ->
    [Server0, _, Server2] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server0, Server2], raft_based_metadata_store_phase1),

    join_size_3_cluster(Config, Servers).

join_khepri_mnesia_khepri_cluster_reverse(Config) ->
    [Server0, _, Server2] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server0, Server2], raft_based_metadata_store_phase1),

    join_size_3_cluster(Config, lists:reverse(Servers)).

join_khepri_khepri_mnesia_cluster(Config) ->
    [Server0, Server1, _] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server0, Server1], raft_based_metadata_store_phase1),

    join_size_3_cluster(Config, Servers).

join_khepri_khepri_mnesia_cluster_reverse(Config) ->
    [Server0, Server1, _] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server0, Server1], raft_based_metadata_store_phase1),

    join_size_3_cluster(Config, lists:reverse(Servers)).

join_mnesia_mnesia_khepri_cluster(Config) ->
    [_, _, Server2] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server2], raft_based_metadata_store_phase1),

    join_size_3_cluster(Config, Servers).

join_mnesia_mnesia_khepri_cluster_reverse(Config) ->
    [_, _, Server2] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server2], raft_based_metadata_store_phase1),

    join_size_3_cluster(Config, lists:reverse(Servers)).

join_mnesia_khepri_mnesia_cluster(Config) ->
    [_, Server1, _] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    
    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server1], raft_based_metadata_store_phase1),

    join_size_3_cluster(Config, Servers).

join_mnesia_khepri_mnesia_cluster_reverse(Config) ->
    [_, Server1, _] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server1], raft_based_metadata_store_phase1),

    join_size_3_cluster(Config, lists:reverse(Servers)).

join_khepri_mnesia_mnesia_cluster(Config) ->
    [Server0, _, _] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server0], raft_based_metadata_store_phase1),

    join_size_3_cluster(Config, Servers).

join_khepri_mnesia_mnesia_cluster_reverse(Config) ->
    [Server0, _, _] = Servers =
        rabbit_ct_broker_helpers:get_node_configs(Config, nodename),

    ok = rabbit_ct_broker_helpers:enable_feature_flag(Config, [Server0], raft_based_metadata_store_phase1),

    join_size_3_cluster(Config, lists:reverse(Servers)).

join_size_2_cluster(Config, [Server0, Server1]) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q)),
    ?assertMatch([_], rpc:call(Server0, rabbit_amqqueue, list, [])),
    
    ok = rabbit_control_helper:command(stop_app, Server1),
    ?assertMatch([_], rpc:call(Server0, rabbit_amqqueue, list, [])),

    ok = rabbit_control_helper:command(join_cluster, Server1, [atom_to_list(Server0)], []),
    ?assertMatch([_], rpc:call(Server0, rabbit_amqqueue, list, [])),

    rabbit_control_helper:command(start_app, Server1),
    ?assertMatch([_], rpc:call(Server0, rabbit_amqqueue, list, [])).

join_size_3_cluster(Config, [Server0, Server1, Server2]) ->
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server0),
    Q = ?config(queue_name, Config),

    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q)),
    ?assertMatch([_], rpc:call(Server0, rabbit_amqqueue, list, [])),
    
    ok = rabbit_control_helper:command(stop_app, Server1),
    ?assertMatch([_], rpc:call(Server0, rabbit_amqqueue, list, [])),

    ok = rabbit_control_helper:command(join_cluster, Server1, [atom_to_list(Server0)], []),
    ?assertMatch([_], rpc:call(Server0, rabbit_amqqueue, list, [])),

    rabbit_control_helper:command(start_app, Server1),
    ?assertMatch([_], rpc:call(Server0, rabbit_amqqueue, list, [])),

    ok = rabbit_control_helper:command(stop_app, Server2),
    ?assertMatch([_], rpc:call(Server0, rabbit_amqqueue, list, [])),

    ok = rabbit_control_helper:command(join_cluster, Server2, [atom_to_list(Server0)], []),
    ?assertMatch([_], rpc:call(Server0, rabbit_amqqueue, list, [])),

    rabbit_control_helper:command(start_app, Server2),
    ?assertMatch([_], rpc:call(Server0, rabbit_amqqueue, list, [])).

declare(Ch, Q) ->
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                           durable   = true,
                                           auto_delete = false,
                                           arguments = []}).
