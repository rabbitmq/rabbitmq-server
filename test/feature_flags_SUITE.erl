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
%% Copyright (c) 2018-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(feature_flags_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([suite/0,
         all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,

         enable_quorum_queue_in_a_healthy_situation/1,
         enable_unsupported_feature_flag_in_a_healthy_situation/1,
         enable_quorum_queue_when_ff_file_is_unwritable/1,
         enable_quorum_queue_with_a_network_partition/1,
         mark_quorum_queue_as_enabled_with_a_network_partition/1
        ]).

suite() ->
    [{timetrap, 5 * 60000}].

all() ->
    [
     {group, unclustered},
     {group, clustered}
    ].

groups() ->
    [
     {unclustered, [],
      [
       enable_quorum_queue_in_a_healthy_situation,
       enable_unsupported_feature_flag_in_a_healthy_situation,
       enable_quorum_queue_when_ff_file_is_unwritable
      ]},
     {clustered, [],
      [
       enable_quorum_queue_in_a_healthy_situation,
       enable_unsupported_feature_flag_in_a_healthy_situation,
       enable_quorum_queue_when_ff_file_is_unwritable,
       enable_quorum_queue_with_a_network_partition,
       mark_quorum_queue_as_enabled_with_a_network_partition
      ]}
    ].

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

init_per_group(clustered, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 5}]);
init_per_group(unclustered, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 1}]);
init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    TestNumber = rabbit_ct_helpers:testcase_number(Config, ?MODULE, Testcase),
    ClusterSize = ?config(rmq_nodes_count, Config),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodes_clustered, false},
        {rmq_nodename_suffix, Testcase},
        {tcp_ports_base, {skip_n_nodes, TestNumber * ClusterSize}},
        {net_ticktime, 5}
      ]),
    Config2 = rabbit_ct_helpers:merge_app_env(
                Config1,
                {rabbit,
                 [{forced_feature_flags_on_init, []},
                  {log, [{file, [{level, debug}]}]}]}),
    Config3 = rabbit_ct_helpers:run_steps(
                Config2,
                rabbit_ct_broker_helpers:setup_steps() ++
                rabbit_ct_client_helpers:setup_steps() ++
                [fun rabbit_ct_broker_helpers:enable_dist_proxy/1,
                 fun rabbit_ct_broker_helpers:cluster_nodes/1]),
    Ret = rabbit_ct_broker_helpers:rpc(
            Config3, 0, rabbit_feature_flags, is_supported, [quorum_queue]),
    case Ret of
        true ->
            Config3;
        false ->
            end_per_testcase(Testcase, Config3),
            {skip, "Quorum queues are unsupported"}
    end.

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

enable_quorum_queue_in_a_healthy_situation(Config) ->
    FeatureName = quorum_queue,
    ClusterSize = ?config(rmq_nodes_count, Config),
    Node = ClusterSize - 1,
    True = lists:duplicate(ClusterSize, true),
    False = lists:duplicate(ClusterSize, false),

    %% The feature flag is supported but disabled initially.
    ?assertEqual(
       True,
       is_feature_flag_supported(Config, FeatureName)),
    ?assertEqual(
       False,
       is_feature_flag_enabled(Config, FeatureName)),

    %% Enabling the feature flag works.
    ?assertEqual(
       ok,
       enable_feature_flag_on(Config, Node, FeatureName)),
    ?assertEqual(
       True,
       is_feature_flag_enabled(Config, FeatureName)),

    %% Re-enabling the feature flag also works.
    ?assertEqual(
       ok,
       enable_feature_flag_on(Config, Node, FeatureName)),
    ?assertEqual(
       True,
       is_feature_flag_enabled(Config, FeatureName)).

enable_unsupported_feature_flag_in_a_healthy_situation(Config) ->
    FeatureName = unsupported_feature_flag,
    ClusterSize = ?config(rmq_nodes_count, Config),
    Node = ClusterSize - 1,
    False = lists:duplicate(ClusterSize, false),

    %% The feature flag is unsupported and thus disabled.
    ?assertEqual(
       False,
       is_feature_flag_supported(Config, FeatureName)),
    ?assertEqual(
       False,
       is_feature_flag_enabled(Config, FeatureName)),

    %% Enabling the feature flag works.
    ?assertEqual(
       {error, unsupported},
       enable_feature_flag_on(Config, Node, FeatureName)),
    ?assertEqual(
       False,
       is_feature_flag_enabled(Config, FeatureName)).

enable_quorum_queue_when_ff_file_is_unwritable(Config) ->
    FeatureName = quorum_queue,
    ClusterSize = ?config(rmq_nodes_count, Config),
    Node = ClusterSize - 1,
    True = lists:duplicate(ClusterSize, true),
    False = lists:duplicate(ClusterSize, false),
    Files = feature_flags_files(Config),

    %% The feature flag is supported but disabled initially.
    ?assertEqual(
       True,
       is_feature_flag_supported(Config, FeatureName)),
    ?assertEqual(
       False,
       is_feature_flag_enabled(Config, FeatureName)),

    %% Restrict permissions on the `feature_flags` files.
    [?assertEqual(ok, file:change_mode(File, 8#0444)) || File <- Files],

    %% Enabling the feature flag works.
    ?assertEqual(
       ok,
       enable_feature_flag_on(Config, Node, FeatureName)),
    ?assertEqual(
       True,
       is_feature_flag_enabled(Config, FeatureName)),

    %% The `feature_flags` file were not updated.
    ?assertEqual(
       lists:duplicate(ClusterSize, {ok, [[]]}),
       [file:consult(File) || File <- feature_flags_files(Config)]),

    %% Stop all nodes and restore permissions on the `feature_flags` files.
    Nodes = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    [?assertEqual(ok, rabbit_ct_broker_helpers:stop_node(Config, N))
     || N <- Nodes],
    [?assertEqual(ok, file:change_mode(File, 8#0644)) || File <- Files],

    %% Restart all nodes and assert the feature flag is still enabled and
    %% the `feature_flags` files were correctly repaired.
    [?assertEqual(ok, rabbit_ct_broker_helpers:start_node(Config, N))
     || N <- lists:reverse(Nodes)],

    ?assertEqual(
       True,
       is_feature_flag_enabled(Config, FeatureName)),
    ?assertEqual(
       lists:duplicate(ClusterSize, {ok, [[FeatureName]]}),
       [file:consult(File) || File <- feature_flags_files(Config)]).

enable_quorum_queue_with_a_network_partition(Config) ->
    FeatureName = quorum_queue,
    ClusterSize = ?config(rmq_nodes_count, Config),
    [A, B, C, D, E] = rabbit_ct_broker_helpers:get_node_configs(
                        Config, nodename),
    True = lists:duplicate(ClusterSize, true),
    False = lists:duplicate(ClusterSize, false),

    %% The feature flag is supported but disabled initially.
    ?assertEqual(
       True,
       is_feature_flag_supported(Config, FeatureName)),
    ?assertEqual(
       False,
       is_feature_flag_enabled(Config, FeatureName)),

    %% Isolate nodes B and E from the rest of the cluster.
    NodePairs = [{B, A},
                 {B, C},
                 {B, D},
                 {E, A},
                 {E, C},
                 {E, D}],
    block(NodePairs),
    timer:sleep(1000),

    %% Enabling the feature flag should fail in the specific case of
    %% `quorum_queue`, if the network is broken.
    ?assertEqual(
       {error, unsupported},
       enable_feature_flag_on(Config, B, FeatureName)),
    ?assertEqual(
       False,
       is_feature_flag_enabled(Config, FeatureName)),

    %% Repair the network and try again to enable the feature flag.
    unblock(NodePairs),
    timer:sleep(1000),
    [?assertEqual(ok, rabbit_ct_broker_helpers:stop_node(Config, N))
     || N <- [A, C, D]],
    [?assertEqual(ok, rabbit_ct_broker_helpers:start_node(Config, N))
     || N <- [A, C, D]],

    %% Enabling the feature flag works.
    ?assertEqual(
       ok,
       enable_feature_flag_on(Config, B, FeatureName)),
    ?assertEqual(
       True,
       is_feature_flag_enabled(Config, FeatureName)).

mark_quorum_queue_as_enabled_with_a_network_partition(Config) ->
    FeatureName = quorum_queue,
    ClusterSize = ?config(rmq_nodes_count, Config),
    [A, B, C, D, E] = rabbit_ct_broker_helpers:get_node_configs(
                        Config, nodename),
    True = lists:duplicate(ClusterSize, true),
    False = lists:duplicate(ClusterSize, false),

    %% The feature flag is supported but disabled initially.
    ?assertEqual(
       True,
       is_feature_flag_supported(Config, FeatureName)),
    ?assertEqual(
       False,
       is_feature_flag_enabled(Config, FeatureName)),

    %% Isolate node B from the rest of the cluster.
    NodePairs = [{B, A},
                 {B, C},
                 {B, D},
                 {B, E}],
    block(NodePairs),
    timer:sleep(1000),

    %% Mark the feature flag as enabled on all nodes from node B. This
    %% is expected to timeout.
    RemoteNodes = [A, C, D, E],
    ?assertEqual(
       {failed_to_mark_feature_flag_as_enabled_on_remote_nodes,
        FeatureName,
        true,
        RemoteNodes},
       rabbit_ct_broker_helpers:rpc(
         Config, B,
         rabbit_feature_flags, mark_as_enabled_remotely,
         [RemoteNodes, FeatureName, true, 20000])),

    RepairFun = fun() ->
                        %% Wait a few seconds before we repair the network.
                        timer:sleep(5000),

                        %% Repair the network and try again to enable
                        %% the feature flag.
                        unblock(NodePairs),
                        timer:sleep(1000)
                end,
    spawn(RepairFun),

    %% Mark the feature flag as enabled on all nodes from node B. This
    %% is expected to work this time.
    ct:pal(?LOW_IMPORTANCE,
           "Marking the feature flag as enabled on remote nodes...", []),
    ?assertEqual(
       ok,
       rabbit_ct_broker_helpers:rpc(
         Config, B,
         rabbit_feature_flags, mark_as_enabled_remotely,
         [RemoteNodes, FeatureName, true, 120000])).

%% FIXME: Finish the testcase above ^

%% -------------------------------------------------------------------
%% Internal helpers.
%% -------------------------------------------------------------------

enable_feature_flag_on(Config, Node, FeatureName) ->
    rabbit_ct_broker_helpers:rpc(
      Config, Node, rabbit_feature_flags, enable, [FeatureName]).

is_feature_flag_supported(Config, FeatureName) ->
    rabbit_ct_broker_helpers:rpc_all(
      Config, rabbit_feature_flags, is_supported, [FeatureName]).

is_feature_flag_enabled(Config, FeatureName) ->
    rabbit_ct_broker_helpers:rpc_all(
      Config, rabbit_feature_flags, is_enabled, [FeatureName]).

feature_flags_files(Config) ->
    rabbit_ct_broker_helpers:rpc_all(
      Config, rabbit_feature_flags, enabled_feature_flags_list_file, []).

block(Pairs)   -> [block(X, Y) || {X, Y} <- Pairs].
unblock(Pairs) -> [allow(X, Y) || {X, Y} <- Pairs].

block(X, Y) ->
    rabbit_ct_broker_helpers:block_traffic_between(X, Y).

allow(X, Y) ->
    rabbit_ct_broker_helpers:allow_traffic_between(X, Y).
