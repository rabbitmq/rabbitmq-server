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

         registry/1,
         enable_quorum_queue_in_a_healthy_situation/1,
         enable_unsupported_feature_flag_in_a_healthy_situation/1,
         enable_quorum_queue_when_ff_file_is_unwritable/1,
         enable_quorum_queue_with_a_network_partition/1,
         mark_quorum_queue_as_enabled_with_a_network_partition/1,

         clustering_ok_with_ff_disabled_everywhere/1,
         clustering_ok_with_ff_enabled_on_some_nodes/1,
         clustering_ok_with_ff_enabled_everywhere/1,
         clustering_ok_with_new_ff_disabled/1,
         clustering_denied_with_new_ff_enabled/1,
         clustering_ok_with_new_ff_disabled_from_plugin_on_some_nodes/1,
         clustering_ok_with_new_ff_enabled_from_plugin_on_some_nodes/1,
         activating_plugin_with_new_ff_disabled/1,
         activating_plugin_with_new_ff_enabled/1
        ]).

-rabbit_feature_flag(
   {ff_a,
    #{desc          => "Feature flag A",
      stability     => stable
     }}).

-rabbit_feature_flag(
   {ff_b,
    #{desc          => "Feature flag B",
      stability     => stable
     }}).

suite() ->
    [{timetrap, 5 * 60000}].

all() ->
    [
     {group, registry},
     {group, enabling_on_single_node},
     {group, enabling_in_cluster},
     {group, clustering},
     {group, activating_plugin}
    ].

groups() ->
    [
     {registry, [],
      [
       registry
      ]},
     {enabling_on_single_node, [],
      [
       enable_quorum_queue_in_a_healthy_situation,
       enable_unsupported_feature_flag_in_a_healthy_situation,
       enable_quorum_queue_when_ff_file_is_unwritable
      ]},
     {enabling_in_cluster, [],
      [
       enable_quorum_queue_in_a_healthy_situation,
       enable_unsupported_feature_flag_in_a_healthy_situation,
       enable_quorum_queue_when_ff_file_is_unwritable,
       enable_quorum_queue_with_a_network_partition,
       mark_quorum_queue_as_enabled_with_a_network_partition
      ]},
     {clustering, [],
      [
       clustering_ok_with_ff_disabled_everywhere,
       clustering_ok_with_ff_enabled_on_some_nodes,
       clustering_ok_with_ff_enabled_everywhere,
       clustering_ok_with_new_ff_disabled,
       clustering_denied_with_new_ff_enabled,
       clustering_ok_with_new_ff_disabled_from_plugin_on_some_nodes,
       clustering_ok_with_new_ff_enabled_from_plugin_on_some_nodes
      ]},
     {activating_plugin, [],
      [
       activating_plugin_with_new_ff_disabled,
       activating_plugin_with_new_ff_enabled
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

init_per_group(enabling_on_single_node, Config) ->
    rabbit_ct_helpers:set_config(
      Config,
      [{rmq_nodes_count, 1}]);
init_per_group(enabling_in_cluster, Config) ->
    rabbit_ct_helpers:set_config(
      Config,
      [{rmq_nodes_count, 5},
       {rmq_nodes_clustered, false}]);
init_per_group(clustering, Config) ->
    DepsDir = ?config(erlang_mk_depsdir, Config),
    PluginSrcDir = filename:join(?config(data_dir, Config), "my_plugin"),
    Args = ["dist",
            "SKIP_DEPS=1",
            {"DEPS_DIR=~s", [DepsDir]}],
    case rabbit_ct_helpers:make(Config, PluginSrcDir, Args) of
        {ok, _} ->
            PluginsDir1 = filename:join(?config(current_srcdir, Config),
                                        "plugins"),
            PluginsDir2 = filename:join(PluginSrcDir, "plugins"),
            PluginsDir = PluginsDir1 ++ ":" ++ PluginsDir2,
            rabbit_ct_helpers:set_config(
              Config,
              [{rmq_nodes_count, 2},
               {rmq_nodes_clustered, false},
               {rmq_plugins_dir, PluginsDir},
               {start_rmq_with_plugins_disabled, true}]);
        {error, _} ->
            {skip, "Failed to compile the `my_plugin` test plugin"}
    end;
init_per_group(activating_plugin, Config) ->
    DepsDir = ?config(erlang_mk_depsdir, Config),
    PluginSrcDir = filename:join(?config(data_dir, Config), "my_plugin"),
    Args = ["test-dist",
            {"DEPS_DIR=~s", [DepsDir]}],
    case rabbit_ct_helpers:make(Config, PluginSrcDir, Args) of
        {ok, _} ->
            PluginsDir = filename:join(PluginSrcDir, "plugins"),
            rabbit_ct_helpers:set_config(
              Config,
              [{rmq_nodes_count, 2},
               {rmq_nodes_clustered, true},
               {rmq_plugins_dir, PluginsDir},
               {start_rmq_with_plugins_disabled, true}]);
        {error, _} ->
            {skip, "Failed to compile the `my_plugin` test plugin"}
    end;
init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    TestNumber = rabbit_ct_helpers:testcase_number(Config, ?MODULE, Testcase),
    case ?config(tc_group_properties, Config) of
        [{name, registry} | _] ->
            application:set_env(
              lager,
              handlers, [{lager_console_backend, [{level, debug}]}]),
            application:set_env(
              lager,
              extra_sinks,
              [{rabbit_log_lager_event,
                [{handlers, [{lager_console_backend, [{level, debug}]}]}]
               }]),
            lager:start(),
            FeatureFlagsFile = filename:join(?config(priv_dir, Config),
                                             rabbit_misc:format(
                                               "feature_flags-~s",
                                               [Testcase])),
            application:set_env(rabbit, feature_flags_file, FeatureFlagsFile),
            rabbit_ct_helpers:set_config(
              Config, {feature_flags_file, FeatureFlagsFile});
        [{name, Name} | _]
          when Name =:= enabling_on_single_node orelse
               Name =:= clustering orelse
               Name =:= activating_plugin ->
            ClusterSize = ?config(rmq_nodes_count, Config),
            Config1 = rabbit_ct_helpers:set_config(
                        Config,
                        [{rmq_nodename_suffix, Testcase},
                         {tcp_ports_base, {skip_n_nodes,
                                           TestNumber * ClusterSize}}
                        ]),
            Config2 = rabbit_ct_helpers:merge_app_env(
                        Config1,
                        {rabbit,
                         [{forced_feature_flags_on_init, []},
                          {log, [{file, [{level, debug}]}]}]}),
            Config3 = rabbit_ct_helpers:run_steps(
                        Config2,
                        rabbit_ct_broker_helpers:setup_steps() ++
                        rabbit_ct_client_helpers:setup_steps()),
            case Config3 of
                {skip, _} ->
                    Config3;
                _ ->
                    QQSupported =
                    rabbit_ct_broker_helpers:is_feature_flag_supported(
                      Config3, quorum_queue),
                    case QQSupported of
                        true ->
                            Config3;
                        false ->
                            end_per_testcase(Testcase, Config3),
                            {skip, "Quorum queues are unsupported"}
                    end
            end;
        [{name, enabling_in_cluster} | _] ->
            ClusterSize = ?config(rmq_nodes_count, Config),
            Config1 = rabbit_ct_helpers:set_config(
                        Config,
                        [{rmq_nodename_suffix, Testcase},
                         {tcp_ports_base, {skip_n_nodes,
                                           TestNumber * ClusterSize}},
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
            case Config3 of
                {skip, _} ->
                    Config3;
                _ ->
                    QQSupported =
                    rabbit_ct_broker_helpers:is_feature_flag_supported(
                      Config3, quorum_queue),
                    case QQSupported of
                        true ->
                            Config3;
                        false ->
                            end_per_testcase(Testcase, Config3),
                            {skip, "Quorum queues are unsupported"}
                    end
            end
    end.

end_per_testcase(Testcase, Config) ->
    Config1 = case ?config(tc_group_properties, Config) of
                  [{name, registry} | _] ->
                      Config;
                  _ ->
                      rabbit_ct_helpers:run_steps(
                        Config,
                        rabbit_ct_client_helpers:teardown_steps() ++
                        rabbit_ct_broker_helpers:teardown_steps())
              end,
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

-define(list_ff(Which),
        lists:sort(maps:keys(rabbit_ff_registry:list(Which)))).

registry(_Config) ->
    %% At first, the registry must be uninitialized.
    ?assertNot(rabbit_ff_registry:is_registry_initialized()),

    %% After initialization, it must know about the feature flags
    %% declared in this testsuite. They must be disabled however.
    rabbit_feature_flags:initialize_registry(),
    ?assert(rabbit_ff_registry:is_registry_initialized()),
    ?assertMatch([ff_a, ff_b], ?list_ff(all)),

    ?assert(rabbit_ff_registry:is_supported(ff_a)),
    ?assert(rabbit_ff_registry:is_supported(ff_b)),
    ?assertNot(rabbit_ff_registry:is_supported(ff_c)),
    ?assertNot(rabbit_ff_registry:is_supported(ff_d)),

    ?assertEqual(erlang:map_size(rabbit_ff_registry:states()), 0),
    ?assertMatch([], ?list_ff(enabled)),
    ?assertMatch([], ?list_ff(state_changing)),
    ?assertMatch([ff_a, ff_b], ?list_ff(disabled)),
    ?assertNot(rabbit_ff_registry:is_enabled(ff_a)),
    ?assertNot(rabbit_ff_registry:is_enabled(ff_b)),
    ?assertNot(rabbit_ff_registry:is_enabled(ff_c)),
    ?assertNot(rabbit_ff_registry:is_enabled(ff_d)),

    %% We can declare a new feature flag at runtime. All of them are
    %% supported but still disabled.
    NewFeatureFlags = #{ff_c =>
                        #{desc => "Feature flag C",
                          provided_by => feature_flags_SUITE,
                          stability => stable}},
    rabbit_feature_flags:initialize_registry(NewFeatureFlags),
    ?assertMatch([ff_a, ff_b, ff_c],
                 lists:sort(maps:keys(rabbit_ff_registry:list(all)))),

    ?assert(rabbit_ff_registry:is_supported(ff_a)),
    ?assert(rabbit_ff_registry:is_supported(ff_b)),
    ?assert(rabbit_ff_registry:is_supported(ff_c)),
    ?assertNot(rabbit_ff_registry:is_supported(ff_d)),

    ?assertEqual(erlang:map_size(rabbit_ff_registry:states()), 0),
    ?assertMatch([], ?list_ff(enabled)),
    ?assertMatch([], ?list_ff(state_changing)),
    ?assertMatch([ff_a, ff_b, ff_c], ?list_ff(disabled)),
    ?assertNot(rabbit_ff_registry:is_enabled(ff_a)),
    ?assertNot(rabbit_ff_registry:is_enabled(ff_b)),
    ?assertNot(rabbit_ff_registry:is_enabled(ff_c)),
    ?assertNot(rabbit_ff_registry:is_enabled(ff_d)),

    %% After enabling `ff_a`, it is actually the case. Others are
    %% supported but remain disabled.
    rabbit_feature_flags:initialize_registry(#{},
                                             #{ff_a => true},
                                             true),
    ?assertMatch([ff_a, ff_b, ff_c],
                 lists:sort(maps:keys(rabbit_ff_registry:list(all)))),

    ?assert(rabbit_ff_registry:is_supported(ff_a)),
    ?assert(rabbit_ff_registry:is_supported(ff_b)),
    ?assert(rabbit_ff_registry:is_supported(ff_c)),
    ?assertNot(rabbit_ff_registry:is_supported(ff_d)),

    ?assertMatch(#{ff_a := true}, rabbit_ff_registry:states()),
    ?assertMatch([ff_a], ?list_ff(enabled)),
    ?assertMatch([], ?list_ff(state_changing)),
    ?assertMatch([ff_b, ff_c], ?list_ff(disabled)),
    ?assert(rabbit_ff_registry:is_enabled(ff_a)),
    ?assertNot(rabbit_ff_registry:is_enabled(ff_b)),
    ?assertNot(rabbit_ff_registry:is_enabled(ff_c)),
    ?assertNot(rabbit_ff_registry:is_enabled(ff_d)),

    %% This time, we mark the state of `ff_c` as `state_changing`. We
    %% expect all other feature flag states to remain unchanged.
    rabbit_feature_flags:initialize_registry(#{},
                                             #{ff_a => false,
                                               ff_c => state_changing},
                                             true),
    ?assertMatch([ff_a, ff_b, ff_c],
                 lists:sort(maps:keys(rabbit_ff_registry:list(all)))),

    ?assert(rabbit_ff_registry:is_supported(ff_a)),
    ?assert(rabbit_ff_registry:is_supported(ff_b)),
    ?assert(rabbit_ff_registry:is_supported(ff_c)),
    ?assertNot(rabbit_ff_registry:is_supported(ff_d)),

    ?assertMatch(#{ff_c := state_changing}, rabbit_ff_registry:states()),
    ?assertMatch([], ?list_ff(enabled)),
    ?assertMatch([ff_c], ?list_ff(state_changing)),
    ?assertMatch([ff_a, ff_b], ?list_ff(disabled)),
    ?assertNot(rabbit_ff_registry:is_enabled(ff_a)),
    ?assertNot(rabbit_ff_registry:is_enabled(ff_b)),
    ?assertMatch(state_changing, rabbit_ff_registry:is_enabled(ff_c)),
    ?assertNot(rabbit_ff_registry:is_enabled(ff_d)),

    %% Finally, we disable `ff_c`. All of them are supported but
    %% disabled.
    rabbit_feature_flags:initialize_registry(#{},
                                             #{ff_b => false,
                                               ff_c => false},
                                             true),
    ?assertMatch([ff_a, ff_b, ff_c],
                 lists:sort(maps:keys(rabbit_ff_registry:list(all)))),

    ?assert(rabbit_ff_registry:is_supported(ff_a)),
    ?assert(rabbit_ff_registry:is_supported(ff_b)),
    ?assert(rabbit_ff_registry:is_supported(ff_c)),
    ?assertNot(rabbit_ff_registry:is_supported(ff_d)),

    ?assertEqual(erlang:map_size(rabbit_ff_registry:states()), 0),
    ?assertMatch([], ?list_ff(enabled)),
    ?assertMatch([], ?list_ff(state_changing)),
    ?assertMatch([ff_a, ff_b, ff_c], ?list_ff(disabled)),
    ?assertNot(rabbit_ff_registry:is_enabled(ff_a)),
    ?assertNot(rabbit_ff_registry:is_enabled(ff_b)),
    ?assertNot(rabbit_ff_registry:is_enabled(ff_c)),
    ?assertNot(rabbit_ff_registry:is_enabled(ff_d)).

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

clustering_ok_with_ff_disabled_everywhere(Config) ->
    %% All feature flags are disabled. Clustering the two nodes should be
    %% accepted because they are compatible.

    FFSubsysOk = is_feature_flag_subsystem_available(Config),

    log_feature_flags_of_all_nodes(Config),
    case FFSubsysOk of
        true  -> ?assertEqual([false, false],
                              is_feature_flag_enabled(Config, quorum_queue));
        false -> ok
    end,

    ?assertEqual(Config, rabbit_ct_broker_helpers:cluster_nodes(Config)),

    log_feature_flags_of_all_nodes(Config),
    case FFSubsysOk of
        true  -> ?assertEqual([false, false],
                              is_feature_flag_enabled(Config, quorum_queue));
        false -> ok
    end,
    ok.

clustering_ok_with_ff_enabled_on_some_nodes(Config) ->
    %% All feature flags are enabled on node 1, but not on node 2.
    %% Clustering the two nodes should be accepted because they are
    %% compatible. Also, feature flags will be enabled on node 2 as a
    %% consequence.
    enable_all_feature_flags_on(Config, 0),

    FFSubsysOk = is_feature_flag_subsystem_available(Config),

    log_feature_flags_of_all_nodes(Config),
    case FFSubsysOk of
        true  -> ?assertEqual([true, false],
                              is_feature_flag_enabled(Config, quorum_queue));
        false -> ok
    end,

    ?assertEqual(Config, rabbit_ct_broker_helpers:cluster_nodes(Config)),

    log_feature_flags_of_all_nodes(Config),
    case FFSubsysOk of
        true  -> ?assertEqual([true, true],
                              is_feature_flag_enabled(Config, quorum_queue));
        false -> ok
    end,
    ok.

clustering_ok_with_ff_enabled_everywhere(Config) ->
    %% All feature flags are enabled. Clustering the two nodes should be
    %% accepted because they are compatible.
    enable_all_feature_flags_everywhere(Config),

    FFSubsysOk = is_feature_flag_subsystem_available(Config),

    log_feature_flags_of_all_nodes(Config),
    case FFSubsysOk of
        true  -> ?assertEqual([true, true],
                              is_feature_flag_enabled(Config, quorum_queue));
        false -> ok
    end,

    ?assertEqual(Config, rabbit_ct_broker_helpers:cluster_nodes(Config)),

    log_feature_flags_of_all_nodes(Config),
    case FFSubsysOk of
        true  -> ?assertEqual([true, true],
                              is_feature_flag_enabled(Config, quorum_queue));
        false -> ok
    end,
    ok.

clustering_ok_with_new_ff_disabled(Config) ->
    %% We declare a new (fake) feature flag on node 1. Clustering the
    %% two nodes should still be accepted because that feature flag is
    %% disabled.
    NewFeatureFlags = #{time_travel =>
                        #{desc => "Time travel with RabbitMQ",
                          provided_by => rabbit,
                          stability => stable}},
    rabbit_ct_broker_helpers:rpc(
      Config, 0,
      rabbit_feature_flags, initialize_registry, [NewFeatureFlags]),

    FFSubsysOk = is_feature_flag_subsystem_available(Config),

    log_feature_flags_of_all_nodes(Config),
    case FFSubsysOk of
        true  -> ?assertEqual([true, false],
                              is_feature_flag_supported(Config, time_travel)),
                 ?assertEqual([false, false],
                              is_feature_flag_enabled(Config, time_travel));
        false -> ok
    end,

    ?assertEqual(Config, rabbit_ct_broker_helpers:cluster_nodes(Config)),

    log_feature_flags_of_all_nodes(Config),
    case FFSubsysOk of
        true  -> ?assertEqual([false, false],
                              is_feature_flag_supported(Config, time_travel)),
                 ?assertEqual([false, false],
                              is_feature_flag_enabled(Config, time_travel));
        false -> ok
    end,
    ok.

clustering_denied_with_new_ff_enabled(Config) ->
    %% We declare a new (fake) feature flag on node 1. Clustering the
    %% two nodes should then be forbidden because node 2 is sure it does
    %% not support it (because the application, `rabbit` is loaded and
    %% it does not have it).
    NewFeatureFlags = #{time_travel =>
                        #{desc => "Time travel with RabbitMQ",
                          provided_by => rabbit,
                          stability => stable}},
    rabbit_ct_broker_helpers:rpc(
      Config, 0,
      rabbit_feature_flags, initialize_registry, [NewFeatureFlags]),
    enable_feature_flag_on(Config, 0, time_travel),

    FFSubsysOk = is_feature_flag_subsystem_available(Config),

    log_feature_flags_of_all_nodes(Config),
    case FFSubsysOk of
        true  -> ?assertEqual([true, false],
                              is_feature_flag_supported(Config, time_travel)),
                 ?assertEqual([true, false],
                              is_feature_flag_enabled(Config, time_travel));
        false -> ok
    end,

    ?assertMatch({skip, _}, rabbit_ct_broker_helpers:cluster_nodes(Config)),

    log_feature_flags_of_all_nodes(Config),
    case FFSubsysOk of
        true  -> ?assertEqual([true, false],
                              is_feature_flag_supported(Config, time_travel)),
                 ?assertEqual([true, false],
                              is_feature_flag_enabled(Config, time_travel));
        false -> ok
    end,
    ok.

clustering_ok_with_new_ff_disabled_from_plugin_on_some_nodes(Config) ->
    %% We first enable the test plugin on node 1, then we try to cluster
    %% them. Even though both nodes don't share the same feature
    %% flags (the test plugin exposes one), they should be considered
    %% compatible and the clustering should be allowed.
    rabbit_ct_broker_helpers:enable_plugin(Config, 0, "my_plugin"),

    FFSubsysOk = is_feature_flag_subsystem_available(Config),

    log_feature_flags_of_all_nodes(Config),
    case FFSubsysOk of
        true  -> ?assertEqual([true, false],
                              is_feature_flag_supported(Config, plugin_ff)),
                 ?assertEqual([false, false],
                              is_feature_flag_enabled(Config, plugin_ff));
        false -> ok
    end,

    ?assertEqual(Config, rabbit_ct_broker_helpers:cluster_nodes(Config)),

    log_feature_flags_of_all_nodes(Config),
    case FFSubsysOk of
        true  -> ?assertEqual([true, true],
                              is_feature_flag_supported(Config, plugin_ff)),
                 ?assertEqual([false, false],
                              is_feature_flag_enabled(Config, plugin_ff));
        false -> ok
    end,
    ok.

clustering_ok_with_new_ff_enabled_from_plugin_on_some_nodes(Config) ->
    %% We first enable the test plugin on node 1 and enable its feature
    %% flag, then we try to cluster them. Even though both nodes don't
    %% share the same feature flags (the test plugin exposes one), they
    %% should be considered compatible and the clustering should be
    %% allowed.
    rabbit_ct_broker_helpers:enable_plugin(Config, 0, "my_plugin"),
    enable_all_feature_flags_on(Config, 0),

    FFSubsysOk = is_feature_flag_subsystem_available(Config),

    log_feature_flags_of_all_nodes(Config),
    case FFSubsysOk of
        true  -> ?assertEqual([true, false],
                              is_feature_flag_supported(Config, plugin_ff)),
                 ?assertEqual([true, false],
                              is_feature_flag_enabled(Config, plugin_ff));
        false -> ok
    end,

    ?assertEqual(Config, rabbit_ct_broker_helpers:cluster_nodes(Config)),

    log_feature_flags_of_all_nodes(Config),
    case FFSubsysOk of
        true  -> ?assertEqual([true, true],
                              is_feature_flag_supported(Config, plugin_ff)),
                 ?assertEqual([true, true],
                              is_feature_flag_enabled(Config, plugin_ff));
        false -> ok
    end,
    ok.

activating_plugin_with_new_ff_disabled(Config) ->
    %% Both nodes are clustered. A new plugin is enabled on node 1
    %% and this plugin has a new feature flag node 2 does know about.
    %% Enabling the plugin is allowed because nodes remain compatible,
    %% as the plugin is missing on one node so it can't conflict.

    FFSubsysOk = is_feature_flag_subsystem_available(Config),

    log_feature_flags_of_all_nodes(Config),
    case FFSubsysOk of
        true  -> ?assertEqual([false, false],
                              is_feature_flag_supported(Config, plugin_ff)),
                 ?assertEqual([false, false],
                              is_feature_flag_enabled(Config, plugin_ff));
        false -> ok
    end,

    rabbit_ct_broker_helpers:enable_plugin(Config, 0, "my_plugin"),

    log_feature_flags_of_all_nodes(Config),
    case FFSubsysOk of
        true  -> ?assertEqual([true, true],
                              is_feature_flag_supported(Config, plugin_ff)),
                 ?assertEqual([false, false],
                              is_feature_flag_enabled(Config, plugin_ff));
        false -> ok
    end,
    ok.

activating_plugin_with_new_ff_enabled(Config) ->
    %% Both nodes are clustered. A new plugin is enabled on node 1
    %% and this plugin has a new feature flag node 2 does know about.
    %% Enabling the plugin is allowed because nodes remain compatible,
    %% as the plugin is missing on one node so it can't conflict.
    %% Enabling the plugin's feature flag is also permitted for this
    %% same reason.

    FFSubsysOk = is_feature_flag_subsystem_available(Config),

    log_feature_flags_of_all_nodes(Config),
    case FFSubsysOk of
        true  -> ?assertEqual([false, false],
                              is_feature_flag_supported(Config, plugin_ff)),
                 ?assertEqual([false, false],
                              is_feature_flag_enabled(Config, plugin_ff));
        false -> ok
    end,

    rabbit_ct_broker_helpers:enable_plugin(Config, 0, "my_plugin"),
    enable_feature_flag_on(Config, 0, plugin_ff),

    log_feature_flags_of_all_nodes(Config),
    case FFSubsysOk of
        true  -> ?assertEqual([true, true],
                              is_feature_flag_supported(Config, plugin_ff)),
                 ?assertEqual([true, true],
                              is_feature_flag_enabled(Config, plugin_ff));
        false -> ok
    end,
    ok.

%% -------------------------------------------------------------------
%% Internal helpers.
%% -------------------------------------------------------------------

enable_feature_flag_on(Config, Node, FeatureName) ->
    rabbit_ct_broker_helpers:rpc(
      Config, Node, rabbit_feature_flags, enable, [FeatureName]).

enable_all_feature_flags_on(Config, Node) ->
    rabbit_ct_broker_helpers:rpc(
      Config, Node, rabbit_feature_flags, enable_all, []).

enable_all_feature_flags_everywhere(Config) ->
    rabbit_ct_broker_helpers:rpc_all(
      Config, rabbit_feature_flags, enable_all, []).

is_feature_flag_supported(Config, FeatureName) ->
    rabbit_ct_broker_helpers:rpc_all(
      Config, rabbit_feature_flags, is_supported, [FeatureName]).

is_feature_flag_enabled(Config, FeatureName) ->
    rabbit_ct_broker_helpers:rpc_all(
      Config, rabbit_feature_flags, is_enabled, [FeatureName]).

is_feature_flag_subsystem_available(Config) ->
    lists:all(
      fun(B) -> B end,
      rabbit_ct_broker_helpers:rpc_all(
        Config, erlang, function_exported, [rabbit_feature_flags, list, 0])).

feature_flags_files(Config) ->
    rabbit_ct_broker_helpers:rpc_all(
      Config, rabbit_feature_flags, enabled_feature_flags_list_file, []).

log_feature_flags_of_all_nodes(Config) ->
    rabbit_ct_broker_helpers:rpc_all(
      Config, rabbit_feature_flags, info, [#{color => false,
                                             lines => false}]).

block(Pairs)   -> [block(X, Y) || {X, Y} <- Pairs].
unblock(Pairs) -> [allow(X, Y) || {X, Y} <- Pairs].

block(X, Y) ->
    rabbit_ct_broker_helpers:block_traffic_between(X, Y).

allow(X, Y) ->
    rabbit_ct_broker_helpers:allow_traffic_between(X, Y).
