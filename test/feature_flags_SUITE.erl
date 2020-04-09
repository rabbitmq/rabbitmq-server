%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2018-2020 VMware, Inc. or its affiliates.  All rights reserved.
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

         registry_general_usage/1,
         registry_concurrent_reloads/1,
         enable_feature_flag_in_a_healthy_situation/1,
         enable_unsupported_feature_flag_in_a_healthy_situation/1,
         enable_feature_flag_when_ff_file_is_unwritable/1,
         enable_feature_flag_with_a_network_partition/1,
         mark_feature_flag_as_enabled_with_a_network_partition/1,

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

suite() ->
    [{timetrap, {minutes, 15}}].

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
       registry_general_usage,
       registry_concurrent_reloads
      ]},
     {enabling_on_single_node, [],
      [
       enable_feature_flag_in_a_healthy_situation,
       enable_unsupported_feature_flag_in_a_healthy_situation,
       enable_feature_flag_when_ff_file_is_unwritable
      ]},
     {enabling_in_cluster, [],
      [
       enable_feature_flag_in_a_healthy_situation,
       enable_unsupported_feature_flag_in_a_healthy_situation,
       enable_feature_flag_when_ff_file_is_unwritable,
       enable_feature_flag_with_a_network_partition,
       mark_feature_flag_as_enabled_with_a_network_partition
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
    Config1 = rabbit_ct_helpers:set_config(
                Config,
                [{rmq_nodes_count, 2},
                 {rmq_nodes_clustered, false},
                 {start_rmq_with_plugins_disabled, true}]),
    rabbit_ct_helpers:run_setup_steps(Config1, [
        fun build_my_plugin/1,
        fun work_around_cli_and_rabbit_circular_dep/1
      ]);
init_per_group(activating_plugin, Config) ->
    Config1 = rabbit_ct_helpers:set_config(
                Config,
                [{rmq_nodes_count, 2},
                 {rmq_nodes_clustered, true},
                 {start_rmq_with_plugins_disabled, true}]),
    rabbit_ct_helpers:run_setup_steps(Config1, [
        fun build_my_plugin/1,
        fun work_around_cli_and_rabbit_circular_dep/1
      ]);
init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    TestNumber = rabbit_ct_helpers:testcase_number(Config, ?MODULE, Testcase),
    case ?config(tc_group_properties, Config) of
        [{name, registry} | _] ->
            application:set_env(lager, colored, true),
            application:set_env(
              lager,
              handlers, [{lager_console_backend, [{level, debug}]}]),
            application:set_env(
              lager,
              extra_sinks,
              [{rabbit_log_lager_event,
                [{handlers, [{lager_console_backend, [{level, debug}]}]}]
               },
               {rabbit_log_feature_flags_lager_event,
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
                    case is_feature_flag_subsystem_available(Config3) of
                        true ->
                            %% We can declare a new feature flag at
                            %% runtime. All of them are supported but
                            %% still disabled.
                            declare_arbitrary_feature_flag(Config3),
                            Config3;
                        false ->
                            end_per_testcase(Testcase, Config3),
                            {skip, "Feature flags subsystem unavailable"}
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
                    case is_feature_flag_subsystem_available(Config3) of
                        true ->
                            %% We can declare a new feature flag at
                            %% runtime. All of them are supported but
                            %% still disabled.
                            declare_arbitrary_feature_flag(Config3),
                            Config3;
                        false ->
                            end_per_testcase(Testcase, Config3),
                            {skip, "Feature flags subsystem unavailable"}
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

registry_general_usage(_Config) ->
    %% At first, the registry must be uninitialized.
    ?assertNot(rabbit_ff_registry:is_registry_initialized()),

    FeatureFlags = #{ff_a =>
                     #{desc      => "Feature flag A",
                       stability => stable},
                     ff_b =>
                     #{desc      => "Feature flag B",
                       stability => stable}},
    rabbit_feature_flags:inject_test_feature_flags(
      feature_flags_to_app_attrs(FeatureFlags)),

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
                          provided_by => ?MODULE,
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

registry_concurrent_reloads(_Config) ->
    case rabbit_ff_registry:is_registry_initialized() of
        true  -> ok;
        false -> rabbit_feature_flags:initialize_registry()
    end,
    ?assert(rabbit_ff_registry:is_registry_initialized()),

    Parent = self(),

    MakeName = fun(I) ->
                       list_to_atom(rabbit_misc:format("ff_~2..0b", [I]))
               end,

    ProcIs = lists:seq(1, 10),
    Fun = fun(I) ->
                  %% Each process will declare its own feature flag to
                  %% make sure that each generated registry module is
                  %% different, and we don't loose previously declared
                  %% feature flags.
                  Name = MakeName(I),
                  Desc = rabbit_misc:format("Feature flag ~b", [I]),
                  NewFF = #{Name =>
                            #{desc      => Desc,
                              stability => stable}},
                  rabbit_feature_flags:initialize_registry(NewFF),
                  unlink(Parent)
          end,

    %% Prepare feature flags which the spammer process should get at
    %% some point.
    FeatureFlags = #{ff_a =>
                     #{desc      => "Feature flag A",
                       stability => stable},
                     ff_b =>
                     #{desc      => "Feature flag B",
                       stability => stable}},
    rabbit_feature_flags:inject_test_feature_flags(
      feature_flags_to_app_attrs(FeatureFlags)),

    %% Spawn a process which heavily uses the registry.
    FinalFFList = lists:sort(
                    maps:keys(FeatureFlags) ++
                    [MakeName(I) || I <- ProcIs]),
    Spammer = spawn_link(fun() -> registry_spammer([], FinalFFList) end),
    rabbit_log_feature_flags:info(
      ?MODULE_STRING ": Started registry spammer (~p)",
      [self()]),

    %% We acquire the lock from the main process to synchronize the test
    %% processes we are about to spawn.
    Lock = rabbit_feature_flags:registry_loading_lock(),
    ThisNode = [node()],
    rabbit_log_feature_flags:info(
      ?MODULE_STRING ": Acquiring registry load lock"),
    global:set_lock(Lock, ThisNode),

    Pids = [begin
                Pid = spawn_link(fun() -> Fun(I) end),
                _ = erlang:monitor(process, Pid),
                Pid
            end
            || I <- ProcIs],

    %% We wait for one second to make sure all processes were started
    %% and already sleep on the lock. Not really "make sure" because
    %% we don't have a way to verify this fact, but it must be enough,
    %% right?
    timer:sleep(1000),
    rabbit_log_feature_flags:info(
      ?MODULE_STRING ": Releasing registry load lock"),
    global:del_lock(Lock, ThisNode),

    rabbit_log_feature_flags:info(
      ?MODULE_STRING ": Wait for test processes to finish"),
    lists:foreach(
      fun(Pid) ->
              receive {'DOWN', _, process, Pid, normal} -> ok end
      end,
      Pids),

    %% We wait for one more second to make sure the spammer sees
    %% all added feature flags.
    timer:sleep(1000),

    unlink(Spammer),
    exit(Spammer, normal).

registry_spammer(CurrentFeatureNames, FinalFeatureNames) ->
    %% Infinite loop.
    case ?list_ff(all) of
        CurrentFeatureNames ->
            registry_spammer(CurrentFeatureNames, FinalFeatureNames);
        FinalFeatureNames ->
            rabbit_log_feature_flags:info(
              ?MODULE_STRING ": Registry spammer: all feature flags "
              "appeared"),
            registry_spammer1(FinalFeatureNames);
        NewFeatureNames
          when length(NewFeatureNames) > length(CurrentFeatureNames) ->
            registry_spammer(NewFeatureNames, FinalFeatureNames)
    end.

registry_spammer1(FeatureNames) ->
    ?assertEqual(FeatureNames, ?list_ff(all)),
    registry_spammer1(FeatureNames).

enable_feature_flag_in_a_healthy_situation(Config) ->
    FeatureName = ff_from_testsuite,
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

enable_feature_flag_when_ff_file_is_unwritable(Config) ->
    QQSupported = rabbit_ct_broker_helpers:is_feature_flag_supported(
                    Config, quorum_queue),
    case QQSupported of
        true  -> do_enable_feature_flag_when_ff_file_is_unwritable(Config);
        false -> {skip, "Quorum queues are unsupported"}
    end.

do_enable_feature_flag_when_ff_file_is_unwritable(Config) ->
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

enable_feature_flag_with_a_network_partition(Config) ->
    FeatureName = ff_from_testsuite,
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
    %% `ff_from_testsuite`, if the network is broken.
    ?assertEqual(
       {error, unsupported},
       enable_feature_flag_on(Config, B, FeatureName)),
    ?assertEqual(
       False,
       is_feature_flag_enabled(Config, FeatureName)),

    %% Repair the network and try again to enable the feature flag.
    unblock(NodePairs),
    timer:sleep(10000),
    [?assertEqual(ok, rabbit_ct_broker_helpers:stop_node(Config, N))
     || N <- [A, C, D]],
    [?assertEqual(ok, rabbit_ct_broker_helpers:start_node(Config, N))
     || N <- [A, C, D]],
    declare_arbitrary_feature_flag(Config),

    %% Enabling the feature flag works.
    ?assertEqual(
       ok,
       enable_feature_flag_on(Config, B, FeatureName)),
    ?assertEqual(
       True,
       is_feature_flag_enabled(Config, FeatureName)).

mark_feature_flag_as_enabled_with_a_network_partition(Config) ->
    FeatureName = ff_from_testsuite,
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
        true ->
            ?assertEqual([true, true],
                         is_feature_flag_supported(Config, ff_from_testsuite)),
            ?assertEqual([false, false],
                         is_feature_flag_enabled(Config, ff_from_testsuite));
        false ->
            ok
    end,

    ?assertEqual(Config, rabbit_ct_broker_helpers:cluster_nodes(Config)),

    log_feature_flags_of_all_nodes(Config),
    case FFSubsysOk of
        true ->
            ?assertEqual([true, true],
                         is_feature_flag_supported(Config, ff_from_testsuite)),
            ?assertEqual([false, false],
                         is_feature_flag_enabled(Config, ff_from_testsuite));
        false ->
            ok
    end,
    ok.

clustering_ok_with_ff_enabled_on_some_nodes(Config) ->
    %% The test feature flag is enabled on node 1, but not on node 2.
    %% Clustering the two nodes should be accepted because they are
    %% compatible. Also, the feature flag will be enabled on node 2 as a
    %% consequence.
    enable_feature_flag_on(Config, 0, ff_from_testsuite),

    FFSubsysOk = is_feature_flag_subsystem_available(Config),

    log_feature_flags_of_all_nodes(Config),
    case FFSubsysOk of
        true ->
            ?assertEqual([true, true],
                         is_feature_flag_supported(Config, ff_from_testsuite)),
            ?assertEqual([true, false],
                         is_feature_flag_enabled(Config, ff_from_testsuite));
        false ->
            ok
    end,

    ?assertEqual(Config, rabbit_ct_broker_helpers:cluster_nodes(Config)),

    log_feature_flags_of_all_nodes(Config),
    case FFSubsysOk of
        true ->
            ?assertEqual([true, true],
                         is_feature_flag_enabled(Config, ff_from_testsuite));
        false ->
            ok
    end,
    ok.

clustering_ok_with_ff_enabled_everywhere(Config) ->
    %% The test feature flags is enabled. Clustering the two nodes
    %% should be accepted because they are compatible.
    enable_feature_flag_everywhere(Config, ff_from_testsuite),

    FFSubsysOk = is_feature_flag_subsystem_available(Config),

    log_feature_flags_of_all_nodes(Config),
    case FFSubsysOk of
        true ->
            ?assertEqual([true, true],
                         is_feature_flag_enabled(Config, ff_from_testsuite));
        false ->
            ok
    end,

    ?assertEqual(Config, rabbit_ct_broker_helpers:cluster_nodes(Config)),

    log_feature_flags_of_all_nodes(Config),
    case FFSubsysOk of
        true ->
            ?assertEqual([true, true],
                         is_feature_flag_enabled(Config, ff_from_testsuite));
        false ->
            ok
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
    enable_feature_flag_on(Config, 0, plugin_ff),

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

build_my_plugin(Config) ->
    PluginSrcDir = filename:join(?config(data_dir, Config), "my_plugin"),
    PluginsDir = filename:join(PluginSrcDir, "plugins"),
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_plugins_dir, PluginsDir}]),
    {MyPlugin, OtherPlugins} = list_my_plugin_plugins(PluginSrcDir),
    case MyPlugin of
        [] ->
            DepsDir = ?config(erlang_mk_depsdir, Config),
            Args = ["test-dist",
                    {"DEPS_DIR=~s", [DepsDir]},
                    %% We clear ALL_DEPS_DIRS to make sure they are
                    %% not recompiled when the plugin is built. `rabbit`
                    %% was previously compiled with -DTEST and if it is
                    %% recompiled because of this plugin, it will be
                    %% recompiled without -DTEST: the testsuite depends
                    %% on test code so we can't allow that.
                    %%
                    %% Note that we do not clear the DEPS variable:
                    %% we need it to be correct because it is used to
                    %% generate `my_plugin.app` (and a RabbitMQ plugin
                    %% must depend on `rabbit`).
                    "ALL_DEPS_DIRS="],
            case rabbit_ct_helpers:make(Config1, PluginSrcDir, Args) of
                {ok, _} ->
                    {_, OtherPlugins1} = list_my_plugin_plugins(PluginSrcDir),
                    remove_other_plugins(PluginSrcDir, OtherPlugins1),
                    update_cli_path(Config1, PluginSrcDir);
                {error, _} ->
                    {skip, "Failed to compile the `my_plugin` test plugin"}
            end;
        _ ->
            remove_other_plugins(PluginSrcDir, OtherPlugins),
            update_cli_path(Config1, PluginSrcDir)
    end.

update_cli_path(Config, PluginSrcDir) ->
    SbinDir = filename:join(PluginSrcDir, "sbin"),
    Rabbitmqctl = filename:join(SbinDir, "rabbitmqctl"),
    RabbitmqPlugins = filename:join(SbinDir, "rabbitmq-plugins"),
    RabbitmqQueues = filename:join(SbinDir, "rabbitmq-queues"),
    case filelib:is_regular(Rabbitmqctl) of
        true ->
            ct:pal(?LOW_IMPORTANCE,
                   "Switching to CLI in e.g. ~s", [Rabbitmqctl]),
            rabbit_ct_helpers:set_config(
              Config,
              [{rabbitmqctl_cmd, Rabbitmqctl},
               {rabbitmq_plugins_cmd, RabbitmqPlugins},
               {rabbitmq_queues_cmd, RabbitmqQueues}]);
        false ->
            Config
    end.

list_my_plugin_plugins(PluginSrcDir) ->
    Files = filelib:wildcard("plugins/*", PluginSrcDir),
    lists:partition(
      fun(Path) ->
              Filename = filename:basename(Path),
              re:run(Filename, "^my_plugin-", [{capture, none}]) =:= match
      end, Files).

remove_other_plugins(PluginSrcDir, OtherPlugins) ->
    [ok = file:delete(filename:join(PluginSrcDir, Filename))
     || Filename <- OtherPlugins].

work_around_cli_and_rabbit_circular_dep(Config) ->
    %% FIXME: We also need to copy `rabbit` in `my_plugins` plugins
    %% directory, not because `my_plugin` depends on it, but because the
    %% CLI erroneously depends on the broker.
    %%
    %% This can't be fixed easily because this is a circular dependency
    %% (i.e. the broker depends on the CLI). So until a proper solution
    %% is implemented, keep this second copy of the broker for the CLI
    %% to find it.
    InitialPluginsDir = filename:join(
                          ?config(current_srcdir, Config),
                          "plugins"),
    PluginsDir = ?config(rmq_plugins_dir, Config),
    lists:foreach(
      fun(Path) ->
              Filename = filename:basename(Path),
              IsRabbit = re:run(
                           Filename,
                           "^rabbit-", [{capture, none}]) =:= match,
              case IsRabbit of
                  true ->
                      Dest = filename:join(PluginsDir, Filename),
                      ct:pal(
                        ?LOW_IMPORTANCE,
                        "Copy `~s` to `~s` to fix CLI erroneous "
                        "dependency on `rabbit`", [Path, Dest]),
                      {ok, _} = file:copy(Path, Dest);
                  false ->
                      ok
              end
      end,
      filelib:wildcard(filename:join(InitialPluginsDir, "*"))),
    Config.

enable_feature_flag_on(Config, Node, FeatureName) ->
    rabbit_ct_broker_helpers:rpc(
      Config, Node, rabbit_feature_flags, enable, [FeatureName]).

enable_feature_flag_everywhere(Config, FeatureName) ->
    rabbit_ct_broker_helpers:rpc_all(
      Config, rabbit_feature_flags, enable, [FeatureName]).

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

feature_flags_to_app_attrs(FeatureFlags) when is_map(FeatureFlags) ->
    [{?MODULE, % Application
      ?MODULE, % Module
      maps:to_list(FeatureFlags)}].

declare_arbitrary_feature_flag(Config) ->
    FeatureFlags = #{ff_from_testsuite =>
                     #{desc => "My feature flag",
                       stability => stable}},
    rabbit_ct_broker_helpers:rpc_all(
      Config,
      rabbit_feature_flags,
      inject_test_feature_flags,
      [feature_flags_to_app_attrs(FeatureFlags)]),
    ok.

block(Pairs)   -> [block(X, Y) || {X, Y} <- Pairs].
unblock(Pairs) -> [allow(X, Y) || {X, Y} <- Pairs].

block(X, Y) ->
    rabbit_ct_broker_helpers:block_traffic_between(X, Y).

allow(X, Y) ->
    rabbit_ct_broker_helpers:allow_traffic_between(X, Y).
