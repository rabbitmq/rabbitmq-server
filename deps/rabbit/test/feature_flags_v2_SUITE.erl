%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(feature_flags_v2_SUITE).

-include_lib("kernel/include/logger.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-export([suite/0,
         all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,

         mf_count_runs/3,
         mf_wait_and_count_runs/3,
         mf_wait_and_count_runs_v2_enable/1,
         mf_wait_and_count_runs_v2_post_enable/1,

         enable_unknown_feature_flag_on_a_single_node/1,
         enable_supported_feature_flag_on_a_single_node/1,
         enable_unknown_feature_flag_in_a_3node_cluster/1,
         enable_supported_feature_flag_in_a_3node_cluster/1,
         enable_partially_supported_feature_flag_in_a_3node_cluster/1,
         enable_unsupported_feature_flag_in_a_3node_cluster/1,
         enable_feature_flag_in_cluster_and_add_member_after/1,
         enable_feature_flag_in_cluster_and_add_member_concurrently_mfv1/1,
         enable_feature_flag_in_cluster_and_add_member_concurrently_mfv2/1,
         enable_feature_flag_in_cluster_and_remove_member_concurrently_mfv1/1,
         enable_feature_flag_in_cluster_and_remove_member_concurrently_mfv2/1,
         enable_feature_flag_with_post_enable/1
        ]).

suite() ->
    [{timetrap, {minutes, 1}}].

all() ->
    [
     {group, feature_flags_v1},
     {group, feature_flags_v2}
    ].

groups() ->
    Groups =
    [
     {cluster_size_1, [parallel],
      [
       enable_unknown_feature_flag_on_a_single_node,
       enable_supported_feature_flag_on_a_single_node
      ]},
     {cluster_size_3, [parallel],
      [
       enable_unknown_feature_flag_in_a_3node_cluster,
       enable_supported_feature_flag_in_a_3node_cluster,
       enable_partially_supported_feature_flag_in_a_3node_cluster,
       enable_unsupported_feature_flag_in_a_3node_cluster,
       enable_feature_flag_in_cluster_and_add_member_after,
       enable_feature_flag_in_cluster_and_add_member_concurrently_mfv1,
       enable_feature_flag_in_cluster_and_add_member_concurrently_mfv2,
       enable_feature_flag_in_cluster_and_remove_member_concurrently_mfv1,
       enable_feature_flag_in_cluster_and_remove_member_concurrently_mfv2,
       enable_feature_flag_with_post_enable
      ]}
    ],
    [
     {feature_flags_v1, [], Groups},
     {feature_flags_v2, [], Groups}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    logger:set_primary_config(level, debug),
    rabbit_ct_helpers:run_steps(
      Config,
      [fun rabbit_ct_helpers:redirect_logger_to_ct_logs/1]).

end_per_suite(Config) ->
    Config.

init_per_group(feature_flags_v1, Config) ->
    rabbit_ct_helpers:set_config(Config, {enable_feature_flags_v2, false});
init_per_group(feature_flags_v2, Config) ->
    rabbit_ct_helpers:set_config(Config, {enable_feature_flags_v2, true});
init_per_group(cluster_size_1, Config) ->
    rabbit_ct_helpers:set_config(Config, {nodes_count, 1});
init_per_group(cluster_size_3, Config) ->
    rabbit_ct_helpers:set_config(Config, {nodes_count, 3});
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:run_steps(
      Config,
      [fun(Cfg) -> start_slave_nodes(Cfg, Testcase) end]).

end_per_testcase(_Testcase, Config) ->
    rabbit_ct_helpers:run_steps(
      Config,
      [fun stop_slave_nodes/1]).

start_slave_nodes(Config, Testcase) ->
    NodesCount = ?config(nodes_count, Config),
    ct:pal("Starting ~b slave node(s):", [NodesCount]),
    Parent = self(),
    Starters = [spawn_link(
                  fun() ->
                          start_slave_node(Parent, Config, Testcase, N)
                  end)
                || N <- lists:seq(1, NodesCount)],
    Nodes = lists:sort(
              [receive
                   {node, Starter, Node} -> Node
               end || Starter <- Starters]),
    ct:pal("Started ~b slave node(s): ~p", [NodesCount, Nodes]),
    rabbit_ct_helpers:set_config(Config, {nodes, Nodes}).

start_slave_node(Parent, Config, Testcase, N) ->
    Prefix = case ?config(enable_feature_flags_v2, Config) of
                 false -> "ffv1";
                 true  -> "ffv2"
             end,
    Name = list_to_atom(
             rabbit_misc:format("~s-~s-~b", [Prefix, Testcase, N])),
    ct:pal("- Starting slave node `~s@...`", [Name]),
    {ok, Node} = slave:start(net_adm:localhost(), Name),
    ct:pal("- Slave node `~s` started", [Node]),

    TestCodePath = filename:dirname(code:which(?MODULE)),
    true = rpc:call(Node, code, add_path, [TestCodePath]),
    ok = run_on_node(Node, fun setup_slave_node/1, [Config]),
    ct:pal("- Slave node `~s` configured", [Node]),
    Parent ! {node, self(), Node}.

stop_slave_nodes(Config) ->
    Nodes = ?config(nodes, Config),
    ct:pal("Stopping ~b slave nodes:", [length(Nodes)]),
    lists:foreach(fun stop_slave_node/1, Nodes),
    rabbit_ct_helpers:delete_config(Config, nodes).

stop_slave_node(Node) ->
    ct:pal("- Stopping slave node `~s`...", [Node]),
    ok = slave:stop(Node).

connect_nodes([FirstNode | OtherNodes] = Nodes) ->
    lists:foreach(
      fun(Node) -> pong = rpc:call(Node, net_adm, ping, [FirstNode]) end,
      OtherNodes),
    Cluster = lists:sort(
                [FirstNode | rpc:call(FirstNode, erlang, nodes, [])]),
    ?assert(lists:all(fun(Node) -> lists:member(Node, Cluster) end, Nodes)).

run_on_node(Node, Fun) ->
    run_on_node(Node, Fun, []).

run_on_node(Node, Fun, Args) ->
    rpc:call(Node, erlang, apply, [Fun, Args]).

%% -------------------------------------------------------------------
%% Slave node configuration.
%% -------------------------------------------------------------------

setup_slave_node(Config) ->
    ok = setup_logger(),
    ok = setup_feature_flags_file(Config),
    ok = start_controller(),
    ok = maybe_enable_feature_flags_v2(Config),
    ok.

setup_logger() ->
    logger:set_primary_config(level, debug),
    ok.

setup_feature_flags_file(Config) ->
    %% The `feature_flags_file' is set to a specific location in the test log
    %% directory.
    FeatureFlagsFile = filename:join(
                         ?config(priv_dir, Config),
                         rabbit_misc:format("feature_flags-~s", [node()])),
    ?LOG_INFO("Setting `feature_flags_file to \"~ts\"", [FeatureFlagsFile]),
    case application:load(rabbit) of
        ok                           -> ok;
        {error, {already_loaded, _}} -> ok
    end,
    ok = application:set_env(rabbit, feature_flags_file, FeatureFlagsFile),
    ok.

start_controller() ->
    ?LOG_INFO("Starting feature flags controller"),
    {ok, Pid} = rabbit_ff_controller:start(),
    ?LOG_INFO("Feature flags controller: ~p", [Pid]),
    ok.

maybe_enable_feature_flags_v2(Config) ->
    EnableFFv2 = ?config(enable_feature_flags_v2, Config),
    case EnableFFv2 of
        true  -> ok = rabbit_feature_flags:enable(feature_flags_v2);
        false -> ok
    end,
    IsEnabled = rabbit_feature_flags:is_enabled(feature_flags_v2),
    ?LOG_INFO("`feature_flags_v2` enabled: ~s", [IsEnabled]),
    ?assertEqual(EnableFFv2, IsEnabled),
    ok.

override_running_nodes(Nodes) when is_list(Nodes) ->
    ct:pal("Overriding (running) remote nodes for ~p", [Nodes]),
    _ = [begin
             ok = rpc:call(
                    Node, rabbit_feature_flags, override_nodes,
                    [Nodes]),
             ?assertEqual(
                Nodes,
                lists:sort(
                  [Node |
                   rpc:call(Node, rabbit_feature_flags, remote_nodes, [])])),
             ?assertEqual(
                Nodes,
                rpc:call(Node, rabbit_ff_controller, all_nodes, [])),

             ok = rpc:call(
                    Node, rabbit_feature_flags, override_running_nodes,
                    [Nodes]),
             ?assertEqual(
                Nodes,
                lists:sort(
                  [Node |
                   rpc:call(
                     Node, rabbit_feature_flags, running_remote_nodes, [])])),
             ?assertEqual(
                Nodes,
                rpc:call(Node, rabbit_ff_controller, running_nodes, []))
         end
         || Node <- Nodes],
    ok.

inject_on_nodes(Nodes, FeatureFlags) ->
    ct:pal(
      "Injecting feature flags on nodes~n"
      "  Nodes:         ~p~n"
      "  Feature flags: ~p~n",
     [Nodes, FeatureFlags]),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   ?assertEqual(
                      ok,
                      rabbit_feature_flags:inject_test_feature_flags(
                        FeatureFlags)),
                   ok
           end,
           [])
         || Node <- Nodes],
    ok.

%% -------------------------------------------------------------------
%% Migration functions.
%% -------------------------------------------------------------------

-define(PT_MIGRATION_FUN_RUNS, {?MODULE, migration_fun_runs}).

bump_runs() ->
    Count = persistent_term:get(?PT_MIGRATION_FUN_RUNS, 0),
    persistent_term:put(?PT_MIGRATION_FUN_RUNS, Count + 1),
    ok.

mf_count_runs(_FeatureName, _FeatureProps, enable) ->
    bump_runs(),
    ok.

mf_wait_and_count_runs(_FeatureName, _FeatureProps, enable) ->
    Peer = get_peer_proc(),
    Peer ! {node(), self(), waiting},
    ?LOG_NOTICE("Migration function: waiting for signal from ~p...", [Peer]),
    receive proceed -> ok end,
    ?LOG_NOTICE("Migration function: unblocked!", []),
    bump_runs(),
    ok.

mf_wait_and_count_runs_v2_enable(_Args) ->
    Peer = get_peer_proc(),
    Peer ! {node(), self(), waiting},
    ?LOG_NOTICE("Migration function: waiting for signal from ~p...", [Peer]),
    receive proceed -> ok end,
    ?LOG_NOTICE("Migration function: unblocked!", []),
    bump_runs(),
    ok.

mf_wait_and_count_runs_v2_post_enable(_Args) ->
    Peer = get_peer_proc(),
    Peer ! {node(), self(), waiting},
    ?LOG_NOTICE("Migration function: waiting for signal from ~p...", [Peer]),
    receive proceed -> ok end,
    ?LOG_NOTICE("Migration function: unblocked!", []),
    bump_runs(),
    ok.

-define(PT_PEER_PROC, {?MODULE, peer_proc}).

record_peer_proc(Peer) ->
    ?LOG_ALERT("Recording peer=~p", [Peer]),
    persistent_term:put(?PT_PEER_PROC, Peer).

get_peer_proc() ->
    persistent_term:get(?PT_PEER_PROC).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

enable_unknown_feature_flag_on_a_single_node(Config) ->
    [Node] = ?config(nodes, Config),
    ok = run_on_node(
           Node, fun enable_unknown_feature_flag_on_a_single_node/0).

enable_unknown_feature_flag_on_a_single_node() ->
    FeatureName = ?FUNCTION_NAME,
    ?assertNot(rabbit_feature_flags:is_supported(FeatureName)),
    ?assertNot(rabbit_feature_flags:is_enabled(FeatureName)),

    %% The node doesn't know about the feature flag and thus rejects the
    %% request.
    ?assertEqual(
       {error, unsupported}, rabbit_feature_flags:enable(FeatureName)),
    ?assertNot(rabbit_feature_flags:is_supported(FeatureName)),
    ?assertNot(rabbit_feature_flags:is_enabled(FeatureName)),
    ok.

enable_supported_feature_flag_on_a_single_node(Config) ->
    [Node] = ?config(nodes, Config),
    ok = run_on_node(
           Node, fun enable_supported_feature_flag_on_a_single_node/0).

enable_supported_feature_flag_on_a_single_node() ->
    FeatureName = ?FUNCTION_NAME,
    FeatureFlags = #{FeatureName => #{provided_by => ?MODULE,
                                      stability => stable}},
    ?assertEqual(
       ok, rabbit_feature_flags:inject_test_feature_flags(FeatureFlags)),
    ?assert(rabbit_feature_flags:is_supported(FeatureName)),
    ?assertNot(rabbit_feature_flags:is_enabled(FeatureName)),

    ?assertEqual(ok, rabbit_feature_flags:enable(FeatureName)),
    ?assert(rabbit_feature_flags:is_supported(FeatureName)),
    ?assert(rabbit_feature_flags:is_enabled(FeatureName)),
    ok.

enable_unknown_feature_flag_in_a_3node_cluster(Config) ->
    Nodes = ?config(nodes, Config),
    connect_nodes(Nodes),
    override_running_nodes(Nodes),

    FeatureName = ?FUNCTION_NAME,

    ct:pal(
      "Checking the feature flag is unsupported and disabled on all nodes"),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   ?assertNot(rabbit_feature_flags:is_supported(FeatureName)),
                   ?assertNot(rabbit_feature_flags:is_enabled(FeatureName)),
                   ok
           end,
           [])
         || Node <- Nodes],

    %% No nodes know about the feature flag and thus all reject the request.
    ct:pal("Trying to enable the feature flag on all nodes"),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   ?assertEqual(
                      {error, unsupported},
                      rabbit_feature_flags:enable(FeatureName)),
                   ok
           end,
           [])
         || Node <- Nodes],
    ct:pal(
      "Checking the feature flag is still unsupported and disabled on all "
      "nodes"),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   ?assertNot(rabbit_feature_flags:is_supported(FeatureName)),
                   ?assertNot(rabbit_feature_flags:is_enabled(FeatureName)),
                   ok
           end,
           [])
         || Node <- Nodes],
    ok.

enable_supported_feature_flag_in_a_3node_cluster(Config) ->
    Nodes = ?config(nodes, Config),
    connect_nodes(Nodes),
    override_running_nodes(Nodes),

    FeatureName = ?FUNCTION_NAME,
    FeatureFlags = #{FeatureName => #{provided_by => ?MODULE,
                                      stability => stable}},
    inject_on_nodes(Nodes, FeatureFlags),

    ct:pal(
      "Checking the feature flag is supported but disabled on all nodes"),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   ?assert(rabbit_feature_flags:is_supported(FeatureName)),
                   ?assertNot(rabbit_feature_flags:is_enabled(FeatureName)),
                   ok
           end,
           [])
         || Node <- Nodes],

    %% The first call enables the feature flag, the following calls are
    %% idempotent and do nothing.
    ct:pal("Enabling the feature flag on all nodes"),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   ?assertEqual(ok, rabbit_feature_flags:enable(FeatureName)),
                   ok
           end,
           [])
         || Node <- Nodes],
    ct:pal("Checking the feature flag is supported and enabled on all nodes"),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   ?assert(rabbit_feature_flags:is_supported(FeatureName)),
                   ?assert(rabbit_feature_flags:is_enabled(FeatureName)),
                   ok
           end,
           [])
         || Node <- Nodes],
    ok.

enable_partially_supported_feature_flag_in_a_3node_cluster(Config) ->
    [FirstNode | OtherNodes] = Nodes = ?config(nodes, Config),
    connect_nodes(Nodes),
    override_running_nodes(Nodes),
    UsingFFv1 = not ?config(enable_feature_flags_v2, Config),

    %% This time, we inject the feature flag on a single node only. The other
    %% nodes don't know about it.
    FeatureName = ?FUNCTION_NAME,
    FeatureFlags = #{FeatureName => #{provided_by => ?MODULE,
                                      stability => stable}},
    inject_on_nodes([FirstNode], FeatureFlags),

    case UsingFFv1 of
        true ->
            %% With `feature_flags_v1', the code would have shared the new
            %% feature flags with remote nodes, so let's run that here. In the
            %% end, the testcase is similar to
            %% `enable_supported_feature_flag_in_a_3node_cluster'.
            ct:pal("Refreshing feature flags after app load"),
            ok = run_on_node(
                   FirstNode,
                   fun() ->
                           ?assertEqual(
                              ok,
                              rabbit_feature_flags:
                              share_new_feature_flags_after_app_load(
                                FeatureFlags, infinity)),
                           ok
                   end,
                   []);
        false ->
            ok
    end,

    ct:pal(
      "Checking the feature flag is supported but disabled on all nodes"),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   ?assert(rabbit_feature_flags:is_supported(FeatureName)),
                   ?assertNot(rabbit_feature_flags:is_enabled(FeatureName)),
                   ok
           end,
           [])
         || Node <- Nodes],

    %% The first call enables the feature flag, the following calls are
    %% idempotent and do nothing.
    ct:pal("Enabling the feature flag on all nodes"),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   ?assertEqual(
                      ok,
                      rabbit_feature_flags:enable(FeatureName)),
                   ok
           end,
           [])
         || Node <- Nodes],
    ct:pal(
      "Checking the feature flag is supported on all nodes and enabled on "
      "all nodes (v1) or the node knowing it only (v2)"),
    ok = run_on_node(
           FirstNode,
           fun() ->
                   ?assert(rabbit_feature_flags:is_supported(FeatureName)),
                   ?assert(rabbit_feature_flags:is_enabled(FeatureName)),
                   ok
           end,
           []),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   case UsingFFv1 of
                       true ->
                           ?assert(
                              rabbit_feature_flags:is_supported(FeatureName)),
                           ?assert(
                              rabbit_feature_flags:is_enabled(FeatureName));
                       false ->
                           ?assert(
                              rabbit_feature_flags:is_supported(FeatureName)),
                           ?assertNot(
                              rabbit_feature_flags:is_enabled(FeatureName))
                   end,
                   ok
           end,
           [])
         || Node <- OtherNodes],
    ok.

enable_unsupported_feature_flag_in_a_3node_cluster(Config) ->
    [FirstNode | _OtherNodes] = Nodes = ?config(nodes, Config),
    connect_nodes(Nodes),
    override_running_nodes(Nodes),

    %% We inject the feature flag on a single node only. We tell it is
    %% provided by `rabbit' which was already loaded and scanned while
    %% configuring the node. This way, we ensure the feature flag is
    %% considered supported by the node where is was injected, but
    %% unsupported by other nodes.
    FeatureName = ?FUNCTION_NAME,
    FeatureFlags = #{FeatureName => #{provided_by => rabbit,
                                      stability => stable}},
    inject_on_nodes([FirstNode], FeatureFlags),

    ct:pal(
      "Checking the feature flag is unsupported and disabled on all nodes"),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   ?assertNot(rabbit_feature_flags:is_supported(FeatureName)),
                   ?assertNot(rabbit_feature_flags:is_enabled(FeatureName)),
                   ok
           end,
           [])
         || Node <- Nodes],

    %% The feature flag is unsupported, thus all reject the request.
    ct:pal("Enabling the feature flag on all nodes (denied)"),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   ?assertEqual(
                      {error, unsupported},
                      rabbit_feature_flags:enable(FeatureName)),
                   ok
           end,
           [])
         || Node <- Nodes],
    ct:pal("Checking the feature flag is still disabled on all nodes"),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   ?assertNot(rabbit_feature_flags:is_enabled(FeatureName)),
                   ok
           end,
           [])
         || Node <- Nodes],
    ok.

enable_feature_flag_in_cluster_and_add_member_after(Config) ->
    AllNodes = [NewNode | [FirstNode | _] = Nodes] = ?config(nodes, Config),
    connect_nodes(Nodes),
    override_running_nodes([NewNode]),
    override_running_nodes(Nodes),

    FeatureName = ?FUNCTION_NAME,
    FeatureFlags = #{FeatureName =>
                     #{provided_by => ?MODULE,
                       stability => stable,
                       migration_fun => {?MODULE, mf_count_runs}}},
    inject_on_nodes(AllNodes, FeatureFlags),

    ct:pal(
      "Checking the feature flag is supported but disabled on all nodes"),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   ?assert(rabbit_feature_flags:is_supported(FeatureName)),
                   ?assertNot(rabbit_feature_flags:is_enabled(FeatureName)),
                   ok
           end,
           [])
         || Node <- AllNodes],

    ct:pal("Enabling the feature flag in the cluster"),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   ?assertEqual(ok, rabbit_feature_flags:enable(FeatureName)),
                   ok
           end,
           [])
         || Node <- Nodes],
    ct:pal("Checking the feature flag is enabled in the initial cluster"),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   ?assert(rabbit_feature_flags:is_enabled(FeatureName)),
                   ok
           end,
           [])
         || Node <- Nodes],
    ct:pal("Checking the feature flag is still disabled on the new node"),
    ok = run_on_node(
           NewNode,
           fun() ->
                   ?assertNot(rabbit_feature_flags:is_enabled(FeatureName)),
                   ok
           end,
           []),

    %% Check compatibility between NewNodes and Nodes.
    ok = run_on_node(
           NewNode,
           fun() ->
                   ?assertEqual(
                      ok,
                      rabbit_feature_flags:check_node_compatibility(
                        FirstNode)),
                   ok
           end, []),

    %% Add node to cluster and synchronize feature flags.
    connect_nodes(AllNodes),
    override_running_nodes(AllNodes),
    ct:pal(
      "Synchronizing feature flags in the expanded cluster~n"
      "~n"
      "NOTE: Error messages about crashed migration functions can be "
      "ignored for feature~n"
      "      flags other than `~s`~n"
      "      because they assume they run inside RabbitMQ.",
      [FeatureName]),
    ok = run_on_node(
           NewNode,
           fun() ->
                   ?assertEqual(
                      ok,
                      rabbit_feature_flags:sync_feature_flags_with_cluster(
                        Nodes, true)),
                   ok
           end, []),

    ct:pal("Checking the feature flag is enabled in the expanded cluster"),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   ?assert(rabbit_feature_flags:is_enabled(FeatureName)),
                   %% With both feature flags v1 and v2, the migration
                   %% function is executed on the node where `enable()' was
                   %% called, and then on the node joining the cluster.
                   Count = case Node of
                               FirstNode -> 1;
                               NewNode   -> 1;
                               _         -> 0
                           end,
                   ?assertEqual(
                      Count,
                      persistent_term:get(?PT_MIGRATION_FUN_RUNS, 0)),
                   ok
           end,
           [])
         || Node <- AllNodes],
    ok.

enable_feature_flag_in_cluster_and_add_member_concurrently_mfv1(Config) ->
    AllNodes = [NewNode | [FirstNode | _] = Nodes] = ?config(nodes, Config),
    connect_nodes(Nodes),
    override_running_nodes([NewNode]),
    override_running_nodes(Nodes),

    FeatureName = ?FUNCTION_NAME,
    FeatureFlags = #{FeatureName =>
                     #{provided_by => ?MODULE,
                       stability => stable,
                       migration_fun => {?MODULE, mf_wait_and_count_runs}}},
    inject_on_nodes(AllNodes, FeatureFlags),

    ct:pal(
      "Checking the feature flag is supported but disabled on all nodes"),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   ?assert(rabbit_feature_flags:is_supported(FeatureName)),
                   ?assertNot(rabbit_feature_flags:is_enabled(FeatureName)),
                   ok
           end,
           [])
         || Node <- AllNodes],

    ct:pal(
      "Enabling the feature flag in the cluster (in a separate process)"),
    Peer = self(),
    Enabler = spawn_link(
                fun() ->
                        ok =
                        run_on_node(
                          FirstNode,
                          fun() ->
                                  %% The migration function uses the `Peer'
                                  %% PID (the process executing the testcase)
                                  %% to notify its own PID and wait for a
                                  %% signal from `Peer' to proceed and finish
                                  %% the migration.
                                  record_peer_proc(Peer),
                                  ?assertEqual(
                                     ok,
                                     rabbit_feature_flags:enable(
                                       FeatureName)),
                                  ok
                          end,
                          [])
                end),

    %% By waiting for the message from one of the migration function
    %% instances, we make sure the feature flags controller on `FirstNode' is
    %% blocked and waits for a message from this process. Therefore, we are
    %% sure the feature flag is in the `state_changing' state and we can try
    %% to add a new node and sync its feature flags.
    FirstNodeMigFunPid = receive
                             {FirstNode, MigFunPid1, waiting} -> MigFunPid1
                         end,

    %% Check compatibility between NewNodes and Nodes. This doesn't block.
    ok = run_on_node(
           NewNode,
           fun() ->
                   ?assertEqual(
                      ok,
                      rabbit_feature_flags:check_node_compatibility(
                        FirstNode)),
                   ok
           end, []),

    %% Add node to cluster and synchronize feature flags. The synchronization
    %% blocks.
    connect_nodes(AllNodes),
    override_running_nodes(AllNodes),
    ct:pal(
      "Synchronizing feature flags in the expanded cluster (in a separate "
      "process)~n"
      "~n"
      "NOTE: Error messages about crashed migration functions can be "
      "ignored for feature~n"
      "      flags other than `~s`~n"
      "      because they assume they run inside RabbitMQ.",
      [FeatureName]),
    Syncer = spawn_link(
               fun() ->
                       ok =
                       run_on_node(
                         NewNode,
                         fun() ->
                                 record_peer_proc(Peer),
                                 ?assertEqual(
                                    ok,
                                    rabbit_feature_flags:
                                    sync_feature_flags_with_cluster(
                                      Nodes, true)),
                                 ok
                         end, [])
               end),

    ct:pal(
      "Checking the feature flag state is changing in the initial cluster"),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   ?assertEqual(
                      state_changing,
                      rabbit_feature_flags:is_enabled(
                        FeatureName, non_blocking)),
                   ok
           end,
           [])
         || Node <- Nodes],

    ct:pal("Checking the feature flag is still disabled on the new node"),
    ok = run_on_node(
           NewNode,
           fun() ->
                   ?assertNot(rabbit_feature_flags:is_enabled(FeatureName)),
                   ok
           end,
           []),

    %% Unblock the migration functions on `Nodes'.
    UsingFFv1 = not ?config(enable_feature_flags_v2, Config),
    EnablerMRef = erlang:monitor(process, Enabler),
    SyncerMRef = erlang:monitor(process, Syncer),
    unlink(Enabler),
    unlink(Syncer),
    ExpectedNodes = case UsingFFv1 of
                        true ->
                            %% With v1, the migration function runs on a
                            %% single node in the cluster only in this
                            %% scenario.
                            %%
                            %% The reason is that the new node joined during
                            %% the migration and the feature flag was marked
                            %% as enabled there as well, even though the
                            %% migration function possibly didn't know about
                            %% it. This is one of the problems
                            %% `feature_flags_v2' fixes.
                            [FirstNode];
                        false ->
                            %% With v2 but still using the old migration
                            %% function API (taking 3 arguments), the
                            %% migration function is executed on the node
                            %% where `enable()' was called, and then on the
                            %% node joining the cluster, thanks to the
                            %% synchronization.
                            [FirstNode, NewNode]
                    end,

    %% Unblock the migration function for which we already consumed the
    %% `waiting' notification.
    FirstMigratedNode = node(FirstNodeMigFunPid),
    ?assertEqual(FirstNode, FirstMigratedNode),
    ct:pal(
      "Unblocking first node (~p @ ~s)",
      [FirstNodeMigFunPid, FirstMigratedNode]),
    FirstNodeMigFunPid ! proceed,

    %% Unblock the rest and collect the node names of all migration functions
    %% which ran.
    ct:pal("Unblocking other nodes, including the joining one"),
    OtherMigratedNodes = [receive
                              {Node, MigFunPid2, waiting} ->
                                  MigFunPid2 ! proceed,
                                  Node
                          end || Node <- ExpectedNodes -- [FirstMigratedNode]],
    MigratedNodes = [FirstMigratedNode | OtherMigratedNodes],
    ?assertEqual(lists:sort(ExpectedNodes), lists:sort(MigratedNodes)),

    ct:pal("Waiting for spawned processes to terminate"),
    receive
        {'DOWN', EnablerMRef, process, Enabler, EnablerReason} ->
            ?assertEqual(normal, EnablerReason)
    end,
    receive
        {'DOWN', SyncerMRef, process, Syncer, SyncerReason} ->
            ?assertEqual(normal, SyncerReason)
    end,

    ct:pal("Checking the feature flag is enabled in the expanded cluster"),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   ?assert(rabbit_feature_flags:is_enabled(FeatureName)),
                   ?assert(
                      not lists:member(Node, MigratedNodes) orelse
                      1 =:= persistent_term:get(?PT_MIGRATION_FUN_RUNS, 0)),
                   ok
           end,
           [])
         || Node <- AllNodes],
    ok.

enable_feature_flag_in_cluster_and_add_member_concurrently_mfv2(Config) ->
    AllNodes = [NewNode | [FirstNode | _] = Nodes] = ?config(nodes, Config),
    connect_nodes(Nodes),
    override_running_nodes([NewNode]),
    override_running_nodes(Nodes),

    FeatureName = ?FUNCTION_NAME,
    FeatureFlags = #{FeatureName =>
                     #{provided_by => ?MODULE,
                       stability => stable,
                       callbacks =>
                       #{enable =>
                         {?MODULE, mf_wait_and_count_runs_v2_enable}}}},
    inject_on_nodes(AllNodes, FeatureFlags),

    ct:pal(
      "Checking the feature flag is supported but disabled on all nodes"),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   ?assert(rabbit_feature_flags:is_supported(FeatureName)),
                   ?assertNot(rabbit_feature_flags:is_enabled(FeatureName)),
                   ok
           end,
           [])
         || Node <- AllNodes],

    ct:pal(
      "Enabling the feature flag in the cluster (in a separate process)"),
    Peer = self(),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   %% The migration function uses the `Peer' PID (the process
                   %% executing the testcase) to notify its own PID and wait
                   %% for a signal from `Peer' to proceed and finish the
                   %% migration.
                   record_peer_proc(Peer),
                   ok
           end,
           [])
         || Node <- AllNodes],
    Enabler = spawn_link(
                fun() ->
                        ok =
                        run_on_node(
                          FirstNode,
                          fun() ->
                                  ?assertEqual(
                                     ok,
                                     rabbit_feature_flags:enable(
                                       FeatureName)),
                                  ok
                          end,
                          [])
                end),

    %% By waiting for the message from one of the migration function
    %% instances, we make sure the feature flags controller on `FirstNode' is
    %% blocked and waits for a message from this process. Therefore, we are
    %% sure the feature flag is in the `state_changing' state and we can try
    %% to add a new node and sync its feature flags.
    FirstNodeMigFunPid = receive
                             {_Node, MigFunPid1, waiting} -> MigFunPid1
                         end,

    %% Check compatibility between NewNodes and Nodes. This doesn't block.
    ok = run_on_node(
           NewNode,
           fun() ->
                   ?assertEqual(
                      ok,
                      rabbit_feature_flags:check_node_compatibility(
                        FirstNode)),
                   ok
           end, []),

    %% Add node to cluster and synchronize feature flags. The synchronization
    %% blocks.
    connect_nodes(AllNodes),
    override_running_nodes(AllNodes),
    ct:pal(
      "Synchronizing feature flags in the expanded cluster (in a separate "
      "process)~n"
      "~n"
      "NOTE: Error messages about crashed migration functions can be "
      "ignored for feature~n"
      "      flags other than `~s`~n"
      "      because they assume they run inside RabbitMQ.",
      [FeatureName]),
    Syncer = spawn_link(
               fun() ->
                       ok =
                       run_on_node(
                         NewNode,
                         fun() ->
                                 ?assertEqual(
                                    ok,
                                    rabbit_feature_flags:
                                    sync_feature_flags_with_cluster(
                                      Nodes, true)),
                                 ok
                         end, [])
               end),

    ct:pal(
      "Checking the feature flag state is changing in the initial cluster"),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   ?assertEqual(
                      state_changing,
                      rabbit_feature_flags:is_enabled(
                        FeatureName, non_blocking)),
                   ok
           end,
           [])
         || Node <- Nodes],

    ct:pal("Checking the feature flag is still disabled on the new node"),
    ok = run_on_node(
           NewNode,
           fun() ->
                   ?assertNot(rabbit_feature_flags:is_enabled(FeatureName)),
                   ok
           end,
           []),

    %% Unblock the migration functions on `Nodes'.
    EnablerMRef = erlang:monitor(process, Enabler),
    SyncerMRef = erlang:monitor(process, Syncer),
    unlink(Enabler),
    unlink(Syncer),

    %% The migration function runs on all clustered nodes with v2, including
    %% the one joining the cluster, thanks to the synchronization.
    %%
    %% When this testcase runs with feature flags v1, the feature flag we want
    %% to enable uses the migration function API v2: this implicitly enables
    %% `feature_flags_v2'. As part of the synchronization, the node still on
    %% feature flags v1 will try to sync `feature_flags_v2' specificaly first.
    %% After that, the controller-based sync proceeds.
    ExpectedNodes = Nodes ++ [NewNode],

    %% Unblock the migration function for which we already consumed the
    %% `waiting' notification.
    FirstMigratedNode = node(FirstNodeMigFunPid),
    ct:pal(
      "Unblocking first node (~p @ ~s)",
      [FirstNodeMigFunPid, FirstMigratedNode]),
    FirstNodeMigFunPid ! proceed,

    %% Unblock the rest and collect the node names of all migration functions
    %% which ran.
    ct:pal("Unblocking other nodes, including the joining one"),
    OtherMigratedNodes = [receive
                              {Node, MigFunPid2, waiting} ->
                                  MigFunPid2 ! proceed,
                                  Node
                          end || Node <- ExpectedNodes -- [FirstMigratedNode]],
    MigratedNodes = [FirstMigratedNode | OtherMigratedNodes],
    ?assertEqual(lists:sort(ExpectedNodes), lists:sort(MigratedNodes)),

    ct:pal("Waiting for spawned processes to terminate"),
    receive
        {'DOWN', EnablerMRef, process, Enabler, EnablerReason} ->
            ?assertEqual(normal, EnablerReason)
    end,
    receive
        {'DOWN', SyncerMRef, process, Syncer, SyncerReason} ->
            ?assertEqual(normal, SyncerReason)
    end,

    ct:pal("Checking the feature flag is enabled in the expanded cluster"),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   ?assert(rabbit_feature_flags:is_enabled(FeatureName)),
                   ?assertEqual(
                      1,
                      persistent_term:get(?PT_MIGRATION_FUN_RUNS, 0)),
                   ok
           end,
           [])
         || Node <- AllNodes],
    ok.

enable_feature_flag_in_cluster_and_remove_member_concurrently_mfv1(Config) ->
    AllNodes = [LeavingNode | [FirstNode | _] = Nodes] = ?config(
                                                            nodes, Config),
    connect_nodes(AllNodes),
    override_running_nodes(AllNodes),

    FeatureName = ?FUNCTION_NAME,
    FeatureFlags = #{FeatureName =>
                     #{provided_by => ?MODULE,
                       stability => stable,
                       migration_fun => {?MODULE, mf_wait_and_count_runs}}},
    inject_on_nodes(AllNodes, FeatureFlags),

    ct:pal(
      "Checking the feature flag is supported but disabled on all nodes"),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   ?assert(rabbit_feature_flags:is_supported(FeatureName)),
                   ?assertNot(rabbit_feature_flags:is_enabled(FeatureName)),
                   ok
           end,
           [])
         || Node <- AllNodes],

    UsingFFv1 = not ?config(enable_feature_flags_v2, Config),
    ExpectedRet = case UsingFFv1 of
                      true  -> ok;
                      false -> {error, {badrpc, nodedown}}
                  end,
    ct:pal(
      "Enabling the feature flag in the cluster (in a separate process)"),
    Peer = self(),
    Enabler = spawn_link(
                fun() ->
                        ok =
                        run_on_node(
                          FirstNode,
                          fun() ->
                                  %% The migration function uses the `Peer'
                                  %% PID (the process executing the testcase)
                                  %% to notify its own PID and wait for a
                                  %% signal from `Peer' to proceed and finish
                                  %% the migration.
                                  record_peer_proc(Peer),
                                  ?assertEqual(
                                     ExpectedRet,
                                     rabbit_feature_flags:enable(
                                       FeatureName)),
                                  ok
                          end,
                          [])
                end),

    %% By waiting for the message from one of the migration function
    %% instances, we make sure the feature flags controller on `FirstNode' is
    %% blocked and waits for a message from this process. Therefore, we are
    %% sure the feature flag is in the `state_changing' state and we can try
    %% to add a new node and sync its feature flags.
    FirstNodeMigFunPid = receive
                             {_Node, MigFunPid1, waiting} -> MigFunPid1
                         end,

    %% Remove node from cluster.
    stop_slave_node(LeavingNode),
    override_running_nodes(Nodes),

    %% Unblock the migration functions on `Nodes'.
    EnablerMRef = erlang:monitor(process, Enabler),
    unlink(Enabler),

    %% Unblock the migration function for which we already consumed the
    %% `waiting' notification.
    FirstMigratedNode = node(FirstNodeMigFunPid),
    ct:pal(
      "Unblocking first node (~p @ ~s)",
      [FirstNodeMigFunPid, FirstMigratedNode]),
    FirstNodeMigFunPid ! proceed,

    ct:pal("Waiting for spawned processes to terminate"),
    receive
        {'DOWN', EnablerMRef, process, Enabler, EnablerReason} ->
            ?assertEqual(normal, EnablerReason)
    end,

    ct:pal(
      "Checking the feature flag is enabled (v1) or disabled (v2) in the "
      "cluster"),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   case UsingFFv1 of
                       true ->
                           ?assert(
                              rabbit_feature_flags:is_enabled(FeatureName));
                       false ->
                           ?assertNot(
                              rabbit_feature_flags:is_enabled(FeatureName))
                   end,
                   ok
           end,
           [])
         || Node <- Nodes],
    ok.

enable_feature_flag_in_cluster_and_remove_member_concurrently_mfv2(Config) ->
    AllNodes = [LeavingNode | [FirstNode | _] = Nodes] = ?config(
                                                            nodes, Config),
    connect_nodes(AllNodes),
    override_running_nodes(AllNodes),

    FeatureName = ?FUNCTION_NAME,
    FeatureFlags = #{FeatureName =>
                     #{provided_by => ?MODULE,
                       stability => stable,
                       callbacks =>
                       #{enable =>
                         {?MODULE, mf_wait_and_count_runs_v2_enable}}}},
    inject_on_nodes(AllNodes, FeatureFlags),

    UsingFFv1 = not ?config(enable_feature_flags_v2, Config),

    ct:pal(
      "Checking the feature flag is supported but disabled on all nodes"),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   ?assert(rabbit_feature_flags:is_supported(FeatureName)),
                   ?assertNot(rabbit_feature_flags:is_enabled(FeatureName)),
                   ok
           end,
           [])
         || Node <- AllNodes],

    ct:pal(
      "Enabling the feature flag in the cluster (in a separate process)"),
    Peer = self(),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   %% The migration function uses the `Peer' PID (the process
                   %% executing the testcase) to notify its own PID and wait
                   %% for a signal from `Peer' to proceed and finish the
                   %% migration.
                   record_peer_proc(Peer),
                   ok
           end,
           [])
         || Node <- AllNodes],
    Enabler = spawn_link(
                fun() ->
                        ok =
                        run_on_node(
                          FirstNode,
                          fun() ->
                                  ?assertEqual(
                                     {error, {badrpc, nodedown}},
                                     rabbit_feature_flags:enable(
                                       FeatureName)),
                                  ok
                          end,
                          [])
                end),

    %% By waiting for the message from one of the migration function
    %% instances, we make sure the feature flags controller on `FirstNode' is
    %% blocked and waits for a message from this process. Therefore, we are
    %% sure the feature flag is in the `state_changing' state and we can try
    %% to add a new node and sync its feature flags.
    FirstNodeMigFunPid = receive
                             {FirstNode, MigFunPid1, waiting} -> MigFunPid1
                         end,

    %% Remove node from cluster.
    stop_slave_node(LeavingNode),
    override_running_nodes(Nodes),

    %% Unblock the migration functions on `Nodes'.
    EnablerMRef = erlang:monitor(process, Enabler),
    unlink(Enabler),

    %% The migration function runs on all clustered nodes with v2.
    %%
    %% When this testcase runs with feature flags v1, the feature flag we want
    %% to enable uses the migration function API v2: this implicitly enables
    %% `feature_flags_v2'. As part of the synchronization, the node still on
    %% feature flags v1 will try to sync `feature_flags_v2' specificaly first.
    %% After that, the controller-based sync proceeds.
    ExpectedNodes = Nodes,

    %% Unblock the migration function for which we already consumed the
    %% `waiting' notification.
    FirstMigratedNode = node(FirstNodeMigFunPid),
    ?assertEqual(FirstNode, FirstMigratedNode),
    ct:pal(
      "Unblocking first node (~p @ ~s)",
      [FirstNodeMigFunPid, FirstMigratedNode]),
    FirstNodeMigFunPid ! proceed,

    %% Unblock the rest and collect the node names of all migration functions
    %% which ran.
    ct:pal("Unblocking other nodes"),
    OtherMigratedNodes = [receive
                              {Node, MigFunPid2, waiting} ->
                                  MigFunPid2 ! proceed,
                                  Node
                          end || Node <- ExpectedNodes -- [FirstMigratedNode]],
    MigratedNodes = [FirstMigratedNode | OtherMigratedNodes],
    ?assertEqual(lists:sort(ExpectedNodes), lists:sort(MigratedNodes)),

    ct:pal("Waiting for spawned processes to terminate"),
    receive
        {'DOWN', EnablerMRef, process, Enabler, EnablerReason} ->
            ?assertEqual(normal, EnablerReason)
    end,

    ct:pal(
      "Checking the feature flag is enabled (v1) or disabled (v2) in the "
      "cluster"),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   case UsingFFv1 of
                       true ->
                           ?assertNot(
                              rabbit_feature_flags:is_enabled(FeatureName));
                       false ->
                           ?assertNot(
                              rabbit_feature_flags:is_enabled(FeatureName))
                   end,
                   ok
           end,
           [])
         || Node <- Nodes],
    ok.

enable_feature_flag_with_post_enable(Config) ->
    AllNodes = [NewNode | [FirstNode | _] = Nodes] = ?config(nodes, Config),
    connect_nodes(Nodes),
    override_running_nodes([NewNode]),
    override_running_nodes(Nodes),

    FeatureName = ?FUNCTION_NAME,
    FeatureFlags = #{FeatureName =>
                     #{provided_by => ?MODULE,
                       stability => stable,
                       callbacks =>
                       #{post_enable =>
                         {?MODULE, mf_wait_and_count_runs_v2_post_enable}}}},
    inject_on_nodes(AllNodes, FeatureFlags),

    ct:pal(
      "Checking the feature flag is supported but disabled on all nodes"),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   ?assert(rabbit_feature_flags:is_supported(FeatureName)),
                   ?assertNot(rabbit_feature_flags:is_enabled(FeatureName)),
                   ok
           end,
           [])
         || Node <- AllNodes],

    ct:pal(
      "Enabling the feature flag in the cluster (in a separate process)"),
    Peer = self(),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   %% The migration function uses the `Peer' PID (the process
                   %% executing the testcase) to notify its own PID and wait
                   %% for a signal from `Peer' to proceed and finish the
                   %% migration.
                   record_peer_proc(Peer),
                   ok
           end,
           [])
         || Node <- AllNodes],
    Enabler = spawn_link(
                fun() ->
                        ok =
                        run_on_node(
                          FirstNode,
                          fun() ->
                                  ?assertEqual(
                                     ok,
                                     rabbit_feature_flags:enable(
                                       FeatureName)),
                                  ok
                          end,
                          [])
                end),

    %% By waiting for the message from one of the migration function
    %% instances, we make sure the feature flags controller on `FirstNode' is
    %% blocked and waits for a message from this process. Therefore, we are
    %% sure the feature flag is in the `state_changing' state and we can try
    %% to add a new node and sync its feature flags.
    FirstNodeMigFunPid = receive
                             {_Node, MigFunPid1, waiting} -> MigFunPid1
                         end,

    %% Check compatibility between NewNodes and Nodes. This doesn't block.
    ok = run_on_node(
           NewNode,
           fun() ->
                   ?assertEqual(
                      ok,
                      rabbit_feature_flags:check_node_compatibility(
                        FirstNode)),
                   ok
           end, []),

    %% Add node to cluster and synchronize feature flags. The synchronization
    %% blocks.
    connect_nodes(AllNodes),
    override_running_nodes(AllNodes),
    ct:pal(
      "Synchronizing feature flags in the expanded cluster (in a separate "
      "process)~n"
      "~n"
      "NOTE: Error messages about crashed migration functions can be "
      "ignored for feature~n"
      "      flags other than `~s`~n"
      "      because they assume they run inside RabbitMQ.",
      [FeatureName]),
    Syncer = spawn_link(
               fun() ->
                       ok =
                       run_on_node(
                         NewNode,
                         fun() ->
                                 ?assertEqual(
                                    ok,
                                    rabbit_feature_flags:
                                    sync_feature_flags_with_cluster(
                                      Nodes, true)),
                                 ok
                         end, [])
               end),

    ct:pal(
      "Checking the feature flag is enabled in the initial cluster"),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   ?assert(
                      rabbit_feature_flags:is_enabled(
                        FeatureName, non_blocking)),
                   ok
           end,
           [])
         || Node <- Nodes],

    ct:pal("Checking the feature flag is still disabled on the new node"),
    ok = run_on_node(
           NewNode,
           fun() ->
                   ?assertNot(rabbit_feature_flags:is_enabled(FeatureName)),
                   ok
           end,
           []),

    %% Unblock the migration functions on `Nodes'.
    EnablerMRef = erlang:monitor(process, Enabler),
    SyncerMRef = erlang:monitor(process, Syncer),
    unlink(Enabler),
    unlink(Syncer),

    %% The migration function runs on all clustered nodes with v2, including
    %% the one joining the cluster, thanks to the synchronization.
    %%
    %% When this testcase runs with feature flags v1, the feature flag we want
    %% to enable uses the migration function API v2: this implicitly enables
    %% `feature_flags_v2'. As part of the synchronization, the node still on
    %% feature flags v1 will try to sync `feature_flags_v2' specificaly first.
    %% After that, the controller-based sync proceeds.
    ExpectedNodes = Nodes ++ [NewNode],

    %% Unblock the migration function for which we already consumed the
    %% `waiting' notification.
    FirstMigratedNode = node(FirstNodeMigFunPid),
    ct:pal(
      "Unblocking first node (~p @ ~s)",
      [FirstNodeMigFunPid, FirstMigratedNode]),
    FirstNodeMigFunPid ! proceed,

    %% Unblock the rest and collect the node names of all migration functions
    %% which ran.
    ct:pal("Unblocking other nodes, including the joining one"),
    OtherMigratedNodes = [receive
                              {Node, MigFunPid2, waiting} ->
                                  MigFunPid2 ! proceed,
                                  Node
                          end || Node <- ExpectedNodes -- [FirstMigratedNode]],
    MigratedNodes = [FirstMigratedNode | OtherMigratedNodes],
    ?assertEqual(lists:sort(ExpectedNodes), lists:sort(MigratedNodes)),

    ct:pal("Waiting for spawned processes to terminate"),
    receive
        {'DOWN', EnablerMRef, process, Enabler, EnablerReason} ->
            ?assertEqual(normal, EnablerReason)
    end,
    receive
        {'DOWN', SyncerMRef, process, Syncer, SyncerReason} ->
            ?assertEqual(normal, SyncerReason)
    end,

    ct:pal("Checking the feature flag is enabled in the expanded cluster"),
    _ = [ok =
         run_on_node(
           Node,
           fun() ->
                   ?assert(rabbit_feature_flags:is_enabled(FeatureName)),
                   ?assertEqual(
                      1,
                      persistent_term:get(?PT_MIGRATION_FUN_RUNS, 0)),
                   ok
           end,
           [])
         || Node <- AllNodes],
    ok.
