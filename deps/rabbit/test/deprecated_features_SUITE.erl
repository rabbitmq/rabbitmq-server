%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(deprecated_features_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-export([suite/0,
         all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,

         use_unknown_deprecated_feature/1,
         use_deprecated_feature_permitted_by_default_everywhere/1,
         use_deprecated_feature_denied_by_default_everywhere/1,
         use_deprecated_feature_disconnected_everywhere/1,
         use_deprecated_feature_removed_everywhere/1,
         override_permitted_by_default_in_configuration/1,
         override_denied_by_default_in_configuration/1,
         override_disconnected_in_configuration/1,
         override_removed_in_configuration/1,
         change_from_denied_to_permitted_in_configuration_and_restart/1,
         change_from_denied_to_permitted_in_configuration_but_needed/1,
         has_is_feature_used_cb_returning_false/1,
         has_is_feature_used_cb_returning_true/1,
         get_appropriate_warning_when_permitted/1,
         get_appropriate_warning_when_denied/1,
         get_appropriate_warning_when_disconnected/1,
         get_appropriate_warning_when_removed/1,
         deprecated_feature_enabled_if_feature_flag_depends_on_it/1,
         list_all_deprecated_features/1,
         list_used_deprecated_features/1,

         feature_is_unused/1,
         feature_is_used/1
        ]).

suite() ->
    [{timetrap, {minutes, 1}}].

all() ->
    [
     {group, cluster_size_1},
     {group, cluster_size_3}
    ].

groups() ->
    Tests = [
             use_unknown_deprecated_feature,
             use_deprecated_feature_permitted_by_default_everywhere,
             use_deprecated_feature_denied_by_default_everywhere,
             use_deprecated_feature_disconnected_everywhere,
             use_deprecated_feature_removed_everywhere,
             override_permitted_by_default_in_configuration,
             override_denied_by_default_in_configuration,
             override_disconnected_in_configuration,
             override_removed_in_configuration,
             change_from_denied_to_permitted_in_configuration_and_restart,
             change_from_denied_to_permitted_in_configuration_but_needed,
             has_is_feature_used_cb_returning_false,
             has_is_feature_used_cb_returning_true,
             get_appropriate_warning_when_permitted,
             get_appropriate_warning_when_denied,
             get_appropriate_warning_when_disconnected,
             get_appropriate_warning_when_removed,
             deprecated_feature_enabled_if_feature_flag_depends_on_it,
             list_all_deprecated_features,
             list_used_deprecated_features
            ],
    [
     {cluster_size_1, [], Tests},
     {cluster_size_3, [], Tests}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    logger:set_primary_config(level, debug),
    rabbit_ct_helpers:run_setup_steps(Config, []).

end_per_suite(Config) ->
    Config.

init_per_group(cluster_size_1, Config) ->
    rabbit_ct_helpers:set_config(Config, {rmq_nodes_count, 1});
init_per_group(cluster_size_3, Config) ->
    rabbit_ct_helpers:set_config(Config, [{rmq_nodes_count, 3},
                                          {rmq_nodes_clustered, true}]);
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, Config) ->
    Config.

init_per_testcase(Testcase, Config)
  when Testcase =:= change_from_denied_to_permitted_in_configuration_and_restart orelse
       Testcase =:= change_from_denied_to_permitted_in_configuration_but_needed ->
    Config1 = rabbit_ct_helpers:set_config(
                Config, [{rmq_nodename_suffix, Testcase}]),
    Config2 = rabbit_ct_helpers:testcase_started(Config1, Testcase),
    Config3 = rabbit_ct_helpers:run_steps(
                Config2, rabbit_ct_broker_helpers:setup_steps()),
    Config3;
init_per_testcase(Testcase, Config) ->
    NodesCount = ?config(rmq_nodes_count, Config),
    NodenamePrefix = list_to_atom(
                       lists:flatten(
                         io_lib:format("~s-cs~b", [Testcase, NodesCount]))),
    rabbit_ct_helpers:run_steps(
      Config,
      [fun(Cfg) ->
               feature_flags_v2_SUITE:start_slave_nodes(Cfg, NodenamePrefix)
       end]).

end_per_testcase(Testcase, Config)
  when Testcase =:= change_from_denied_to_permitted_in_configuration_and_restart orelse
       Testcase =:= change_from_denied_to_permitted_in_configuration_but_needed ->
    Config1 = rabbit_ct_helpers:run_steps(
                Config,
                rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase);
end_per_testcase(_Testcase, Config) ->
    rabbit_ct_helpers:run_steps(
      Config,
      [fun feature_flags_v2_SUITE:stop_slave_nodes/1]).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

use_unknown_deprecated_feature(Config) ->
    AllNodes = ?config(nodes, Config),
    FeatureName = ?FUNCTION_NAME,
    _ = [ok =
         feature_flags_v2_SUITE:run_on_node(
           Node,
           fun() ->
                   ?assertNot(rabbit_feature_flags:is_supported(FeatureName)),
                   ?assertNot(rabbit_feature_flags:is_enabled(FeatureName)),
                   ?assert(
                      rabbit_deprecated_features:is_permitted(FeatureName)),

                   %% The node doesn't know about the deprecated feature and
                   %% thus rejects the request.
                   ?assertEqual(
                      {error, unsupported},
                      rabbit_feature_flags:enable(FeatureName)),
                   ?assertNot(rabbit_feature_flags:is_supported(FeatureName)),
                   ?assertNot(rabbit_feature_flags:is_enabled(FeatureName)),
                   ?assert(
                      rabbit_deprecated_features:is_permitted(FeatureName)),
                   ok
           end)
         || Node <- AllNodes].

use_deprecated_feature_permitted_by_default_everywhere(Config) ->
    [FirstNode | _] = AllNodes = ?config(nodes, Config),
    feature_flags_v2_SUITE:connect_nodes(AllNodes),
    feature_flags_v2_SUITE:override_running_nodes(AllNodes),

    FeatureName = ?FUNCTION_NAME,
    FeatureFlags = #{FeatureName =>
                     #{provided_by => rabbit,
                       deprecation_phase => permitted_by_default}},
    ?assertEqual(
       ok,
       feature_flags_v2_SUITE:inject_on_nodes(AllNodes, FeatureFlags)),

    _ = [ok =
         feature_flags_v2_SUITE:run_on_node(
           Node,
           fun() ->
                   ?assert(rabbit_feature_flags:is_supported(FeatureName)),
                   ?assertNot(rabbit_feature_flags:is_enabled(FeatureName)),
                   ?assert(
                      rabbit_deprecated_features:is_permitted(FeatureName)),
                   ok
           end)
         || Node <- AllNodes],

    ok = feature_flags_v2_SUITE:run_on_node(
           FirstNode,
           fun() ->
                   ?assertEqual(
                      ok,
                      rabbit_feature_flags:enable(FeatureName)),
                   ok
           end),

    _ = [ok =
         feature_flags_v2_SUITE:run_on_node(
           Node,
           fun() ->
                   ?assert(rabbit_feature_flags:is_supported(FeatureName)),
                   ?assert(rabbit_feature_flags:is_enabled(FeatureName)),
                   ?assertNot(
                      rabbit_deprecated_features:is_permitted(FeatureName)),
                   ok
           end )
         || Node <- AllNodes].

use_deprecated_feature_denied_by_default_everywhere(Config) ->
    AllNodes = ?config(nodes, Config),
    feature_flags_v2_SUITE:connect_nodes(AllNodes),
    feature_flags_v2_SUITE:override_running_nodes(AllNodes),

    FeatureName = ?FUNCTION_NAME,
    FeatureFlags = #{FeatureName =>
                     #{provided_by => rabbit,
                       deprecation_phase => denied_by_default}},
    ?assertEqual(
       ok,
       feature_flags_v2_SUITE:inject_on_nodes(AllNodes, FeatureFlags)),

    _ = [ok =
         feature_flags_v2_SUITE:run_on_node(
           Node,
           fun() ->
                   ?assert(rabbit_feature_flags:is_supported(FeatureName)),
                   ?assert(rabbit_feature_flags:is_enabled(FeatureName)),
                   ?assertNot(
                      rabbit_deprecated_features:is_permitted(FeatureName)),
                   ok
           end)
         || Node <- AllNodes].

use_deprecated_feature_disconnected_everywhere(Config) ->
    AllNodes = ?config(nodes, Config),
    feature_flags_v2_SUITE:connect_nodes(AllNodes),
    feature_flags_v2_SUITE:override_running_nodes(AllNodes),

    FeatureName = ?FUNCTION_NAME,
    FeatureFlags = #{FeatureName =>
                     #{provided_by => rabbit,
                       deprecation_phase => disconnected}},
    ?assertEqual(
       ok,
       feature_flags_v2_SUITE:inject_on_nodes(AllNodes, FeatureFlags)),

    _ = [ok =
         feature_flags_v2_SUITE:run_on_node(
           Node,
           fun() ->
                   ?assert(rabbit_feature_flags:is_supported(FeatureName)),
                   ?assert(rabbit_feature_flags:is_enabled(FeatureName)),
                   ?assertNot(
                      rabbit_deprecated_features:is_permitted(FeatureName)),
                   ok
           end)
         || Node <- AllNodes].

use_deprecated_feature_removed_everywhere(Config) ->
    AllNodes = ?config(nodes, Config),
    feature_flags_v2_SUITE:connect_nodes(AllNodes),
    feature_flags_v2_SUITE:override_running_nodes(AllNodes),

    FeatureName = ?FUNCTION_NAME,
    FeatureFlags = #{FeatureName =>
                     #{provided_by => rabbit,
                       deprecation_phase => removed}},
    ?assertEqual(
       ok,
       feature_flags_v2_SUITE:inject_on_nodes(AllNodes, FeatureFlags)),

    _ = [ok =
         feature_flags_v2_SUITE:run_on_node(
           Node,
           fun() ->
                   ?assert(rabbit_feature_flags:is_supported(FeatureName)),
                   ?assert(rabbit_feature_flags:is_enabled(FeatureName)),
                   ?assertNot(
                      rabbit_deprecated_features:is_permitted(FeatureName)),
                   ok
           end)
         || Node <- AllNodes].

override_permitted_by_default_in_configuration(Config) ->
    AllNodes = ?config(nodes, Config),
    feature_flags_v2_SUITE:connect_nodes(AllNodes),
    feature_flags_v2_SUITE:override_running_nodes(AllNodes),

    FeatureName = ?FUNCTION_NAME,
    FeatureFlags = #{FeatureName =>
                     #{provided_by => rabbit,
                       deprecation_phase => permitted_by_default}},

    _ = [ok =
         feature_flags_v2_SUITE:run_on_node(
           Node,
           fun() ->
                   application:set_env(
                     rabbit, permit_deprecated_features,
                     #{FeatureName => false}, [{persistent, false}])
           end)
         || Node <- AllNodes],

    ?assertEqual(
       ok,
       feature_flags_v2_SUITE:inject_on_nodes(AllNodes, FeatureFlags)),

    _ = [ok =
         feature_flags_v2_SUITE:run_on_node(
           Node,
           fun() ->
                   ?assert(rabbit_feature_flags:is_supported(FeatureName)),
                   ?assert(rabbit_feature_flags:is_enabled(FeatureName)),
                   ?assertNot(
                      rabbit_deprecated_features:is_permitted(FeatureName)),
                   ok
           end)
         || Node <- AllNodes].

override_denied_by_default_in_configuration(Config) ->
    AllNodes = ?config(nodes, Config),
    feature_flags_v2_SUITE:connect_nodes(AllNodes),
    feature_flags_v2_SUITE:override_running_nodes(AllNodes),

    FeatureName = ?FUNCTION_NAME,
    FeatureFlags = #{FeatureName =>
                     #{provided_by => rabbit,
                       deprecation_phase => denied_by_default}},

    _ = [ok =
         feature_flags_v2_SUITE:run_on_node(
           Node,
           fun() ->
                   application:set_env(
                     rabbit, permit_deprecated_features,
                     #{FeatureName => true}, [{persistent, false}])
           end)
         || Node <- AllNodes],

    ?assertEqual(
       ok,
       feature_flags_v2_SUITE:inject_on_nodes(AllNodes, FeatureFlags)),

    _ = [ok =
         feature_flags_v2_SUITE:run_on_node(
           Node,
           fun() ->
                   ?assert(rabbit_feature_flags:is_supported(FeatureName)),
                   ?assertNot(rabbit_feature_flags:is_enabled(FeatureName)),
                   ?assert(
                      rabbit_deprecated_features:is_permitted(FeatureName)),
                   ok
           end)
         || Node <- AllNodes].

override_disconnected_in_configuration(Config) ->
    AllNodes = ?config(nodes, Config),
    feature_flags_v2_SUITE:connect_nodes(AllNodes),
    feature_flags_v2_SUITE:override_running_nodes(AllNodes),

    FeatureName = ?FUNCTION_NAME,
    FeatureFlags = #{FeatureName =>
                     #{provided_by => rabbit,
                       deprecation_phase => disconnected}},

    _ = [ok =
         feature_flags_v2_SUITE:run_on_node(
           Node,
           fun() ->
                   application:set_env(
                     rabbit, permit_deprecated_features,
                     #{FeatureName => true}, [{persistent, false}])
           end)
         || Node <- AllNodes],

    ?assertEqual(
       ok,
       feature_flags_v2_SUITE:inject_on_nodes(AllNodes, FeatureFlags)),

    _ = [ok =
         feature_flags_v2_SUITE:run_on_node(
           Node,
           fun() ->
                   ?assert(rabbit_feature_flags:is_supported(FeatureName)),
                   ?assert(rabbit_feature_flags:is_enabled(FeatureName)),
                   ?assertNot(
                      rabbit_deprecated_features:is_permitted(FeatureName)),
                   ok
           end)
         || Node <- AllNodes].

override_removed_in_configuration(Config) ->
    AllNodes = ?config(nodes, Config),
    feature_flags_v2_SUITE:connect_nodes(AllNodes),
    feature_flags_v2_SUITE:override_running_nodes(AllNodes),

    FeatureName = ?FUNCTION_NAME,
    FeatureFlags = #{FeatureName =>
                     #{provided_by => rabbit,
                       deprecation_phase => removed}},

    _ = [ok =
         feature_flags_v2_SUITE:run_on_node(
           Node,
           fun() ->
                   application:set_env(
                     rabbit, permit_deprecated_features,
                     #{FeatureName => true}, [{persistent, false}])
           end)
         || Node <- AllNodes],

    ?assertEqual(
       ok,
       feature_flags_v2_SUITE:inject_on_nodes(AllNodes, FeatureFlags)),

    _ = [ok =
         feature_flags_v2_SUITE:run_on_node(
           Node,
           fun() ->
                   ?assert(rabbit_feature_flags:is_supported(FeatureName)),
                   ?assert(rabbit_feature_flags:is_enabled(FeatureName)),
                   ?assertNot(
                      rabbit_deprecated_features:is_permitted(FeatureName)),
                   ok
           end)
         || Node <- AllNodes].

change_from_denied_to_permitted_in_configuration_and_restart(Config) ->
    [FirstNode | _] = AllNodes = rabbit_ct_broker_helpers:get_node_configs(
                                   Config, nodename),

    FeatureName = ?FUNCTION_NAME,
    FeatureFlags = #{FeatureName =>
                     #{provided_by => rabbit,
                       deprecation_phase => denied_by_default}},

    Ok = [ok || _ <- AllNodes],
    True = [true || _ <- AllNodes],
    False = [false || _ <- AllNodes],

    ?assertEqual(
       Ok,
       rabbit_ct_broker_helpers:rpc_all(
         Config,
         rabbit_feature_flags, inject_test_feature_flags, [FeatureFlags])),
    ?assertEqual(
       Ok,
       rabbit_ct_broker_helpers:rpc_all(
         Config,
         rabbit_feature_flags, refresh_feature_flags_after_app_load, [])),

    ?assertEqual(
       True,
       feature_flags_SUITE:is_feature_flag_supported(Config, FeatureName)),
    ?assertEqual(
       True,
       feature_flags_SUITE:is_feature_flag_enabled(Config, FeatureName)),
    ?assertEqual(
       False,
       is_deprecated_feature_permitted(Config, FeatureName)),

    ok = rabbit_ct_broker_helpers:rpc(
           Config, FirstNode,
           application, set_env,
           [rabbit, permit_deprecated_features, #{FeatureName => true}, [{persistent, true}]]),
    ok = rabbit_ct_broker_helpers:restart_broker(Config, FirstNode),

    ?assertEqual(
       True,
       feature_flags_SUITE:is_feature_flag_supported(Config, FeatureName)),
    ?assertEqual(
       False,
       feature_flags_SUITE:is_feature_flag_enabled(Config, FeatureName)),
    ?assertEqual(
       True,
       is_deprecated_feature_permitted(Config, FeatureName)).

change_from_denied_to_permitted_in_configuration_but_needed(Config) ->
    [FirstNode | _] = AllNodes = rabbit_ct_broker_helpers:get_node_configs(
                                   Config, nodename),

    FeatureName = ?FUNCTION_NAME,
    ParentFeatureName = parent_feature_flag,
    GrandParentFeatureName = grand_parent_feature_flag,
    FeatureFlags = #{FeatureName =>
                     #{provided_by => rabbit,
                       deprecation_phase => denied_by_default},
                     ParentFeatureName =>
                     #{provided_by => rabbit,
                       stability => experimental,
                       depends_on => [FeatureName]},
                     GrandParentFeatureName =>
                     #{provided_by => rabbit,
                       stability => experimental,
                       depends_on => [ParentFeatureName]}},

    Ok = [ok || _ <- AllNodes],
    True = [true || _ <- AllNodes],
    False = [false || _ <- AllNodes],

    ?assertEqual(
       Ok,
       rabbit_ct_broker_helpers:rpc_all(
         Config,
         rabbit_feature_flags, inject_test_feature_flags, [FeatureFlags])),
    ?assertEqual(
       Ok,
       rabbit_ct_broker_helpers:rpc_all(
         Config,
         rabbit_feature_flags, refresh_feature_flags_after_app_load, [])),
    ?assertEqual(
       ok,
       rabbit_ct_broker_helpers:enable_feature_flag(
         Config, GrandParentFeatureName)),

    ?assertEqual(
       True,
       feature_flags_SUITE:is_feature_flag_supported(Config, FeatureName)),
    ?assertEqual(
       True,
       feature_flags_SUITE:is_feature_flag_enabled(Config, FeatureName)),
    ?assertEqual(
       False,
       is_deprecated_feature_permitted(Config, FeatureName)),

    ok = rabbit_ct_broker_helpers:rpc(
           Config, FirstNode,
           application, set_env,
           [rabbit, permit_deprecated_features, #{FeatureName => true}, [{persistent, true}]]),
    ok = rabbit_ct_broker_helpers:restart_broker(Config, FirstNode),

    ?assertEqual(
       True,
       feature_flags_SUITE:is_feature_flag_supported(Config, FeatureName)),
    ?assertEqual(
       True,
       feature_flags_SUITE:is_feature_flag_enabled(Config, FeatureName)),
    ?assertEqual(
       False,
       is_deprecated_feature_permitted(Config, FeatureName)).

is_deprecated_feature_permitted(Config, FeatureName) ->
    rabbit_ct_broker_helpers:rpc_all(
      Config, rabbit_deprecated_features, is_permitted, [FeatureName]).

has_is_feature_used_cb_returning_false(Config) ->
    AllNodes = ?config(nodes, Config),
    feature_flags_v2_SUITE:connect_nodes(AllNodes),
    feature_flags_v2_SUITE:override_running_nodes(AllNodes),

    FeatureName = ?FUNCTION_NAME,
    FeatureFlags = #{FeatureName =>
                     #{provided_by => rabbit,
                       deprecation_phase => denied_by_default,
                       callbacks => #{is_feature_used =>
                                      {?MODULE, feature_is_unused}}}},
    ?assertEqual(
       ok,
       feature_flags_v2_SUITE:inject_on_nodes(AllNodes, FeatureFlags)),

    _ = [ok =
         feature_flags_v2_SUITE:run_on_node(
           Node,
           fun() ->
                   ?assert(rabbit_feature_flags:is_supported(FeatureName)),
                   ?assert(rabbit_feature_flags:is_enabled(FeatureName)),
                   ?assertNot(
                      rabbit_deprecated_features:is_permitted(FeatureName)),
                   ok
           end)
         || Node <- AllNodes].

feature_is_unused(_Args) ->
    false.

has_is_feature_used_cb_returning_true(Config) ->
    AllNodes = ?config(nodes, Config),
    feature_flags_v2_SUITE:connect_nodes(AllNodes),
    feature_flags_v2_SUITE:override_running_nodes(AllNodes),

    FeatureName = ?FUNCTION_NAME,
    FeatureFlags = #{FeatureName =>
                     #{provided_by => rabbit,
                       deprecation_phase => denied_by_default,
                       callbacks => #{is_feature_used =>
                                      {?MODULE, feature_is_used}}}},
    ?assertEqual(
       {error, {failed_to_deny_deprecated_features, [FeatureName]}},
       feature_flags_v2_SUITE:inject_on_nodes(AllNodes, FeatureFlags)),

    %% The deprecated feature is marked as denied when the registry is
    %% initialized/updated. It is the refresh that will return an error (the
    %% one returned above).
    _ = [ok =
         feature_flags_v2_SUITE:run_on_node(
           Node,
           fun() ->
                   ?assert(rabbit_feature_flags:is_supported(FeatureName)),
                   ?assert(rabbit_feature_flags:is_enabled(FeatureName)),
                   ?assertNot(
                      rabbit_deprecated_features:is_permitted(FeatureName)),
                   ok
           end)
         || Node <- AllNodes].

feature_is_used(_Args) ->
    true.

-define(MSGS, #{when_permitted => "permitted",
                when_denied => "denied",
                when_removed => "removed"}).

get_appropriate_warning_when_permitted(Config) ->
    [FirstNode | _] = AllNodes = ?config(nodes, Config),
    feature_flags_v2_SUITE:connect_nodes(AllNodes),
    feature_flags_v2_SUITE:override_running_nodes(AllNodes),

    FeatureName = ?FUNCTION_NAME,
    FeatureFlags = #{FeatureName =>
                     #{provided_by => rabbit,
                       deprecation_phase => permitted_by_default,
                       messages => ?MSGS}},
    ?assertEqual(
       ok,
       feature_flags_v2_SUITE:inject_on_nodes(AllNodes, FeatureFlags)),

    _ = [ok =
         feature_flags_v2_SUITE:run_on_node(
           Node,
           fun() ->
                   ?assert(
                      rabbit_deprecated_features:is_permitted(FeatureName)),
                   ?assertEqual(
                      maps:get(when_permitted, ?MSGS),
                      rabbit_deprecated_features:get_warning(FeatureName)),
                   ok
           end)
         || Node <- AllNodes],

    ok = feature_flags_v2_SUITE:run_on_node(
           FirstNode,
           fun() ->
                   ?assertEqual(
                      ok,
                      rabbit_feature_flags:enable(FeatureName)),
                   ok
           end),

    _ = [ok =
         feature_flags_v2_SUITE:run_on_node(
           Node,
           fun() ->
                   ?assertNot(
                      rabbit_deprecated_features:is_permitted(FeatureName)),
                   ?assertEqual(
                      maps:get(when_denied, ?MSGS),
                      rabbit_deprecated_features:get_warning(FeatureName)),
                   ok
           end)
         || Node <- AllNodes].

get_appropriate_warning_when_denied(Config) ->
    [FirstNode | _] = AllNodes = ?config(nodes, Config),
    feature_flags_v2_SUITE:connect_nodes(AllNodes),
    feature_flags_v2_SUITE:override_running_nodes(AllNodes),

    FeatureName = ?FUNCTION_NAME,
    FeatureFlags = #{FeatureName =>
                     #{provided_by => rabbit,
                       deprecation_phase => denied_by_default,
                       messages => ?MSGS}},

    _ = [ok =
         feature_flags_v2_SUITE:run_on_node(
           Node,
           fun() ->
                   application:set_env(
                     rabbit, permit_deprecated_features,
                     #{FeatureName => true}, [{persistent, false}])
           end)
         || Node <- AllNodes],

    ?assertEqual(
       ok,
       feature_flags_v2_SUITE:inject_on_nodes(AllNodes, FeatureFlags)),

    _ = [ok =
         feature_flags_v2_SUITE:run_on_node(
           Node,
           fun() ->
                   ?assert(
                      rabbit_deprecated_features:is_permitted(FeatureName)),
                   ?assertEqual(
                      maps:get(when_permitted, ?MSGS),
                      rabbit_deprecated_features:get_warning(FeatureName)),
                   ok
           end)
         || Node <- AllNodes],

    ok = feature_flags_v2_SUITE:run_on_node(
           FirstNode,
           fun() ->
                   ?assertEqual(
                      ok,
                      rabbit_feature_flags:enable(FeatureName)),
                   ok
           end),

    _ = [ok =
         feature_flags_v2_SUITE:run_on_node(
           Node,
           fun() ->
                   ?assertNot(
                      rabbit_deprecated_features:is_permitted(FeatureName)),
                   ?assertEqual(
                      maps:get(when_denied, ?MSGS),
                      rabbit_deprecated_features:get_warning(FeatureName)),
                   ok
           end)
         || Node <- AllNodes].

get_appropriate_warning_when_disconnected(Config) ->
    AllNodes = ?config(nodes, Config),
    feature_flags_v2_SUITE:connect_nodes(AllNodes),
    feature_flags_v2_SUITE:override_running_nodes(AllNodes),

    FeatureName = ?FUNCTION_NAME,
    FeatureFlags = #{FeatureName =>
                     #{provided_by => rabbit,
                       deprecation_phase => disconnected,
                       messages => ?MSGS}},
    ?assertEqual(
       ok,
       feature_flags_v2_SUITE:inject_on_nodes(AllNodes, FeatureFlags)),

    _ = [ok =
         feature_flags_v2_SUITE:run_on_node(
           Node,
           fun() ->
                   ?assertNot(
                      rabbit_deprecated_features:is_permitted(FeatureName)),
                   ?assertEqual(
                      maps:get(when_removed, ?MSGS),
                      rabbit_deprecated_features:get_warning(FeatureName)),
                   ok
           end)
         || Node <- AllNodes].

get_appropriate_warning_when_removed(Config) ->
    AllNodes = ?config(nodes, Config),
    feature_flags_v2_SUITE:connect_nodes(AllNodes),
    feature_flags_v2_SUITE:override_running_nodes(AllNodes),

    FeatureName = ?FUNCTION_NAME,
    FeatureFlags = #{FeatureName =>
                     #{provided_by => rabbit,
                       deprecation_phase => disconnected,
                       messages => ?MSGS}},
    ?assertEqual(
       ok,
       feature_flags_v2_SUITE:inject_on_nodes(AllNodes, FeatureFlags)),

    _ = [ok =
         feature_flags_v2_SUITE:run_on_node(
           Node,
           fun() ->
                   ?assertNot(
                      rabbit_deprecated_features:is_permitted(FeatureName)),
                   ?assertEqual(
                      maps:get(when_removed, ?MSGS),
                      rabbit_deprecated_features:get_warning(FeatureName)),
                   ok
           end)
         || Node <- AllNodes].

deprecated_feature_enabled_if_feature_flag_depends_on_it(Config) ->
    [FirstNode | _] = AllNodes = ?config(nodes, Config),
    feature_flags_v2_SUITE:connect_nodes(AllNodes),
    feature_flags_v2_SUITE:override_running_nodes(AllNodes),

    FeatureName = ?FUNCTION_NAME,
    FeatureFlags = #{FeatureName =>
                     #{provided_by => rabbit,
                       deprecation_phase => permitted_by_default},

                     my_feature_flag =>
                     #{provided_by => rabbit,
                       stability => experimental,
                       depends_on => [FeatureName]}},
    ?assertEqual(
       ok,
       feature_flags_v2_SUITE:inject_on_nodes(AllNodes, FeatureFlags)),

    _ = [ok =
         feature_flags_v2_SUITE:run_on_node(
           Node,
           fun() ->
                   ?assert(rabbit_feature_flags:is_supported(FeatureName)),
                   ?assertNot(rabbit_feature_flags:is_enabled(FeatureName)),
                   ?assert(
                      rabbit_deprecated_features:is_permitted(FeatureName)),
                   ok
           end)
         || Node <- AllNodes],

    ok = feature_flags_v2_SUITE:run_on_node(
           FirstNode,
           fun() ->
                   ?assertEqual(
                      ok,
                      rabbit_feature_flags:enable(my_feature_flag)),
                   ok
           end),

    _ = [ok =
         feature_flags_v2_SUITE:run_on_node(
           Node,
           fun() ->
                   ?assert(rabbit_feature_flags:is_enabled(my_feature_flag)),

                   ?assert(rabbit_feature_flags:is_supported(FeatureName)),
                   ?assert(rabbit_feature_flags:is_enabled(FeatureName)),
                   ?assertNot(
                      rabbit_deprecated_features:is_permitted(FeatureName)),
                   ok
           end )
         || Node <- AllNodes],

    _ = [ok =
         feature_flags_v2_SUITE:run_on_node(
           Node,
           fun() ->
                   ?assertEqual(
                      ok,
                      rabbit_ff_registry_factory:reset_registry()),
                   ok
           end )
         || Node <- AllNodes],

    _ = [ok =
         feature_flags_v2_SUITE:run_on_node(
           Node,
           fun() ->
                   ?assertEqual(
                      ok,
                      rabbit_ff_registry_factory:initialize_registry()),
                   ok
           end )
         || Node <- AllNodes],

    _ = [ok =
         feature_flags_v2_SUITE:run_on_node(
           Node,
           fun() ->
                   ?assert(rabbit_feature_flags:is_enabled(my_feature_flag)),

                   ?assert(rabbit_feature_flags:is_supported(FeatureName)),
                   ?assert(rabbit_feature_flags:is_enabled(FeatureName)),
                   ?assertNot(
                      rabbit_deprecated_features:is_permitted(FeatureName)),
                   ok
           end )
         || Node <- AllNodes].

list_all_deprecated_features(Config) ->
    [FirstNode | _] = AllNodes = ?config(nodes, Config),
    feature_flags_v2_SUITE:connect_nodes(AllNodes),
    feature_flags_v2_SUITE:override_running_nodes(AllNodes),

    FeatureName = ?FUNCTION_NAME,
    FeatureFlags = #{FeatureName =>
                     #{provided_by => rabbit,
                       deprecation_phase => permitted_by_default}},
    ?assertEqual(
       ok,
       feature_flags_v2_SUITE:inject_on_nodes(AllNodes, FeatureFlags)),

    feature_flags_v2_SUITE:run_on_node(
      FirstNode,
      fun() ->
              Map = rabbit_deprecated_features:list(all),
              ?assert(maps:is_key(FeatureName, Map))
      end).

list_used_deprecated_features(Config) ->
    [FirstNode | _] = AllNodes = ?config(nodes, Config),
    feature_flags_v2_SUITE:connect_nodes(AllNodes),
    feature_flags_v2_SUITE:override_running_nodes(AllNodes),

    UsedFeatureName = used_deprecated_feature,
    UnusedFeatureName = unused_deprecated_feature,
    FeatureFlags = #{UsedFeatureName =>
                         #{provided_by => rabbit,
                           deprecation_phase => permitted_by_default,
                           callbacks => #{is_feature_used => {?MODULE, feature_is_used}}},
                     UnusedFeatureName =>
                         #{provided_by => rabbit,
                           deprecation_phase => permitted_by_default,
                           callbacks => #{is_feature_used => {?MODULE, feature_is_unused}}}},
    ?assertEqual(
       ok,
       feature_flags_v2_SUITE:inject_on_nodes(AllNodes, FeatureFlags)),

    feature_flags_v2_SUITE:run_on_node(
      FirstNode,
      fun() ->
              Map = rabbit_deprecated_features:list(used),
              ?assertNot(maps:is_key(UnusedFeatureName, Map)),
              ?assert(maps:is_key(UsedFeatureName, Map))
      end).
