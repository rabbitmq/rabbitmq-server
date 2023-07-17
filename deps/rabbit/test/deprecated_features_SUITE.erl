%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2023 VMware, Inc. or its affiliates.  All rights reserved.
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
         has_is_feature_used_cb_returning_false/1,
         has_is_feature_used_cb_returning_true/1,
         get_appropriate_warning_when_permitted/1,
         get_appropriate_warning_when_denied/1,
         get_appropriate_warning_when_disconnected/1,
         get_appropriate_warning_when_removed/1,
         deprecated_feature_enabled_if_feature_flag_depends_on_it/1,

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
             has_is_feature_used_cb_returning_false,
             has_is_feature_used_cb_returning_true,
             get_appropriate_warning_when_permitted,
             get_appropriate_warning_when_denied,
             get_appropriate_warning_when_disconnected,
             get_appropriate_warning_when_removed,
             deprecated_feature_enabled_if_feature_flag_depends_on_it
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
    rabbit_ct_helpers:run_setup_steps(
      Config,
      [fun rabbit_ct_helpers:redirect_logger_to_ct_logs/1]).

end_per_suite(Config) ->
    Config.

init_per_group(cluster_size_1, Config) ->
    rabbit_ct_helpers:set_config(Config, {nodes_count, 1});
init_per_group(cluster_size_3, Config) ->
    rabbit_ct_helpers:set_config(Config, {nodes_count, 3});
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    NodesCount = ?config(nodes_count, Config),
    NodenamePrefix = list_to_atom(
                       lists:flatten(
                         io_lib:format("~s-cs~b", [Testcase, NodesCount]))),
    rabbit_ct_helpers:run_steps(
      Config,
      [fun(Cfg) ->
               feature_flags_v2_SUITE:start_slave_nodes(Cfg, NodenamePrefix)
       end]).

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
