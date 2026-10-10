%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_plugin_versioning_SUITE).

-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
      {group, parallel_tests}
    ].

groups() ->
    [
      {parallel_tests, [parallel], [
          version_support,
          plugin_validation,
          supported_release_series
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

version_support(_Config) ->
    Examples = [
     {[], "any version", true}
    ,{[], "0.0.0", true}
    ,{[], "3.5.6", true}
    ,{["something"], "something", true}
    ,{["3.5.4"], "something", false}
    ,{["3.4.5", "3.6.0"], "0.0.0", true}
    ,{["3.4.5", "3.6.0"], "", true}
    ,{["3.4.5"], <<"0.0.0">>, true}
    ,{["3.4.5"], <<>>, true}
    ,{["3.4.5"], <<"3.4.5">>, true}
    ,{["3.4.5"], <<"3.4.6">>, false}
    ,{["something", "~3.5.6"], "3.5.7", true}
    ,{["3.4.0", "3.5.6"], "3.6.1", false}
    ,{["3.5.2", "3.6.1", "3.7.1"], "3.5.2", true}
    ,{["3.5.2", "3.6.1", "3.7.1"], "3.5.1", false}
    %% `X.Y.Z` is an exact version, not a series.
    ,{["3.5.2", "3.6.1", "3.7.1"], "3.6.2", false}
    ,{["3.5.2", "~3.6.1", "3.7.1"], "3.6.2", true}
    %% `X.Y` is the whole `X.Y.x` series, `X` is the whole `X.x` series.
    ,{["3.5", "3.6", "3.7"], "3.5.1", true}
    ,{["3"], "3.5.1", true}
    ,{["3.6.2.333"], "3.6.2.333", true}
    ,{["3.6.2.333"], "3.6.2.334", false}
    ,{["3.5.2", "~3.6.1"], "3.6.2.999", true}
    ,{["3.6.0", "3.7.0"], "3.6.3-alpha.1", false}
    ,{["~3.6.0", "3.7.0"], "3.6.3-alpha.1", true}
    ,{["3.6.0", "3.7.0"], "3.7.0-alpha.89", true}
    ,{["^4.0.0"], "4.4.0", true}
    ,{["^4.0.0"], "5.0.0", false}
    ,{["~4.2.10"], "4.2.11", true}
    ,{["~4.2.10"], "4.3.0", false}
    ,{["~4.2"], "4.3.0", false}
    ,{["~> 4.2"], "4.3.0", true}
    ,{["~> 4.2"], "5.0.0", false}
    ,{["~> 4.2.10"], "4.3.0", false}
    ,{[">= 4.0.0 < 4.3.0"], "4.2.9", true}
    ,{[">=4.0.0 <4.3.0"], "4.3.0", false}
    ,{["4.0.0 - 4.2.0"], "4.2.0", true}
    ,{["4.x"], "4.9.0", true}
    ,{["*"], "4.0.0", true}
    ,{["4.3.0", "^4.5.0"], "4.4.5", false}
    ,{["4.3.0", "^4.5.0"], "4.6.0", true}
    ,{["4.3.0 || ^4.5.0"], "4.3.0", true}
    ,{["4.3.0 || ^4.5.0"], "4.3.21", false}
    ,{["4.3.x || ^4.5.0"], "4.3.21", true}
    ,{["4.3.x || ^4.5.0"], "4.4.5", false}
    ,{["= 4.4.0"], "4.4.0", true}
    ,{["= 4.4.0"], "4.4.1", false}
    ,{[<<"^4.0.0">>], "4.4.0", true}
    %% The broker version is compared without its pre-release suffix.
    ,{["^4.0.0"], "4.4.0-alpha.1", true}
    ,{["^4.0.0"], "5.0.0-alpha.1", false}
    ,{["~> 4.3"], "tanzu+rabbitmq.v4.3.12.dev", true}
    ,{["tanzu+rabbitmq.v4.3.12.dev"], "tanzu+rabbitmq.v4.3.12.dev", true}
    ,{["tanzu+rabbitmq.v4.3.12.dev"], "4.3.13", false}
    ,{["4.4.0-1"], "4.4.0-1", true}
    ,{["4.4.0-1"], "4.4.1", false}
    ,{["4.4.0"], "not.a.version", false}
    ,{["not.a.version"], "not.a.version", true}
    ,{["^^4.0.0", "~4.4.0"], "4.4.1", true}
    ,{["^^4.0.0"], "4.4.1", false}
    ,{[">= 4.0.0-beta"], "4.0.0", false}
    ,{["4.4.0+abc123"], "4.4.0+abc123", true}
    ,{["4.4.0-beta.1"], "4.4.0-beta.1", true}
    ,{["4.4.0-beta.1"], "4.4.1", false}
    ,{["=3.6.2"], "3.6.2.999", true}
    ],

    lists:foreach(
        fun({Versions, RabbitVersion, Expected}) ->
            {Expected, RabbitVersion, Versions} =
                {rabbit_plugins:is_version_supported(RabbitVersion, Versions),
                 RabbitVersion, Versions}
        end,
        Examples),
    ok.

-record(validation_example, {rabbit_version, plugins, errors, valid}).

plugin_validation(_Config) ->
    Examples = [
        #validation_example{
         rabbit_version = "3.7.1",
         plugins =
          [{plugin_a, "3.7.2", ["3.5.6", "3.7.1"], []},
           {plugin_b, "3.7.2", ["~3.7.0"], [{plugin_a, ["3.6.3", "~3.7.1"]}]}],
         errors = [],
         valid = [plugin_a, plugin_b]},

        #validation_example{
         rabbit_version = "3.7.1",
         plugins =
          [{plugin_a, "3.7.1", ["3.7.6"], []},
           {plugin_b, "3.7.2", ["~3.7.0"], [{plugin_a, ["3.6.3", "3.7.0"]}]}],
         errors =
          [{plugin_a, [{broker_version_mismatch, "3.7.1", ["3.7.6"]}]},
           {plugin_b, [{missing_dependency, plugin_a}]}],
         valid = []
        },

        #validation_example{
         rabbit_version = "3.7.1",
         plugins =
          [{plugin_a, "3.7.1", ["3.7.6"], []},
           {plugin_b, "3.7.2", ["~3.7.0"], [{plugin_a, ["3.7.0"]}]},
           {plugin_c, "3.7.2", ["~3.7.0"], [{plugin_b, ["3.7.3"]}]}],
         errors =
          [{plugin_a, [{broker_version_mismatch, "3.7.1", ["3.7.6"]}]},
           {plugin_b, [{missing_dependency, plugin_a}]},
           {plugin_c, [{missing_dependency, plugin_b}]}],
         valid = []
        },

        #validation_example{
         rabbit_version = "3.7.1",
         plugins =
          [{plugin_a, "3.7.1", ["3.7.1"], []},
           {plugin_b, "3.7.2", ["~3.7.0"], [{plugin_a, ["3.7.3"]}]},
           {plugin_d, "3.7.2", ["~3.7.0"], [{plugin_c, ["3.7.3"]}]}],
         errors =
          [{plugin_b, [{{dependency_version_mismatch, "3.7.1", ["3.7.3"]}, plugin_a}]},
           {plugin_d, [{missing_dependency, plugin_c}]}],
         valid = [plugin_a]
        },
        #validation_example{
         rabbit_version = "3.7.1",
         plugins =
          [{plugin_a, "3.7.1", ["^3.7.0"], []},
           {plugin_b, "3.7.2", ["~3.7.0"], [{plugin_a, ["^3.8.0"]}]},
           {plugin_c, "3.7.2", ["~3.7.0"], [{plugin_a, ["^3.7.0"]}]}],
         errors =
          [{plugin_b, [{{dependency_version_mismatch, "3.7.1", ["^3.8.0"]}, plugin_a}]}],
         valid = [plugin_a, plugin_c]
        },
        #validation_example{
         rabbit_version = "0.0.0",
         plugins =
          [{plugin_a, "", ["3.7.1"], []},
           {plugin_b, "3.7.2", ["~3.7.0"], [{plugin_a, ["3.7.3"]}]}],
         errors = [],
         valid  = [plugin_a, plugin_b]
        }],
    lists:foreach(
        fun(#validation_example{rabbit_version = RabbitVersion,
                                plugins = PluginsExamples,
                                errors  = Errors,
                                valid   = ExpectedValid}) ->
            Plugins = make_plugins(PluginsExamples),
            {Valid, Invalid} = rabbit_plugins:validate_plugins(Plugins,
                                                               RabbitVersion),
            Errors = lists:reverse(Invalid),
            ExpectedValid = lists:map(fun(#plugin{name = Name}) ->
                                              Name
                                      end,
                                      Valid)
        end,
        Examples),
    ok.

supported_release_series(_Config) ->
    Versions = ["3.13.0", "3.13.7", "3.13.15", "3.13.21",
                "4.0.0", "4.0.9", "4.0.26",
                "4.1.0", "4.1.8", "4.1.17",
                "4.2.0", "4.2.10", "4.2.12",
                "4.3.0", "4.3.6", "4.3.7",
                "4.4.0-alpha.1", "4.4.0",
                "5.0.0-beta.1", "5.0.0", "5.0.3",
                "5.1.0", "5.1.2"],
    Examples =
        [{["~3.13.0", "~4.1.0", "~4.2.0", "~4.3.0"],
          ["3.13.0", "3.13.7", "3.13.15", "3.13.21",
           "4.1.0", "4.1.8", "4.1.17",
           "4.2.0", "4.2.10", "4.2.12",
           "4.3.0", "4.3.6", "4.3.7"]},
         {["~3.13.15", "~4.1.10", "~4.2.11", "~4.3.5"],
          ["3.13.15", "3.13.21", "4.1.17", "4.2.11", "4.2.12", "4.3.6", "4.3.7"]},
         {["~> 3.13", "^4.1.0"],
          ["3.13.0", "3.13.7", "3.13.15", "3.13.21",
           "4.1.0", "4.1.8", "4.1.17",
           "4.2.0", "4.2.10", "4.2.12",
           "4.3.0", "4.3.6", "4.3.7",
           "4.4.0-alpha.1", "4.4.0"]},
         {["^4.0.0 || ^5.0.0"],
          ["4.0.0", "4.0.9", "4.0.26",
           "4.1.0", "4.1.8", "4.1.17",
           "4.2.0", "4.2.10", "4.2.12",
           "4.3.0", "4.3.6", "4.3.7",
           "4.4.0-alpha.1", "4.4.0",
           "5.0.0-beta.1", "5.0.0", "5.0.3", "5.1.0", "5.1.2"]},
         {[">= 4.2.0"],
          ["4.2.0", "4.2.10", "4.2.12",
           "4.3.0", "4.3.6", "4.3.7",
           "4.4.0-alpha.1", "4.4.0",
           "5.0.0-beta.1", "5.0.0", "5.0.3", "5.1.0", "5.1.2"]},
         {[">= 4.1.0 < 5.1.0"],
          ["4.1.0", "4.1.8", "4.1.17",
           "4.2.0", "4.2.10", "4.2.12",
           "4.3.0", "4.3.6", "4.3.7",
           "4.4.0-alpha.1", "4.4.0",
           "5.0.0-beta.1", "5.0.0", "5.0.3"]},
         {["4.2.0 - 4.3"],
          ["4.2.0", "4.2.10", "4.2.12", "4.3.0", "4.3.6", "4.3.7"]},
         {["~4.3.6"], ["4.3.6", "4.3.7"]},
         {["~> 4.3"], ["4.3.0", "4.3.6", "4.3.7", "4.4.0-alpha.1", "4.4.0"]},
         {["4.x"],
          ["4.0.0", "4.0.9", "4.0.26",
           "4.1.0", "4.1.8", "4.1.17",
           "4.2.0", "4.2.10", "4.2.12",
           "4.3.0", "4.3.6", "4.3.7",
           "4.4.0-alpha.1", "4.4.0"]},
         {["5.0.x", "5.1.x"],
          ["5.0.0-beta.1", "5.0.0", "5.0.3", "5.1.0", "5.1.2"]},
         {["^5.1.0"], ["5.1.0", "5.1.2"]},
         {["< 4.0.0"], ["3.13.0", "3.13.7", "3.13.15", "3.13.21"]},
         {["4.0.0"], ["4.0.0"]},
         {["~4.0.0"], ["4.0.0", "4.0.9", "4.0.26"]}],
    lists:foreach(
      fun({Requirements, Supported}) ->
              lists:foreach(
                fun(Version) ->
                        Expected = lists:member(Version, Supported),
                        ?assertEqual({Requirements, Version, Expected},
                                     {Requirements, Version,
                                      rabbit_plugins:is_version_supported(
                                        Version, Requirements)})
                end, Versions)
      end, Examples).

make_plugins(Plugins) ->
    lists:map(
        fun({Name, Version, RabbitVersions, PluginsVersions}) ->
            Deps = [K || {K,_V} <- PluginsVersions],
            #plugin{name = Name,
                    version = Version,
                    dependencies = Deps,
                    broker_version_requirements = RabbitVersions,
                    dependency_version_requirements = PluginsVersions}
        end,
        Plugins).
