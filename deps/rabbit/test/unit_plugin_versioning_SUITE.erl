%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2016-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_plugin_versioning_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
      {group, parallel_tests}
    ].

groups() ->
    [
      {parallel_tests, [parallel], [
          version_support,
          plugin_validation
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
     {[], "any version", true} %% anything goes
    ,{[], "0.0.0", true}       %% ditto
    ,{[], "3.5.6", true}       %% ditto
    ,{["something"], "something", true}            %% equal values match
    ,{["3.5.4"], "something", false}
    ,{["3.4.5", "3.6.0"], "0.0.0", true}           %% zero version always match
    ,{["3.4.5", "3.6.0"], "", true}                %% empty version always match
    ,{["something", "3.5.6"], "3.5.7", true}       %% 3.5.7 matches ~> 3.5.6
    ,{["3.4.0", "3.5.6"], "3.6.1", false}          %% 3.6.x isn't supported
    ,{["3.5.2", "3.6.1", "3.7.1"], "3.5.2", true}  %% 3.5.2 matches ~> 3.5.2
    ,{["3.5.2", "3.6.1", "3.7.1"], "3.5.1", false} %% lesser than the lower boundary
    ,{["3.5.2", "3.6.1", "3.7.1"], "3.6.2", true}  %% 3.6.2 matches ~> 3.6.1
    ,{["3.5.2", "3.6.1", "3.6.8"], "3.6.2", true}  %% 3.6.2 still matches ~> 3.6.1
    ,{["3.5", "3.6", "3.7"], "3.5.1", true}        %% x.y values equal to x.y.0
    ,{["3"], "3.5.1", false}                       %% x values are not supported
    ,{["3.5.2", "3.6.1"], "3.6.2.999", true}       %% x.y.z.p values are supported
    ,{["3.5.2", "3.6.2.333"], "3.6.2.999", true}   %% x.y.z.p values are supported
    ,{["3.5.2", "3.6.2.333"], "3.6.2.222", false}  %% x.y.z.p values are supported
    ,{["3.6.0", "3.7.0"], "3.6.3-alpha.1", true}   %% Pre-release versions handled like semver part
    ,{["3.6.0", "3.7.0"], "3.7.0-alpha.89", true}
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
           {plugin_b, "3.7.2", ["3.7.0"], [{plugin_a, ["3.6.3", "3.7.1"]}]}],
         errors = [],
         valid = [plugin_a, plugin_b]},

        #validation_example{
         rabbit_version = "3.7.1",
         plugins =
          [{plugin_a, "3.7.1", ["3.7.6"], []},
           {plugin_b, "3.7.2", ["3.7.0"], [{plugin_a, ["3.6.3", "3.7.0"]}]}],
         errors =
          [{plugin_a, [{broker_version_mismatch, "3.7.1", ["3.7.6"]}]},
           {plugin_b, [{missing_dependency, plugin_a}]}],
         valid = []
        },

        #validation_example{
         rabbit_version = "3.7.1",
         plugins =
          [{plugin_a, "3.7.1", ["3.7.6"], []},
           {plugin_b, "3.7.2", ["3.7.0"], [{plugin_a, ["3.7.0"]}]},
           {plugin_c, "3.7.2", ["3.7.0"], [{plugin_b, ["3.7.3"]}]}],
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
           {plugin_b, "3.7.2", ["3.7.0"], [{plugin_a, ["3.7.3"]}]},
           {plugin_d, "3.7.2", ["3.7.0"], [{plugin_c, ["3.7.3"]}]}],
         errors =
          [{plugin_b, [{{dependency_version_mismatch, "3.7.1", ["3.7.3"]}, plugin_a}]},
           {plugin_d, [{missing_dependency, plugin_c}]}],
         valid = [plugin_a]
        },
        #validation_example{
         rabbit_version = "0.0.0",
         plugins =
          [{plugin_a, "", ["3.7.1"], []},
           {plugin_b, "3.7.2", ["3.7.0"], [{plugin_a, ["3.7.3"]}]}],
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
            ExpectedValid = lists:reverse(lists:map(fun(#plugin{name = Name}) ->
                                                        Name
                                                    end,
                                                    Valid))
        end,
        Examples),
    ok.

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
