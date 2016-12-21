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
%% Copyright (c) 2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(plugins_SUITE).

-include_lib("common_test/include/ct.hrl").

-compile(export_all).

all() ->
    [
        active_with_single_plugin_dir,
        active_with_multiple_plugin_dirs
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    application:load(rabbit),
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

active_with_single_plugin_dir(Config) ->
    DataDir = rabbit_ct_helpers:get_config(Config, data_dir),
    PluginsDir1 = filename:join(DataDir, "plugins1"),

    true = code:add_path(filename:join([PluginsDir1,
                                 "mock_rabbitmq_plugins_01-0.1.0.ez",
                                 "mock_rabbitmq_plugins_01-0.1.0", "ebin"])),
    {ok, _} = application:ensure_all_started(mock_rabbitmq_plugins_01),
    application:set_env(rabbit, plugins_dir, PluginsDir1),

    [mock_rabbitmq_plugins_01] = rabbit_plugins:active().

active_with_multiple_plugin_dirs(Config) ->
    DataDir = rabbit_ct_helpers:get_config(Config, data_dir),
    PluginsDir1 = filename:join(DataDir, "plugins1"),
    PluginsDir2 = filename:join(DataDir, "plugins2"),

    true = code:add_path(filename:join([PluginsDir1,
                                 "mock_rabbitmq_plugins_01-0.1.0.ez",
                                 "mock_rabbitmq_plugins_01-0.1.0", "ebin"])),
    {ok, _} = application:ensure_all_started(mock_rabbitmq_plugins_01),
    application:set_env(rabbit, plugins_dir, PluginsDir1 ++ ":" ++ PluginsDir2),

    [mock_rabbitmq_plugins_01] = rabbit_plugins:active().
