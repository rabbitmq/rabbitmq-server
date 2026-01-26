%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2024-2025 Broadcom. All Rights Reserved.
%% The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%


-module(rabbit_access_control_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-include_lib("rabbit_common/include/rabbit.hrl").

-export([all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,

         expiry_timestamp/1,
         with_enabled_plugin/1,
         with_enabled_plugin_plus_internal/1,
         with_missing_plugin/1,
         with_missing_plugin_plus_internal/1,
         with_disabled_plugin/1,
         with_disabled_plugin_plus_internal/1
        ]).

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================


all() ->
    [{group, unit_tests},
     {group, integration_tests}].

groups() ->
    [{unit_tests, [], [expiry_timestamp]},
     {integration_tests, [], [with_enabled_plugin,
                              with_enabled_plugin_plus_internal,
                              with_missing_plugin,
                              with_missing_plugin_plus_internal,
                              with_disabled_plugin,
                              with_disabled_plugin_plus_internal]}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(integration_tests, Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config);
init_per_group(_Group, Config) ->
    Config.

end_per_group(integration_tests, Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config);
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(Testcase, Config)
  when Testcase =:= with_missing_plugin orelse
       Testcase =:= with_missing_plugin_plus_internal ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    do_init_per_testcase(Testcase, Config, []);
init_per_testcase(Testcase, Config)
  when Testcase =:= with_enabled_plugin orelse
       Testcase =:= with_enabled_plugin_plus_internal orelse
       Testcase =:= with_disabled_plugin orelse
       Testcase =:= with_disabled_plugin_plus_internal ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    do_init_per_testcase(Testcase, Config, [fun build_my_plugin/1]);
init_per_testcase(_TestCase, Config) ->
    Config.

do_init_per_testcase(Testcase, Config, PrepSteps) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    TestNumber = rabbit_ct_helpers:testcase_number(Config, ?MODULE, Testcase),
    ClusterSize = 1,
    Config1 = rabbit_ct_helpers:set_config(
                Config,
                [{rmq_nodename_suffix, Testcase},
                 {rmq_nodes_count, ClusterSize},
                 {tcp_ports_base, {skip_n_nodes, TestNumber * ClusterSize}},
                 {start_rmq_with_plugins_disabled, true}]),
    Config2 = rabbit_ct_helpers:merge_app_env(
                Config1,
                {rabbit,
                 [{log, [{file, [{level, debug}]}]}]}),
    Config3 = rabbit_ct_helpers:run_steps(
                Config2,
                PrepSteps ++
                rabbit_ct_broker_helpers:setup_steps() ++
                rabbit_ct_client_helpers:setup_steps()),
    Config3.

end_per_testcase(Testcase, Config)
  when Testcase =:= with_enabled_plugin orelse
       Testcase =:= with_enabled_plugin_plus_internal orelse
       Testcase =:= with_missing_plugin orelse
       Testcase =:= with_missing_plugin_plus_internal orelse
       Testcase =:= with_disabled_plugin orelse
       Testcase =:= with_disabled_plugin_plus_internal ->
    Config1 = rabbit_ct_helpers:run_steps(
                Config,
                rabbit_ct_client_helpers:teardown_steps() ++
                rabbit_ct_broker_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase);
end_per_testcase(_TestCase, _Config) ->
    meck:unload(),
    ok.

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

expiry_timestamp(_) ->
    %% test rabbit_access_control:expiry_timestamp/1 returns the earliest expiry time
    Now = os:system_time(seconds),
    BeforeNow = Now - 60,
    %% returns now
    ok = meck:new(rabbit_expiry_backend, [non_strict]),
    meck:expect(rabbit_expiry_backend, expiry_timestamp, fun (_) -> Now end),
    %% return a bit before now (so the earliest expiry time)
    ok = meck:new(rabbit_earlier_expiry_backend, [non_strict]),
    meck:expect(rabbit_earlier_expiry_backend, expiry_timestamp, fun (_) -> BeforeNow end),
    %% return 'never' (no expiry)
    ok = meck:new(rabbit_no_expiry_backend, [non_strict]),
    meck:expect(rabbit_no_expiry_backend, expiry_timestamp, fun (_) -> never end),

    %% never expires
    User1 = #user{authz_backends = [{rabbit_no_expiry_backend, unused}]},
    ?assertEqual(never, rabbit_access_control:expiry_timestamp(User1)),

    %% returns the result from the backend that expires
    User2 = #user{authz_backends = [{rabbit_expiry_backend, unused},
                                    {rabbit_no_expiry_backend, unused}]},
    ?assertEqual(Now, rabbit_access_control:expiry_timestamp(User2)),

    %% returns earliest expiry time
    User3 = #user{authz_backends = [{rabbit_expiry_backend, unused},
                                    {rabbit_earlier_expiry_backend, unused},
                                    {rabbit_no_expiry_backend, unused}]},
    ?assertEqual(BeforeNow, rabbit_access_control:expiry_timestamp(User3)),

    %% returns earliest expiry time
    User4 = #user{authz_backends = [{rabbit_earlier_expiry_backend, unused},
                                    {rabbit_expiry_backend, unused},
                                    {rabbit_no_expiry_backend, unused}]},
    ?assertEqual(BeforeNow, rabbit_access_control:expiry_timestamp(User4)),

    %% returns earliest expiry time
    User5 = #user{authz_backends = [{rabbit_no_expiry_backend, unused},
                                    {rabbit_earlier_expiry_backend, unused},
                                    {rabbit_expiry_backend, unused}]},
    ?assertEqual(BeforeNow, rabbit_access_control:expiry_timestamp(User5)),

    %% returns earliest expiry time
    User6 = #user{authz_backends = [{rabbit_no_expiry_backend, unused},
                                    {rabbit_expiry_backend, unused},
                                    {rabbit_earlier_expiry_backend, unused}]},
    ?assertEqual(BeforeNow, rabbit_access_control:expiry_timestamp(User6)),

    %% returns the result from the backend that expires
    User7 = #user{authz_backends = [{rabbit_no_expiry_backend, unused},
                                    {rabbit_expiry_backend, unused}]},
    ?assertEqual(Now, rabbit_access_control:expiry_timestamp(User7)),
    ok.

with_enabled_plugin(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    ok = rabbit_ct_broker_helpers:enable_plugin(Config, Node, my_auth_plugin),
    rabbit_ct_broker_helpers:stop_broker(Config, Node),

    rabbit_ct_broker_helpers:rpc(
      Config, Node, os, unsetenv, ["RABBITMQ_ENABLED_PLUGINS"]),
    rabbit_ct_broker_helpers:rpc(
      Config, Node, os, unsetenv, ["LEAVE_PLUGINS_DISABLED"]),

    AuthBackends = [my_auth_plugin],
    rabbit_ct_broker_helpers:rpc(
      Config, Node, application, set_env,
      [rabbit, auth_backends, AuthBackends, [{persistent, true}]]),

    ?assertEqual(
       ok,
       rabbit_ct_broker_helpers:start_broker(Config, Node)),

    ?assertMatch({error, {auth_failure, _}}, test_connection(Config, Node)),

    ok.

with_enabled_plugin_plus_internal(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    ok = rabbit_ct_broker_helpers:enable_plugin(Config, Node, my_auth_plugin),
    rabbit_ct_broker_helpers:stop_broker(Config, Node),

    rabbit_ct_broker_helpers:rpc(
      Config, Node, os, unsetenv, ["RABBITMQ_ENABLED_PLUGINS"]),
    rabbit_ct_broker_helpers:rpc(
      Config, Node, os, unsetenv, ["LEAVE_PLUGINS_DISABLED"]),

    AuthBackends = [my_auth_plugin, rabbit_auth_backend_internal],
    rabbit_ct_broker_helpers:rpc(
      Config, Node, application, set_env,
      [rabbit, auth_backends, AuthBackends, [{persistent, true}]]),

    ?assertEqual(
       ok,
       rabbit_ct_broker_helpers:start_broker(Config, Node)),

    ?assertEqual(ok, test_connection(Config, Node)),

    ok.

with_missing_plugin(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    rabbit_ct_broker_helpers:stop_broker(Config, Node),

    AuthBackends = [my_auth_plugin],
    rabbit_ct_broker_helpers:rpc(
      Config, Node, application, set_env,
      [rabbit, auth_backends, AuthBackends, [{persistent, true}]]),

    ?assertThrow(
       {error,
        {rabbit,
         {{error,
           "Authentication/authorization backends require plugins to be "
           "enabled; see logs for details"},
          _}}},
       rabbit_ct_broker_helpers:start_broker(Config, Node)),

    ?awaitMatch(
       true,
       check_log(Config, Node, "no plugins available provide this module"),
       30000),

    ok.

with_missing_plugin_plus_internal(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    rabbit_ct_broker_helpers:stop_broker(Config, Node),

    AuthBackends = [my_auth_plugin, rabbit_auth_backend_internal],
    rabbit_ct_broker_helpers:rpc(
      Config, Node, application, set_env,
      [rabbit, auth_backends, AuthBackends, [{persistent, true}]]),

    ?assertEqual(
       ok,
       rabbit_ct_broker_helpers:start_broker(Config, Node)),

    ?awaitMatch(
       true,
       check_log(Config, Node, "no plugins available provide this module"),
       30000),

    ?assertEqual(ok, test_connection(Config, Node)),

    ok.

with_disabled_plugin(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    rabbit_ct_broker_helpers:stop_broker(Config, Node),

    AuthBackends = [my_auth_plugin],
    rabbit_ct_broker_helpers:rpc(
      Config, Node, application, set_env,
      [rabbit, auth_backends, AuthBackends, [{persistent, true}]]),

    ?assertThrow(
       {error,
        {rabbit,
         {{error,
           "Authentication/authorization backends require plugins to be "
           "enabled; see logs for details"},
          _}}},
       rabbit_ct_broker_helpers:start_broker(Config, Node)),

    ?awaitMatch(
       true,
       check_log(
         Config, Node,
         "the `my_auth_plugin` plugin must be enabled in order to use "
         "this auth backend"),
       30000),

    ok.

with_disabled_plugin_plus_internal(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    rabbit_ct_broker_helpers:stop_broker(Config, Node),

    AuthBackends = [my_auth_plugin, rabbit_auth_backend_internal],
    rabbit_ct_broker_helpers:rpc(
      Config, Node, application, set_env,
      [rabbit, auth_backends, AuthBackends, [{persistent, true}]]),

    ?assertEqual(
       ok,
       rabbit_ct_broker_helpers:start_broker(Config, Node)),

    ?awaitMatch(
       true,
       check_log(
         Config, Node,
         "the `my_auth_plugin` plugin must be enabled in order to use "
         "this auth backend"),
       30000),

    ?assertEqual(ok, test_connection(Config, Node)),

    ok.

%% -------------------------------------------------------------------
%% Internal helpers.
%% -------------------------------------------------------------------

build_my_plugin(Config) ->
    DataDir = filename:join(
                filename:dirname(filename:dirname(?config(data_dir, Config))),
                ?MODULE_STRING ++ "_data"),
    PluginSrcDir = filename:join(DataDir, "my_auth_plugin"),
    PluginsDir = filename:join(PluginSrcDir, "plugins"),
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_plugins_dir, PluginsDir}]),
    {MyPlugin, OtherPlugins} = list_my_plugin_plugins(PluginSrcDir),
    case MyPlugin of
        [] ->
            DepsDir = ?config(erlang_mk_depsdir, Config),
            Args = ["test-dist",
                    {"DEPS_DIR=~ts", [DepsDir]},
                    %% We clear ALL_DEPS_DIRS to make sure they are
                    %% not recompiled when the plugin is built. `rabbit`
                    %% was previously compiled with -DTEST and if it is
                    %% recompiled because of this plugin, it will be
                    %% recompiled without -DTEST: the testsuite depends
                    %% on test code so we can't allow that.
                    %%
                    %% Note that we do not clear the DEPS variable: we need
                    %% it to be correct because it is used to generate
                    %% `my_auth_plugin.app` (and a RabbitMQ plugin must
                    %% depend on `rabbit`).
                    "ALL_DEPS_DIRS="],
            case rabbit_ct_helpers:make(Config1, PluginSrcDir, Args) of
                {ok, _} ->
                    {_, OtherPlugins1} = list_my_plugin_plugins(PluginSrcDir),
                    remove_other_plugins(PluginSrcDir, OtherPlugins1),
                    update_cli_path(Config1, PluginSrcDir);
                {error, _} ->
                    {skip,
                     "Failed to compile the `my_auth_plugin` test plugin"}
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
                   "Switching to CLI in e.g. ~ts", [Rabbitmqctl]),
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
              re:run(Filename, "^my_auth_plugin-", [{capture, none}]) =:= match
      end, Files).

remove_other_plugins(PluginSrcDir, OtherPlugins) ->
    ok = rabbit_file:recursive_delete(
           [filename:join(PluginSrcDir, OtherPlugin)
            || OtherPlugin <- OtherPlugins]).

check_log(Config, Node, Msg) ->
    LogLocations = rabbit_ct_broker_helpers:rpc(
                     Config, Node,
                     rabbit, log_locations, []),
    lists:any(
      fun(LogLocation) ->
              check_log1(LogLocation, Msg)
      end, LogLocations).

check_log1(LogLocation, Msg) ->
    case filelib:is_regular(LogLocation) of
        true ->
            {ok, Content} = file:read_file(LogLocation),
            ReOpts = [{capture, none}, multiline, unicode],
            match =:= re:run(Content, Msg, ReOpts);
        false ->
            false
    end.

test_connection(Config, Node) ->
    case rabbit_ct_client_helpers:open_unmanaged_connection(Config, Node) of
        Conn when is_pid(Conn) ->
            ok = rabbit_ct_client_helpers:close_connection(Conn),
            ok;
        {error, _} = Error ->
            Error
    end.
