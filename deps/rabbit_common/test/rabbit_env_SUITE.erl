%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2019-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_env_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0,
         suite/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,
         check_data_dir/1,
         check_default_values/1,
         check_values_from_reachable_remote_node/1,
         check_values_from_offline_remote_node/1,
         check_context_to_app_env_vars/1,
         check_context_to_code_path/1,
         check_RABBITMQ_ADVANCED_CONFIG_FILE/1,
         check_RABBITMQ_CONFIG_FILE/1,
         check_RABBITMQ_CONFIG_FILES/1,
         check_RABBITMQ_DEFAULT_PASS/1,
         check_RABBITMQ_DEFAULT_USER/1,
         check_RABBITMQ_DEFAULT_VHOST/1,
         check_RABBITMQ_DIST_PORT/1,
         check_RABBITMQ_ENABLED_PLUGINS/1,
         check_RABBITMQ_ENABLED_PLUGINS_FILE/1,
         check_RABBITMQ_ERLANG_COOKIE/1,
         check_RABBITMQ_FEATURE_FLAGS_FILE/1,
         check_RABBITMQ_KEEP_PID_FILE_ON_EXIT/1,
         check_RABBITMQ_LOG/1,
         check_RABBITMQ_LOG_BASE/1,
         check_RABBITMQ_LOGS/1,
         check_RABBITMQ_MNESIA_BASE/1,
         check_RABBITMQ_MNESIA_DIR/1,
         check_RABBITMQ_MOTD_FILE/1,
         check_RABBITMQ_NODE_IP_ADDRESS/1,
         check_RABBITMQ_NODE_PORT/1,
         check_RABBITMQ_NODENAME/1,
         check_RABBITMQ_PID_FILE/1,
         check_RABBITMQ_PLUGINS_DIR/1,
         check_RABBITMQ_PLUGINS_EXPAND_DIR/1,
         check_RABBITMQ_PRODUCT_NAME/1,
         check_RABBITMQ_PRODUCT_VERSION/1,
         check_RABBITMQ_QUORUM_DIR/1,
         check_RABBITMQ_STREAM_DIR/1,
         check_RABBITMQ_UPGRADE_LOG/1,
         check_RABBITMQ_USE_LOGNAME/1,
         check_value_is_yes/1,
         check_log_process_env/1,
         check_log_context/1,
         check_get_used_env_vars/1,
         check_parse_conf_env_file_output/1
        ]).

all() ->
    [
     check_data_dir,
     check_default_values,
     check_values_from_reachable_remote_node,
     check_values_from_offline_remote_node,
     check_context_to_app_env_vars,
     check_context_to_code_path,
     check_RABBITMQ_ADVANCED_CONFIG_FILE,
     check_RABBITMQ_CONFIG_FILE,
     check_RABBITMQ_CONFIG_FILES,
     check_RABBITMQ_DEFAULT_PASS,
     check_RABBITMQ_DEFAULT_USER,
     check_RABBITMQ_DEFAULT_VHOST,
     check_RABBITMQ_DIST_PORT,
     check_RABBITMQ_ENABLED_PLUGINS,
     check_RABBITMQ_ENABLED_PLUGINS_FILE,
     check_RABBITMQ_ERLANG_COOKIE,
     check_RABBITMQ_FEATURE_FLAGS_FILE,
     check_RABBITMQ_KEEP_PID_FILE_ON_EXIT,
     check_RABBITMQ_LOG,
     check_RABBITMQ_LOG_BASE,
     check_RABBITMQ_LOGS,
     check_RABBITMQ_MNESIA_BASE,
     check_RABBITMQ_MNESIA_DIR,
     check_RABBITMQ_MOTD_FILE,
     check_RABBITMQ_NODE_IP_ADDRESS,
     check_RABBITMQ_NODE_PORT,
     check_RABBITMQ_NODENAME,
     check_RABBITMQ_PID_FILE,
     check_RABBITMQ_PLUGINS_DIR,
     check_RABBITMQ_PLUGINS_EXPAND_DIR,
     check_RABBITMQ_PRODUCT_NAME,
     check_RABBITMQ_PRODUCT_VERSION,
     check_RABBITMQ_QUORUM_DIR,
     check_RABBITMQ_UPGRADE_LOG,
     check_RABBITMQ_USE_LOGNAME,
     check_value_is_yes,
     check_log_process_env,
     check_log_context,
     check_get_used_env_vars,
     check_parse_conf_env_file_output
    ].

suite() ->
    [{timetrap, {seconds, 10}}].

groups() ->
    [
     {parallel_tests, [parallel], all()}
    ].

init_per_suite(Config) ->
    persistent_term:put({rabbit_env, load_conf_env_file}, false),
    Config.

end_per_suite(Config) ->
    persistent_term:erase({rabbit_env, load_conf_env_file}),
    Config.

init_per_group(_, Config) -> Config.
end_per_group(_, Config) -> Config.

init_per_testcase(_, Config) -> Config.
end_per_testcase(_, Config) -> Config.

check_data_dir(_) ->
    {Variable, ExpValue} = case os:type() of
                               {win32, _} ->
                                   {"RABBITMQ_BASE",
                                    "value of RABBITMQ_BASE"};
                               {unix, _} ->
                                   {"SYS_PREFIX",
                                    "value of SYS_PREFIX/var/lib/rabbitmq"}
                           end,
    Value = "value of " ++ Variable,
    os:putenv(Variable, Value),
    ?assertMatch(#{data_dir := ExpValue}, rabbit_env:get_context()),

    os:unsetenv(Variable),
    ?assertNotMatch(#{data_dir := ExpValue}, rabbit_env:get_context()),
    ?assertMatch(#{data_dir := _}, rabbit_env:get_context()),

    os:unsetenv(Variable).

check_default_values(_) ->
    %% When `rabbit_env` is built with `TEST` defined, we can override
    %% the OS type.
    persistent_term:put({rabbit_env, os_type}, {unix, undefined}),
    UnixContext = rabbit_env:get_context(),

    persistent_term:put({rabbit_env, os_type}, {win32, undefined}),
    SavedAppData = os:getenv("APPDATA"),
    os:putenv("APPDATA", "%APPDATA%"),
    Win32Context = rabbit_env:get_context(),
    case SavedAppData of
        false -> os:unsetenv("APPDATA");
        _     -> os:putenv("APPDATA", SavedAppData)
    end,

    persistent_term:erase({rabbit_env, os_type}),

    {RFFValue, RFFOrigin} = forced_feature_flags_on_init_expect(),

    Node = get_default_nodename(),
    NodeS = atom_to_list(Node),

    Origins = #{
      additional_config_files => default,
      advanced_config_file => default,
      amqp_ipaddr => default,
      amqp_tcp_port => default,
      conf_env_file => default,
      default_user => default,
      default_pass => default,
      default_vhost => default,
      enabled_plugins => default,
      enabled_plugins_file => default,
      erlang_cookie => default,
      erlang_dist_tcp_port => default,
      feature_flags_file => default,
      forced_feature_flags_on_init => RFFOrigin,
      interactive_shell => default,
      keep_pid_file_on_exit => default,
      log_base_dir => default,
      log_feature_flags_registry => default,
      log_levels => default,
      main_config_file => default,
      main_log_file => default,
      mnesia_base_dir => default,
      mnesia_dir => default,
      motd_file => default,
      nodename => default,
      nodename_type => default,
      os_type => environment,
      output_supports_colors => default,
      pid_file => default,
      plugins_expand_dir => default,
      plugins_path => default,
      product_name => default,
      product_version => default,
      quorum_queue_dir => default,
      rabbitmq_home => default,
      stream_queue_dir => default,
      upgrade_log_file => default
     },

    ?assertEqual(
       #{additional_config_files => "/etc/rabbitmq/conf.d/*.conf",
         advanced_config_file => "/etc/rabbitmq/advanced.config",
         amqp_ipaddr => "auto",
         amqp_tcp_port => 5672,
         conf_env_file => "/etc/rabbitmq/rabbitmq-env.conf",
         config_base_dir => "/etc/rabbitmq",
         data_dir => "/var/lib/rabbitmq",
         dbg_mods => [],
         dbg_output => stdout,
         default_user => undefined,
         default_pass => undefined,
         default_vhost => undefined,
         enabled_plugins => undefined,
         enabled_plugins_file => "/etc/rabbitmq/enabled_plugins",
         erlang_cookie => undefined,
         erlang_dist_tcp_port => 25672,
         feature_flags_file =>
           "/var/lib/rabbitmq/mnesia/" ++ NodeS ++ "-feature_flags",
         forced_feature_flags_on_init => RFFValue,
         interactive_shell => false,
         keep_pid_file_on_exit => false,
         log_base_dir => "/var/log/rabbitmq",
         log_feature_flags_registry => false,
         log_levels => undefined,
         main_config_file => "/etc/rabbitmq/rabbitmq",
         main_log_file => "/var/log/rabbitmq/" ++ NodeS ++ ".log",
         mnesia_base_dir => "/var/lib/rabbitmq/mnesia",
         mnesia_dir => "/var/lib/rabbitmq/mnesia/" ++ NodeS,
         motd_file => "/etc/rabbitmq/motd",
         nodename => Node,
         nodename_type => shortnames,
         os_type => {unix, undefined},
         output_supports_colors => true,
         pid_file => "/var/lib/rabbitmq/mnesia/" ++ NodeS ++ ".pid",
         plugins_expand_dir =>
           "/var/lib/rabbitmq/mnesia/" ++ NodeS ++ "-plugins-expand",
         plugins_path => maps:get(plugins_path, UnixContext),
         product_name => undefined,
         product_version => undefined,
         quorum_queue_dir =>
           "/var/lib/rabbitmq/mnesia/" ++ NodeS ++ "/quorum",
         rabbitmq_home => maps:get(rabbitmq_home, UnixContext),
         stream_queue_dir =>
           "/var/lib/rabbitmq/mnesia/" ++ NodeS ++ "/stream",
         split_nodename => rabbit_nodes_common:parts(Node),
         sys_prefix => "",
         upgrade_log_file =>
           "/var/log/rabbitmq/" ++ NodeS ++ "_upgrade.log",

         var_origins => Origins#{sys_prefix => default}},
       UnixContext),

    ?assertEqual(
       #{additional_config_files => "%APPDATA%/RabbitMQ/conf.d/*.conf",
         advanced_config_file => "%APPDATA%/RabbitMQ/advanced.config",
         amqp_ipaddr => "auto",
         amqp_tcp_port => 5672,
         conf_env_file => "%APPDATA%/RabbitMQ/rabbitmq-env-conf.bat",
         config_base_dir => "%APPDATA%/RabbitMQ",
         data_dir => "%APPDATA%/RabbitMQ",
         dbg_mods => [],
         dbg_output => stdout,
         default_user => undefined,
         default_pass => undefined,
         default_vhost => undefined,
         enabled_plugins => undefined,
         enabled_plugins_file => "%APPDATA%/RabbitMQ/enabled_plugins",
         erlang_cookie => undefined,
         erlang_dist_tcp_port => 25672,
         feature_flags_file =>
           "%APPDATA%/RabbitMQ/db/" ++ NodeS ++ "-feature_flags",
         forced_feature_flags_on_init => RFFValue,
         interactive_shell => false,
         keep_pid_file_on_exit => false,
         log_base_dir => "%APPDATA%/RabbitMQ/log",
         log_feature_flags_registry => false,
         log_levels => undefined,
         main_config_file => "%APPDATA%/RabbitMQ/rabbitmq",
         main_log_file => "%APPDATA%/RabbitMQ/log/" ++ NodeS ++ ".log",
         mnesia_base_dir => "%APPDATA%/RabbitMQ/db",
         mnesia_dir => "%APPDATA%/RabbitMQ/db/" ++ NodeS ++ "-mnesia",
         motd_file => "%APPDATA%/RabbitMQ/motd.txt",
         nodename => Node,
         nodename_type => shortnames,
         os_type => {win32, undefined},
         output_supports_colors => false,
         pid_file => "%APPDATA%/RabbitMQ/db/" ++ NodeS ++ ".pid",
         plugins_expand_dir =>
           "%APPDATA%/RabbitMQ/db/" ++ NodeS ++ "-plugins-expand",
         plugins_path => maps:get(plugins_path, Win32Context),
         product_name => undefined,
         product_version => undefined,
         quorum_queue_dir =>
           "%APPDATA%/RabbitMQ/db/" ++ NodeS ++ "-mnesia/quorum",
         rabbitmq_base => "%APPDATA%/RabbitMQ",
         rabbitmq_home => maps:get(rabbitmq_home, Win32Context),
         stream_queue_dir =>
           "%APPDATA%/RabbitMQ/db/" ++ NodeS ++ "-mnesia/stream",
         split_nodename => rabbit_nodes_common:parts(Node),
         upgrade_log_file =>
           "%APPDATA%/RabbitMQ/log/" ++ NodeS ++ "_upgrade.log",

         var_origins => Origins#{rabbitmq_base => default}},
       Win32Context).

forced_feature_flags_on_init_expect() ->
    %% In the case of mixed-versions-cluster testing in CI, the test
    %% sets $RABBITMQ_FEATURE_FLAGS to an empty string. This obviously
    %% changes the context returned by rabbit_env.
    case os:getenv("RABBITMQ_FEATURE_FLAGS") of
        false -> {undefined, default};
        ""    -> {[], environment}
    end.

check_values_from_reachable_remote_node(Config) ->
    PrivDir = ?config(priv_dir, Config),

    MnesiaDir = filename:join(PrivDir, "mnesia"),
    RabbitAppDir = filename:join(PrivDir, "rabbit"),
    RabbitEbinDir = filename:join(RabbitAppDir, "ebin"),

    FeatureFlagsFile = filename:join(PrivDir, "feature_flags"),
    PluginsDir = filename:join(PrivDir, "plugins"),
    EnabledPluginsFile = filename:join(PrivDir, "enabled_plugins"),

    ok = file:make_dir(MnesiaDir),
    ok = file:make_dir(RabbitAppDir),
    ok = file:make_dir(RabbitEbinDir),

    %% Create a fake `rabbit` application.
    App = {application,
           rabbit,
           [{vsn, "fake-rabbit"}]},
    AppFile = filename:join(RabbitEbinDir, "rabbit.app"),
    AppContent = io_lib:format("~p.~n", [App]),
    ok = file:write_file(AppFile, AppContent),

    %% Start a fake RabbitMQ node.
    Node = rabbit_nodes_common:make(
             {atom_to_list(?FUNCTION_NAME), "localhost"}),
    NodeS = atom_to_list(Node),
    true = os:putenv("RABBITMQ_NODENAME", NodeS),
    Args = ["-noinput",
            "-sname", atom_to_list(Node),
            "-pa", filename:dirname(code:which(rabbit_env)),
            "-pa", filename:dirname(code:where_is_file("rabbit_common.app")),
            "-pa", filename:dirname(code:which(rabbit)),
            "-pa", RabbitEbinDir,
            "-mnesia", "dir",
            rabbit_misc:format("~p", [MnesiaDir]),
            "-rabbit", "feature_flags_file",
            rabbit_misc:format("~p", [FeatureFlagsFile]),
            "-rabbit", "plugins_dir",
            rabbit_misc:format("~p", [PluginsDir]),
            "-rabbit", "enabled_plugins_file",
            rabbit_misc:format("~p", [EnabledPluginsFile]),
            "-eval",
            "ok = application:load(mnesia),"
            "ok = application:load(rabbit)."],
    PortName = {spawn_executable, os:find_executable("erl")},
    PortSettings = [{cd, PrivDir},
                    {args, Args},
                    {env, [{"ERL_LIBS", false}]},
                    {line, 512},
                    exit_status,
                    stderr_to_stdout],
    ct:pal(
      "Starting fake RabbitMQ node with the following settings:~n~p",
      [PortSettings]),
    Pid = spawn_link(
            fun() ->
                    Port = erlang:open_port(PortName, PortSettings),
                    consume_stdout(Port, Node)
            end),
    wait_for_remote_node(Node),

    try
        persistent_term:put({rabbit_env, os_type}, {unix, undefined}),
        UnixContext = rabbit_env:get_context(Node),

        persistent_term:erase({rabbit_env, os_type}),

        {RFFValue, RFFOrigin} = forced_feature_flags_on_init_expect(),

        Origins = #{
          additional_config_files => default,
          advanced_config_file => default,
          amqp_ipaddr => default,
          amqp_tcp_port => default,
          conf_env_file => default,
          default_user => default,
          default_pass => default,
          default_vhost => default,
          enabled_plugins => default,
          enabled_plugins_file => remote_node,
          erlang_cookie => default,
          erlang_dist_tcp_port => default,
          feature_flags_file => remote_node,
          forced_feature_flags_on_init => RFFOrigin,
          interactive_shell => default,
          keep_pid_file_on_exit => default,
          log_base_dir => default,
          log_feature_flags_registry => default,
          log_levels => default,
          main_config_file => default,
          main_log_file => default,
          mnesia_base_dir => default,
          mnesia_dir => remote_node,
          motd_file => default,
          nodename => environment,
          nodename_type => default,
          os_type => environment,
          output_supports_colors => default,
          pid_file => default,
          plugins_expand_dir => default,
          plugins_path => remote_node,
          product_name => default,
          product_version => default,
          quorum_queue_dir => default,
          rabbitmq_home => default,
          stream_queue_dir => default,
          upgrade_log_file => default
         },

        ?assertEqual(
           #{additional_config_files => "/etc/rabbitmq/conf.d/*.conf",
             advanced_config_file => "/etc/rabbitmq/advanced.config",
             amqp_ipaddr => "auto",
             amqp_tcp_port => 5672,
             conf_env_file => "/etc/rabbitmq/rabbitmq-env.conf",
             config_base_dir => "/etc/rabbitmq",
             data_dir => "/var/lib/rabbitmq",
             dbg_mods => [],
             dbg_output => stdout,
             default_user => undefined,
             default_pass => undefined,
             default_vhost => undefined,
             enabled_plugins => undefined,
             enabled_plugins_file => EnabledPluginsFile,
             erlang_cookie => undefined,
             erlang_dist_tcp_port => 25672,
             feature_flags_file => FeatureFlagsFile,
             forced_feature_flags_on_init => RFFValue,
             from_remote_node => {Node, 10000},
             interactive_shell => false,
             keep_pid_file_on_exit => false,
             log_base_dir => "/var/log/rabbitmq",
             log_feature_flags_registry => false,
             log_levels => undefined,
             main_config_file => "/etc/rabbitmq/rabbitmq",
             main_log_file => "/var/log/rabbitmq/" ++ NodeS ++ ".log",
             mnesia_base_dir => undefined,
             mnesia_dir => MnesiaDir,
             motd_file => undefined,
             nodename => Node,
             nodename_type => shortnames,
             os_type => {unix, undefined},
             output_supports_colors => true,
             pid_file => undefined,
             plugins_expand_dir => undefined,
             plugins_path => PluginsDir,
             product_name => undefined,
             product_version => undefined,
             quorum_queue_dir => MnesiaDir ++ "/quorum",
             rabbitmq_home => maps:get(rabbitmq_home, UnixContext),
             stream_queue_dir => MnesiaDir ++ "/stream",
             split_nodename => rabbit_nodes_common:parts(Node),
             sys_prefix => "",
             upgrade_log_file =>
               "/var/log/rabbitmq/" ++ NodeS ++ "_upgrade.log",

             var_origins => Origins#{sys_prefix => default}},
           UnixContext)
    after
        os:unsetenv("RABBITMQ_NODENAME"),
        unlink(Pid),
        rpc:call(Node, erlang, halt, [])
    end.

consume_stdout(Port, Nodename) ->
    receive
        {Port, {exit_status, X}} ->
            ?assertEqual(0, X);
        {Port, {data, Out}} ->
            ct:pal("stdout: ~p", [Out]),
            consume_stdout(Port, Nodename)
    end.

wait_for_remote_node(Nodename) ->
    case net_adm:ping(Nodename) of
        pong -> ok;
        pang -> timer:sleep(200),
                wait_for_remote_node(Nodename)
    end.

check_values_from_offline_remote_node(_) ->
    Node = rabbit_nodes_common:make(
             {atom_to_list(?FUNCTION_NAME), "localhost"}),
    NodeS = atom_to_list(Node),
    true = os:putenv("RABBITMQ_NODENAME", NodeS),

    persistent_term:put({rabbit_env, os_type}, {unix, undefined}),
    UnixContext = rabbit_env:get_context(offline),

    persistent_term:erase({rabbit_env, os_type}),
    os:unsetenv("RABBITMQ_NODENAME"),

    {RFFValue, RFFOrigin} = forced_feature_flags_on_init_expect(),

    Origins = #{
      additional_config_files => default,
      advanced_config_file => default,
      amqp_ipaddr => default,
      amqp_tcp_port => default,
      conf_env_file => default,
      default_user => default,
      default_pass => default,
      default_vhost => default,
      enabled_plugins => default,
      enabled_plugins_file => default,
      erlang_cookie => default,
      erlang_dist_tcp_port => default,
      feature_flags_file => default,
      forced_feature_flags_on_init => RFFOrigin,
      interactive_shell => default,
      keep_pid_file_on_exit => default,
      log_base_dir => default,
      log_feature_flags_registry => default,
      log_levels => default,
      main_config_file => default,
      main_log_file => default,
      mnesia_base_dir => default,
      mnesia_dir => default,
      motd_file => default,
      nodename => environment,
      nodename_type => default,
      os_type => environment,
      output_supports_colors => default,
      pid_file => default,
      plugins_expand_dir => default,
      plugins_path => default,
      product_name => default,
      product_version => default,
      quorum_queue_dir => default,
      rabbitmq_home => default,
      stream_queue_dir => default,
      upgrade_log_file => default
     },

    ?assertEqual(
       #{additional_config_files => "/etc/rabbitmq/conf.d/*.conf",
         advanced_config_file => "/etc/rabbitmq/advanced.config",
         amqp_ipaddr => "auto",
         amqp_tcp_port => 5672,
         conf_env_file => "/etc/rabbitmq/rabbitmq-env.conf",
         config_base_dir => "/etc/rabbitmq",
         data_dir => "/var/lib/rabbitmq",
         dbg_mods => [],
         dbg_output => stdout,
         default_user => undefined,
         default_pass => undefined,
         default_vhost => undefined,
         enabled_plugins => undefined,
         enabled_plugins_file => undefined,
         erlang_cookie => undefined,
         erlang_dist_tcp_port => 25672,
         feature_flags_file => undefined,
         forced_feature_flags_on_init => RFFValue,
         from_remote_node => offline,
         interactive_shell => false,
         keep_pid_file_on_exit => false,
         log_base_dir => "/var/log/rabbitmq",
         log_feature_flags_registry => false,
         log_levels => undefined,
         main_config_file => "/etc/rabbitmq/rabbitmq",
         main_log_file => "/var/log/rabbitmq/" ++ NodeS ++ ".log",
         mnesia_base_dir => undefined,
         mnesia_dir => undefined,
         motd_file => undefined,
         nodename => Node,
         nodename_type => shortnames,
         os_type => {unix, undefined},
         output_supports_colors => true,
         pid_file => undefined,
         plugins_expand_dir => undefined,
         plugins_path => undefined,
         product_name => undefined,
         product_version => undefined,
         quorum_queue_dir => undefined,
         rabbitmq_home => maps:get(rabbitmq_home, UnixContext),
         stream_queue_dir => undefined,
         split_nodename => rabbit_nodes_common:parts(Node),
         sys_prefix => "",
         upgrade_log_file =>
           "/var/log/rabbitmq/" ++ NodeS ++ "_upgrade.log",

         var_origins => Origins#{sys_prefix => default}},
       UnixContext).

check_context_to_app_env_vars(_) ->
    %% When `rabbit_env` is built with `TEST` defined, we can override
    %% the OS type.
    persistent_term:put({rabbit_env, os_type}, {unix, undefined}),
    UnixContext = rabbit_env:get_context(),

    persistent_term:erase({rabbit_env, os_type}),

    Vars = [{mnesia, dir, maps:get(mnesia_dir, UnixContext)},
            {ra, data_dir, maps:get(quorum_queue_dir, UnixContext)},
            {osiris, data_dir, maps:get(stream_queue_dir, UnixContext)},
            {rabbit, feature_flags_file,
             maps:get(feature_flags_file, UnixContext)},
            {rabbit, plugins_dir, maps:get(plugins_path, UnixContext)},
            {rabbit, plugins_expand_dir,
             maps:get(plugins_expand_dir, UnixContext)},
            {rabbit, enabled_plugins_file,
             maps:get(enabled_plugins_file, UnixContext)}],

    lists:foreach(
      fun({App, Param, _}) ->
              ?assertEqual(undefined, application:get_env(App, Param))
      end,
      Vars),

    rabbit_env:context_to_app_env_vars(UnixContext),
    lists:foreach(
      fun({App, Param, Value}) ->
              ?assertEqual({ok, Value}, application:get_env(App, Param))
      end,
      Vars),

    lists:foreach(
      fun({App, Param, _}) ->
              application:unset_env(App, Param),
              ?assertEqual(undefined, application:get_env(App, Param))
      end,
      Vars),

    rabbit_env:context_to_app_env_vars_no_logging(UnixContext),
    lists:foreach(
      fun({App, Param, Value}) ->
              ?assertEqual({ok, Value}, application:get_env(App, Param))
      end,
      Vars).

check_context_to_code_path(Config) ->
    PrivDir = ?config(priv_dir, Config),
    PluginsDir1 = filename:join(
                     PrivDir, rabbit_misc:format("~s-1", [?FUNCTION_NAME])),
    MyPlugin1Dir = filename:join(PluginsDir1, "my_plugin1"),
    MyPlugin1EbinDir = filename:join(MyPlugin1Dir, "ebin"),
    PluginsDir2 = filename:join(
                     PrivDir, rabbit_misc:format("~s-2", [?FUNCTION_NAME])),
    MyPlugin2Dir = filename:join(PluginsDir2, "my_plugin2"),
    MyPlugin2EbinDir = filename:join(MyPlugin2Dir, "ebin"),

    ok = file:make_dir(PluginsDir1),
    ok = file:make_dir(MyPlugin1Dir),
    ok = file:make_dir(MyPlugin1EbinDir),
    ok = file:make_dir(PluginsDir2),
    ok = file:make_dir(MyPlugin2Dir),
    ok = file:make_dir(MyPlugin2EbinDir),

    %% On Unix.
    %%
    %% We can't test the Unix codepath on Windows because the drive letter
    %% separator conflicts with the path separator (they are both ':').
    %% However, the Windows codepath can be tested on both Unix and Windows.
    case os:type() of
        {unix, _} ->
            UnixPluginsPath = PluginsDir1 ++ ":" ++ PluginsDir2,
            true = os:putenv("RABBITMQ_PLUGINS_DIR", UnixPluginsPath),
            persistent_term:put({rabbit_env, os_type}, {unix, undefined}),
            UnixContext = rabbit_env:get_context(),

            persistent_term:erase({rabbit_env, os_type}),
            os:unsetenv("RABBITMQ_PLUGINS_DIR"),

            ?assertEqual(UnixPluginsPath, maps:get(plugins_path, UnixContext)),

            OldCodePath1 = code:get_path(),
            ?assertNot(lists:member(MyPlugin1EbinDir, OldCodePath1)),
            ?assertNot(lists:member(MyPlugin2EbinDir, OldCodePath1)),

            rabbit_env:context_to_code_path(UnixContext),

            NewCodePath1 = code:get_path(),
            ?assert(lists:member(MyPlugin1EbinDir, NewCodePath1)),
            ?assert(lists:member(MyPlugin2EbinDir, NewCodePath1)),
            ?assertEqual(
               [MyPlugin1EbinDir, MyPlugin2EbinDir],
               lists:filter(
                 fun(Dir) ->
                         Dir =:= MyPlugin1EbinDir orelse
                         Dir =:= MyPlugin2EbinDir
                 end, NewCodePath1)),

            true = code:del_path(MyPlugin1EbinDir),
            true = code:del_path(MyPlugin2EbinDir);
        _ ->
            ok
    end,

    %% On Windows.
    Win32PluginsPath = PluginsDir1 ++ ";" ++ PluginsDir2,
    true = os:putenv("RABBITMQ_PLUGINS_DIR", Win32PluginsPath),
    persistent_term:put({rabbit_env, os_type}, {win32, undefined}),
    Win32Context = rabbit_env:get_context(),

    persistent_term:erase({rabbit_env, os_type}),
    os:unsetenv("RABBITMQ_PLUGINS_DIR"),

    ?assertEqual(Win32PluginsPath, maps:get(plugins_path, Win32Context)),

    OldCodePath2 = code:get_path(),
    ?assertNot(lists:member(MyPlugin1EbinDir, OldCodePath2)),
    ?assertNot(lists:member(MyPlugin2EbinDir, OldCodePath2)),

    rabbit_env:context_to_code_path(Win32Context),

    NewCodePath2 = code:get_path(),
    ?assert(lists:member(MyPlugin1EbinDir, NewCodePath2)),
    ?assert(lists:member(MyPlugin2EbinDir, NewCodePath2)),
    ?assertEqual(
       [MyPlugin1EbinDir, MyPlugin2EbinDir],
       lists:filter(
         fun(Dir) ->
                 Dir =:= MyPlugin1EbinDir orelse
                 Dir =:= MyPlugin2EbinDir
         end, NewCodePath2)),

    true = code:del_path(MyPlugin1EbinDir),
    true = code:del_path(MyPlugin2EbinDir).

check_RABBITMQ_ADVANCED_CONFIG_FILE(_) ->
    Value1 = random_string(),
    Value2 = random_string(),
    check_prefixed_variable("RABBITMQ_ADVANCED_CONFIG_FILE",
                            advanced_config_file,
                            '_',
                            Value1, Value1,
                            Value2, Value2).

check_RABBITMQ_CONFIG_FILE(_) ->
    Value1 = random_string(),
    Value2 = random_string(),
    check_prefixed_variable("RABBITMQ_CONFIG_FILE",
                            main_config_file,
                            '_',
                            Value1, Value1,
                            Value2, Value2).

check_RABBITMQ_CONFIG_FILES(_) ->
    Value1 = random_string(),
    Value2 = random_string(),
    check_prefixed_variable("RABBITMQ_CONFIG_FILES",
                            additional_config_files,
                            '_',
                            Value1, Value1,
                            Value2, Value2).

check_RABBITMQ_DEFAULT_PASS(_) ->
    Value1 = random_string(),
    check_variable("RABBITMQ_DEFAULT_PASS",
                   default_pass,
                   Value1, list_to_binary(Value1)).

check_RABBITMQ_DEFAULT_USER(_) ->
    Value1 = random_string(),
    check_variable("RABBITMQ_DEFAULT_USER",
                   default_user,
                   Value1, list_to_binary(Value1)).

check_RABBITMQ_DEFAULT_VHOST(_) ->
    Value1 = random_string(),
    check_variable("RABBITMQ_DEFAULT_VHOST",
                   default_vhost,
                   Value1, list_to_binary(Value1)).

check_RABBITMQ_DIST_PORT(_) ->
    Value1 = random_int(),
    Value2 = random_int(),
    check_prefixed_variable("RABBITMQ_DIST_PORT",
                            erlang_dist_tcp_port,
                            25672,
                            integer_to_list(Value1), Value1,
                            integer_to_list(Value2), Value2).

check_RABBITMQ_ENABLED_PLUGINS(_) ->
    Value1 = [random_atom(), random_atom()],
    Value2 = [random_atom(), random_atom()],
    check_prefixed_variable("RABBITMQ_ENABLED_PLUGINS",
                            enabled_plugins,
                            '_',
                            "", [],
                            "", []),
    check_prefixed_variable("RABBITMQ_ENABLED_PLUGINS",
                            enabled_plugins,
                            '_',
                            rabbit_misc:format("~s,~s", Value1), Value1,
                            rabbit_misc:format("~s,~s", Value2), Value2).

check_RABBITMQ_ENABLED_PLUGINS_FILE(_) ->
    Value1 = random_string(),
    Value2 = random_string(),
    check_prefixed_variable("RABBITMQ_ENABLED_PLUGINS_FILE",
                            enabled_plugins_file,
                            '_',
                            Value1, Value1,
                            Value2, Value2).

check_RABBITMQ_ERLANG_COOKIE(_) ->
    Value1 = random_atom(),
    check_variable("RABBITMQ_ERLANG_COOKIE",
                   erlang_cookie,
                   atom_to_list(Value1), Value1).

check_RABBITMQ_FEATURE_FLAGS_FILE(_) ->
    Value1 = random_string(),
    check_variable("RABBITMQ_FEATURE_FLAGS_FILE",
                   feature_flags_file,
                   Value1, Value1).

check_RABBITMQ_KEEP_PID_FILE_ON_EXIT(_) ->
    Value1 = true,
    Value2 = false,
    check_prefixed_variable("RABBITMQ_KEEP_PID_FILE_ON_EXIT",
                            keep_pid_file_on_exit,
                            false,
                            atom_to_list(Value1), Value1,
                            atom_to_list(Value2), Value2).

check_RABBITMQ_LOG(_) ->
    Value1 = random_string(),
    Value2 = random_string(),
    check_prefixed_variable("RABBITMQ_LOG",
                            log_levels,
                            '_',
                            "critical", #{global => critical},
                            "emergency", #{global => emergency}),
    check_prefixed_variable("RABBITMQ_LOG",
                            log_levels,
                            '_',
                            Value1, #{Value1 => info},
                            Value2, #{Value2 => info}),
    check_prefixed_variable("RABBITMQ_LOG",
                            log_levels,
                            '_',
                            Value1 ++ ",none", #{global => none,
                                                 Value1 => info},
                            Value2 ++ ",none", #{global => none,
                                                 Value2 => info}),
    check_prefixed_variable("RABBITMQ_LOG",
                            log_levels,
                            '_',
                            Value1 ++ "=debug", #{Value1 => debug},
                            Value2 ++ "=info", #{Value2 => info}),
    check_prefixed_variable("RABBITMQ_LOG",
                            log_levels,
                            '_',
                            Value1 ++ ",-color", #{Value1 => info,
                                                   color => false},
                            Value2 ++ ",+color", #{Value2 => info,
                                                   color => true}),
    check_prefixed_variable("RABBITMQ_LOG",
                            log_levels,
                            '_',
                            Value1 ++ "=notice,-color", #{Value1 => notice,
                                                          color => false},
                            Value2 ++ "=warning,+color", #{Value2 => warning,
                                                           color => true}),
    check_prefixed_variable("RABBITMQ_LOG",
                            log_levels,
                            '_',
                            Value1 ++ "=error," ++ Value2, #{Value1 => error,
                                                             Value2 => info},
                            Value2 ++ "=alert," ++ Value1, #{Value1 => info,
                                                             Value2 => alert}).

check_RABBITMQ_LOG_BASE(_) ->
    Value1 = random_string(),
    Value2 = random_string(),
    check_prefixed_variable("RABBITMQ_LOG_BASE",
                            log_base_dir,
                            '_',
                            Value1, Value1,
                            Value2, Value2).

check_RABBITMQ_LOGS(_) ->
    Value1 = random_string(),
    Value2 = random_string(),
    check_prefixed_variable("RABBITMQ_LOGS",
                            main_log_file,
                            '_',
                            Value1, Value1,
                            Value2, Value2).

check_RABBITMQ_UPGRADE_LOG(_) ->
    Value1 = random_string(),
    Value2 = random_string(),
    check_prefixed_variable("RABBITMQ_UPGRADE_LOG",
                            upgrade_log_file,
                            '_',
                            Value1, Value1,
                            Value2, Value2).

check_RABBITMQ_MNESIA_BASE(_) ->
    Value1 = random_string(),
    Value2 = random_string(),
    check_prefixed_variable("RABBITMQ_MNESIA_BASE",
                            mnesia_base_dir,
                            '_',
                            Value1, Value1,
                            Value2, Value2).

check_RABBITMQ_MNESIA_DIR(_) ->
    Value1 = random_string(),
    Value2 = random_string(),
    check_prefixed_variable("RABBITMQ_MNESIA_DIR",
                            mnesia_dir,
                            '_',
                            Value1, Value1,
                            Value2, Value2).

check_RABBITMQ_NODE_IP_ADDRESS(_) ->
    Value1 = random_string(),
    Value2 = random_string(),
    check_prefixed_variable("RABBITMQ_NODE_IP_ADDRESS",
                            amqp_ipaddr,
                            "auto",
                            Value1, Value1,
                            Value2, Value2).

check_RABBITMQ_NODE_PORT(_) ->
    Value1 = random_int(),
    Value2 = random_int(),
    check_prefixed_variable("RABBITMQ_NODE_PORT",
                            amqp_tcp_port,
                            5672,
                            integer_to_list(Value1), Value1,
                            integer_to_list(Value2), Value2).

check_RABBITMQ_NODENAME(_) ->
    DefaultNodename = get_default_nodename(),
    {_, DefaultHostname} = rabbit_nodes_common:parts(DefaultNodename),

    Value1 = random_atom(),
    Value2 = random_atom(),
    check_prefixed_variable("RABBITMQ_NODENAME",
                            nodename,
                            DefaultNodename,
                            atom_to_list(Value1),
                            list_to_atom(
                              atom_to_list(Value1) ++ "@" ++ DefaultHostname),
                            atom_to_list(Value2),
                            list_to_atom(
                              atom_to_list(Value2) ++ "@" ++ DefaultHostname)),

    Value3 = list_to_atom(random_string() ++ "@" ++ random_string()),
    Value4 = list_to_atom(random_string() ++ "@" ++ random_string()),
    check_prefixed_variable("RABBITMQ_NODENAME",
                            nodename,
                            DefaultNodename,
                            atom_to_list(Value3), Value3,
                            atom_to_list(Value4), Value4).

check_RABBITMQ_PID_FILE(_) ->
    Value1 = random_string(),
    Value2 = random_string(),
    check_prefixed_variable("RABBITMQ_PID_FILE",
                            pid_file,
                            '_',
                            Value1, Value1,
                            Value2, Value2).

check_RABBITMQ_PLUGINS_DIR(_) ->
    Value1 = random_string(),
    Value2 = random_string(),
    check_prefixed_variable("RABBITMQ_PLUGINS_DIR",
                            plugins_path,
                            '_',
                            Value1, Value1,
                            Value2, Value2).

check_RABBITMQ_PLUGINS_EXPAND_DIR(_) ->
    Value1 = random_string(),
    Value2 = random_string(),
    check_prefixed_variable("RABBITMQ_PLUGINS_EXPAND_DIR",
                            plugins_expand_dir,
                            '_',
                            Value1, Value1,
                            Value2, Value2).

check_RABBITMQ_PRODUCT_NAME(_) ->
    Value1 = random_string(),
    Value2 = random_string(),
    check_prefixed_variable("RABBITMQ_PRODUCT_NAME",
                            product_name,
                            '_',
                            Value1, Value1,
                            Value2, Value2).

check_RABBITMQ_PRODUCT_VERSION(_) ->
    Value1 = random_string(),
    Value2 = random_string(),
    check_prefixed_variable("RABBITMQ_PRODUCT_VERSION",
                            product_version,
                            '_',
                            Value1, Value1,
                            Value2, Value2).

check_RABBITMQ_MOTD_FILE(_) ->
    Value1 = random_string(),
    Value2 = random_string(),
    check_prefixed_variable("RABBITMQ_MOTD_FILE",
                            motd_file,
                            '_',
                            Value1, Value1,
                            Value2, Value2).

check_RABBITMQ_QUORUM_DIR(_) ->
    Value1 = random_string(),
    Value2 = random_string(),
    check_prefixed_variable("RABBITMQ_QUORUM_DIR",
                            quorum_queue_dir,
                            '_',
                            Value1, Value1,
                            Value2, Value2).

check_RABBITMQ_STREAM_DIR(_) ->
    Value1 = random_string(),
    Value2 = random_string(),
    check_prefixed_variable("RABBITMQ_STREAM_DIR",
                            stream_queue_dir,
                            '_',
                            Value1, Value1,
                            Value2, Value2).
check_RABBITMQ_USE_LOGNAME(_) ->
    check_prefixed_variable("RABBITMQ_USE_LONGNAME",
                            nodename_type,
                            shortnames,
                            "true", longnames,
                            "false", shortnames).

check_value_is_yes(_) ->
    ?assert(rabbit_env:value_is_yes("1")),
    ?assert(rabbit_env:value_is_yes("yes")),
    ?assert(rabbit_env:value_is_yes("true")),
    ?assertNot(rabbit_env:value_is_yes("0")),
    ?assertNot(rabbit_env:value_is_yes("no")),
    ?assertNot(rabbit_env:value_is_yes("false")),
    ?assertNot(rabbit_env:value_is_yes(random_string() ++ ".")).

check_log_process_env(_) ->
    ok = rabbit_env:log_process_env().

check_log_context(_) ->
    Context = rabbit_env:get_context(),
    ok = rabbit_env:log_context(Context).

check_get_used_env_vars(_) ->
    os:putenv("RABBITMQ_LOGS", "-"),
    os:putenv("CONFIG_FILE", "filename"),
    Vars = rabbit_env:get_used_env_vars(),
    ?assert(lists:keymember("RABBITMQ_LOGS", 1, Vars)),
    ?assert(lists:keymember("CONFIG_FILE", 1, Vars)),
    ?assertNot(lists:keymember("HOME", 1, Vars)),
    ?assertNot(lists:keymember("PATH", 1, Vars)),
    os:unsetenv("RABBITMQ_LOGS"),
    os:unsetenv("CONFIG_FILE").

check_variable(Variable, Key, ValueToSet, Comparison) ->
    os:putenv(Variable, ValueToSet),
    ?assertMatch(#{Key := Comparison}, rabbit_env:get_context()),

    os:unsetenv(Variable),
    Context = rabbit_env:get_context(),
    ?assertNotMatch(#{Key := Comparison}, Context),
    ?assertMatch(#{Key := _}, Context).

check_prefixed_variable("RABBITMQ_" ++ Variable = PrefixedVariable,
                        Key,
                        DefaultValue,
                        Value1ToSet, Comparison1,
                        Value2ToSet, Comparison2) ->
    os:putenv(Variable, Value1ToSet),
    os:unsetenv(PrefixedVariable),
    ?assertMatch(#{Key := Comparison1}, rabbit_env:get_context()),

    os:putenv(PrefixedVariable, Value2ToSet),
    ?assertMatch(#{Key := Comparison2}, rabbit_env:get_context()),

    os:unsetenv(Variable),
    os:unsetenv(PrefixedVariable),
    Context = rabbit_env:get_context(),
    case DefaultValue of
        '_' ->
            ?assertNotMatch(#{Key := Comparison1}, Context),
            ?assertNotMatch(#{Key := Comparison2}, Context),
            ?assertMatch(#{Key := _}, Context);
        _ ->
            ?assertMatch(#{Key := DefaultValue}, Context)
    end.

random_int()    -> rand:uniform(50000).
random_string() -> integer_to_list(random_int()).
random_atom()   -> list_to_atom(random_string()).

get_default_nodename() ->
    CTNode = node(),
    NodeS = re:replace(
              atom_to_list(CTNode),
              "^[^@]+@(.*)$",
              "rabbit@\\1",
              [{return, list}]),
    list_to_atom(NodeS).

check_parse_conf_env_file_output(_) ->
    ?assertEqual(
       #{},
       rabbit_env:parse_conf_env_file_output2(
         [],
         #{}
        )),
    ?assertEqual(
       #{"UNQUOTED" => "a",
         "UNICODE" => [43, 43, 32, 1550, 32],
         "SINGLE_QUOTED" => "b",
         "DOUBLE_QUOTED" => "c",
         "SINGLE_DOLLAR" => "d"},
       rabbit_env:parse_conf_env_file_output2(
         %% a relatively rarely used Unicode character
         ["++ ؎ ",
          "UNQUOTED=a",
          "UNICODE='++ ؎ '",
          "SINGLE_QUOTED='b'",
          "DOUBLE_QUOTED=\"c\"",
          "SINGLE_DOLLAR=$'d'"],
         #{}
        )),
    ?assertEqual(
       #{"DOUBLE_QUOTED" => "\\' \" \\v",
         "SINGLE_DOLLAR" => "' \" \\ \007 z z z z"},
       rabbit_env:parse_conf_env_file_output2(
         ["DOUBLE_QUOTED=\"\\' \\\" \\v\"",
          "SINGLE_DOLLAR=$'\\' \\\" \\\\ \\a \\172 \\x7a \\u007A \\U0000007a'"
         ],
         #{}
        )),
    ?assertEqual(
       #{"A" => "a",
         "B" => "b",
         "SINGLE_QUOTED_MULTI_LINE" => "\n'foobar'",
         "DOUBLE_QUOTED_MULTI_LINE" => "Line1\nLine2"},
       rabbit_env:parse_conf_env_file_output2(
         ["A=a",
          "SINGLE_QUOTED_MULTI_LINE='",
          "'\"'\"'foobar'\"'\"",
          "DOUBLE_QUOTED_MULTI_LINE=\"Line1",
          "Line\\",
          "2\"",
          "B=b"],
         #{}
        )),
    ?assertEqual(
       #{"shellHook" =>
         "\n"
         "function isShellInteractive {\n"
         "  # shell is interactive if $- contains 'i'\n"
         "  [[ $- == *i* ]]\n"
         "}\n"},
       rabbit_env:parse_conf_env_file_output2(
         ["shellHook='",
          "function isShellInteractive {",
          "  # shell is interactive if $- contains '\\''i'\\''",
          "  [[ $- == *i* ]]",
          "}",
          "'"],
         #{}
        )).
