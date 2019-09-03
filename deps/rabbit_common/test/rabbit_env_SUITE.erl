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
%% Copyright (c) 2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_env_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0,
         groups/0,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,
         check_data_dir/1,
         check_default_values/1,
         check_RABBITMQ_ADVANCED_CONFIG_FILE/1,
         check_RABBITMQ_CONFIG_FILE/1,
         check_RABBITMQ_DIST_PORT/1,
         check_RABBITMQ_ENABLED_PLUGINS/1,
         check_RABBITMQ_ENABLED_PLUGINS_FILE/1,
         check_RABBITMQ_FEATURE_FLAGS_FILE/1,
         check_RABBITMQ_KEEP_PID_FILE_ON_EXIT/1,
         check_RABBITMQ_LOG/1,
         check_RABBITMQ_LOG_BASE/1,
         check_RABBITMQ_LOGS/1,
         check_RABBITMQ_MNESIA_BASE/1,
         check_RABBITMQ_MNESIA_DIR/1,
         check_RABBITMQ_NODE_IP_ADDRESS/1,
         check_RABBITMQ_NODE_PORT/1,
         check_RABBITMQ_NODENAME/1,
         check_RABBITMQ_PID_FILE/1,
         check_RABBITMQ_PLUGINS_DIR/1,
         check_RABBITMQ_PLUGINS_EXPAND_DIR/1,
         check_RABBITMQ_QUORUM_DIR/1,
         check_RABBITMQ_UPGRADE_LOG/1,
         check_RABBITMQ_USE_LOGNAME/1,
         check_value_is_yes/1
        ]).

all() ->
    [
     check_data_dir,
     check_default_values,
     check_RABBITMQ_ADVANCED_CONFIG_FILE,
     check_RABBITMQ_CONFIG_FILE,
     check_RABBITMQ_DIST_PORT,
     check_RABBITMQ_ENABLED_PLUGINS,
     check_RABBITMQ_ENABLED_PLUGINS_FILE,
     check_RABBITMQ_FEATURE_FLAGS_FILE,
     check_RABBITMQ_KEEP_PID_FILE_ON_EXIT,
     check_RABBITMQ_LOG,
     check_RABBITMQ_LOG_BASE,
     check_RABBITMQ_LOGS,
     check_RABBITMQ_MNESIA_BASE,
     check_RABBITMQ_MNESIA_DIR,
     check_RABBITMQ_NODE_IP_ADDRESS,
     check_RABBITMQ_NODE_PORT,
     check_RABBITMQ_NODENAME,
     check_RABBITMQ_PID_FILE,
     check_RABBITMQ_PLUGINS_DIR,
     check_RABBITMQ_PLUGINS_EXPAND_DIR,
     check_RABBITMQ_QUORUM_DIR,
     check_RABBITMQ_UPGRADE_LOG,
     check_RABBITMQ_USE_LOGNAME,
     check_value_is_yes
    ].

groups() ->
    [
     {parallel_tests, [parallel], all()}
    ].

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
    ?assertMatch(#{data_dir := _}, rabbit_env:get_context()).

check_default_values(_) ->
    %% When `rabbit_env` is built with `TEST` defined, we can override
    %% the OS type.
    persistent_term:put({rabbit_env, load_conf_env_file}, false),
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
    persistent_term:erase({rabbit_env, load_conf_env_file}),

    Node = get_default_nodename(),
    NodeS = atom_to_list(Node),

    ?assertEqual(
       #{advanced_config_file => "/etc/rabbitmq/advanced.config",
         amqp_ipaddr_and_tcp_port => {"auto", 5672},
         conf_env_file => "/etc/rabbitmq/rabbitmq-env.conf",
         config_base_dir => "/etc/rabbitmq",
         data_dir => "/var/lib/rabbitmq",
         dbg_mods => [],
         dbg_output => stdout,
         enabled_plugins => undefined,
         enabled_plugins_file => "/etc/rabbitmq/enabled_plugins",
         erlang_dist_tcp_port => 25672,
         feature_flags_file =>
           "/var/lib/rabbitmq/mnesia/" ++ NodeS ++ "-feature_flags",
         interactive_shell => false,
         keep_pid_file_on_exit => false,
         log_base_dir => "/var/log/rabbitmq",
         log_levels => undefined,
         main_config_file => "/etc/rabbitmq/rabbitmq",
         main_log_file => "/var/log/rabbitmq/" ++ NodeS ++ ".log",
         mnesia_base_dir => "/var/lib/rabbitmq/mnesia",
         mnesia_dir => "/var/lib/rabbitmq/mnesia/" ++ NodeS,
         nodename => Node,
         nodename_type => shortnames,
         os_type => {unix, undefined},
         output_supports_colors => true,
         pid_file => "/var/lib/rabbitmq/mnesia/" ++ NodeS ++ ".pid",
         plugins_expand_dir =>
           "/var/lib/rabbitmq/mnesia/" ++ NodeS ++ "-plugins-expand",
         plugins_path => maps:get(plugins_path, UnixContext),
         quorum_queue_dir =>
           "/var/lib/rabbitmq/mnesia/" ++ NodeS ++ "/quorum",
         rabbitmq_home => maps:get(rabbitmq_home, UnixContext),
         split_nodename => rabbit_nodes_common:parts(Node),
         upgrade_log_file =>
           "/var/log/rabbitmq/" ++ NodeS ++ "_upgrade.log"},
       UnixContext),

    ?assertEqual(
       #{advanced_config_file => "%APPDATA%/RabbitMQ/advanced.config",
         amqp_ipaddr_and_tcp_port => {"auto", 5672},
         conf_env_file => "%APPDATA%/RabbitMQ/rabbitmq-env-conf.bat",
         config_base_dir => "%APPDATA%/RabbitMQ",
         data_dir => "%APPDATA%/RabbitMQ",
         dbg_mods => [],
         dbg_output => stdout,
         enabled_plugins => undefined,
         enabled_plugins_file => "%APPDATA%/RabbitMQ/enabled_plugins",
         erlang_dist_tcp_port => 25672,
         feature_flags_file =>
           "%APPDATA%/RabbitMQ/db/" ++ NodeS ++ "-feature_flags",
         interactive_shell => false,
         keep_pid_file_on_exit => false,
         log_base_dir => "%APPDATA%/RabbitMQ/log",
         log_levels => undefined,
         main_config_file => "%APPDATA%/RabbitMQ/rabbitmq",
         main_log_file => "%APPDATA%/RabbitMQ/log/" ++ NodeS ++ ".log",
         mnesia_base_dir => "%APPDATA%/RabbitMQ/db",
         mnesia_dir => "%APPDATA%/RabbitMQ/db/" ++ NodeS ++ "-mnesia",
         nodename => Node,
         nodename_type => shortnames,
         os_type => {win32, undefined},
         output_supports_colors => false,
         pid_file => "%APPDATA%/RabbitMQ/db/" ++ NodeS ++ ".pid",
         plugins_expand_dir =>
           "%APPDATA%/RabbitMQ/db/" ++ NodeS ++ "-plugins-expand",
         plugins_path => maps:get(plugins_path, Win32Context),
         quorum_queue_dir =>
           "%APPDATA%/RabbitMQ/db/" ++ NodeS ++ "-mnesia/quorum",
         rabbitmq_home => maps:get(rabbitmq_home, Win32Context),
         split_nodename => rabbit_nodes_common:parts(Node),
         upgrade_log_file =>
           "%APPDATA%/RabbitMQ/log/" ++ NodeS ++ "_upgrade.log"},
       Win32Context).

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
                            Value1, #{Value1 => info},
                            Value2, #{Value2 => info}).

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
                            amqp_ipaddr_and_tcp_port,
                            {"auto", 5672},
                            Value1, {Value1, 5672},
                            Value2, {Value2, 5672}).

check_RABBITMQ_NODE_PORT(_) ->
    Value1 = random_int(),
    Value2 = random_int(),
    check_prefixed_variable("RABBITMQ_NODE_PORT",
                            amqp_ipaddr_and_tcp_port,
                            {"auto", 5672},
                            integer_to_list(Value1), {"auto", Value1},
                            integer_to_list(Value2), {"auto", Value2}).

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

check_RABBITMQ_QUORUM_DIR(_) ->
    Value1 = random_string(),
    Value2 = random_string(),
    check_prefixed_variable("RABBITMQ_QUORUM_DIR",
                            quorum_queue_dir,
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
