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
%% Copyright (c) 2016-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(unit_log_config_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

all() ->
    [
    default,
    env_var_tty,
    config_file_handler,
    config_file_handler_level,
    config_file_handler_rotation,
    config_console_handler,
    config_exchange_handler,
    config_syslog_handler,
    config_syslog_handler_options,
    config_multiple_handlers,

    env_var_overrides_config,
    env_var_disable_log,

    config_sinks_level,
    config_sink_file,
    config_sink_file_override_config_handler_file,

    config_handlers_merged_with_lager_handlers,
    sink_handlers_merged_with_lager_extra_sinks_handlers,
    sink_file_rewrites_file_backends
    ].

init_per_testcase(_, Config) ->
    application:load(rabbit),
    application:load(lager),
    application:unset_env(rabbit, log),
    application:unset_env(rabbit, lager_log_root),
    application:unset_env(rabbit, lager_default_file),
    application:unset_env(rabbit, lager_upgrade_file),
    application:unset_env(lager, handlers),
    application:unset_env(lager, rabbit_handlers),
    application:unset_env(lager, extra_sinks),
    os:unsetenv("RABBITMQ_LOGS_source"),
    Config.

end_per_testcase(_, Config) ->
    application:unset_env(rabbit, log),
    application:unset_env(rabbit, lager_log_root),
    application:unset_env(rabbit, lager_default_file),
    application:unset_env(rabbit, lager_upgrade_file),
    application:unset_env(lager, handlers),
    application:unset_env(lager, rabbit_handlers),
    application:unset_env(lager, extra_sinks),
    os:unsetenv("RABBITMQ_LOGS_source"),
    application:unload(rabbit),
    application:unload(lager),
    Config.

sink_file_rewrites_file_backends(_) ->
    application:set_env(rabbit, log, [
        %% Disable rabbit file handler
        {file, [{file, false}]},
        {categories, [{federation, [{file, "federation.log"}, {level, warning}]}]}
    ]),

    LagerHandlers = [
        {lager_file_backend, [{file, "lager_file.log"}, {level, error}]},
        {lager_file_backend, [{file, "lager_file_1.log"}, {level, error}]},
        {lager_console_backend, [{level, info}]},
        {lager_exchange_backend, [{level, info}]}
    ],
    application:set_env(lager, handlers, LagerHandlers),
    rabbit_lager:configure_lager(),

    ExpectedSinks = sort_sinks(sink_rewrite_sinks()),
    ?assertEqual(ExpectedSinks, sort_sinks(application:get_env(lager, extra_sinks, undefined))).

sink_rewrite_sinks() ->
    [{error_logger_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_channel_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_connection_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_federation_lager_event,
        [{handlers,[
            {lager_file_backend,
                    [{date, ""},
                     {file, "federation.log"},
                     {formatter_config, formatter_config()},
                     {level, warning},
                     {size, 0}]},
            {lager_console_backend, [{level, warning}]},
            {lager_exchange_backend, [{level, warning}]}
        ]},
         {rabbit_handlers,[
            {lager_file_backend,
                    [{date, ""},
                     {file, "federation.log"},
                     {formatter_config, formatter_config()},
                     {level, warning},
                     {size, 0}]},
            {lager_console_backend, [{level, warning}]},
            {lager_exchange_backend, [{level, warning}]}
        ]}]},
     {rabbit_log_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_ldap_lager_event,
               [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
                {rabbit_handlers,
                 [{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_mirroring_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_queue_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_ra_lager_event,
      [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
       {rabbit_handlers,
        [{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_upgrade_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]}
     ].

sink_handlers_merged_with_lager_extra_sinks_handlers(_) ->
    application:set_env(rabbit, log, [
        {file,     [{file, "rabbit_file.log"}, {level, debug}]},
        {console,  [{enabled, true}, {level, error}]},
        {exchange, [{enabled, true}, {level, error}]},
        {categories, [
            {connection, [{level, debug}]},
            {channel, [{level, warning}, {file, "channel_log.log"}]}
        ]}
    ]),

    LagerSinks = [
        {rabbit_log_connection_lager_event,
            [{handlers,
                [{lager_file_backend,
                    [{file, "connection_lager.log"},
                     {level, info}]}]}]},
        {rabbit_log_channel_lager_event,
            [{handlers,
                [{lager_console_backend, [{level, debug}]},
                 {lager_exchange_backend, [{level, debug}]},
                 {lager_file_backend, [{level, error},
                                       {file, "channel_lager.log"}]}]}]}],

    application:set_env(lager, extra_sinks, LagerSinks),
    rabbit_lager:configure_lager(),

    ExpectedSinks = sort_sinks([
        {error_logger_lager_event,
            [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
             {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
         {rabbit_log_channel_lager_event,
            [{handlers,[
                {lager_console_backend, [{level, error},
                                         {formatter_config, formatter_config()}]},
                {lager_exchange_backend, [{level, error},
                                        {formatter_config, formatter_config()}]},
                {lager_file_backend,
                    [{date, ""},
                     {file, "channel_log.log"},
                     {formatter_config, formatter_config()},
                     {level, warning},
                     {size, 0}]},
                {lager_console_backend, [{level, debug}]},
                {lager_exchange_backend, [{level, debug}]},
                {lager_file_backend, [{level, error},
                                      {file, "channel_lager.log"}]}
                ]},
             {rabbit_handlers,[
                {lager_console_backend, [{level, error},
                                         {formatter_config, formatter_config()}]},
                {lager_exchange_backend, [{level, error},
                                        {formatter_config, formatter_config()}]},
                {lager_file_backend,
                    [{date, ""},
                     {file, "channel_log.log"},
                     {formatter_config, formatter_config()},
                     {level, warning},
                     {size, 0}]}]}
             ]},
         {rabbit_log_connection_lager_event,
            [{handlers,[{lager_forwarder_backend,[lager_event,debug]},
                        {lager_file_backend, [{file, "connection_lager.log"}, {level, info}]}]},
             {rabbit_handlers,[{lager_forwarder_backend,[lager_event,debug]}]}]},
         {rabbit_log_federation_lager_event,
            [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
             {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
         {rabbit_log_lager_event,
            [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
             {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
         {rabbit_log_ldap_lager_event,
                   [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
                    {rabbit_handlers,
                     [{lager_forwarder_backend,[lager_event,inherit]}]}]},
         {rabbit_log_mirroring_lager_event,
            [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
             {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
         {rabbit_log_queue_lager_event,
            [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
             {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
         {rabbit_log_ra_lager_event,
            [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
             {rabbit_handlers,
            [{lager_forwarder_backend,[lager_event,inherit]}]}]},
         {rabbit_log_upgrade_lager_event,
            [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
             {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]}]),

    ?assertEqual(ExpectedSinks, sort_sinks(application:get_env(lager, extra_sinks, undefined))).

config_handlers_merged_with_lager_handlers(_) ->
    application:set_env(rabbit, log, [
        {file,    [{file, "rabbit_file.log"}, {level, debug}]},
        {console, [{enabled, true}, {level, error}]},
        {exchange,  [{enabled, true}, {level, error}]},
        {syslog,  [{enabled, true}]}
    ]),

    LagerHandlers = [
        {lager_file_backend, [{file, "lager_file.log"}, {level, info}]},
        {lager_console_backend, [{level, info}]},
        {lager_exchange_backend, [{level, info}]},
        {lager_exchange_backend, [{level, info}]}
    ],
    application:set_env(lager, handlers, LagerHandlers),
    rabbit_lager:configure_lager(),

    FileHandlers = default_expected_handlers("rabbit_file.log", debug),
    ConsoleHandlers = expected_console_handler(error),
    RabbitHandlers = expected_rabbit_handler(error),
    SyslogHandlers = expected_syslog_handler(),

    ExpectedRabbitHandlers = sort_handlers(FileHandlers ++ ConsoleHandlers ++ RabbitHandlers ++ SyslogHandlers),
    ExpectedHandlers = sort_handlers(ExpectedRabbitHandlers ++ LagerHandlers),

    ?assertEqual(ExpectedRabbitHandlers, sort_handlers(application:get_env(lager, rabbit_handlers, undefined))),
    ?assertEqual(ExpectedHandlers, sort_handlers(application:get_env(lager, handlers, undefined))).

config_sinks_level(_) ->
    DefaultLogFile = "rabbit_default.log",
    application:set_env(rabbit, lager_default_file, DefaultLogFile),

    application:set_env(rabbit, log, [
        {categories, [
            {connection, [{level, warning}]},
            {channel, [{level, debug}]},
            {mirroring, [{level, error}]}
        ]}
    ]),

    rabbit_lager:configure_lager(),

    ExpectedSinks = sort_sinks(level_sinks()),
    ExpectedSinks = sort_sinks(application:get_env(lager, extra_sinks, undefined)).

level_sinks() ->
    [{error_logger_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_channel_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,debug]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,debug]}]}]},
     {rabbit_log_connection_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,warning]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,warning]}]}]},
     {rabbit_log_federation_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_ldap_lager_event,
               [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
                {rabbit_handlers,
                 [{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_mirroring_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,error]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,error]}]}]},
     {rabbit_log_queue_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_ra_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,
        [{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_upgrade_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]}
     ].

config_sink_file(_) ->
    DefaultLogFile = "rabbit_default.log",
    application:set_env(rabbit, lager_default_file, DefaultLogFile),

    application:set_env(rabbit, log, [
        {console, [{enabled, true}]},
        {exchange, [{enabled, true}]},
        {file, [{level, error}]},
        {categories, [
            {connection, [{file, "connection.log"}, {level, warning}]}
        ]}
    ]),

    rabbit_lager:configure_lager(),

    ExpectedSinks = sort_sinks(file_sinks()),
    ?assertEqual(ExpectedSinks, sort_sinks(application:get_env(lager, extra_sinks, undefined))).

config_sink_file_override_config_handler_file(_) ->
    DefaultLogFile = "rabbit_default.log",
    application:set_env(rabbit, lager_default_file, DefaultLogFile),

    NonDefaultLogFile = "rabbit_not_default.log",

    application:set_env(rabbit, log, [
        {file, [{file, NonDefaultLogFile}, {level, error}]},
        {console, [{enabled, true}]},
        {exchange, [{enabled, true}]},
        {categories, [
            {connection, [{file, "connection.log"}, {level, warning}]}
        ]}
    ]),

    rabbit_lager:configure_lager(),

    ExpectedSinks = sort_sinks(file_sinks()),
    ?assertEqual(ExpectedSinks, sort_sinks(application:get_env(lager, extra_sinks, undefined))).

file_sinks() ->
    [{error_logger_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_channel_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_connection_lager_event,
        [{handlers,[
            {lager_console_backend, [{level, warning},
                                     {formatter_config, formatter_config()}]},
            {lager_exchange_backend, [{level, warning},
                                    {formatter_config, formatter_config()}]},
            {lager_file_backend,
                [{date, ""},
                 {file, "connection.log"},
                 {formatter_config, formatter_config()},
                 {level, error},
                 {size, 0}]}]},
         {rabbit_handlers,[
            {lager_console_backend, [{level, warning},
                                     {formatter_config, formatter_config()}]},
            {lager_exchange_backend, [{level, warning},
                                    {formatter_config, formatter_config()}]},
            {lager_file_backend,
                [{date, ""},
                 {file, "connection.log"},
                 {formatter_config, formatter_config()},
                 {level, error},
                 {size, 0}]}]}
         ]},
     {rabbit_log_federation_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_ldap_lager_event,
               [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
                {rabbit_handlers,
                 [{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_mirroring_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_queue_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_ra_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,
        [{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_upgrade_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]}
     ].

config_multiple_handlers(_) ->
    DefaultLogFile = "rabbit_default.log",
    application:set_env(rabbit, lager_default_file, DefaultLogFile),

    application:set_env(rabbit, log, [
        %% Disable file output
        {file, [{file, false}]},
        %% Enable console output
        {console, [{enabled, true}]},
        %% Enable exchange output
        {exchange, [{enabled, true}]},
        %% Enable a syslog output
        {syslog, [{enabled, true}, {level, error}]}]),

    rabbit_lager:configure_lager(),

    ConsoleHandlers = expected_console_handler(),
    RabbitHandlers = expected_rabbit_handler(),
    SyslogHandlers = expected_syslog_handler(error),

    ExpectedHandlers = sort_handlers(SyslogHandlers ++ ConsoleHandlers ++ RabbitHandlers),

    ?assertEqual(ExpectedHandlers, sort_handlers(application:get_env(lager, handlers, undefined))),
    ?assertEqual(ExpectedHandlers, sort_handlers(application:get_env(lager, rabbit_handlers, undefined))).

config_console_handler(_) ->
    DefaultLogFile = "rabbit_default.log",
    application:set_env(rabbit, lager_default_file, DefaultLogFile),
    application:set_env(rabbit, log, [{console, [{enabled, true}]}]),

    rabbit_lager:configure_lager(),

    FileHandlers = default_expected_handlers(DefaultLogFile),
    ConsoleHandlers = expected_console_handler(),

    ExpectedHandlers = sort_handlers(FileHandlers ++ ConsoleHandlers),

    ?assertEqual(ExpectedHandlers, sort_handlers(application:get_env(lager, handlers, undefined))),
    ?assertEqual(ExpectedHandlers, sort_handlers(application:get_env(lager, rabbit_handlers, undefined))).

config_exchange_handler(_) ->
    DefaultLogFile = "rabbit_default.log",
    application:set_env(rabbit, lager_default_file, DefaultLogFile),
    application:set_env(rabbit, log, [{exchange, [{enabled, true}]}]),

    rabbit_lager:configure_lager(),

    FileHandlers = default_expected_handlers(DefaultLogFile),
    ExchangeHandlers = expected_rabbit_handler(),

    ExpectedHandlers = sort_handlers(FileHandlers ++ ExchangeHandlers),

    ?assertEqual(ExpectedHandlers, sort_handlers(application:get_env(lager, handlers, undefined))),
    ?assertEqual(ExpectedHandlers, sort_handlers(application:get_env(lager, rabbit_handlers, undefined))).

expected_console_handler() ->
    expected_console_handler(info).

expected_console_handler(Level) ->
    [{lager_console_backend, [{level, Level},
                              {formatter_config, formatter_config()}]}].

expected_rabbit_handler() ->
    expected_rabbit_handler(info).

expected_rabbit_handler(Level) ->
    [{lager_exchange_backend, [{level, Level},
                             {formatter_config, formatter_config()}]}].

config_syslog_handler(_) ->
    DefaultLogFile = "rabbit_default.log",
    application:set_env(rabbit, lager_default_file, DefaultLogFile),
    application:set_env(rabbit, log, [{syslog, [{enabled, true}]}]),

    rabbit_lager:configure_lager(),

    FileHandlers = default_expected_handlers(DefaultLogFile),
    SyslogHandlers = expected_syslog_handler(),

    ExpectedHandlers = sort_handlers(FileHandlers ++ SyslogHandlers),

    ?assertEqual(ExpectedHandlers, sort_handlers(application:get_env(lager, handlers, undefined))),
    ?assertEqual(ExpectedHandlers, sort_handlers(application:get_env(lager, rabbit_handlers, undefined))).

config_syslog_handler_options(_) ->
    DefaultLogFile = "rabbit_default.log",
    application:set_env(rabbit, lager_default_file, DefaultLogFile),
    application:set_env(rabbit, log, [{syslog, [{enabled, true},
                                                {level, warning}]}]),

    rabbit_lager:configure_lager(),

    FileHandlers = default_expected_handlers(DefaultLogFile),
    SyslogHandlers = expected_syslog_handler(warning),

    ExpectedHandlers = sort_handlers(FileHandlers ++ SyslogHandlers),

    ?assertEqual(ExpectedHandlers, sort_handlers(application:get_env(lager, handlers, undefined))),
    ?assertEqual(ExpectedHandlers, sort_handlers(application:get_env(lager, rabbit_handlers, undefined))).

expected_syslog_handler() ->
    expected_syslog_handler(info).

expected_syslog_handler(Level) ->
    [{syslog_lager_backend, [Level,
                             {},
                             {lager_default_formatter, syslog_formatter_config()}]}].

env_var_overrides_config(_) ->
    EnvLogFile = "rabbit_default.log",
    application:set_env(rabbit, lager_default_file, EnvLogFile),

    ConfigLogFile = "rabbit_not_default.log",
    application:set_env(rabbit, log, [{file, [{file, ConfigLogFile}]}]),

    os:putenv("RABBITMQ_LOGS_source", "environment"),
    rabbit_lager:configure_lager(),

    ExpectedHandlers = default_expected_handlers(EnvLogFile),
    ?assertEqual(ExpectedHandlers, sort_handlers(application:get_env(lager, handlers, undefined))),
    ?assertEqual(ExpectedHandlers, sort_handlers(application:get_env(lager, rabbit_handlers, undefined))).

env_var_disable_log(_) ->
    application:set_env(rabbit, lager_default_file, false),

    ConfigLogFile = "rabbit_not_default.log",
    application:set_env(rabbit, log, [{file, [{file, ConfigLogFile}]}]),

    os:putenv("RABBITMQ_LOGS_source", "environment"),
    rabbit_lager:configure_lager(),

    ExpectedHandlers = [],
    ?assertEqual(ExpectedHandlers, sort_handlers(application:get_env(lager, handlers, undefined))),
    ?assertEqual(ExpectedHandlers, sort_handlers(application:get_env(lager, rabbit_handlers, undefined))).

config_file_handler(_) ->
    DefaultLogFile = "rabbit_default.log",
    application:set_env(rabbit, lager_default_file, DefaultLogFile),

    NonDefaultLogFile = "rabbit_not_default.log",
    application:set_env(rabbit, log, [{file, [{file, NonDefaultLogFile}]}]),

    rabbit_lager:configure_lager(),

    ExpectedHandlers = default_expected_handlers(NonDefaultLogFile),
    ?assertEqual(ExpectedHandlers, sort_handlers(application:get_env(lager, handlers, undefined))),
    ?assertEqual(ExpectedHandlers, sort_handlers(application:get_env(lager, rabbit_handlers, undefined))).

config_file_handler_level(_) ->
    DefaultLogFile = "rabbit_default.log",
    application:set_env(rabbit, lager_default_file, DefaultLogFile),

    application:set_env(rabbit, log, [{file, [{level, warning}]}]),
    rabbit_lager:configure_lager(),

    ExpectedHandlers = default_expected_handlers(DefaultLogFile, warning),
    ?assertEqual(ExpectedHandlers, sort_handlers(application:get_env(lager, handlers, undefined))),
    ?assertEqual(ExpectedHandlers, sort_handlers(application:get_env(lager, rabbit_handlers, undefined))).

config_file_handler_rotation(_) ->
    DefaultLogFile = "rabbit_default.log",
    application:set_env(rabbit, lager_default_file, DefaultLogFile),

    application:set_env(rabbit, log, [{file, [{date, "$D0"}, {size, 5000}, {count, 10}]}]),
    rabbit_lager:configure_lager(),

    ExpectedHandlers = sort_handlers(default_expected_handlers(DefaultLogFile, info, 5000, "$D0", [{count, 10}])),
    ?assertEqual(ExpectedHandlers, sort_handlers(application:get_env(lager, handlers, undefined))),
    ?assertEqual(ExpectedHandlers, sort_handlers(application:get_env(lager, rabbit_handlers, undefined))).

default(_) ->
    LogRoot = "/tmp/log_base",
    application:set_env(rabbit, lager_log_root, LogRoot),
    LogFile = "rabbit_default.log",
    application:set_env(rabbit, lager_default_file, LogFile),
    LogUpgradeFile = "rabbit_default_upgrade.log",
    application:set_env(rabbit, lager_upgrade_file, LogUpgradeFile),

    rabbit_lager:configure_lager(),

    ExpectedHandlers = default_expected_handlers(LogFile),
    LogRoot = application:get_env(lager, log_root, undefined),
    ExpectedHandlers = sort_handlers(application:get_env(lager, handlers, undefined)),
    ?assertEqual(ExpectedHandlers, sort_handlers(application:get_env(lager, rabbit_handlers, undefined))),

    ExpectedSinks = default_expected_sinks(LogUpgradeFile),
    ?assertEqual(ExpectedSinks, sort_sinks(application:get_env(lager, extra_sinks, undefined))).

default_expected_handlers(File) ->
    default_expected_handlers(File, info, 0, "").
default_expected_handlers(File, Level) ->
    default_expected_handlers(File, Level, 0, "").
default_expected_handlers(File, Level, RotSize, RotDate) ->
    default_expected_handlers(File, Level, RotSize, RotDate, []).
default_expected_handlers(File, Level, RotSize, RotDate, Extra) ->
    [{lager_file_backend,
        [{date, RotDate},
         {file, File},
         {formatter_config, formatter_config()},
         {level, Level},
         {size, RotSize}] ++ Extra}].

default_expected_sinks(UpgradeFile) ->
    [{error_logger_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_channel_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_connection_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_federation_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_ldap_lager_event,
               [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
                {rabbit_handlers,
                 [{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_mirroring_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_queue_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_ra_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,
        [{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_upgrade_lager_event,
        [{handlers,
            [{lager_file_backend,
                [{date,[]},
                 {file, UpgradeFile},
                 {formatter_config, formatter_config()},
                 {level,info},
                 {size,0}]}]},
         {rabbit_handlers,
            [{lager_file_backend,
                [{date,[]},
                 {file, UpgradeFile},
                 {formatter_config, formatter_config()},
                 {level,info},
                 {size,0}]}]}]}].

env_var_tty(_) ->
    application:set_env(rabbit, lager_log_root, "/tmp/log_base"),
    application:set_env(rabbit, lager_default_file, tty),
    application:set_env(rabbit, lager_upgrade_file, tty),
    %% tty can only be set explicitly
    os:putenv("RABBITMQ_LOGS_source", "environment"),

    rabbit_lager:configure_lager(),

    ExpectedHandlers = tty_expected_handlers(),
    ?assertEqual(ExpectedHandlers, sort_handlers(application:get_env(lager, handlers, undefined))),
    ?assertEqual(ExpectedHandlers, sort_handlers(application:get_env(lager, rabbit_handlers, undefined))),

    %% Upgrade sink will be different.
    ExpectedSinks = tty_expected_sinks(),
    ?assertEqual(ExpectedSinks, sort_sinks(application:get_env(lager, extra_sinks, undefined))).

tty_expected_handlers() ->
    [{lager_console_backend,
        [{formatter_config, formatter_config()},
         {level, info}]}].

tty_expected_sinks() ->
    [{error_logger_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_channel_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_connection_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_federation_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_lager_event,
        [{handlers, [{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers, [{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_ldap_lager_event,
               [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
                {rabbit_handlers,
                 [{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_mirroring_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_queue_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_ra_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,
        [{lager_forwarder_backend,[lager_event,inherit]}]}]},
     {rabbit_log_upgrade_lager_event,
        [{handlers,[{lager_forwarder_backend,[lager_event,inherit]}]},
         {rabbit_handlers,[{lager_forwarder_backend,[lager_event,inherit]}]}]}].

sort_sinks(Sinks) ->
    lists:ukeysort(1,
        lists:map(
            fun({Name, Config}) ->
                Handlers = proplists:get_value(handlers, Config),
                RabbitHandlers = proplists:get_value(rabbit_handlers, Config),
                {Name, lists:ukeymerge(1,
                            [{handlers, sort_handlers(Handlers)},
                             {rabbit_handlers, sort_handlers(RabbitHandlers)}],
                            lists:ukeysort(1, Config))}
            end,
            Sinks)).

sort_handlers(Handlers) ->
    lists:keysort(1,
        lists:map(
            fun
            ({Name, [{Atom, _}|_] = Config}) when is_atom(Atom) ->
                {Name, lists:ukeysort(1, Config)};
            %% Non-proplist configuration. forwarder backend
            (Other) ->
                Other
            end,
            Handlers)).

formatter_config() ->
    [date," ",time," ",color,"[",severity, "] ", {pid,[]}, " ",message,"\n"].

syslog_formatter_config() ->
    [color,"[",severity, "] ", {pid,[]}, " ",message,"\n"].
