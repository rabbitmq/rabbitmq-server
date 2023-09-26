%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2016-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(logging_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("kernel/include/logger.hrl").
-include_lib("rabbit_common/include/logging.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-export([suite/0,
         all/0,
         groups/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,

         logging_with_default_config_works/1,
         setting_log_levels_in_env_works/1,
         setting_log_levels_in_config_works/1,
         setting_log_rotation_in_config_works/1,
         setting_log_rotation_defaults_in_config_works/1,
         setting_log_levels_in_config_with_output_overridden_in_env_works/1,
         setting_message_format_works/1,
         setting_level_format_works/1,
         setting_time_format_works/1,
         logging_as_single_line_works/1,
         logging_as_multi_line_works/1,
         formatting_as_json_configured_in_env_works/1,
         formatting_as_json_configured_in_config_works/1,
         formatting_as_json_using_epoch_secs_timestamps_works/1,
         renaming_json_fields_works/1,
         removing_specific_json_fields_works/1,
         removing_non_mentionned_json_fields_works/1,
         configuring_verbosity_works/1,

         logging_to_stdout_configured_in_env_works/1,
         logging_to_stdout_configured_in_config_works/1,
         logging_to_stderr_configured_in_env_works/1,
         logging_to_stderr_configured_in_config_works/1,
         formatting_with_colors_works/1,
         formatting_without_colors_works/1,

         logging_to_exchange_works/1,
         update_log_exchange_config/1,

         logging_to_syslog_works/1]).

suite() ->
    [{timetrap, {minutes, 1}}].

all() ->
    [
     {group, file_output},
     {group, console_output},
     {group, exchange_output},
     {group, syslog_output}
    ].

groups() ->
    [
     {file_output, [],
      [logging_with_default_config_works,
       setting_log_levels_in_env_works,
       setting_log_levels_in_config_works,
       setting_log_rotation_in_config_works,
       setting_log_rotation_defaults_in_config_works,
       setting_log_levels_in_config_with_output_overridden_in_env_works,
       setting_message_format_works,
       setting_level_format_works,
       setting_time_format_works,
       logging_as_single_line_works,
       logging_as_multi_line_works,
       formatting_as_json_configured_in_env_works,
       formatting_as_json_configured_in_config_works,
       formatting_as_json_using_epoch_secs_timestamps_works,
       renaming_json_fields_works,
       removing_specific_json_fields_works,
       removing_non_mentionned_json_fields_works,
       configuring_verbosity_works]},

     {console_output, [],
      [logging_to_stdout_configured_in_env_works,
       logging_to_stdout_configured_in_config_works,
       logging_to_stderr_configured_in_env_works,
       logging_to_stderr_configured_in_config_works,
       formatting_with_colors_works,
       formatting_without_colors_works]},

     {exchange_output, [],
      [logging_to_exchange_works,
       update_log_exchange_config]},

     {syslog_output, [],
      [logging_to_syslog_works]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    Config.

init_per_group(syslog_output, Config) ->
    Config1 = start_fake_syslogd(Config),
    TcpPort = ?config(syslogd_tcp_port, Config1),
    ok = application:set_env(
           syslog, logger, [],
           [{persistent, true}]),
    ok = application:set_env(
           syslog, syslog_error_logger, false,
           [{persistent, true}]),
    ok = application:set_env(
           syslog, protocol, {rfc3164, tcp},
           [{persistent, true}]),
    ok = application:set_env(
           syslog, dest_port, TcpPort,
           [{persistent, true}]),
    {ok, _} = application:ensure_all_started(syslog),
    Config1;
init_per_group(_, Config) ->
    Config.

end_per_group(syslog_output, Config) ->
    ok = application:stop(syslog),
    stop_fake_syslogd(Config);
end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    GroupProps = ?config(tc_group_properties, Config),
    Group = proplists:get_value(name, GroupProps),
    case Group of
        %% The exchange output requires RabbitMQ to run. All testcases in this
        %% group will run in the context of that RabbitMQ node.
        exchange_output ->
            ExchProps = [{enabled, true},
                         {level, debug}] ,
            Config1 = rabbit_ct_helpers:set_config(
                        Config,
                        [{rmq_nodes_count, 1},
                         {rmq_nodename_suffix, Testcase}]),
            Config2 = rabbit_ct_helpers:merge_app_env(
                        Config1,
                        {rabbit, [{log, [{exchange, ExchProps},
                                         {file, [{level, debug}]}]}]}),
            rabbit_ct_helpers:run_steps(
              Config2,
              rabbit_ct_broker_helpers:setup_steps() ++
              rabbit_ct_client_helpers:setup_steps());

        %% Other groups and testcases run the tested code directly without a
        %% RabbitMQ node running.
        _ ->
            remove_all_handlers(),
            application:unset_env(rabbit, log),
            LogBaseDir = filename:join(
                           ?config(priv_dir, Config),
                           atom_to_list(Testcase)),
            rabbit_ct_helpers:set_config(
              Config, {log_base_dir, LogBaseDir})
    end.

end_per_testcase(Testcase, Config) ->
    Config1 = case rabbit_ct_helpers:get_config(Config, rmq_nodes_count) of
                  undefined ->
                      application:unset_env(rabbit, log),
                      Config;
                  _ ->
                      rabbit_ct_helpers:run_steps(
                        Config,
                        rabbit_ct_client_helpers:teardown_steps() ++
                        rabbit_ct_broker_helpers:teardown_steps())
              end,
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

remove_all_handlers() ->
    _ = [logger:remove_handler(Id)
         || #{id := Id} <- logger:get_handler_config()].

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

logging_with_default_config_works(Config) ->
    Context = default_context(Config),
    rabbit_prelaunch_logging:clear_config_run_number(),
    rabbit_prelaunch_logging:setup(Context),

    Handlers = logger:get_handler_config(),

    MainFileHandler = get_handler_by_id(Handlers, rmq_1_file_1),
    MainFile = main_log_file_in_context(Context),
    ?assertNotEqual(undefined, MainFileHandler),
    ?assertMatch(
       #{level := info,
         module := rabbit_logger_std_h,
         filter_default := log,
         filters := [{progress_reports, {_, stop}},
                     {rmqlog_filter, {_, #{global := info}}}],
         formatter := {rabbit_logger_text_fmt, _},
         config := #{type := file,
                     file := MainFile}},
       MainFileHandler),

    ContainsLogEntryFun1 = ping_log(rmq_1_file_1, info),
    rabbit_ct_helpers:await_condition(ContainsLogEntryFun1, 30_000),
    ContainsLogEntryFun2 = ping_log(rmq_1_file_1, info,
                                    #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
    rabbit_ct_helpers:await_condition(ContainsLogEntryFun2, 30_000),
    ContainsLogEntry3 = ping_log(rmq_1_file_1, info,
                                 #{domain => ['3rd_party']}),
    rabbit_ct_helpers:await_condition(ContainsLogEntry3, 30_000),
    ContainsLogEntry4 = ping_log(rmq_1_file_1, info,
                                 #{domain => ?RMQLOG_DOMAIN_UPGRADE}),
    rabbit_ct_helpers:await_condition(ContainsLogEntry4, 30_000),
    ok.

setting_log_levels_in_env_works(Config) ->
    GlobalLevel = warning,
    PrelaunchLevel = error,
    MinLevel = rabbit_prelaunch_logging:get_less_severe_level(
                 GlobalLevel, PrelaunchLevel),
    #{var_origins := Origins0} = Context0 = default_context(Config),
    Context = Context0#{log_levels => #{global => GlobalLevel,
                                        "prelaunch" => PrelaunchLevel},
                        var_origins => Origins0#{log_levels => environment}},
    rabbit_prelaunch_logging:clear_config_run_number(),
    rabbit_prelaunch_logging:setup(Context),

    Handlers = logger:get_handler_config(),

    MainFileHandler = get_handler_by_id(Handlers, rmq_1_file_1),
    MainFile = main_log_file_in_context(Context),
    ?assertNotEqual(undefined, MainFileHandler),
    ?assertMatch(
       #{level := MinLevel,
         module := rabbit_logger_std_h,
         filter_default := log,
         filters := [{progress_reports, {_, stop}},
                     {rmqlog_filter, {_, #{global := GlobalLevel,
                                           prelaunch := PrelaunchLevel}}}],
         formatter := {rabbit_logger_text_fmt, _},
         config := #{type := file,
                     file := MainFile}},
       MainFileHandler),

    ContainsLogEntry1 = ping_log(rmq_1_file_1, info),
    ContainsLogEntry2 = ping_log(rmq_1_file_1, info,
                                 #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
    ContainsLogEntry3 = ping_log(rmq_1_file_1, info,
                                 #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ContainsLogEntry4 = ping_log(rmq_1_file_1, GlobalLevel,
                                 #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ContainsLogEntry5 = ping_log(rmq_1_file_1, info,
                                 #{domain => ['3rd_party']}),
    ContainsLogEntry6 = ping_log(rmq_1_file_1, info,
                                 #{domain => ?RMQLOG_DOMAIN_UPGRADE}),
    %% This is testing that the log entry is NOT present. Random sleeps
    %% are not ideal, but in this case we can just wait a reasonable
    %% amount of time and then check for absence.
    timer:sleep(10_000),
    ?assertNot(ContainsLogEntry1()),
    ?assertNot(ContainsLogEntry2()),
    ?assertNot(ContainsLogEntry3()),
    ?assertNot(ContainsLogEntry4()),
    ?assertNot(ContainsLogEntry5()),
    ?assertNot(ContainsLogEntry6()),

    ContainsLogEntry7 = ping_log(rmq_1_file_1, GlobalLevel),
    rabbit_ct_helpers:await_condition(ContainsLogEntry7, 30_000),
    ContainsLogEntry8 = ping_log(rmq_1_file_1, GlobalLevel,
                                 #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
    rabbit_ct_helpers:await_condition(ContainsLogEntry8, 30_000),
    ContainsLogEntry9 = ping_log(rmq_1_file_1, PrelaunchLevel,
                                 #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    rabbit_ct_helpers:await_condition(ContainsLogEntry9, 30_000),
    ContainsLogEntry10 = ping_log(rmq_1_file_1, GlobalLevel,
                                  #{domain => ['3rd_party']}),
    rabbit_ct_helpers:await_condition(ContainsLogEntry10, 30_000),
    ContainsLogEntry11 = ping_log(rmq_1_file_1, GlobalLevel,
                                  #{domain => ?RMQLOG_DOMAIN_UPGRADE}),
    rabbit_ct_helpers:await_condition(ContainsLogEntry11, 30_000),
    ok.

setting_log_levels_in_config_works(Config) ->
    GlobalLevel = warning,
    PrelaunchLevel = error,
    MinLevel = rabbit_prelaunch_logging:get_less_severe_level(
                 GlobalLevel, PrelaunchLevel),
    Context = default_context(Config),
    ok = application:set_env(
           rabbit, log,
           [{file, [{level, GlobalLevel}]},
            {categories, [{prelaunch, [{level, PrelaunchLevel}]}]}],
           [{persistent, true}]),
    rabbit_prelaunch_logging:clear_config_run_number(),
    rabbit_prelaunch_logging:setup(Context),

    Handlers = logger:get_handler_config(),

    MainFileHandler = get_handler_by_id(Handlers, rmq_1_file_1),
    MainFile = main_log_file_in_context(Context),
    ?assertNotEqual(undefined, MainFileHandler),
    ?assertMatch(
       #{level := MinLevel,
         module := rabbit_logger_std_h,
         filter_default := log,
         filters := [{progress_reports, {_, stop}},
                     {rmqlog_filter, {_, #{global := GlobalLevel,
                                           prelaunch := PrelaunchLevel}}}],
         formatter := {rabbit_logger_text_fmt, _},
         config := #{type := file,
                     file := MainFile}},
       MainFileHandler),

    ContainsLogEntry1 = ping_log(rmq_1_file_1, info),
    ContainsLogEntry2 = ping_log(rmq_1_file_1, info,
                                 #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
    ContainsLogEntry3 = ping_log(rmq_1_file_1, info,
                                 #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ContainsLogEntry4 = ping_log(rmq_1_file_1, GlobalLevel,
                                 #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ContainsLogEntry5 = ping_log(rmq_1_file_1, info,
                                 #{domain => ['3rd_party']}),
    ContainsLogEntry6 = ping_log(rmq_1_file_1, info,
                                 #{domain => ?RMQLOG_DOMAIN_UPGRADE}),

    %% This is testing that the log entry is NOT present. Random sleeps
    %% are not ideal, but in this case we can just wait a reasonable
    %% amount of time and then check for absence.
    timer:sleep(10_000),
    ?assertNot(ContainsLogEntry1()),
    ?assertNot(ContainsLogEntry2()),
    ?assertNot(ContainsLogEntry3()),
    ?assertNot(ContainsLogEntry4()),
    ?assertNot(ContainsLogEntry5()),
    ?assertNot(ContainsLogEntry6()),

    ContainsLogEntry7 = ping_log(rmq_1_file_1, GlobalLevel),
    rabbit_ct_helpers:await_condition(ContainsLogEntry7, 30_000),
    ContainsLogEntry8 = ping_log(rmq_1_file_1, GlobalLevel,
                                 #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
    rabbit_ct_helpers:await_condition(ContainsLogEntry8, 30_000),
    ContainsLogEntry9 = ping_log(rmq_1_file_1, PrelaunchLevel,
                                 #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    rabbit_ct_helpers:await_condition(ContainsLogEntry9, 30_000),
    ContainsLogEntry10 = ping_log(rmq_1_file_1, GlobalLevel,
                                  #{domain => ['3rd_party']}),
    rabbit_ct_helpers:await_condition(ContainsLogEntry10, 30_000),
    ContainsLogEntry11 = ping_log(rmq_1_file_1, GlobalLevel,
                                  #{domain => ?RMQLOG_DOMAIN_UPGRADE}),
    rabbit_ct_helpers:await_condition(ContainsLogEntry11, 30_000),
    ok.

setting_log_rotation_in_config_works(Config) ->
    Context = default_context(Config),
    ok = application:set_env(
           rabbit, log,
           [{file, [{date, "$D0"},
                    {compress, true},
                    {size, 100},
                    {count, 1}
                    ]},
            {categories, [{queue, [{file, "queue.log"},
                                   {rotate_on_date, "$D0"},
                                   {compress_on_rotate, true},
                                   {max_no_bytes, 200},
                                   {max_no_files, 2}
                                  ]}]}],
           [{persistent, true}]),
    rabbit_prelaunch_logging:clear_config_run_number(),
    rabbit_prelaunch_logging:setup(Context),

    Handlers = logger:get_handler_config(),
    MainFileHandler = get_handler_by_id(Handlers, rmq_1_file_2),
    MainFile = main_log_file_in_context(Context),
    ?assertNotEqual(undefined, MainFileHandler),
    ?assertMatch(
       #{config := #{file := MainFile,
                     type := file,
                     rotate_on_date := "$D0",
                     compress_on_rotate := true,
                     max_no_bytes := 100,
                     max_no_files := 1
                    }},
       MainFileHandler,
      "rotation config present in the main handler"),

    QueueFileHandler = get_handler_by_id(Handlers, rmq_1_file_1),
    QueueFile = log_file_in_context(Context, "queue.log"),
    ?assertNotEqual(undefined, QueueFileHandler),
    ?assertMatch(
       #{config := #{file := QueueFile,
                     type := file,
                     rotate_on_date := "$D0",
                     compress_on_rotate := true,
                     max_no_bytes := 200,
                     max_no_files := 2
                    }},
       QueueFileHandler,
      "rotation config present in the cat handler"),
    ok.

setting_log_rotation_defaults_in_config_works(Config) ->
    Context = default_context(Config),
    ok = application:set_env(
           rabbit, log,
           [{file, []},
            {categories, [{queue, [{file, "queue.log"}]},
                          {default, [{rotate_on_date, "$D0"},
                                     {compress_on_rotate, true},
                                     {max_no_bytes, 300},
                                     {max_no_files, 3}
                                    ]}]}],
           [{persistent, true}]),
    rabbit_prelaunch_logging:clear_config_run_number(),
    rabbit_prelaunch_logging:setup(Context),

    Handlers = logger:get_handler_config(),
    MainFileHandler = get_handler_by_id(Handlers, rmq_1_file_2),
    MainFile = main_log_file_in_context(Context),
    ?assertNotEqual(undefined, MainFileHandler),
    ?assertMatch(
       #{config := #{file := MainFile,
                     type := file,
                     rotate_on_date := "$D0",
                     compress_on_rotate := true,
                     max_no_bytes := 300,
                     max_no_files := 3
                    }},
       MainFileHandler,
      "rotation config present in the main handler"),

    QueueFileHandler = get_handler_by_id(Handlers, rmq_1_file_1),
    QueueFile = log_file_in_context(Context, "queue.log"),
    ?assertNotEqual(undefined, QueueFileHandler),
    ?assertMatch(
       #{config := #{file := QueueFile,
                     type := file,
                     rotate_on_date := "$D0",
                     compress_on_rotate := true,
                     max_no_bytes := 300,
                     max_no_files := 3
                    }},
       QueueFileHandler,
      "rotation config present in the queue handler"),
    ok.

setting_log_levels_in_config_with_output_overridden_in_env_works(Config) ->
    #{var_origins := Origins0} = Context0 = default_context(Config),
    Context = Context0#{main_log_file => "-",
                        var_origins => Origins0#{
                                         main_log_file => environment}},
    ok = application:set_env(
           rabbit, log, [{console, [{level, debug}]}],
           [{persistent, true}]),
    rabbit_prelaunch_logging:clear_config_run_number(),
    rabbit_prelaunch_logging:setup(Context),

    Handlers = logger:get_handler_config(),

    StddevHandler = get_handler_by_id(Handlers, rmq_1_stdout),
    ?assertNotEqual(undefined, StddevHandler),
    ?assertMatch(
       #{level := debug,
         module := rabbit_logger_std_h,
         filter_default := log,
         filters := [{progress_reports, {_, log}},
                     {rmqlog_filter, {_, #{global := debug}}}],
         formatter := {rabbit_logger_text_fmt, _},
         config := #{type := standard_io}},
       StddevHandler),

    ContainsLogEntry1 = ping_log(rmq_1_stdout, debug, Config),
    rabbit_ct_helpers:await_condition(ContainsLogEntry1, 30_000),
    ContainsLogEntry2 = ping_log(rmq_1_stdout, debug,
                                 #{domain => ?RMQLOG_DOMAIN_GLOBAL}, Config),
    rabbit_ct_helpers:await_condition(ContainsLogEntry2, 30_000),
    ContainsLogEntry3 = ping_log(rmq_1_stdout, debug,
                                 #{domain => ['3rd_party']}, Config),
    rabbit_ct_helpers:await_condition(ContainsLogEntry3, 30_000),
    ContainsLogEntry4 = ping_log(rmq_1_stdout, debug,
                                 #{domain => ?RMQLOG_DOMAIN_UPGRADE}, Config),
    rabbit_ct_helpers:await_condition(ContainsLogEntry4, 30_000),
    ok.

setting_message_format_works(Config) ->
    Context = default_context(Config),
    Format = ["level=", level, " ",
              "md_key=", md_key, " ",
              {md_key, ["known_md_key=", md_key], []}, " ",
              {unknown_field, ["unknown_field=", unknown_field], ["unknown_field_spotted"]}, " ",
              "unknown_field=", unknown_field, " ",
              "msg=", msg],
    {PrefixFormat, LineFormat} =
    rabbit_prelaunch_early_logging:determine_prefix(Format),
    ok = application:set_env(
           rabbit, log,
           [{file, [{formatter, {rabbit_logger_text_fmt,
                                 #{prefix_format => PrefixFormat,
                                   line_format => LineFormat}}}]}],
           [{persistent, true}]),
    rabbit_prelaunch_logging:clear_config_run_number(),
    rabbit_prelaunch_logging:setup(Context),

    Metadata = #{md_key => "md_value"},
    {RandomMsg, Line} = log_and_return_line(Context, Metadata),

    RandomMsgBin = list_to_binary(RandomMsg),
    ?assertEqual(
       <<"level=warning ",
         "md_key=md_value ",
         "known_md_key=md_value ",
         "unknown_field_spotted ",
         "unknown_field=<unknown unknown_field> ",
         "msg=", RandomMsgBin/binary>>,
       Line).

setting_level_format_works(Config) ->
    LevelFormats = #{lc  => "warning",
                     uc  => "WARNING",
                     lc3 => "wrn",
                     uc3 => "WRN",
                     lc4 => "warn",
                     uc4 => "WARN"},
    maps:fold(
      fun(LevelFormat, LevelName, Acc) ->
              remove_all_handlers(),
              setting_level_format_works(
                LevelFormat, list_to_binary(LevelName), Config),
              Acc
      end, ok, LevelFormats).

setting_level_format_works(LevelFormat, LevelName, Config) ->
    Context = default_context(Config),
    Format = [level, " ", msg],
    {PrefixFormat, LineFormat} =
    rabbit_prelaunch_early_logging:determine_prefix(Format),
    ok = application:set_env(
           rabbit, log,
           [{file, [{formatter, {rabbit_logger_text_fmt,
                                 #{level_format => LevelFormat,
                                   prefix_format => PrefixFormat,
                                   line_format => LineFormat}}}]}],
           [{persistent, true}]),
    rabbit_prelaunch_logging:clear_config_run_number(),
    rabbit_prelaunch_logging:setup(Context),

    {RandomMsg, Line} = log_and_return_line(Context, #{}),

    RandomMsgBin = list_to_binary(RandomMsg),
    ?assertEqual(
       <<LevelName/binary, " ", RandomMsgBin/binary>>,
       Line).

setting_time_format_works(Config) ->
    DateTime = "2018-05-01T16:17:58.123456+01:00",
    Timestamp = calendar:rfc3339_to_system_time(
                  DateTime, [{unit, microsecond}]),
    TimeFormats =
    #{{rfc3339, $T, "+01:00"} => DateTime,
      {rfc3339, $\s, "+01:00"} => "2018-05-01 16:17:58.123456+01:00",
      {epoch, usecs, binary} => integer_to_list(Timestamp),
      {epoch, secs, binary} => io_lib:format("~.6.0f", [Timestamp / 1000000]),
      {universal,
       "~4..0b-~2..0b-~2..0b "
       "~2..0b:~2..0b:~2..0b.~3..0b",
       [year, month, day,
        hour, minute, second,
        {second_fractional, 3}]} => "2018-05-01 15:17:58.123"},
    maps:fold(
      fun(TimeFormat, TimeValue, Acc) ->
              remove_all_handlers(),
              setting_time_format_works(
                Timestamp, TimeFormat, list_to_binary(TimeValue), Config),
              Acc
      end, ok, TimeFormats).

setting_time_format_works(Timestamp, TimeFormat, TimeValue, Config) ->
    Context = default_context(Config),
    Format = [time, " ", msg],
    {PrefixFormat, LineFormat} =
    rabbit_prelaunch_early_logging:determine_prefix(Format),
    ok = application:set_env(
           rabbit, log,
           [{file, [{formatter, {rabbit_logger_text_fmt,
                                 #{time_format => TimeFormat,
                                   prefix_format => PrefixFormat,
                                   line_format => LineFormat}}}]}],
           [{persistent, true}]),
    rabbit_prelaunch_logging:clear_config_run_number(),
    rabbit_prelaunch_logging:setup(Context),

    Metadata = #{time => Timestamp},
    {RandomMsg, Line} = log_and_return_line(Context, Metadata),

    RandomMsgBin = list_to_binary(RandomMsg),
    ?assertEqual(
       <<TimeValue/binary, " ", RandomMsgBin/binary>>,
       Line).

logging_as_single_line_works(Config) ->
    logging_as_single_or_multi_line_works(false, Config).

logging_as_multi_line_works(Config) ->
    logging_as_single_or_multi_line_works(true, Config).

logging_as_single_or_multi_line_works(AsMultiline, Config) ->
    Context = default_context(Config),
    Format = [time, " ", msg],
    {PrefixFormat, LineFormat} =
    rabbit_prelaunch_early_logging:determine_prefix(Format),
    ok = application:set_env(
           rabbit, log,
           [{file, [{formatter, {rabbit_logger_text_fmt,
                                 #{single_line => not AsMultiline,
                                   prefix_format => PrefixFormat,
                                   line_format => LineFormat}}}]}],
           [{persistent, true}]),
    rabbit_prelaunch_logging:clear_config_run_number(),
    rabbit_prelaunch_logging:setup(Context),

    RandomMsg1 = get_random_string(
                   32,
                   "abcdefghijklmnopqrstuvwxyz"
                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
    RandomMsg2 = get_random_string(
                   32,
                   "abcdefghijklmnopqrstuvwxyz"
                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
    ?LOG_WARNING(RandomMsg1 ++ "\n" ++ RandomMsg2, #{}),

    rabbit_logger_std_h:filesync(rmq_1_file_1),
    MainFile = main_log_file_in_context(Context),
    {ok, Content} = file:read_file(MainFile),
    ReOpts = [{capture, none}, multiline],
    case AsMultiline of
        true ->
            match = re:run(Content, RandomMsg1 ++ "$", ReOpts),
            match = re:run(Content, RandomMsg2 ++ "$", ReOpts);
        false ->
            match = re:run(
                      Content,
                      RandomMsg1 ++ ", " ++ RandomMsg2 ++ "$",
                      ReOpts)
    end.

formatting_as_json_configured_in_env_works(Config) ->
    #{var_origins := Origins0} = Context0 = default_context(Config),
    Context = Context0#{log_levels => #{json => true},
                        var_origins => Origins0#{log_levels => environment}},
    formatting_as_json_works(Config, Context).

formatting_as_json_configured_in_config_works(Config) ->
    Context = default_context(Config),
    ok = application:set_env(
           rabbit, log,
           [{file, [{formatter, {rabbit_logger_json_fmt, #{}}}]}],
           [{persistent, true}]),
    formatting_as_json_works(Config, Context).

formatting_as_json_using_epoch_secs_timestamps_works(Config) ->
    Context = default_context(Config),
    ok = application:set_env(
           rabbit, log,
           [{file, [{formatter, {rabbit_logger_json_fmt,
                                 #{time_format => {epoch, secs, int}}}}]}],
           [{persistent, true}]),
    formatting_as_json_works(Config, Context).

formatting_as_json_works(_, Context) ->
    rabbit_prelaunch_logging:clear_config_run_number(),
    rabbit_prelaunch_logging:setup(Context),

    Handlers = logger:get_handler_config(),

    MainFileHandler = get_handler_by_id(Handlers, rmq_1_file_1),
    MainFile = main_log_file_in_context(Context),
    ?assertNotEqual(undefined, MainFileHandler),
    ?assertMatch(
       #{level := info,
         module := rabbit_logger_std_h,
         filter_default := log,
         filters := [{progress_reports, {_, stop}},
                     {rmqlog_filter, {_, #{global := info}}}],
         formatter := {rabbit_logger_json_fmt, _},
         config := #{type := file,
                     file := MainFile}},
       MainFileHandler),

    ContainsLogEntry = ping_log(rmq_1_file_1, info),
    timer:sleep(10_000),
    ?assertNot(ContainsLogEntry()),

    Metadata = #{atom => rabbit,
                 integer => 1,
                 float => 1.42,
                 string => "string",
                 list => ["s", a, 3],
                 map => #{key => "value"},
                 function => fun get_random_string/2,
                 pid => self(),
                 port => hd(erlang:ports()),
                 ref => erlang:make_ref()},
    {RandomMsg, Term} = log_and_return_json_object(
                          Context, Metadata),

    RandomMsgBin = list_to_binary(RandomMsg),
    ?assertMatch(#{time := _}, Term),
    ?assertMatch(#{level := <<"info">>}, Term),
    ?assertMatch(#{msg := RandomMsgBin}, Term),

    FunBin = list_to_binary(erlang:fun_to_list(maps:get(function, Metadata))),
    PidBin = list_to_binary(erlang:pid_to_list(maps:get(pid, Metadata))),
    PortBin = list_to_binary(erlang:port_to_list(maps:get(port, Metadata))),
    RefBin = list_to_binary(erlang:ref_to_list(maps:get(ref, Metadata))),
    ?assertMatch(#{atom := <<"rabbit">>}, Term),
    ?assertMatch(#{integer := 1}, Term),
    ?assertMatch(#{float := 1.42}, Term),
    ?assertMatch(#{string := <<"string">>}, Term),
    ?assertMatch(#{list := [<<"s">>, <<"a">>, 3]}, Term),
    ?assertMatch(#{map := #{<<"key">> := <<"value">>}}, Term),
    ?assertMatch(#{function := FunBin}, Term),
    ?assertMatch(#{pid := PidBin}, Term),
    ?assertMatch(#{port := PortBin}, Term),
    ?assertMatch(#{ref := RefBin}, Term).

renaming_json_fields_works(Config) ->
    Context = default_context(Config),
    FieldMap = [{integer, int},
                {msg, m},
                {unknown_field, still_unknown_field},
                {'$REST', false}],
    ok = application:set_env(
           rabbit, log,
           [{file, [{formatter, {rabbit_logger_json_fmt,
                                 #{field_map => FieldMap}}}]}],
           [{persistent, true}]),
    rabbit_prelaunch_logging:clear_config_run_number(),
    rabbit_prelaunch_logging:setup(Context),

    Metadata = #{atom => rabbit,
                 integer => 1,
                 string => "string",
                 list => ["s", a, 3]},
    {RandomMsg, Term} = log_and_return_json_object(Context, Metadata),

    RandomMsgBin = list_to_binary(RandomMsg),
    ?assertMatch(
       #{int := 1,
         m := RandomMsgBin} = M
       when map_size(M) == 2,
            Term).

removing_specific_json_fields_works(Config) ->
    Context = default_context(Config),
    FieldMap = [{integer, integer},
                {msg, msg},
                {list, false}],
    ok = application:set_env(
           rabbit, log,
           [{file, [{formatter, {rabbit_logger_json_fmt,
                                 #{field_map => FieldMap}}}]}],
           [{persistent, true}]),
    rabbit_prelaunch_logging:clear_config_run_number(),
    rabbit_prelaunch_logging:setup(Context),

    Metadata = #{atom => rabbit,
                 integer => 1,
                 string => "string",
                 list => ["s", a, 3]},
    {RandomMsg, Term} = log_and_return_json_object(Context, Metadata),

    RandomMsgBin = list_to_binary(RandomMsg),
    ?assertMatch(
       #{integer := 1,
         msg := RandomMsgBin,
         string := <<"string">>},
       Term).

removing_non_mentionned_json_fields_works(Config) ->
    Context = default_context(Config),
    FieldMap = [{integer, integer},
                {msg, msg},
                {'$REST', false}],
    ok = application:set_env(
           rabbit, log,
           [{file, [{formatter, {rabbit_logger_json_fmt,
                                 #{field_map => FieldMap}}}]}],
           [{persistent, true}]),
    rabbit_prelaunch_logging:clear_config_run_number(),
    rabbit_prelaunch_logging:setup(Context),

    Metadata = #{atom => rabbit,
                 integer => 1,
                 string => "string",
                 list => ["s", a, 3]},
    {RandomMsg, Term} = log_and_return_json_object(Context, Metadata),

    RandomMsgBin = list_to_binary(RandomMsg),
    ?assertMatch(
        #{integer := 1,
          msg := RandomMsgBin} = M
       when map_size(M) == 2,
            Term).

configuring_verbosity_works(Config) ->
    Context = default_context(Config),
    FieldMap = [{verbosity, v},
                {msg, msg},
                {'$REST', false}],
    VerbMap = #{debug => 2,
                info => 1,
                '$REST' => 0},
    ok = application:set_env(
           rabbit, log,
           [{file, [{formatter, {rabbit_logger_json_fmt,
                                 #{field_map => FieldMap,
                                   verbosity_map => VerbMap}}}]}],
           [{persistent, true}]),
    rabbit_prelaunch_logging:clear_config_run_number(),
    rabbit_prelaunch_logging:setup(Context),

    {RandomMsg, Term} = log_and_return_json_object(Context, #{}),

    RandomMsgBin = list_to_binary(RandomMsg),
    ?assertMatch(
       #{v := 1,
         msg := RandomMsgBin} = M
       when map_size(M) == 2,
            Term).

logging_to_stdout_configured_in_env_works(Config) ->
    #{var_origins := Origins0} = Context0 = default_context(Config),
    Context = Context0#{main_log_file => "-",
                        var_origins => Origins0#{
                                         main_log_file => environment}},
    logging_to_stddev_works(standard_io, rmq_1_stdout, Config, Context).

logging_to_stdout_configured_in_config_works(Config) ->
    Context = default_context(Config),
    ok = application:set_env(
           rabbit, log, [{console, [{enabled, true}]}],
           [{persistent, true}]),
    logging_to_stddev_works(standard_io, rmq_1_stdout, Config, Context).

logging_to_stderr_configured_in_env_works(Config) ->
    #{var_origins := Origins0} = Context0 = default_context(Config),
    Context = Context0#{main_log_file => "-stderr",
                        var_origins => Origins0#{
                                         main_log_file => environment}},
    logging_to_stddev_works(standard_error, rmq_1_stderr, Config, Context).

logging_to_stderr_configured_in_config_works(Config) ->
    Context = default_context(Config),
    ok = application:set_env(
           rabbit, log, [{console, [{enabled, true},
                                    {stdio, stderr}]}],
           [{persistent, true}]),
    logging_to_stddev_works(standard_error, rmq_1_stderr, Config, Context).

logging_to_stddev_works(Stddev, Id, Config, Context) ->
    rabbit_prelaunch_logging:clear_config_run_number(),
    rabbit_prelaunch_logging:setup(Context),

    Handlers = logger:get_handler_config(),

    StddevHandler = get_handler_by_id(Handlers, Id),
    ?assertNotEqual(undefined, StddevHandler),
    ?assertMatch(
       #{level := info,
         module := rabbit_logger_std_h,
         filter_default := log,
         filters := [{progress_reports, {_, stop}},
                     {rmqlog_filter, {_, #{global := info}}}],
         formatter := {rabbit_logger_text_fmt, _},
         config := #{type := Stddev}},
       StddevHandler),

    ContainsLogEntry1 = ping_log(Id, info, Config),
    rabbit_ct_helpers:await_condition(ContainsLogEntry1, 30_000),
    ContainsLogEntry2 = ping_log(Id, info,
                                 #{domain => ?RMQLOG_DOMAIN_GLOBAL}, Config),
    rabbit_ct_helpers:await_condition(ContainsLogEntry2, 30_000),
    ContainsLogEntry3 = ping_log(Id, info,
                                 #{domain => ['3rd_party']}, Config),
    rabbit_ct_helpers:await_condition(ContainsLogEntry3, 30_000),
    ContainsLogEntry4 = ping_log(Id, info,
                                 #{domain => ?RMQLOG_DOMAIN_UPGRADE}, Config),
    rabbit_ct_helpers:await_condition(ContainsLogEntry4, 30_000),
    ok.

formatting_with_colors_works(Config) ->
    EscSeqs = make_color_esc_seqs_map(),
    Context = default_context(Config),
    ok = application:set_env(
           rabbit, log, [{console, [{level, debug},
                                    {formatter,
                                     {rabbit_logger_text_fmt,
                                      #{use_colors => true,
                                        color_esc_seqs => EscSeqs}}}]}],
           [{persistent, true}]),
    formatting_maybe_with_colors_works(Config, Context, EscSeqs).

formatting_without_colors_works(Config) ->
    EscSeqs = make_color_esc_seqs_map(),
    Context = default_context(Config),
    ok = application:set_env(
           rabbit, log, [{console, [{level, debug},
                                    {formatter,
                                     {rabbit_logger_text_fmt,
                                      #{use_colors => false,
                                        color_esc_seqs => EscSeqs}}}]}],
           [{persistent, true}]),
    formatting_maybe_with_colors_works(Config, Context, EscSeqs).

make_color_esc_seqs_map() ->
    lists:foldl(
      fun(Lvl, Acc) ->
              EscSeq = "[" ++ atom_to_list(Lvl) ++ " color]",
              Acc#{Lvl => EscSeq}
      end, #{}, rabbit_prelaunch_early_logging:levels()).

formatting_maybe_with_colors_works(Config, Context, _EscSeqs) ->
    rabbit_prelaunch_logging:clear_config_run_number(),
    rabbit_prelaunch_logging:setup(Context),

    ContainsLogEntry1 = ping_log(rmq_1_stdout, debug, Config),
    rabbit_ct_helpers:await_condition(ContainsLogEntry1, 30_000),
    ContainsLogEntry2 = ping_log(rmq_1_stdout, info, Config),
    rabbit_ct_helpers:await_condition(ContainsLogEntry2, 30_000),
    ContainsLogEntry3 = ping_log(rmq_1_stdout, notice, Config),
    rabbit_ct_helpers:await_condition(ContainsLogEntry3, 30_000),
    ContainsLogEntry4 = ping_log(rmq_1_stdout, warning, Config),
    rabbit_ct_helpers:await_condition(ContainsLogEntry4, 30_000),
    ContainsLogEntry5 = ping_log(rmq_1_stdout, error, Config),
    rabbit_ct_helpers:await_condition(ContainsLogEntry5, 30_000),
    ContainsLogEntry6 = ping_log(rmq_1_stdout, critical, Config),
    rabbit_ct_helpers:await_condition(ContainsLogEntry6, 30_000),
    ContainsLogEntry7 = ping_log(rmq_1_stdout, alert, Config),
    rabbit_ct_helpers:await_condition(ContainsLogEntry7, 30_000),
    ContainsLogEntry8 = ping_log(rmq_1_stdout, emergency, Config),
    rabbit_ct_helpers:await_condition(ContainsLogEntry8, 30_000),
    ok.

logging_to_exchange_works(Config) ->
    Handlers = rabbit_ct_broker_helpers:rpc(
                 Config, 0,
                 logger, get_handler_config, []),

    ExchangeHandler = get_handler_by_id(Handlers, rmq_1_exchange),
    ?assertNotEqual(undefined, ExchangeHandler),
    ?assertMatch(
       #{level := debug,
         module := rabbit_logger_exchange_h,
         filter_default := log,
         filters := [{progress_reports, {_, log}},
                     {rmqlog_filter, {_, #{global := debug}}}],
         formatter := {rabbit_logger_text_fmt, _},
         config := #{exchange := _}},
       ExchangeHandler),
    #{config :=
      #{exchange := #resource{name = XName} = Exchange}} = ExchangeHandler,

    %% Wait for the expected exchange to be automatically declared.
    ?awaitMatch({ok, _}, rabbit_ct_broker_helpers:rpc(
                           Config, 0,
                           rabbit_exchange, lookup, [Exchange]),
                30000),

    %% Declare a queue to collect all logged messages.
    {Conn, Chan} = rabbit_ct_client_helpers:open_connection_and_channel(
                     Config),
    QName = <<"log-messages">>,
    ?assertMatch(
       #'queue.declare_ok'{},
       amqp_channel:call(Chan, #'queue.declare'{queue = QName,
                                                durable = true})),
    ?assertMatch(
       #'queue.bind_ok'{},
       amqp_channel:call(Chan, #'queue.bind'{queue = QName,
                                             exchange = XName,
                                             routing_key = <<"#">>})),
    Config1 = rabbit_ct_helpers:set_config(
                Config, {test_channel_and_queue, {Chan, QName}}),

    ContainsLogEntry1 = ping_log(rmq_1_exchange, info, Config1),
    rabbit_ct_helpers:await_condition(ContainsLogEntry1, 30_000),
    ContainsLogEntry2 = ping_log(rmq_1_exchange, info,
                                 #{domain => ?RMQLOG_DOMAIN_GLOBAL}, Config1),
    rabbit_ct_helpers:await_condition(ContainsLogEntry2, 30_000),
    ContainsLogEntry3 = ping_log(rmq_1_exchange, info,
                                 #{domain => ['3rd_party']}, Config1),
    rabbit_ct_helpers:await_condition(ContainsLogEntry3, 30_000),
    ContainsLogEntry4 = ping_log(rmq_1_exchange, info,
                                 #{domain => ?RMQLOG_DOMAIN_UPGRADE}, Config1),
    rabbit_ct_helpers:await_condition(ContainsLogEntry4, 30_000),

    %% increase log level
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0,
           rabbit_prelaunch_logging, set_log_level, [debug]),

    ContainsLogEntry5 = ping_log(rmq_1_exchange, debug, Config1),
    rabbit_ct_helpers:await_condition(ContainsLogEntry5, 30_000),
    ContainsLogEntry6 = ping_log(rmq_1_exchange, debug,
                                 #{domain => ?RMQLOG_DOMAIN_GLOBAL}, Config1),
    rabbit_ct_helpers:await_condition(ContainsLogEntry6, 30_000),

    %% decrease log level
    ok = rabbit_ct_broker_helpers:rpc(
           Config, 0,
           rabbit_prelaunch_logging, set_log_level, [error]),

    ContainsLogEntry7 = ping_log(rmq_1_exchange, error, Config1),
    rabbit_ct_helpers:await_condition(ContainsLogEntry7, 30_000),
    ContainsLogEntry8 = ping_log(rmq_1_exchange, error,
                                 #{domain => ?RMQLOG_DOMAIN_GLOBAL}, Config1),
    rabbit_ct_helpers:await_condition(ContainsLogEntry8, 30_000),

    ContainsLogEntry9 = ping_log(rmq_1_exchange, info, Config1),
    timer:sleep(10_000),
    ?assertNot(ContainsLogEntry9()),

    amqp_channel:call(Chan, #'queue.delete'{queue = QName}),
    rabbit_ct_client_helpers:close_connection_and_channel(Conn, Chan),
    ok.

%% Logging configuration should be manipulated via RabbitMQ.
%% Still if someone would modify log exchange config directly via the
%% OTP logger API, that should work too.
update_log_exchange_config(Config) ->
    {ok, OrigHandlerConfig} =
        rabbit_ct_broker_helpers:rpc(
          Config, 0,
          logger, get_handler_config, [rmq_1_exchange]),

    ok = rabbit_ct_broker_helpers:rpc(
          Config, 0,
          logger, update_formatter_config, [rmq_1_exchange, #{use_colors => true}]),
    {ok, HandlerConfig1} =
        rabbit_ct_broker_helpers:rpc(
          Config, 0,
          logger, get_handler_config, [rmq_1_exchange]),

    %% use_colors config changed from false to true
    ?assertMatch(#{formatter := {_, #{use_colors := false}}}, OrigHandlerConfig),
    ?assertMatch(#{formatter := {_, #{use_colors := true}}}, HandlerConfig1),
    %% no other formatter config changed
    ?assertEqual(
       maps:without([use_colors], element(2, maps:get(formatter, OrigHandlerConfig))),
       maps:without([use_colors], element(2, maps:get(formatter, HandlerConfig1)))),
    %% no other handler config changed
    ?assertEqual(
       maps:without([formatter], OrigHandlerConfig),
       maps:without([formatter], HandlerConfig1)),

    %% should not be possible to change exchange resource or setup_proc in config
    logger:update_handler_config(rmq_1_exchange, config, #{exchange => "foo", setup_proc => self()}),
    {ok, HandlerConfig2} =
        rabbit_ct_broker_helpers:rpc(
          Config, 0,
          logger, get_handler_config, [rmq_1_exchange]),
    ?assertEqual(HandlerConfig1, HandlerConfig2),
    ok.

logging_to_syslog_works(Config) ->
    Context = default_context(Config),
    ok = application:set_env(
           rabbit, log, [{syslog, [{enabled, true}]}],
           [{persistent, true}]),
    rabbit_prelaunch_logging:clear_config_run_number(),
    rabbit_prelaunch_logging:setup(Context),
    clear_syslogd_messages(Config),

    Handlers = logger:get_handler_config(),

    SyslogHandler = get_handler_by_id(Handlers, rmq_1_syslog),
    ?assertNotEqual(undefined, SyslogHandler),
    ?assertMatch(
       #{level := info,
         module := syslog_logger_h,
         filter_default := log,
         filters := [{progress_reports, {_, stop}},
                     {rmqlog_filter, {_, #{global := info}}}],
         formatter := {rabbit_logger_text_fmt, _},
         config := #{}},
       SyslogHandler),

    ContainsLogEntry1 = ping_log(rmq_1_syslog, info, Config),
    rabbit_ct_helpers:await_condition(ContainsLogEntry1, 30_000),
    ContainsLogEntry2 = ping_log(rmq_1_syslog, info,
                                 #{domain => ?RMQLOG_DOMAIN_GLOBAL}, Config),
    rabbit_ct_helpers:await_condition(ContainsLogEntry2, 30_000),
    ContainsLogEntry3 = ping_log(rmq_1_syslog, info,
                                 #{domain => ['3rd_party']}, Config),
    rabbit_ct_helpers:await_condition(ContainsLogEntry3, 30_000),
    ContainsLogEntry4 = ping_log(rmq_1_syslog, info,
                                 #{domain => ?RMQLOG_DOMAIN_UPGRADE}, Config),
    rabbit_ct_helpers:await_condition(ContainsLogEntry4, 30_000),
    ok.

%% -------------------------------------------------------------------
%% Internal functions.
%% -------------------------------------------------------------------

default_context(Config) ->
    LogBaseDir = ?config(log_base_dir, Config),
    MainFile = "rabbit.log",
    #{log_base_dir => LogBaseDir,
      main_log_file => MainFile,
      log_levels => undefined,
      var_origins => #{log_base_dir => default,
                       main_log_file => default,
                       log_levels => default}}.

main_log_file_in_context(#{log_base_dir := LogBaseDir,
                           main_log_file := MainLogFile}) ->
    filename:join(LogBaseDir, MainLogFile).

log_file_in_context(#{log_base_dir := LogBaseDir}, FileName) ->
    filename:join(LogBaseDir, FileName).

get_handler_by_id([#{id := Id} = Handler | _], Id) ->
    Handler;
get_handler_by_id([_ | Rest], Id) ->
    get_handler_by_id(Rest, Id);
get_handler_by_id([], _) ->
    undefined.

%% ping_log calls logger:log/3 and then returns a function that checks
%% the log for the given log entry returning a boolean.
%% This return function can be used with an await condition function,
%% to ensure the log entry is eventually added to the log.
%% Also it can be used to check it's absence.
ping_log(Id, Level) ->
    ping_log(Id, Level, #{}, []).

ping_log(Id, Level, Metadata) when is_map(Metadata) ->
    ping_log(Id, Level, Metadata, []);
ping_log(Id, Level, Config) when is_list(Config) ->
    ping_log(Id, Level, #{}, Config).

ping_log(Id, Level, Metadata, Config) ->
    RandomMsg = get_random_string(
                  32,
                  "abcdefghijklmnopqrstuvwxyz"
                  "ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
    ct:log("Logging \"~ts\" at level ~ts (~tp)", [RandomMsg, Level, Metadata]),
    case need_rpc(Config) of
        false -> logger:log(Level, RandomMsg, Metadata);
        true  -> rabbit_ct_broker_helpers:rpc(
                   Config, 0,
                   logger, log, [Level, RandomMsg, Metadata])
    end,
    check_log(Id, Level, RandomMsg, Config).

need_rpc(Config) ->
    rabbit_ct_helpers:get_config(
      Config, rmq_nodes_count) =/= undefined.

check_log(Id, Level, RandomMsg, Config) ->
    {ok, Handler} = case need_rpc(Config) of
                        false -> logger:get_handler_config(Id);
                        true  -> rabbit_ct_broker_helpers:rpc(
                                   Config, 0,
                                   logger, get_handler_config, [Id])
                    end,
    check_log1(Handler, Level, RandomMsg, Config).

check_log1(#{id := Id,
             module := rabbit_logger_std_h,
             config := #{type := file,
                         file := Filename}},
           _Level,
           RandomMsg,
           Config) ->
    ok = case need_rpc(Config) of
             false -> rabbit_logger_std_h:filesync(Id);
             true  -> rabbit_ct_broker_helpers:rpc(
                        Config, 0,
                        rabbit_logger_std_h, filesync, [Id])
         end,
    fun() ->
            {ok, Content} = file:read_file(Filename),
            ReOpts = [{capture, none}, multiline],
            match =:= re:run(Content, RandomMsg ++ "$", ReOpts)
    end;
check_log1(#{module := Mod,
             config := #{type := Stddev}} = Handler,
           Level,
           RandomMsg,
           Config)
  when ?IS_STD_H_COMPAT(Mod) andalso ?IS_STDDEV(Stddev) ->
    Filename = html_report_filename(Config),
    {ColorStart, ColorEnd} = get_color_config(Handler, Level),
    ReOpts = [{capture, none}, multiline],
    fun() ->
            {ok, Content} = file:read_file(Filename),
            Regex =
                "^" ++ ColorStart ++ ".+" ++ RandomMsg ++ ColorEnd ++ "$",
            match =:= re:run(Content, Regex, ReOpts)
    end;
check_log1(#{module := rabbit_logger_exchange_h},
           _Level,
           RandomMsg,
           Config) ->
    {Chan, QName} = ?config(test_channel_and_queue, Config),
    ReOpts = [{capture, none}, multiline],
    fun() ->
            Ret = amqp_channel:call(
                    Chan, #'basic.get'{queue = QName, no_ack = false}),
            case Ret of
                {#'basic.get_ok'{}, #amqp_msg{payload = Content}} ->
                    match =:= re:run(Content, RandomMsg ++ "$", ReOpts);
                #'basic.get_empty'{} ->
                    false;
                Other ->
                    io:format(standard_error, "OTHER -> ~tp~n", [Other]),
                    false
            end
    end;
check_log1(#{module := syslog_logger_h},
           _Level,
           RandomMsg,
           Config) ->
    ReOpts = [{capture, none}, multiline],
    fun() ->
            Buffer = get_syslogd_messages(Config),
            match =:= re:run(Buffer, RandomMsg ++ "$", ReOpts)
    end.

get_random_string(Length, AllowedChars) ->
    lists:foldl(fun(_, Acc) ->
                        [lists:nth(rand:uniform(length(AllowedChars)),
                                   AllowedChars)]
                        ++ Acc
                end, [], lists:seq(1, Length)).

html_report_filename(Config) ->
    ?config(tc_logfile, Config).

get_color_config(
  #{formatter := {rabbit_logger_text_fmt,
                  #{use_colors := true,
                    color_esc_seqs := EscSeqs}}}, Level) ->
    ColorStart = maps:get(Level, EscSeqs),
    ColorEnd = "\033[0m",
    {escape_for_re(ColorStart), escape_for_re(ColorEnd)};
get_color_config(_, _) ->
    {"", ""}.

escape_for_re(String) ->
    String1 = string:replace(String, "[", "\\[", all),
    string:replace(String1, "]", "\\]", all).

log_and_return_line(Context, Metadata) ->
    RandomMsg = get_random_string(
                  32,
                  "abcdefghijklmnopqrstuvwxyz"
                  "ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
    logger:warning(RandomMsg, Metadata),

    rabbit_logger_std_h:filesync(rmq_1_file_1),
    MainFile = main_log_file_in_context(Context),
    {ok, Content} = file:read_file(MainFile),
    ReOpts = [{capture, first, binary}, multiline],
    {match, [Line]} = re:run(
                        Content,
                        "^.+" ++ RandomMsg ++ ".*$",
                        ReOpts),
    {RandomMsg, Line}.

log_and_return_json_object(Context, Metadata) ->
    RandomMsg = get_random_string(
                  32,
                  "abcdefghijklmnopqrstuvwxyz"
                  "ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
    ?LOG_INFO(RandomMsg, Metadata),

    rabbit_logger_std_h:filesync(rmq_1_file_1),
    MainFile = main_log_file_in_context(Context),
    {ok, Content} = file:read_file(MainFile),
    ReOpts = [{capture, first, binary}, multiline],
    {match, [Line]} = re:run(
                        Content,
                        "^.+\"" ++ RandomMsg ++ "\".+$",
                        ReOpts),

    Term0 = rabbit_json:decode(Line),
    Term = rabbit_data_coercion:atomize_keys(Term0),

    {RandomMsg, Term}.

%% -------------------------------------------------------------------
%% Fake syslog server.
%% -------------------------------------------------------------------

start_fake_syslogd(Config) ->
    Self = self(),
    Pid = spawn(fun() -> syslogd_init(Self) end),
    TcpPort = receive {syslogd_ready, P} -> P end,

    rabbit_ct_helpers:set_config(
      Config, [{syslogd_pid, Pid},
               {syslogd_tcp_port, TcpPort}]).

stop_fake_syslogd(Config) ->
    Pid = ?config(syslogd_pid, Config),
    Pid ! stop,
    Config1 = rabbit_ct_helpers:delete_config(Config, syslogd_pid),
    rabbit_ct_helpers:delete_config(Config1, syslogd_tcp_port).

get_syslogd_messages(Config) ->
    Pid = ?config(syslogd_pid, Config),
    Pid ! {get_messages, self()},
    receive {syslogd_messages, Buffer} -> Buffer end.

clear_syslogd_messages(Config) ->
    Pid = ?config(syslogd_pid, Config),
    Pid ! clear_messages.

syslogd_init(Parent) ->
    {ok, TcpPort, LSock} = open_tcp_listening_sock(22000),
    ct:pal(
      "Fake syslogd ready (~tp), listening on TCP port ~tp",
      [self(), TcpPort]),
    Parent ! {syslogd_ready, TcpPort},
    syslogd_start_loop(LSock).

open_tcp_listening_sock(TcpPort) ->
    Options = [binary,
               {active, true}],
    case gen_tcp:listen(TcpPort, Options) of
        {ok, LSock}         -> {ok, TcpPort, LSock};
        {error, eaddrinuse} -> open_tcp_listening_sock(TcpPort + 1)
    end.

syslogd_start_loop(LSock) ->
    ct:pal("Fake syslogd: accepting new connection", []),
    {ok, Sock} = gen_tcp:accept(LSock),
    ct:pal("Fake syslogd: accepted new connection!", []),
    syslogd_loop(LSock, Sock, [], <<>>).

syslogd_loop(LSock, Sock, Messages, Buffer) ->
    try
        receive
            {tcp, Sock, NewData} ->
                Buffer1 = <<Buffer/binary, NewData/binary>>,
                {NewMessages, Buffer2} = parse_messages(Buffer1),
                syslogd_loop(LSock, Sock, Messages ++ NewMessages, Buffer2);
            {get_messages, From} ->
                ct:pal(
                  "Fake syslogd: sending messages to ~tp:~n~tp",
                  [From, Messages]),
                From ! {syslogd_messages, Messages},
                syslogd_loop(LSock, Sock, Messages, Buffer);
            clear_messages ->
                ct:pal("Fake syslogd: clearing buffer", []),
                syslogd_loop(LSock, Sock, [], Buffer);
            {tcp_closed, Sock} ->
                ct:pal("Fake syslogd: socket closed, restarting loop", []),
                syslogd_start_loop(LSock);
            stop ->
                ct:pal("Fake syslogd: exiting", []),
                _ = gen_tcp:close(Sock),
                _ = gen_tcp:close(LSock);
            Other ->
                ct:pal("Fake syslogd: unhandled message: ~tp", [Other]),
                syslogd_loop(LSock, Sock, Messages, Buffer)
        end
    catch
        C:R:S ->
            ct:pal("~tp ~tp ~tp", [C, R, S]),
            throw(R)
    end.

parse_messages(Buffer) ->
    parse_messages(Buffer, []).

parse_messages(Buffer, Messages) ->
    ReOpts = [{capture, all_but_first, binary}],
    case re:run(Buffer, "^([0-9]+) (.*)", ReOpts) of
        {match, [Length0, Buffer1]} ->
            Length = list_to_integer(binary_to_list(Length0)),
            case Buffer1 of
                <<Message:Length/binary, Buffer2/binary>> ->
                    parse_messages(
                      Buffer2, [<<Message/binary, $\n>> | Messages]);
                _ ->
                    {lists:reverse(Messages), Buffer}
            end;
        _ ->
            {lists:reverse(Messages), Buffer}
    end.
