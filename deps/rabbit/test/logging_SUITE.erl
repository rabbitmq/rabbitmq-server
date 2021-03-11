%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2016-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(logging_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-include_lib("kernel/include/logger.hrl").
-include_lib("rabbit_common/include/logging.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-export([all/0,
         init_per_suite/1,
         end_per_suite/1,
         init_per_group/2,
         end_per_group/2,
         init_per_testcase/2,
         end_per_testcase/2,

         logging_with_default_config_works/1,
         logging_to_stdout_configured_in_env_works/1,
         logging_to_stdout_configured_in_config_works/1,
         logging_to_stderr_configured_in_env_works/1,
         logging_to_exchange_works/1,
         setting_log_levels_in_env_works/1,
         setting_log_levels_in_config_works/1,
         format_messages_as_json_works/1]).

all() ->
    [logging_with_default_config_works,
     logging_to_stdout_configured_in_env_works,
     logging_to_stdout_configured_in_config_works,
     logging_to_stderr_configured_in_env_works,
     logging_to_exchange_works,
     setting_log_levels_in_env_works,
     setting_log_levels_in_config_works,
     format_messages_as_json_works].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    Config.

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(logging_to_exchange_works = Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase),
    Config1 = rabbit_ct_helpers:set_config(
                Config,
                [{rmq_nodes_count, 1},
                 {rmq_nodename_suffix, Testcase}]),
    Config2 = rabbit_ct_helpers:merge_app_env(
                Config1,
                {rabbit, [{log, [{exchange, [{enabled, true},
                                             {level, info}]},
                                 {file, [{level, info}]}]}]}),
    rabbit_ct_helpers:run_steps(
      Config2,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps());
init_per_testcase(Testcase, Config) ->
    remove_all_handlers(),
    application:unset_env(rabbit, log),
    LogBaseDir = filename:join(
                   ?config(priv_dir, Config),
                   atom_to_list(Testcase)),
    Config1 = rabbit_ct_helpers:set_config(
                Config, {log_base_dir, LogBaseDir}),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

end_per_testcase(logging_to_exchange_works, Config) ->
    rabbit_ct_helpers:run_steps(
      Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps());
end_per_testcase(_, Config) ->
    application:unset_env(rabbit, log),
    Config.

remove_all_handlers() ->
    _ = [logger:remove_handler(Id)
         || #{id := Id} <- logger:get_handler_config()].

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
                     {rmqlog_filter, {_, #{global := info,
                                           upgrade := none}}}],
         formatter := {rabbit_logger_text_fmt, _},
         config := #{type := file,
                     file := MainFile}},
       MainFileHandler),

    UpgradeFileHandler = get_handler_by_id(Handlers, rmq_1_file_2),
    UpgradeFile = upgrade_log_file_in_context(Context),
    ?assertNotEqual(undefined, UpgradeFileHandler),
    ?assertMatch(
       #{level := info,
         module := rabbit_logger_std_h,
         filter_default := stop,
         filters := [{rmqlog_filter, {_, #{upgrade := info}}}],
         formatter := {rabbit_logger_text_fmt, _},
         config := #{type := file,
                     file := UpgradeFile}},
       UpgradeFileHandler),

    ?assert(ping_log(rmq_1_file_1, info)),
    ?assert(ping_log(rmq_1_file_1, info,
                     #{domain => ?RMQLOG_DOMAIN_GLOBAL})),
    ?assert(ping_log(rmq_1_file_1, info,
                     #{domain => ['3rd_party']})),
    ?assertNot(ping_log(rmq_1_file_1, info,
                        #{domain => ?RMQLOG_DOMAIN_UPGRADE})),

    ?assert(ping_log(rmq_1_file_2, info,
                     #{domain => ?RMQLOG_DOMAIN_UPGRADE})),
    ?assertNot(ping_log(rmq_1_file_2, info,
                        #{domain => ?RMQLOG_DOMAIN_GLOBAL})),
    ok.

logging_to_stdout_configured_in_env_works(Config) ->
    #{var_origins := Origins0} = Context0 = default_context(Config),
    Context = Context0#{main_log_file => "-",
                        var_origins => Origins0#{
                                         main_log_file => environment}},
    logging_to_stddev_works(standard_io, rmq_1_stdout, Config, Context).

logging_to_stdout_configured_in_config_works(Config) ->
    Context = default_context(Config),
    ok = application:set_env(
           rabbit, log,
           [{console, [{enabled, true}]}],
           [{persistent, true}]),
    logging_to_stddev_works(standard_io, rmq_1_stdout, Config, Context).

logging_to_stderr_configured_in_env_works(Config) ->
    #{var_origins := Origins0} = Context0 = default_context(Config),
    Context = Context0#{main_log_file => "-stderr",
                        var_origins => Origins0#{
                                         main_log_file => environment}},
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
                     {rmqlog_filter, {_, #{global := info,
                                           upgrade := none}}}],
         formatter := {rabbit_logger_text_fmt, _},
         config := #{type := Stddev}},
       StddevHandler),

    UpgradeFileHandler = get_handler_by_id(Handlers, rmq_1_file_1),
    UpgradeFile = upgrade_log_file_in_context(Context),
    ?assertNotEqual(undefined, UpgradeFileHandler),
    ?assertMatch(
       #{level := info,
         module := rabbit_logger_std_h,
         filter_default := stop,
         filters := [{rmqlog_filter, {_, #{upgrade := info}}}],
         formatter := {rabbit_logger_text_fmt, _},
         config := #{type := file,
                     file := UpgradeFile}},
       UpgradeFileHandler),

    ?assert(ping_log(Id, info, Config)),
    ?assert(ping_log(Id, info,
                     #{domain => ?RMQLOG_DOMAIN_GLOBAL}, Config)),
    ?assert(ping_log(Id, info,
                     #{domain => ['3rd_party']}, Config)),
    ?assertNot(ping_log(Id, info,
                        #{domain => ?RMQLOG_DOMAIN_UPGRADE}, Config)),

    ?assert(ping_log(rmq_1_file_1, info,
                     #{domain => ?RMQLOG_DOMAIN_UPGRADE})),
    ?assertNot(ping_log(rmq_1_file_1, info,
                        #{domain => ?RMQLOG_DOMAIN_GLOBAL})),
    ok.

logging_to_exchange_works(Config) ->
    Context = rabbit_ct_broker_helpers:rpc(
                Config, 0,
                rabbit_prelaunch, get_context, []),
    Handlers = rabbit_ct_broker_helpers:rpc(
                 Config, 0,
                 logger, get_handler_config, []),

    ExchangeHandler = get_handler_by_id(Handlers, rmq_1_exchange),
    ?assertNotEqual(undefined, ExchangeHandler),
    ?assertMatch(
       #{level := info,
         module := rabbit_logger_exchange_h,
         filter_default := log,
         filters := [{progress_reports, {_, stop}},
                     {rmqlog_filter, {_, #{global := info,
                                           upgrade := none}}}],
         formatter := {rabbit_logger_text_fmt, _},
         config := #{exchange := _}},
       ExchangeHandler),
    #{config :=
      #{exchange := #resource{name = XName} = Exchange}} = ExchangeHandler,

    UpgradeFileHandler = get_handler_by_id(Handlers, rmq_1_file_2),
    UpgradeFile = upgrade_log_file_in_context(Context),
    ?assertNotEqual(undefined, UpgradeFileHandler),
    ?assertMatch(
       #{level := info,
         module := rabbit_logger_std_h,
         filter_default := stop,
         filters := [{rmqlog_filter, {_, #{upgrade := info}}}],
         formatter := {rabbit_logger_text_fmt, _},
         config := #{type := file,
                     file := UpgradeFile}},
       UpgradeFileHandler),

    %% Wait for the expected exchange to be automatically declared.
    lists:any(
      fun(_) ->
              Ret = rabbit_ct_broker_helpers:rpc(
                      Config, 0,
                      rabbit_exchange, lookup, [Exchange]),
              case Ret of
                  {ok, _} -> true;
                  _       -> timer:sleep(500),
                             false
              end
      end, lists:seq(1, 20)),

    %% Declare a queue to collect all logged messages.
    {Conn, Chan} = rabbit_ct_client_helpers:open_connection_and_channel(
                     Config),
    QName = <<"log-messages">>,
    ?assertMatch(
       #'queue.declare_ok'{},
       amqp_channel:call(Chan, #'queue.declare'{queue = QName,
                                                durable = false})),
    ?assertMatch(
       #'queue.bind_ok'{},
       amqp_channel:call(Chan, #'queue.bind'{queue = QName,
                                             exchange = XName,
                                             routing_key = <<"#">>})),
    Config1 = rabbit_ct_helpers:set_config(
                Config, {test_channel_and_queue, {Chan, QName}}),

    ?assert(ping_log(rmq_1_exchange, info, Config1)),
    ?assert(ping_log(rmq_1_exchange, info,
                     #{domain => ?RMQLOG_DOMAIN_GLOBAL}, Config1)),
    ?assert(ping_log(rmq_1_exchange, info,
                     #{domain => ['3rd_party']}, Config1)),
    ?assertNot(ping_log(rmq_1_exchange, info,
                        #{domain => ?RMQLOG_DOMAIN_UPGRADE}, Config1)),

    ?assert(ping_log(rmq_1_file_2, info,
                     #{domain => ?RMQLOG_DOMAIN_UPGRADE}, Config)),
    ?assertNot(ping_log(rmq_1_file_2, info,
                        #{domain => ?RMQLOG_DOMAIN_GLOBAL}, Config)),

    amqp_channel:call(Chan, #'queue.delete'{queue = QName}),
    rabbit_ct_client_helpers:close_connection_and_channel(Conn, Chan),
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
                                           prelaunch := PrelaunchLevel,
                                           upgrade := none}}}],
         formatter := {rabbit_logger_text_fmt, _},
         config := #{type := file,
                     file := MainFile}},
       MainFileHandler),

    UpgradeFileHandler = get_handler_by_id(Handlers, rmq_1_file_2),
    UpgradeFile = upgrade_log_file_in_context(Context),
    ?assertNotEqual(undefined, UpgradeFileHandler),
    ?assertMatch(
       #{level := info,
         module := rabbit_logger_std_h,
         filter_default := stop,
         filters := [{rmqlog_filter, {_, #{upgrade := info}}}],
         formatter := {rabbit_logger_text_fmt, _},
         config := #{type := file,
                     file := UpgradeFile}},
       UpgradeFileHandler),

    ?assertNot(ping_log(rmq_1_file_1, info)),
    ?assertNot(ping_log(rmq_1_file_1, info,
                        #{domain => ?RMQLOG_DOMAIN_GLOBAL})),
    ?assertNot(ping_log(rmq_1_file_1, info,
                        #{domain => ?RMQLOG_DOMAIN_PRELAUNCH})),
    ?assertNot(ping_log(rmq_1_file_1, GlobalLevel,
                        #{domain => ?RMQLOG_DOMAIN_PRELAUNCH})),
    ?assertNot(ping_log(rmq_1_file_1, info,
                        #{domain => ['3rd_party']})),
    ?assertNot(ping_log(rmq_1_file_1, info,
                        #{domain => ?RMQLOG_DOMAIN_UPGRADE})),

    ?assert(ping_log(rmq_1_file_1, GlobalLevel)),
    ?assert(ping_log(rmq_1_file_1, GlobalLevel,
                     #{domain => ?RMQLOG_DOMAIN_GLOBAL})),
    ?assert(ping_log(rmq_1_file_1, PrelaunchLevel,
                     #{domain => ?RMQLOG_DOMAIN_PRELAUNCH})),
    ?assert(ping_log(rmq_1_file_1, GlobalLevel,
                     #{domain => ['3rd_party']})),
    ?assertNot(ping_log(rmq_1_file_1, GlobalLevel,
                        #{domain => ?RMQLOG_DOMAIN_UPGRADE})),

    ?assert(ping_log(rmq_1_file_2, GlobalLevel,
                     #{domain => ?RMQLOG_DOMAIN_UPGRADE})),
    ?assertNot(ping_log(rmq_1_file_2, GlobalLevel,
                        #{domain => ?RMQLOG_DOMAIN_GLOBAL})),
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
                                           prelaunch := PrelaunchLevel,
                                           upgrade := none}}}],
         formatter := {rabbit_logger_text_fmt, _},
         config := #{type := file,
                     file := MainFile}},
       MainFileHandler),

    UpgradeFileHandler = get_handler_by_id(Handlers, rmq_1_file_2),
    UpgradeFile = upgrade_log_file_in_context(Context),
    ?assertNotEqual(undefined, UpgradeFileHandler),
    ?assertMatch(
       #{level := info,
         module := rabbit_logger_std_h,
         filter_default := stop,
         filters := [{rmqlog_filter, {_, #{upgrade := info}}}],
         formatter := {rabbit_logger_text_fmt, _},
         config := #{type := file,
                     file := UpgradeFile}},
       UpgradeFileHandler),

    ?assertNot(ping_log(rmq_1_file_1, info)),
    ?assertNot(ping_log(rmq_1_file_1, info,
                        #{domain => ?RMQLOG_DOMAIN_GLOBAL})),
    ?assertNot(ping_log(rmq_1_file_1, info,
                        #{domain => ?RMQLOG_DOMAIN_PRELAUNCH})),
    ?assertNot(ping_log(rmq_1_file_1, GlobalLevel,
                        #{domain => ?RMQLOG_DOMAIN_PRELAUNCH})),
    ?assertNot(ping_log(rmq_1_file_1, info,
                        #{domain => ['3rd_party']})),
    ?assertNot(ping_log(rmq_1_file_1, info,
                        #{domain => ?RMQLOG_DOMAIN_UPGRADE})),

    ?assert(ping_log(rmq_1_file_1, GlobalLevel)),
    ?assert(ping_log(rmq_1_file_1, GlobalLevel,
                     #{domain => ?RMQLOG_DOMAIN_GLOBAL})),
    ?assert(ping_log(rmq_1_file_1, PrelaunchLevel,
                     #{domain => ?RMQLOG_DOMAIN_PRELAUNCH})),
    ?assert(ping_log(rmq_1_file_1, GlobalLevel,
                     #{domain => ['3rd_party']})),
    ?assertNot(ping_log(rmq_1_file_1, GlobalLevel,
                        #{domain => ?RMQLOG_DOMAIN_UPGRADE})),

    ?assert(ping_log(rmq_1_file_2, GlobalLevel,
                     #{domain => ?RMQLOG_DOMAIN_UPGRADE})),
    ?assertNot(ping_log(rmq_1_file_2, GlobalLevel,
                        #{domain => ?RMQLOG_DOMAIN_GLOBAL})),
    ok.

format_messages_as_json_works(Config) ->
    #{var_origins := Origins0} = Context0 = default_context(Config),
    Context = Context0#{log_levels => #{json => true},
                        var_origins => Origins0#{log_levels => environment}},
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
                     {rmqlog_filter, {_, #{global := info,
                                           upgrade := none}}}],
         formatter := {rabbit_logger_json_fmt, _},
         config := #{type := file,
                     file := MainFile}},
       MainFileHandler),

    ?assertNot(ping_log(rmq_1_file_1, info)),

    RandomMsg = get_random_string(
                  32,
                  "abcdefghijklmnopqrstuvwxyz"
                  "ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
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
    ?LOG_INFO(RandomMsg, Metadata),

    rabbit_logger_std_h:filesync(rmq_1_file_1),
    {ok, Content} = file:read_file(MainFile),
    ReOpts = [{capture, first, binary}, multiline],
    {match, [Line]} = re:run(
                        Content,
                        "^.+\"" ++ RandomMsg ++ "\".+$",
                        ReOpts),
    Term = jsx:decode(Line, [return_maps, {labels, attempt_atom}]),

    RandomMsgBin = list_to_binary(RandomMsg),
    ?assertMatch(#{time := _}, Term),
    ?assertMatch(#{level := <<"info">>}, Term),
    ?assertMatch(#{msg := RandomMsgBin}, Term),

    Meta = maps:get(meta, Term),
    FunBin = list_to_binary(erlang:fun_to_list(maps:get(function, Metadata))),
    PidBin = list_to_binary(erlang:pid_to_list(maps:get(pid, Metadata))),
    PortBin = list_to_binary(erlang:port_to_list(maps:get(port, Metadata))),
    RefBin = list_to_binary(erlang:ref_to_list(maps:get(ref, Metadata))),
    ?assertMatch(#{atom := <<"rabbit">>}, Meta),
    ?assertMatch(#{integer := 1}, Meta),
    ?assertMatch(#{float := 1.42}, Meta),
    ?assertMatch(#{string := <<"string">>}, Meta),
    ?assertMatch(#{list := [<<"s">>, <<"a">>, 3]}, Meta),
    ?assertMatch(#{map := #{key := <<"value">>}}, Meta),
    ?assertMatch(#{function := FunBin}, Meta),
    ?assertMatch(#{pid := PidBin}, Meta),
    ?assertMatch(#{port := PortBin}, Meta),
    ?assertMatch(#{ref := RefBin}, Meta).

%% -------------------------------------------------------------------
%% Internal functions.
%% -------------------------------------------------------------------

default_context(Config) ->
    LogBaseDir = ?config(log_base_dir, Config),
    MainFile = "rabbit.log",
    UpgradeFile = "rabbit_upgrade.log",
    #{log_base_dir => LogBaseDir,
      main_log_file => MainFile,
      upgrade_log_file => UpgradeFile,
      log_levels => undefined,
      var_origins => #{log_base_dir => default,
                       main_log_file => default,
                       upgrade_log_file => default,
                       log_levels => default}}.

main_log_file_in_context(#{log_base_dir := LogBaseDir,
                           main_log_file := MainLogFile}) ->
    filename:join(LogBaseDir, MainLogFile).

upgrade_log_file_in_context(#{log_base_dir := LogBaseDir,
                              upgrade_log_file := UpgradeLogFile}) ->
    filename:join(LogBaseDir, UpgradeLogFile).

get_handler_by_id([#{id := Id} = Handler | _], Id) ->
    Handler;
get_handler_by_id([_ | Rest], Id) ->
    get_handler_by_id(Rest, Id);
get_handler_by_id([], _) ->
    undefined.

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
    ct:log("Logging \"~ts\" at level ~ts (~p)", [RandomMsg, Level, Metadata]),
    case need_rpc(Config) of
        false -> logger:log(Level, RandomMsg, Metadata);
        true  -> rabbit_ct_broker_helpers:rpc(
                   Config, 0,
                   logger, log, [Level, RandomMsg, Metadata])
    end,
    check_log(Id, RandomMsg, Config).

need_rpc(Config) ->
    rabbit_ct_helpers:get_config(
      Config, rmq_nodes_count) =/= undefined.

check_log(Id, RandomMsg, Config) ->
    {ok, Handler} = case need_rpc(Config) of
                        false -> logger:get_handler_config(Id);
                        true  -> rabbit_ct_broker_helpers:rpc(
                                   Config, 0,
                                   logger, get_handler_config, [Id])
                    end,
    check_log1(Handler, RandomMsg, Config).

check_log1(#{id := Id,
             module := rabbit_logger_std_h,
             config := #{type := file,
                         file := Filename}},
           RandomMsg,
           Config) ->
    ok = case need_rpc(Config) of
             false -> rabbit_logger_std_h:filesync(Id);
             true  -> rabbit_ct_broker_helpers:rpc(
                        Config, 0,
                        rabbit_logger_std_h, filesync, [Id])
         end,
    {ok, Content} = file:read_file(Filename),
    ReOpts = [{capture, none}, multiline],
    match =:= re:run(Content, RandomMsg ++ "$", ReOpts);
check_log1(#{module := Mod,
             config := #{type := Stddev}},
           RandomMsg,
           Config)
  when ?IS_STD_H_COMPAT(Mod) andalso ?IS_STDDEV(Stddev) ->
    Filename = html_report_filename(Config),
    ReOpts = [{capture, none}, multiline],
    lists:any(
      fun(_) ->
              {ok, Content} = file:read_file(Filename),
              case re:run(Content, RandomMsg ++ "$", ReOpts) of
                  match -> true;
                  _     -> timer:sleep(500),
                           false
              end
      end, lists:seq(1, 10));
check_log1(#{module := rabbit_logger_exchange_h},
           RandomMsg,
           Config) ->
    {Chan, QName} = ?config(test_channel_and_queue, Config),
    ReOpts = [{capture, none}, multiline],
    lists:any(
      fun(_) ->
              Ret = amqp_channel:call(
                      Chan, #'basic.get'{queue = QName, no_ack = false}),
              case Ret of
                  {#'basic.get_ok'{}, #amqp_msg{payload = Content}} ->
                      case re:run(Content, RandomMsg ++ "$", ReOpts) of
                          match -> true;
                          _     -> timer:sleep(500),
                                   false
                      end;
                  #'basic.get_empty'{} ->
                      timer:sleep(500),
                      false;
                  Other ->
                      io:format(standard_error, "OTHER -> ~p~n", [Other]),
                      timer:sleep(500),
                      false
              end
      end, lists:seq(1, 10)).

get_random_string(Length, AllowedChars) ->
    lists:foldl(fun(_, Acc) ->
                        [lists:nth(rand:uniform(length(AllowedChars)),
                                   AllowedChars)]
                        ++ Acc
                end, [], lists:seq(1, Length)).

html_report_filename(Config) ->
    ?config(tc_logfile, Config).
