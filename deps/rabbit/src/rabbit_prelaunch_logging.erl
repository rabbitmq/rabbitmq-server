%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2019-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @author The RabbitMQ team
%% @copyright 2019-2021 VMware, Inc. or its affiliates.
%%
%% @doc
%% This module manages the configuration of the Erlang Logger facility. In
%% other words, it translates the RabbitMQ logging configuration (in the
%% Cuttlefish format or classic Erlang-term-based configuration) into Erlang
%% Logger handler setups.
%%
%% Configuring the Erlang Logger is done in two steps:
%% <ol>
%% <li>Logger handler configurations are created based on the configuration
%% and the context (see {@link //rabbit_common/rabbit_env}).</li>
%% <li>Created handlers are installed (i.e. they become active). Any handlers
%% previously installed by this module are removed.</li>
%% </ol>
%%
%% It also takes care of setting the `$ERL_CRASH_DUMP' variable to enable
%% Erlang core dumps.
%%
%% Note that before this module handles the Erlang Logger, {@link
%% //rabbitmq_prelaunch/rabbit_prelaunch_early_logging} configures basic
%% logging to have messages logged as soon as possible during RabbitMQ
%% startup.
%%
%% == How to configure RabbitMQ logging ==
%%
%% RabbitMQ supports a main/default logging output and per-category outputs.
%% An output is a combination of a destination (a text file or stdout for
%% example) and a message formatted (e.g. plain text or JSON).
%%
%% Here is the Erlang-term-based configuration expected and supported by this
%% module:
%%
%% ```
%% {rabbit, [
%%   {log_root, string()},
%%   {log, [
%%     {categories, [
%%       {default, [
%%         {level, Level}
%%       ]},
%%       {CategoryName, [
%%         {level, Level},
%%         {file, Filename}
%%       ]}
%%     ]},
%%
%%     {console, [
%%       {level, Level},
%%       {enabled, boolean()}
%%     ]},
%%
%%     {exchange, [
%%       {level, Level},
%%       {enabled, boolean()}
%%     ]},
%%
%%     {file, [
%%       {level, Level},
%%       {file, Filename | false},
%%       {date, RotationDateSpec},
%%       {size, RotationSize},
%%       {count, RotationCount},
%%     ]},
%%
%%     {journald, [
%%       {level, Level},
%%       {enabled, boolean()},
%%       {fields, proplists:proplist()}
%%     ]}
%%
%%     {syslog, [
%%       {level, Level},
%%       {enabled, boolean()}
%%     ]}
%%   ]}
%% ]}.
%%
%% Level = logger:level().
%% Filename = file:filename().
%% RotationDateSpec = string(). % Pattern format used by newsyslog.conf(5).
%% RotationSize = non_neg_integer() | infinity.
%% RotationCount = non_neg_integer().
%% '''
%%
%% See `priv/schema/rabbit.schema' for the definition of the Cuttlefish
%% configuration schema.

-module(rabbit_prelaunch_logging).

-include_lib("kernel/include/logger.hrl").
-include_lib("rabbit_common/include/logging.hrl").

-export([setup/1,
         set_log_level/1,
         log_locations/0]).

-ifdef(TEST).
-export([clear_config_run_number/0,
         get_less_severe_level/2]).
-endif.

-export_type([log_location/0]).

-type log_location() :: file:filename() | string().
%% A short description of an output.
%%
%% If the output is the console, the location is either `"<stdout>"' or
%% `"<stderr>"'.
%%
%% If the output is an exchange, the location is the string `"exchange:"' with
%% the exchange name appended.
%%
%% If the output is a file, the location is the absolute filename.
%%
%% If the output is journald, the location is `"<journald>"'.
%%
%% If the output is syslog, the location is the string `"syslog:"' with the
%% syslog server hostname appended.

-type category_name() :: atom().
%% The name of a log category.
%% Erlang Logger uses the concept of "domain" which is an ordered list of
%% atoms. A category is mapped to the domain `[?RMQLOG_SUPER_DOMAIN_NAME,
%% Category]'. In other words, a category is a subdomain of the `rabbitmq'
%% domain.

-type console_props() :: [{level, logger:level()} |
                          {enabled, boolean()} |
                          {stdio, stdout | stderr} |
                          {formatter, {atom(), term()}}].
%% Console properties are the parameters in the configuration file for a
%% console-based handler.

-type exchange_props() :: [{level, logger:level()} |
                           {enabled, boolean()} |
                           {formatter, {atom(), term()}}].
%% Exchange properties are the parameters in the configuration file for an
%% exchange-based handler.

-type file_props() :: [{level, logger:level()} |
                       {file, file:filename() | false} |
                       {date, string()} |
                       {size, non_neg_integer()} |
                       {count, non_neg_integer()} |
                       {formatter, {atom(), term()}}].
%% File properties are the parameters in the configuration file for a
%% file-based handler.

-type journald_props() :: [{level, logger:level()} |
                           {enabled, boolean()} |
                           {fields, proplists:proplist()}].
%% journald properties are the parameters in the configuration file for a
%% journald-based handler.

-type syslog_props() :: [{level, logger:level()} |
                         {enabled, boolean()} |
                         {formatter, {atom(), term()}}].
%% Syslog properties are the parameters in the configuration file for a
%% syslog-based handler.

-type main_log_env() :: [{console, console_props()} |
                         {exchange, exchange_props()} |
                         {file, file_props()} |
                         {journald, journald_props()} |
                         {syslog, syslog_props()}].
%% The main log environment is the parameters in the configuration file for
%% the main log handler (i.e. where all messages go by default).

-type per_cat_env() :: [{level, logger:level()} |
                        {file, file:filename()}].
%% A per-category log environment is the parameters in the configuration file
%% for a specific category log handler. There can be one per category.

-type default_cat_env() :: [{level, logger:level()}].
%% The `default' category log environment is special (read: awkward) in the
%% configuration file. It is used to change the log level of the main log
%% handler.

-type log_app_env() :: [main_log_env() |
                        {categories, [{default, default_cat_env()} |
                                      {category_name(), per_cat_env()}]}].
%% The value for the `log' key in the `rabbit' application environment.

-type global_log_config() :: #{level => logger:level() | all | none,
                               outputs := [logger:handler_config()]}.
%% This is the internal structure used to prepare the handlers for the
%% main/global messages (i.e. not marked with a specific category).

-type per_cat_log_config() :: global_log_config().
%% This is the internal structure used to prepare the handlers for
%% category-specific messages.

-type log_config() :: #{global := global_log_config(),
                        per_category := #{
                          category_name() => per_cat_log_config()}}.
%% This is the internal structure to store the global and per-category
%% configurations, to prepare the final handlers.

-type handler_key() :: atom().
%% Key used to deduplicate handlers before they are installed in Logger.

-type id_assignment_state() :: #{config_run_number := pos_integer(),
                                 next_file := pos_integer()}.
%% State used while assigning IDs to handlers.

-spec setup(rabbit_env:context()) -> ok.
%% @doc
%% Configures or reconfigures logging.
%%
%% The logging framework is the builtin Erlang Logger API. The configuration
%% is based on the configuration file and the environment.
%%
%% In addition to logging, it sets the `$ERL_CRASH_DUMP' environment variable
%% to enable Erlang crash dumps.
%%
%% @param Context the RabbitMQ context (see {@link
%% //rabbitmq_prelaunch/rabbit_prelaunch:get_context/0}).

setup(Context) ->
    ?LOG_DEBUG("\n== Logging ==",
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ok = compute_config_run_number(),
    ok = set_ERL_CRASH_DUMP_envvar(Context),
    ok = configure_logger(Context).

-spec set_log_level(logger:level()) -> ok | {error, term()}.
%% @doc
%% Changes the log level.
%%
%% The log level is changed for the following layers of filtering:
%% <ul>
%% <li>the primary log level</li>
%% <li>the module log level(s)</li>
%% <li>the handler log level(s)</li>
%% </ul>
%%
%% @param Level the new log level.

set_log_level(Level) ->
    %% Primary log level.
    ?LOG_DEBUG(
       "Logging: changing primary log level to ~s", [Level],
       #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
    logger:set_primary_config(level, Level),

    %% Per-module log level.
    lists:foreach(
      fun({Module, _}) ->
              ?LOG_DEBUG(
                 "Logging: changing '~s' module log level to ~s",
                 [Module, Level],
                 #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
              _ = logger:set_module_level(Module, Level)
      end, logger:get_module_level()),

    %% Per-handler log level.
    %% In the handler, we have the "top-level" log level for that handler, plus
    %% per-domain (per-category) levels inside a filter. We need to change all
    %% of them to the new level.
    lists:foreach(
      fun
          (#{id := Id, filters := Filters}) ->
              ?LOG_DEBUG(
                 "Logging: changing '~s' handler log level to ~s",
                 [Id, Level],
                 #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
              Filters1 = lists:map(
                           fun
                               %% We only modify the filter we know about.
                               %% The argument to that filter is of the form:
                               %%     #{CatName => Level}
                               %% (where CatName if an atom())
                               %%
                               %% We don't change the level for a category if
                               %% it is set to 'none' because it means the
                               %% category is filtered out by this filter.
                               ({?FILTER_NAME, {Fun, Arg}}) when is_map(Arg) ->
                                   Arg1 = maps:map(
                                            fun
                                                (_, none) -> none;
                                                (_, _)    -> Level
                                            end, Arg),
                                   {?FILTER_NAME, {Fun, Arg1}};
                               %% We also change what we do with Erlang
                               %% progress reports.
                               ({progress_reports, {Fun, _}}) ->
                                   Action = case Level of
                                                debug -> log;
                                                _     -> stop
                                            end,
                                   {progress_reports, {Fun, Action}};
                               %% Other filters are left untouched.
                               (Filter) ->
                                   Filter
                           end, Filters),
<<<<<<< HEAD
=======
              %% If the log level is set to `debug', we turn off burst limit to
              %% make sure all debug messages make it.
              Config1 = adjust_burst_limit(Config, Level),
>>>>>>> d0b7a33a0f (Logging: Add comments explaining when burst limit is disabled)
              logger:set_handler_config(Id, filters, Filters1),
              logger:set_handler_config(Id, level, Level),
              ok;
          (#{id := Id}) ->
              ?LOG_DEBUG(
                 "Logging: changing '~s' handler log level to ~s",
                 [Id, Level],
                 #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
<<<<<<< HEAD
=======
              %% If the log level is set to `debug', we turn off burst limit to
              %% make sure all debug messages make it.
              Config1 = adjust_burst_limit(Config, Level),
              logger:set_handler_config(Id, config, Config1),
>>>>>>> d0b7a33a0f (Logging: Add comments explaining when burst limit is disabled)
              logger:set_handler_config(Id, level, Level),
              ok
      end, logger:get_handler_config()),
    ok.

-spec log_locations() -> [file:filename() | string()].
%% @doc
%% Returns the list of output locations.
%%
%% For file-based handlers, the absolute filename is returned.
%%
%% For console-based handlers, a string literal is returned; either
%% `"<stdout>"' or `"<stderr>"'.
%%
%% For exchange-based handlers, a string of the form `"exchange:Exchange"' is
%% returned, where `Exchange' is the name of the exchange.
%%
%% For journald-based handlers, a string literal is returned; `"<journald>"'.
%%
%% For syslog-based handlers, a string of the form `"syslog:Hostname"' is
%% returned, where `Hostname' is either the hostname of the remote syslog
%% server, or an empty string if none were configured (which means log to
%% localhost).
%%
%% @returns the list of output locations.
%%
%% @see log_location()

log_locations() ->
    Handlers = logger:get_handler_config(),
    log_locations(Handlers, []).

log_locations([#{module := Mod,
                 config := #{type := file,
                             file := Filename}} | Rest],
              Locations)
  when ?IS_STD_H_COMPAT(Mod) ->
    Locations1 = add_once(Locations, Filename),
    log_locations(Rest, Locations1);
log_locations([#{module := Mod,
                 config := #{type := standard_io}} | Rest],
              Locations)
  when ?IS_STD_H_COMPAT(Mod) ->
    Locations1 = add_once(Locations, "<stdout>"),
    log_locations(Rest, Locations1);
log_locations([#{module := Mod,
                 config := #{type := standard_error}} | Rest],
              Locations)
  when ?IS_STD_H_COMPAT(Mod) ->
    Locations1 = add_once(Locations, "<stderr>"),
    log_locations(Rest, Locations1);
log_locations([#{module := systemd_journal_h} | Rest],
              Locations) ->
    Locations1 = add_once(Locations, "<journald>"),
    log_locations(Rest, Locations1);
log_locations([#{module := syslog_logger_h} | Rest],
              Locations) ->
    Host = application:get_env(syslog, dest_host, ""),
    Locations1 = add_once(
                   Locations,
                   rabbit_misc:format("syslog:~s", [Host])),
    log_locations(Rest, Locations1);
log_locations([#{module := rabbit_logger_exchange_h,
                 config := #{exchange := Exchange}} | Rest],
              Locations) ->
    Locations1 = add_once(
                   Locations,
                   rabbit_misc:format("exchange:~p", [Exchange])),
    log_locations(Rest, Locations1);
log_locations([_ | Rest], Locations) ->
    log_locations(Rest, Locations);
log_locations([], Locations) ->
    lists:sort(Locations).

add_once(Locations, Location) ->
    case lists:member(Location, Locations) of
        false -> [Location | Locations];
        true  -> Locations
    end.

%% -------------------------------------------------------------------
%% ERL_CRASH_DUMP setting.
%% -------------------------------------------------------------------

-spec set_ERL_CRASH_DUMP_envvar(rabbit_env:context()) -> ok.

set_ERL_CRASH_DUMP_envvar(Context) ->
    case os:getenv("ERL_CRASH_DUMP") of
        false ->
            LogBaseDir = get_log_base_dir(Context),
            ErlCrashDump = filename:join(LogBaseDir, "erl_crash.dump"),
            ?LOG_DEBUG(
              "Setting $ERL_CRASH_DUMP environment variable to \"~ts\"",
              [ErlCrashDump],
              #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            os:putenv("ERL_CRASH_DUMP", ErlCrashDump),
            ok;
        ErlCrashDump ->
            ?LOG_DEBUG(
              "$ERL_CRASH_DUMP environment variable already set to \"~ts\"",
              [ErlCrashDump],
              #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            ok
    end.

-spec get_log_base_dir(rabbit_env:context()) -> file:filename().
%% @doc
%% Returns the log base directory.
%%
%% The precedence is:
%% <ol>
%% <li>the $RABBITMQ_LOG_BASE variable if overriden in the environment</li>
%% <li>the value of `log_root' in the application environment</li>
%% <li>the default value</li>
%% </ol>
%%
%% @param Context the RabbitMQ context (see {@link
%% //rabbitmq_prelaunch/rabbit_prelaunch:get_context/0}).
%% @returns an absolute path to a directory.

get_log_base_dir(#{log_base_dir := LogBaseDirFromEnv} = Context) ->
    case rabbit_env:has_var_been_overridden(Context, log_base_dir) of
        false -> application:get_env(rabbit, log_root, LogBaseDirFromEnv);
        true  -> LogBaseDirFromEnv
    end.

%% -------------------------------------------------------------------
%% Logger's handlers configuration.
%% -------------------------------------------------------------------

-define(CONFIG_RUN_NUMBER_KEY, {?MODULE, config_run_number}).

-spec compute_config_run_number() -> ok.
%% @doc
%% Compute the next configuration run number.
%%
%% We use a "configuration run number" to distinguish previously installed
%% handlers (if any) from the ones we want to install in a subsequent call of
%% {@link setup/1}.
%%
%% The configuration run number appears in the generated IDs for handlers.
%% This is how we can filter old ones.
%%
%% @returns an positive integer.

compute_config_run_number() ->
    RunNum = persistent_term:get(?CONFIG_RUN_NUMBER_KEY, 0),
    ok = persistent_term:put(?CONFIG_RUN_NUMBER_KEY, RunNum + 1).

-spec get_config_run_number() -> pos_integer().

get_config_run_number() ->
    persistent_term:get(?CONFIG_RUN_NUMBER_KEY).

-ifdef(TEST).
-spec clear_config_run_number() -> ok.
%% @doc
%% Clears the recorded configuration run number.
%%
%% In testsuites, we want to be able to reset that number to 1 because
%% testcases have expectations on handler IDs.

clear_config_run_number() ->
    _ = persistent_term:erase(?CONFIG_RUN_NUMBER_KEY),
    ok.
-endif.

-spec configure_logger(rabbit_env:context()) -> ok.

configure_logger(Context) ->
    %% Configure main handlers.
    %% We distinguish them by their type and possibly other
    %% parameters (file name, syslog settings, etc.).
    LogConfig0 = get_log_configuration_from_app_env(),
    LogConfig1 = handle_default_and_overridden_outputs(LogConfig0, Context),
    LogConfig2 = apply_log_levels_from_env(LogConfig1, Context),
    LogConfig3 = make_filenames_absolute(LogConfig2, Context),
    LogConfig4 = configure_formatters(LogConfig3, Context),

    %% At this point, the log configuration is complete: we know the global
    %% parameters as well as the per-category settings.
    %%
    %% Now, we turn that into a map of handlers. We use a map to deduplicate
    %% handlers. For instance, if the same file is used for global logging and
    %% a specific category.
    %%
    %% The map is then converted to a list, once the deduplication is done and
    %% IDs are assigned to handlers.
    Handlers = create_logger_handlers_conf(LogConfig4),
    ?LOG_DEBUG(
       "Logging: logger handlers:~n  ~p", [Handlers],
       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),

    %% We can now install the new handlers. The function takes care of
    %% removing previously configured handlers (after installing the new
    %% ones to ensure we don't loose a message).
    ok = install_handlers(Handlers),

    %% Let's log a message per log level (if debug logging is enabled). This
    %% is handy if the user wants to verify the configuration is what he
    %% expects.
    ok = maybe_log_test_messages(LogConfig3).

-spec get_log_configuration_from_app_env() -> log_config().

get_log_configuration_from_app_env() ->
    %% The log configuration in the Cuttlefish configuration file or the
    %% application environment is not structured logically. This functions is
    %% responsible for extracting the configuration and organize it. If one day
    %% we decide to fix the configuration structure, we just have to modify
    %% this function and normalize_*().
    Env = get_log_app_env(),
    EnvWithoutCats = proplists:delete(categories, Env),
    DefaultAndCatProps = proplists:get_value(categories, Env, []),
    DefaultProps = proplists:get_value(default, DefaultAndCatProps, []),
    CatProps = proplists:delete(default, DefaultAndCatProps),

    %% This "normalization" turns the RabbitMQ-specific configuration into a
    %% structure which stores Logger handler configurations. That structure is
    %% later modified to reach the final handler configurations.
    PerCatConfig = maps:from_list(
                     [{Cat, normalize_per_cat_log_config(Props)}
                      || {Cat, Props} <- CatProps]),
    GlobalConfig = normalize_main_log_config(EnvWithoutCats, DefaultProps),
    #{global => GlobalConfig,
      per_category => PerCatConfig}.

-spec get_log_app_env() -> log_app_env().

get_log_app_env() ->
    application:get_env(rabbit, log, []).

-spec normalize_main_log_config(main_log_env(), default_cat_env()) ->
    global_log_config().

normalize_main_log_config(Props, DefaultProps) ->
    Outputs = case proplists:get_value(level, DefaultProps) of
                  undefined -> #{outputs => []};
                  Level     -> #{outputs => [],
                                 level => Level}
              end,
    Props1 = compute_implicitly_enabled_output(Props),
    normalize_main_log_config1(Props1, Outputs).

compute_implicitly_enabled_output(Props) ->
    {ConsoleEnabled, Props1} = compute_implicitly_enabled_output(
                                 console, Props),
    {ExchangeEnabled, Props2} = compute_implicitly_enabled_output(
                                  exchange, Props1),
    {JournaldEnabled, Props3} = compute_implicitly_enabled_output(
                                  journald, Props2),
    {SyslogEnabled, Props4} = compute_implicitly_enabled_output(
                                syslog, Props3),
    FileDisabledByDefault =
    ConsoleEnabled orelse
    ExchangeEnabled orelse
    JournaldEnabled orelse
    SyslogEnabled,

    FileProps = proplists:get_value(file, Props4, []),
    case is_output_explicitely_enabled(FileProps) of
        true ->
            Props4;
        false ->
            case FileDisabledByDefault of
                true ->
                    FileProps1 = lists:keystore(
                                   file, 1, FileProps, {file, false}),
                    lists:keystore(
                      file, 1, Props4, {file, FileProps1});
                false ->
                    Props4
            end
    end.

compute_implicitly_enabled_output(PropName, Props) ->
    SubProps = proplists:get_value(PropName, Props, []),
    {Enabled, SubProps1} = compute_implicitly_enabled_output1(SubProps),
    {Enabled,
     lists:keystore(PropName, 1, Props, {PropName, SubProps1})}.

compute_implicitly_enabled_output1(SubProps) ->
    %% We consider the output enabled or disabled if:
    %%     * it is explicitely marked as such, or
    %%     * the level is set to a log level (enabled) or `none' (disabled)
    Enabled = proplists:get_value(
                enabled, SubProps,
                proplists:get_value(level, SubProps, none) =/= none),
    {Enabled,
     lists:keystore(enabled, 1, SubProps, {enabled, Enabled})}.

is_output_explicitely_enabled(FileProps) ->
    %% We consider the output enabled or disabled if:
    %%     * the file is explicitely set, or
    %%     * the level is set to a log level (enabled) or `none' (disabled)
    File = proplists:get_value(file, FileProps),
    Level = proplists:get_value(level, FileProps),
    is_list(File) orelse (Level =/= undefined andalso Level =/= none).

normalize_main_log_config1([{Type, Props} | Rest],
                           #{outputs := Outputs} = LogConfig) ->
    Outputs1 = normalize_main_output(Type, Props, Outputs),
    LogConfig1 = LogConfig#{outputs => Outputs1},
    normalize_main_log_config1(Rest, LogConfig1);
normalize_main_log_config1([], LogConfig) ->
    LogConfig.

-spec normalize_main_output
(console, console_props(), [logger:handler_config()]) ->
    [logger:handler_config()];
(exchange, exchange_props(), [logger:handler_config()]) ->
    [logger:handler_config()];
(file, file_props(), [logger:handler_config()]) ->
    [logger:handler_config()];
(journald, journald_props(), [logger:handler_config()]) ->
    [logger:handler_config()];
(syslog, syslog_props(), [logger:handler_config()]) ->
    [logger:handler_config()].

normalize_main_output(console, Props, Outputs) ->
    normalize_main_console_output(
      Props,
      #{module => rabbit_logger_std_h,
        config => #{type => standard_io}},
      Outputs);
normalize_main_output(exchange, Props, Outputs) ->
    normalize_main_exchange_output(
      Props,
      #{module => rabbit_logger_exchange_h,
        config => #{}},
      Outputs);
normalize_main_output(file, Props, Outputs) ->
    normalize_main_file_output(
      Props,
      #{module => rabbit_logger_std_h,
        config => #{type => file}},
      Outputs);
normalize_main_output(journald, Props, Outputs) ->
    normalize_main_journald_output(
      Props,
      #{module => systemd_journal_h,
        config => #{}},
      Outputs);
normalize_main_output(syslog, Props, Outputs) ->
    normalize_main_syslog_output(
      Props,
      #{module => syslog_logger_h,
        config => #{}},
      Outputs).

-spec normalize_main_file_output(file_props(), logger:handler_config(),
                                 [logger:handler_config()]) ->
    [logger:handler_config()].

normalize_main_file_output(Props, Output, Outputs) ->
    Enabled = case proplists:get_value(file, Props) of
                  false     -> false;
                  _         -> true
              end,
    case Enabled of
        true  -> normalize_main_file_output1(Props, Output, Outputs);
        false -> remove_main_file_output(Outputs)
    end.

normalize_main_file_output1(
  [{file, Filename} | Rest],
  #{config := Config} = Output, Outputs) ->
    Output1 = Output#{config => Config#{file => Filename}},
    normalize_main_file_output1(Rest, Output1, Outputs);
normalize_main_file_output1(
  [{level, Level} | Rest],
  Output, Outputs) ->
    Output1 = Output#{level => Level},
    normalize_main_file_output1(Rest, Output1, Outputs);
normalize_main_file_output1(
  [{date, DateSpec} | Rest],
  #{config := Config} = Output, Outputs) ->
    Output1 = Output#{config => Config#{rotate_on_date => DateSpec}},
    normalize_main_file_output1(Rest, Output1, Outputs);
normalize_main_file_output1(
  [{size, Size} | Rest],
  #{config := Config} = Output, Outputs) ->
    Output1 = Output#{config => Config#{max_no_bytes => Size}},
    normalize_main_file_output1(Rest, Output1, Outputs);
normalize_main_file_output1(
  [{count, Count} | Rest],
  #{config := Config} = Output, Outputs) ->
    Output1 = Output#{config => Config#{max_no_files => Count}},
    normalize_main_file_output1(Rest, Output1, Outputs);
normalize_main_file_output1(
  [{formatter, undefined} | Rest],
  Output, Outputs) ->
    normalize_main_file_output1(Rest, Output, Outputs);
normalize_main_file_output1(
  [{formatter, Formatter} | Rest],
  Output, Outputs) ->
    Output1 = Output#{formatter => Formatter},
    normalize_main_file_output1(Rest, Output1, Outputs);
normalize_main_file_output1([], Output, Outputs) ->
    [Output | Outputs].

remove_main_file_output(Outputs) ->
    lists:filter(
      fun
          (#{module := rabbit_logger_std_h,
             config := #{type := file}}) -> false;
          (_)                            -> true
      end, Outputs).

-spec normalize_main_console_output(console_props(), logger:handler_config(),
                                    [logger:handler_config()]) ->
    [logger:handler_config()].

normalize_main_console_output(Props, Output, Outputs) ->
    Enabled = proplists:get_value(enabled, Props),
    case Enabled of
        true  -> normalize_main_console_output1(Props, Output, Outputs);
        false -> remove_main_console_output(Output, Outputs)
    end.

normalize_main_console_output1(
  [{enabled, true} | Rest],
  Output, Outputs) ->
    normalize_main_console_output1(Rest, Output, Outputs);
normalize_main_console_output1(
  [{level, Level} | Rest],
  Output, Outputs) ->
    Output1 = Output#{level => Level},
    normalize_main_console_output1(Rest, Output1, Outputs);
normalize_main_console_output1(
  [{stdio, stdout} | Rest],
  #{config := Config} = Output, Outputs) ->
    Config1 = Config#{type => standard_io},
    Output1 = Output#{config => Config1},
    normalize_main_console_output1(Rest, Output1, Outputs);
normalize_main_console_output1(
  [{stdio, stderr} | Rest],
  #{config := Config} = Output, Outputs) ->
    Config1 = Config#{type => standard_error},
    Output1 = Output#{config => Config1},
    normalize_main_console_output1(Rest, Output1, Outputs);
normalize_main_console_output1(
  [{formatter, undefined} | Rest],
  Output, Outputs) ->
    normalize_main_console_output1(Rest, Output, Outputs);
normalize_main_console_output1(
  [{formatter, Formatter} | Rest],
  Output, Outputs) ->
    Output1 = Output#{formatter => Formatter},
    normalize_main_console_output1(Rest, Output1, Outputs);
normalize_main_console_output1([], Output, Outputs) ->
    [Output | Outputs].

remove_main_console_output(
  #{module := Mod1, config := #{type := Stddev}},
  Outputs)
  when ?IS_STD_H_COMPAT(Mod1) andalso
       ?IS_STDDEV(Stddev) ->
    lists:filter(
      fun
          (#{module := Mod2,
             config := #{type := standard_io}})
            when ?IS_STD_H_COMPAT(Mod2) ->
              false;
          (#{module := Mod2,
             config := #{type := standard_error}})
            when ?IS_STD_H_COMPAT(Mod2) ->
              false;
          (_) ->
              true
      end, Outputs).

-spec normalize_main_exchange_output(
        exchange_props(), logger:handler_config(),
        [logger:handler_config()]) ->
    [logger:handler_config()].

normalize_main_exchange_output(Props, Output, Outputs) ->
    Enabled = proplists:get_value(enabled, Props),
    case Enabled of
        true  -> normalize_main_exchange_output1(Props, Output, Outputs);
        false -> remove_main_exchange_output(Output, Outputs)
    end.

normalize_main_exchange_output1(
  [{enabled, true} | Rest],
  Output, Outputs) ->
    normalize_main_exchange_output1(Rest, Output, Outputs);
normalize_main_exchange_output1(
  [{level, Level} | Rest],
  Output, Outputs) ->
    Output1 = Output#{level => Level},
    normalize_main_exchange_output1(Rest, Output1, Outputs);
normalize_main_exchange_output1(
  [{formatter, undefined} | Rest],
  Output, Outputs) ->
    normalize_main_exchange_output1(Rest, Output, Outputs);
normalize_main_exchange_output1(
  [{formatter, Formatter} | Rest],
  Output, Outputs) ->
    Output1 = Output#{formatter => Formatter},
    normalize_main_exchange_output1(Rest, Output1, Outputs);
normalize_main_exchange_output1([], Output, Outputs) ->
    [Output | Outputs].

remove_main_exchange_output(
  #{module := rabbit_logger_exchange_h}, Outputs) ->
    lists:filter(
      fun
          (#{module := rabbit_logger_exchange_h}) -> false;
          (_)                                     -> true
      end, Outputs).

-spec normalize_main_journald_output(journald_props(), logger:handler_config(),
                                     [logger:handler_config()]) ->
    [logger:handler_config()].

normalize_main_journald_output(Props, Output, Outputs) ->
    Enabled = proplists:get_value(enabled, Props),
    case Enabled of
        true  -> normalize_main_journald_output1(Props, Output, Outputs);
        false -> remove_main_journald_output(Output, Outputs)
    end.

normalize_main_journald_output1(
  [{enabled, true} | Rest],
  Output, Outputs) ->
    normalize_main_journald_output1(Rest, Output, Outputs);
normalize_main_journald_output1(
  [{level, Level} | Rest],
  Output, Outputs) ->
    Output1 = Output#{level => Level},
    normalize_main_journald_output1(Rest, Output1, Outputs);
normalize_main_journald_output1(
  [{fields, FieldMapping} | Rest],
  #{config := Config} = Output, Outputs) ->
    Config1 = Config#{fields => FieldMapping},
    Output1 = Output#{config => Config1},
    normalize_main_journald_output1(Rest, Output1, Outputs);
normalize_main_journald_output1(
  [{formatter, undefined} | Rest],
  Output, Outputs) ->
    normalize_main_journald_output1(Rest, Output, Outputs);
normalize_main_journald_output1(
  [{formatter, Formatter} | Rest],
  Output, Outputs) ->
    Output1 = Output#{formatter => Formatter},
    normalize_main_journald_output1(Rest, Output1, Outputs);
normalize_main_journald_output1([], Output, Outputs) ->
    [Output | Outputs].

remove_main_journald_output(
  #{module := systemd_journal_h},
  Outputs) ->
    lists:filter(
      fun
          (#{module := systemd_journal_h}) -> false;
          (_)                              -> true
      end, Outputs).

-spec normalize_main_syslog_output(
        syslog_props(), logger:handler_config(),
        [logger:handler_config()]) ->
    [logger:handler_config()].

normalize_main_syslog_output(Props, Output, Outputs) ->
    Enabled = proplists:get_value(enabled, Props),
    case Enabled of
        true  -> normalize_main_syslog_output1(Props, Output, Outputs);
        false -> remove_main_syslog_output(Output, Outputs)
    end.

normalize_main_syslog_output1(
  [{enabled, true} | Rest],
  Output, Outputs) ->
    normalize_main_syslog_output1(Rest, Output, Outputs);
normalize_main_syslog_output1(
  [{level, Level} | Rest],
  Output, Outputs) ->
    Output1 = Output#{level => Level},
    normalize_main_syslog_output1(Rest, Output1, Outputs);
normalize_main_syslog_output1(
  [{formatter, undefined} | Rest],
  Output, Outputs) ->
    normalize_main_syslog_output1(Rest, Output, Outputs);
normalize_main_syslog_output1(
  [{formatter, Formatter} | Rest],
  Output, Outputs) ->
    Output1 = Output#{formatter => Formatter},
    normalize_main_syslog_output1(Rest, Output1, Outputs);
normalize_main_syslog_output1([], Output, Outputs) ->
    [Output | Outputs].

remove_main_syslog_output(
  #{module := syslog_logger_h}, Outputs) ->
    lists:filter(
      fun
          (#{module := syslog_logger_h}) -> false;
          (_)                            -> true
      end, Outputs).

-spec normalize_per_cat_log_config(per_cat_env()) -> per_cat_log_config().

normalize_per_cat_log_config(Props) ->
    normalize_per_cat_log_config(Props, #{outputs => []}).

normalize_per_cat_log_config([{level, Level} | Rest], LogConfig) ->
    LogConfig1 = LogConfig#{level => Level},
    normalize_per_cat_log_config(Rest, LogConfig1);
normalize_per_cat_log_config([{file, Filename} | Rest],
                             #{outputs := Outputs} = LogConfig) ->
    %% Caution: The `file' property in the per-category configuration only
    %% accepts a filename. It doesn't support the properties of the `file'
    %% property at the global configuration level.
    Output = #{module => rabbit_logger_std_h,
               config => #{type => file,
                           file => Filename}},
    LogConfig1 = LogConfig#{outputs => [Output | Outputs]},
    normalize_per_cat_log_config(Rest, LogConfig1);
normalize_per_cat_log_config([], LogConfig) ->
    LogConfig.

-spec handle_default_and_overridden_outputs(log_config(),
                                            rabbit_env:context()) ->
    log_config().

handle_default_and_overridden_outputs(LogConfig, Context) ->
    LogConfig1 = handle_default_main_output(LogConfig, Context),
    LogConfig2 = handle_default_upgrade_cat_output(LogConfig1, Context),
    LogConfig2.

-spec handle_default_main_output(log_config(), rabbit_env:context()) ->
    log_config().

handle_default_main_output(
  #{global := #{outputs := Outputs} = GlobalConfig} = LogConfig,
  #{main_log_file := MainLogFile} = Context) ->
    NoOutputsConfigured = Outputs =:= [],
    Overridden = rabbit_env:has_var_been_overridden(Context, main_log_file),
    Outputs1 = if
                   NoOutputsConfigured orelse Overridden ->
                       Output0 = log_file_var_to_output(MainLogFile),
                       Output1 = keep_log_level_from_equivalent_output(
                                   Output0, Outputs),
                       [Output1];
                   true ->
                       [case Output of
                            #{module := Mod,
                              config := #{type := file, file := _}}
                              when ?IS_STD_H_COMPAT(Mod) ->
                                Output;
                            #{module := Mod,
                              config := #{type := file} = Config}
                              when ?IS_STD_H_COMPAT(Mod) ->
                                Output#{config =>
                                        Config#{file => MainLogFile}};
                            _ ->
                                Output
                        end || Output <- Outputs]
               end,
    case Outputs1 of
        Outputs -> LogConfig;
        _       -> LogConfig#{
                     global => GlobalConfig#{
                                 outputs => Outputs1}}
    end.

-spec handle_default_upgrade_cat_output(log_config(), rabbit_env:context()) ->
    log_config().

handle_default_upgrade_cat_output(
  #{per_category := PerCatConfig} = LogConfig,
  #{upgrade_log_file := UpgLogFile} = Context) ->
    UpgCatConfig = case PerCatConfig of
                       #{upgrade := CatConfig} -> CatConfig;
                       _                       -> #{outputs => []}
                   end,
    #{outputs := Outputs} = UpgCatConfig,
    NoOutputsConfigured = Outputs =:= [],
    Overridden = rabbit_env:has_var_been_overridden(
                   Context, upgrade_log_file),
    Outputs1 = if
                   NoOutputsConfigured orelse Overridden ->
                       Output0 = log_file_var_to_output(UpgLogFile),
                       Output1 = keep_log_level_from_equivalent_output(
                                   Output0, Outputs),
                       [Output1];
                   true ->
                      Outputs
               end,
    case Outputs1 of
        Outputs -> LogConfig;
        _       -> LogConfig#{
                     per_category => PerCatConfig#{
                                       upgrade => UpgCatConfig#{
                                                    outputs => Outputs1}}}
    end.

-spec log_file_var_to_output(file:filename() | string()) ->
    logger:handler_config().

log_file_var_to_output("-") ->
    #{module => rabbit_logger_std_h,
      config => #{type => standard_io}};
log_file_var_to_output("-stderr") ->
    #{module => rabbit_logger_std_h,
      config => #{type => standard_error}};
log_file_var_to_output("exchange:" ++ _) ->
    #{module => rabbit_logger_exchange_h,
      config => #{}};
log_file_var_to_output("journald:" ++ _) ->
    #{module => systemd_journal_h,
      config => #{}};
log_file_var_to_output("syslog:" ++ _) ->
    #{module => syslog_logger_h,
      config => #{}};
log_file_var_to_output(Filename) ->
    #{module => rabbit_logger_std_h,
      config => #{type => file,
                  file => Filename}}.

-spec keep_log_level_from_equivalent_output(
        logger:handler_config(), [logger:handler_config()]) ->
    logger:handler_config().
%% @doc
%% Keeps the log level from the equivalent output if found in the given list of
%% outputs.
%%
%% If the output is overridden from the environment, or if no output is
%% configured at all (and the default output is used), we should still keep the
%% log level set in the configuration. The idea is that the $RABBITMQ_LOGS
%% environment variable only overrides the output, not its log level (which
%% would be set in $RABBITMQ_LOG).
%%
%% Here is an example of when it is used:
%% * "$RABBITMQ_LOGS=-" is set in the environment
%% * "log.console.level = debug" is set in the configuration file

keep_log_level_from_equivalent_output(
  #{module := Mod, config := #{type := Type}} = Output,
  [#{module := Mod, config := #{type := Type}} = OverridenOutput | _])
  when ?IS_STD_H_COMPAT(Mod) ->
    keep_log_level_from_equivalent_output1(Output, OverridenOutput);
keep_log_level_from_equivalent_output(
  #{module := Mod} = Output,
  [#{module := Mod} = OverridenOutput | _]) ->
    keep_log_level_from_equivalent_output1(Output, OverridenOutput);
keep_log_level_from_equivalent_output(Output, [_ | Rest]) ->
    keep_log_level_from_equivalent_output(Output, Rest);
keep_log_level_from_equivalent_output(Output, []) ->
    Output.

-spec keep_log_level_from_equivalent_output1(
        logger:handler_config(), logger:handler_config()) ->
    logger:handler_config().

keep_log_level_from_equivalent_output1(Output, #{level := Level}) ->
    Output#{level => Level};
keep_log_level_from_equivalent_output1(Output, _) ->
    Output.

-spec apply_log_levels_from_env(log_config(), rabbit_env:context()) ->
    log_config().

apply_log_levels_from_env(LogConfig, #{log_levels := LogLevels})
  when is_map(LogLevels) ->
    %% `LogLevels' comes from the `$RABBITMQ_LOG' environment variable. It has
    %% the following form:
    %%     RABBITMQ_LOG=$cat1=$level1,$cat2=$level2,+color,-json
    %% I.e. it contains either `category=level' or `+flag'/`-flag'.
    %%
    %% Here we want to apply the log levels set from that variable, but we
    %% need to filter out any flags.
    maps:fold(
      fun
          (_, Value, LC) when is_boolean(Value) ->
              %% Ignore the '+color' and '+json' flags.
              LC;
          (global, Level, #{global := GlobalConfig} = LC) ->
              GlobalConfig1 = GlobalConfig#{level => Level},
              LC#{global => GlobalConfig1};
          (CatString, Level, #{per_category := PerCatConfig} = LC) ->
              CatAtom = list_to_atom(CatString),
              CatConfig0 = maps:get(CatAtom, PerCatConfig, #{outputs => []}),
              CatConfig1 = CatConfig0#{level => Level},
              PerCatConfig1 = PerCatConfig#{CatAtom => CatConfig1},
              LC#{per_category => PerCatConfig1}
      end, LogConfig, LogLevels);
apply_log_levels_from_env(LogConfig, _) ->
    LogConfig.

-spec make_filenames_absolute(log_config(), rabbit_env:context()) ->
    log_config().

make_filenames_absolute(
  #{global := GlobalConfig, per_category := PerCatConfig} = LogConfig,
  Context) ->
    LogBaseDir = get_log_base_dir(Context),
    GlobalConfig1 = make_filenames_absolute1(GlobalConfig, LogBaseDir),
    PerCatConfig1 = maps:map(
                      fun(_, CatConfig) ->
                              make_filenames_absolute1(CatConfig, LogBaseDir)
                      end, PerCatConfig),
    LogConfig#{global => GlobalConfig1, per_category => PerCatConfig1}.

make_filenames_absolute1(#{outputs := Outputs} = Config, LogBaseDir) ->
    Outputs1 = lists:map(
                 fun
                     (#{module := Mod,
                        config := #{type := file,
                                    file := Filename} = Cfg} = Output)
                       when ?IS_STD_H_COMPAT(Mod) ->
                         Cfg1 = Cfg#{file => filename:absname(
                                               Filename, LogBaseDir)},
                         Output#{config => Cfg1};
                     (Output) ->
                         Output
                 end, Outputs),
    Config#{outputs => Outputs1}.

-spec configure_formatters(log_config(), rabbit_env:context()) ->
    log_config().

configure_formatters(
  #{global := GlobalConfig, per_category := PerCatConfig} = LogConfig,
  Context) ->
    GlobalConfig1 = configure_formatters1(GlobalConfig, Context),
    PerCatConfig1 = maps:map(
                      fun(_, CatConfig) ->
                              configure_formatters1(CatConfig, Context)
                      end, PerCatConfig),
    LogConfig#{global => GlobalConfig1, per_category => PerCatConfig1}.

configure_formatters1(#{outputs := Outputs} = Config, Context) ->
    %% TODO: Add ability to configure formatters from the Cuttlefish
    %% configuration file. For now, it is only possible from the
    %% `$RABBITMQ_LOG' environment variable.
    ConsFormatter =
    rabbit_prelaunch_early_logging:default_console_formatter(Context),
    FileFormatter =
    rabbit_prelaunch_early_logging:default_file_formatter(Context),
    JournaldFormatter =
    rabbit_prelaunch_early_logging:default_journald_formatter(Context),
    SyslogFormatter =
    rabbit_prelaunch_early_logging:default_syslog_formatter(Context),
    Outputs1 = lists:map(
                 fun
                     (#{module := Mod,
                        config := #{type := Stddev}} = Output)
                       when ?IS_STD_H_COMPAT(Mod) andalso
                            ?IS_STDDEV(Stddev) ->
                         case maps:is_key(formatter, Output) of
                             true  -> Output;
                             false -> Output#{formatter => ConsFormatter}
                         end;
                     (#{module := systemd_journal_h} = Output) ->
                         case maps:is_key(formatter, Output) of
                             true  -> Output;
                             false -> Output#{formatter => JournaldFormatter}
                         end;
                     (#{module := syslog_logger_h} = Output) ->
                         case maps:is_key(formatter, Output) of
                             true  -> Output;
                             false -> Output#{formatter => SyslogFormatter}
                         end;
                     (Output) ->
                         case maps:is_key(formatter, Output) of
                             true  -> Output;
                             false -> Output#{formatter => FileFormatter}
                         end
                 end, Outputs),
    Config#{outputs => Outputs1}.

-spec create_logger_handlers_conf(log_config()) ->
    [logger:handler_config()].

create_logger_handlers_conf(
  #{global := GlobalConfig, per_category := PerCatConfig}) ->
    Handlers0 = create_global_handlers_conf(GlobalConfig),
    Handlers1 = create_per_cat_handlers_conf(PerCatConfig, Handlers0),
    Handlers2 = adjust_log_levels(Handlers1),

    %% assign_handler_ids/1 is also responsible for transforming the map of
    %% handlers into a list. The map was only used to deduplicate handlers.
    assign_handler_ids(Handlers2).

-spec create_global_handlers_conf(global_log_config()) ->
    #{handler_key() := logger:handler_config()}.

create_global_handlers_conf(#{outputs := Outputs} = GlobalConfig) ->
    Handlers = ensure_handlers_conf(Outputs, global, GlobalConfig, #{}),
    maps:map(
      fun(_, Handler) ->
              add_erlang_specific_filters(Handler)
      end, Handlers).

-spec add_erlang_specific_filters(logger:handler_config()) ->
    logger:handler_config().

add_erlang_specific_filters(#{filters := Filters} = Handler) ->
    %% We only log progress reports (from application master and supervisor)
    %% only if the handler level is set to debug.
    Action = case Handler of
                 #{level := debug} -> log;
                 _                 -> stop
             end,
    Filters1 = [{progress_reports, {fun logger_filters:progress/2, Action}}
                | Filters],
    Handler#{filters => Filters1}.

-spec create_per_cat_handlers_conf(
        #{category_name() => per_cat_log_config()},
        #{handler_key() => logger:handler_config()}) ->
    #{handler_key() => logger:handler_config()}.

create_per_cat_handlers_conf(PerCatConfig, Handlers) ->
    maps:fold(
      fun
          (CatName, #{outputs := []} = CatConfig, Hdls) ->
              %% That category has no outputs defined. It means its messages
              %% will go to the global handlers. We still need to update
              %% global handlers to filter the level of the messages from that
              %% category.
              filter_cat_in_global_handlers(Hdls, CatName, CatConfig);
          (CatName, #{outputs := Outputs} = CatConfig, Hdls) ->
              %% That category has specific outputs (i.e. in addition to the
              %% global handlers).
              Hdls1 = ensure_handlers_conf(Outputs, CatName, CatConfig, Hdls),
              %% We need to filter the messages from that category out in the
              %% global handlers.
              filter_out_cat_in_global_handlers(Hdls1, CatName)
      end, Handlers, PerCatConfig).

-spec ensure_handlers_conf(
        [logger:handler_config()], global | category_name(),
        global_log_config() | per_cat_log_config(),
        #{handler_key() => logger:handler_config()}) ->
    #{handler_key() => logger:handler_config()}.

ensure_handlers_conf([Output | Rest], CatName, Config, Handlers) ->
    Key = create_handler_key(Output),
    %% This is where the deduplication happens: either we update the existing
    %% handler (based on the key computed above) or we create a new one.
    Handler = case maps:is_key(Key, Handlers) of
                  false -> create_handler_conf(Output, CatName, Config);
                  true  -> update_handler_conf(maps:get(Key, Handlers),
                                               CatName, Output)
              end,
    Handlers1 = Handlers#{Key => Handler},
    ensure_handlers_conf(Rest, CatName, Config, Handlers1);
ensure_handlers_conf([], _, _, Handlers) ->
    Handlers.

-spec create_handler_key(logger:handler_config()) -> handler_key().

create_handler_key(
  #{module := Mod, config := #{type := file, file := Filename}})
  when ?IS_STD_H_COMPAT(Mod) ->
    {file, Filename};
create_handler_key(
  #{module := Mod, config := #{type := standard_io}})
  when ?IS_STD_H_COMPAT(Mod) ->
    {console, standard_io};
create_handler_key(
  #{module := Mod, config := #{type := standard_error}})
  when ?IS_STD_H_COMPAT(Mod) ->
    {console, standard_error};
create_handler_key(
  #{module := rabbit_logger_exchange_h}) ->
    exchange;
create_handler_key(
  #{module := systemd_journal_h}) ->
    journald;
create_handler_key(
  #{module := syslog_logger_h}) ->
    syslog.

-spec create_handler_conf(
        logger:handler_config(), global | category_name(),
        global_log_config() | per_cat_log_config()) ->
    logger:handler_config().

%% The difference between a global handler and a category handler is the value
%% of `filter_default'. In a global hanler, if a message was not stopped or
%% explicitely accepted by a filter, the message is logged. In a category
%% handler, it is dropped.

create_handler_conf(Output, global, Config) ->
    Level = compute_level_from_config_and_output(Config, Output),
    Output#{level => Level,
            filter_default => log,
            filters => [{?FILTER_NAME,
                         {fun filter_log_event/2, #{global => Level}}}]};
create_handler_conf(Output, CatName, Config) ->
    Level = compute_level_from_config_and_output(Config, Output),
    Output#{level => Level,
            filter_default => stop,
            filters => [{?FILTER_NAME,
                         {fun filter_log_event/2, #{CatName => Level}}}]}.

-spec update_handler_conf(
        logger:handler_config(), global | category_name(),
        logger:handler_config()) ->
    logger:handler_config().

update_handler_conf(
  #{level := ConfiguredLevel} = Handler, global, Output) ->
    case Output of
        #{level := NewLevel} ->
            Handler#{level =>
                     get_less_severe_level(NewLevel, ConfiguredLevel)};
        _ ->
            Handler
    end;
update_handler_conf(Handler, CatName, Output) ->
    add_cat_filter(Handler, CatName, Output).

-spec compute_level_from_config_and_output(
        global_log_config() | per_cat_log_config(),
        logger:handler_config()) ->
    logger:level().
%% @doc
%% Compute the debug level for a handler.
%%
%% The precedence is:
%% <ol>
%% <li>the level of the output</li>
%% <li>the level of the category (or the global level)</li>
%% <li>the default value</li>
%% </ol>

compute_level_from_config_and_output(Config, Output) ->
    case Output of
        #{level := Level} ->
            Level;
        _ ->
            case Config of
                #{level := Level} -> Level;
                _                 -> ?DEFAULT_LOG_LEVEL
            end
    end.

-spec filter_cat_in_global_handlers(
        #{handler_key() => logger:handler_config()}, category_name(),
        per_cat_log_config()) ->
    #{handler_key() => logger:handler_config()}.

filter_cat_in_global_handlers(Handlers, CatName, CatConfig) ->
    maps:map(
      fun
          (_, #{filter_default := log} = Handler) ->
              add_cat_filter(Handler, CatName, CatConfig);
          (_, Handler) ->
              Handler
      end, Handlers).

-spec filter_out_cat_in_global_handlers(
        #{handler_key() => logger:handler_config()}, category_name()) ->
    #{handler_key() => logger:handler_config()}.

filter_out_cat_in_global_handlers(Handlers, CatName) ->
    maps:map(
      fun
          (_, #{filter_default := log, filters := Filters} = Handler) ->
              {_, FilterConfig} = proplists:get_value(?FILTER_NAME, Filters),
              case maps:is_key(CatName, FilterConfig) of
                  true  -> Handler;
                  false -> add_cat_filter(Handler, CatName, #{level => none,
                                                              outputs => []})
              end;
          (_, Handler) ->
              Handler
      end, Handlers).

-spec add_cat_filter(
        logger:handler_config(), category_name(),
        per_cat_log_config() | logger:handler_config()) ->
    logger:handler_config().

add_cat_filter(Handler, CatName, CatConfigOrOutput) ->
    Level = case CatConfigOrOutput of
                #{level := L} -> L;
                _             -> maps:get(level, Handler)
            end,
    do_add_cat_filter(Handler, CatName, Level).

do_add_cat_filter(#{filters := Filters} = Handler, CatName, Level) ->
    {Fun, FilterConfig} = proplists:get_value(?FILTER_NAME, Filters),
    FilterConfig1 = FilterConfig#{CatName => Level},
    Filters1 = lists:keystore(?FILTER_NAME, 1, Filters,
                              {?FILTER_NAME, {Fun, FilterConfig1}}),
    Handler#{filters => Filters1}.

-spec filter_log_event(logger:log_event(), term()) -> logger:filter_return().

filter_log_event(LogEvent, FilterConfig) ->
    rabbit_prelaunch_early_logging:filter_log_event(LogEvent, FilterConfig).

-spec adjust_log_levels(#{handler_key() => logger:handler_config()}) ->
    #{handler_key() => logger:handler_config()}.
%% @doc
%% Adjust handler log level based on the filters' level.
%%
%% If a filter is more permissive, we need to adapt the handler log level so
%% the message makes it to the filter.
%%
%% Also, if the log level is set to `debug', we turn off burst limit to make
%% sure all debug messages make it.

adjust_log_levels(Handlers) ->
    maps:map(
      fun(_, #{level := GeneralLevel, filters := Filters} = Handler) ->
              {_, FilterConfig} = proplists:get_value(?FILTER_NAME, Filters),
              Level = maps:fold(
                        fun(_, LvlA, LvlB) ->
                                get_less_severe_level(LvlA, LvlB)
                        end, GeneralLevel, FilterConfig),
              Handler#{level => Level}
      end, Handlers).

-spec assign_handler_ids(#{handler_key() => logger:handler_config()}) ->
    [logger:handler_config()].

assign_handler_ids(Handlers) ->
    Handlers1 = [maps:get(Key, Handlers)
                 || Key <- lists:sort(maps:keys(Handlers))],
    assign_handler_ids(Handlers1,
                       #{config_run_number => get_config_run_number(),
                         next_file => 1},
                       []).

-spec assign_handler_ids(
        [logger:handler_config()], id_assignment_state(),
        [logger:handler_config()]) ->
    [logger:handler_config()].

assign_handler_ids(
  [#{module := Mod, config := #{type := file}} = Handler | Rest],
  #{next_file := NextFile} = State,
  Result)
  when ?IS_STD_H_COMPAT(Mod) ->
    Id = format_id("file_~b", [NextFile], State),
    Handler1 = Handler#{id => Id},
    assign_handler_ids(
      Rest, State#{next_file => NextFile + 1}, [Handler1 | Result]);
assign_handler_ids(
  [#{module := Mod, config := #{type := standard_io}} = Handler | Rest],
  State,
  Result)
  when ?IS_STD_H_COMPAT(Mod) ->
    Id = format_id("stdout", [], State),
    Handler1 = Handler#{id => Id},
    assign_handler_ids(Rest, State, [Handler1 | Result]);
assign_handler_ids(
  [#{module := Mod, config := #{type := standard_error}} = Handler | Rest],
  State,
  Result)
  when ?IS_STD_H_COMPAT(Mod) ->
    Id = format_id("stderr", [], State),
    Handler1 = Handler#{id => Id},
    assign_handler_ids(Rest, State, [Handler1 | Result]);
assign_handler_ids(
  [#{module := rabbit_logger_exchange_h} = Handler
   | Rest],
  State,
  Result) ->
    Id = format_id("exchange", [], State),
    Handler1 = Handler#{id => Id},
    assign_handler_ids(Rest, State, [Handler1 | Result]);
assign_handler_ids(
  [#{module := systemd_journal_h} = Handler
   | Rest],
  State,
  Result) ->
    Id = format_id("journald", [], State),
    Handler1 = Handler#{id => Id},
    assign_handler_ids(Rest, State, [Handler1 | Result]);
assign_handler_ids(
  [#{module := syslog_logger_h} = Handler
   | Rest],
  State,
  Result) ->
    Id = format_id("syslog", [], State),
    Handler1 = Handler#{id => Id},
    assign_handler_ids(Rest, State, [Handler1 | Result]);
assign_handler_ids([], _, Result) ->
    lists:reverse(Result).

-spec format_id(io:format(), [term()], id_assignment_state()) ->
    logger:handler_id().

format_id(Format, Args, #{config_run_number := RunNum}) ->
    list_to_atom(rabbit_misc:format("rmq_~b_" ++ Format, [RunNum | Args])).

-spec install_handlers([logger:handler_config()]) -> ok | no_return().

install_handlers([]) ->
    ok;
install_handlers(Handlers) ->
    case adjust_running_dependencies(Handlers) of
        ok ->
            ?LOG_NOTICE(
               "Logging: switching to configured handler(s); following "
               "messages may not be visible in this log output",
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            ok = do_install_handlers(Handlers),
            ok = remove_old_handlers(),
            ok = define_primary_level(Handlers),
            ?LOG_NOTICE(
               "Logging: configured log handlers are now ACTIVE",
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH});
        _ ->
            ?LOG_NOTICE(
               "Logging: failed to configure log handlers; keeping existing "
               "handlers",
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            ok
    end.

-spec adjust_running_dependencies([logger:handler_config()]) -> ok | error.

adjust_running_dependencies(Handlers) ->
    %% Based on the log handlers' module, we determine the list of applications
    %% they depend on.
    %%
    %% The DefaultDeps lists all possible dependencies and marked them as
    %% unneeded. Then, if we have a log handler which depends on one of them,
    %% it is marked as needed. This way, we know what needs to be started AND
    %% stopped.
    %%
    %% DefaultDeps is of the form `#{ApplicationName => Needed}'.
    DefaultDeps = #{syslog => false},
    Deps = lists:foldl(
             fun
                 (#{module := syslog_logger_h}, Acc) -> Acc#{syslog => true};
                 (_, Acc)                            -> Acc
             end, DefaultDeps, Handlers),
    adjust_running_dependencies1(maps:to_list(Deps)).

-spec adjust_running_dependencies1([{atom(), boolean()}]) -> ok | error.

adjust_running_dependencies1([{App, true} | Rest]) ->
    ?LOG_DEBUG(
       "Logging: ensure log handler dependency '~s' is started", [App],
       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    case application:ensure_all_started(App) of
        {ok, _} ->
            adjust_running_dependencies1(Rest);
        {error, Reason} ->
            ?LOG_ERROR(
               "Failed to start log handlers dependency '~s': ~p",
               [App, Reason],
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
            error
    end;
adjust_running_dependencies1([{App, false} | Rest]) ->
    ?LOG_DEBUG(
       "Logging: ensure log handler dependency '~s' is stopped", [App],
       #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    case application:stop(App) of
        ok ->
            ok;
        {error, Reason} ->
            ?LOG_NOTICE(
               "Logging: failed to stop log handlers dependency '~s': ~p",
               [App, Reason],
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH})
    end,
    adjust_running_dependencies1(Rest);
adjust_running_dependencies1([]) ->
    ok.

-spec do_install_handlers([logger:handler_config()]) -> ok | no_return().

do_install_handlers([#{id := Id, module := Module} = Handler | Rest]) ->
    case logger:add_handler(Id, Module, Handler) of
        ok ->
            ok = remove_syslog_logger_h_hardcoded_filters(Handler),
            do_install_handlers(Rest);
        {error, {handler_not_added, {open_failed, Filename, Reason}}} ->
            throw({error, {cannot_log_to_file, Filename, Reason}});
        {error, {handler_not_added, Reason}} ->
            throw({error, {cannot_log_to_file, unknown, Reason}})
    end;
do_install_handlers([]) ->
    ok.

remove_syslog_logger_h_hardcoded_filters(
  #{id := Id, module := syslog_logger_h}) ->
    _ = logger:remove_handler_filter(Id, progress),
    _ = logger:remove_handler_filter(Id, remote_gl),
    ok;
remove_syslog_logger_h_hardcoded_filters(_) ->
    ok.

-spec remove_old_handlers() -> ok.

remove_old_handlers() ->
    _ = logger:remove_handler(default),
    RunNum = get_config_run_number(),
    lists:foreach(
      fun(Id) ->
              Ret = re:run(atom_to_list(Id), "^rmq_([0-9]+)_",
                           [{capture, all_but_first, list}]),
              case Ret of
                  {match, [NumStr]} ->
                      Num = erlang:list_to_integer(NumStr),
                      if
                          Num < RunNum ->
                              ?LOG_DEBUG(
                                "Logging: removing old logger handler ~s",
                                [Id],
                                #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
                              ok = logger:remove_handler(Id);
                          true ->
                              ok
                      end;
                  _ ->
                      ok
              end
      end, lists:sort(logger:get_handler_ids())),
    ok.

-spec define_primary_level([logger:handler_config()]) ->
    ok | {error, term()}.

define_primary_level(Handlers) ->
    define_primary_level(Handlers, emergency).

-spec define_primary_level([logger:handler_config()], logger:level()) ->
    ok | {error, term()}.

define_primary_level([#{level := Level} | Rest], PrimaryLevel) ->
    NewLevel = get_less_severe_level(Level, PrimaryLevel),
    define_primary_level(Rest, NewLevel);
define_primary_level([], PrimaryLevel) ->
    logger:set_primary_config(level, PrimaryLevel).

-spec get_less_severe_level(logger:level(), logger:level()) -> logger:level().
%% @doc
%% Compares two log levels and returns the less severe one.
%%
%% @param LevelA the log level to compare to LevelB.
%% @param LevelB the log level to compare to LevelA.
%%
%% @returns the less severe log level.

get_less_severe_level(LevelA, LevelB) ->
    case logger:compare_levels(LevelA, LevelB) of
        lt -> LevelA;
        _  -> LevelB
    end.

-spec maybe_log_test_messages(log_config()) -> ok.

maybe_log_test_messages(
  #{per_category := #{prelaunch := #{level := debug}}}) ->
    log_test_messages();
maybe_log_test_messages(
  #{global := #{level := debug}}) ->
    log_test_messages();
maybe_log_test_messages(_) ->
    ok.

-spec log_test_messages() -> ok.

log_test_messages() ->
    ?LOG_DEBUG("Logging: testing debug log level",
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ?LOG_INFO("Logging: testing info log level",
              #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ?LOG_NOTICE("Logging: testing notice log level",
                #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ?LOG_WARNING("Logging: testing warning log level",
                 #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ?LOG_ERROR("Logging: testing error log level",
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ?LOG_CRITICAL("Logging: testing critical log level",
                  #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ?LOG_ALERT("Logging: testing alert log level",
               #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}),
    ?LOG_EMERGENCY("Logging: testing emergency log level",
                   #{domain => ?RMQLOG_DOMAIN_PRELAUNCH}).
