%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_lager).

-include_lib("rabbit_common/include/rabbit_log.hrl").

%% API
-export([start_logger/0, log_locations/0, fold_sinks/2,
         broker_is_started/0, set_log_level/1]).

%% For test purposes
-export([configure_lager/0]).

start_logger() ->
    ok = maybe_remove_logger_handler(),
    ok = app_utils:stop_applications([lager, syslog]),
    ok = ensure_lager_configured(),
    ok = app_utils:start_applications([lager]),
    fold_sinks(
      fun
          (_, [], Acc) ->
              Acc;
          (SinkName, _, Acc) ->
              lager:log(SinkName, info, self(),
                        "Log file opened with Lager", []),
              Acc
      end, ok),
    ensure_log_working().

broker_is_started() ->
    {ok, HwmCurrent} = application:get_env(lager, error_logger_hwm),
    {ok, HwmOrig} = application:get_env(lager, error_logger_hwm_original),
    case HwmOrig =:= HwmCurrent of
        false ->
            ok = application:set_env(lager, error_logger_hwm, HwmOrig),
            Handlers = gen_event:which_handlers(lager_event),
            lists:foreach(fun(Handler) ->
                              lager:set_loghwm(Handler, HwmOrig)
                          end, Handlers),
            ok;
        _ ->
            ok
    end.

set_log_level(Level) ->
    IsValidLevel = lists:member(Level, lager_util:levels()),
    set_log_level(IsValidLevel, Level).

set_log_level(true, Level) ->
    SinksAndHandlers = [{Sink, gen_event:which_handlers(Sink)} ||
                        Sink <- lager:list_all_sinks()],
    set_sink_log_level(SinksAndHandlers, Level);
set_log_level(_, Level) ->
    {error, {invalid_log_level, Level}}.

set_sink_log_level([], _Level) ->
    ok;
set_sink_log_level([{Sink, Handlers}|Rest], Level) ->
    set_sink_handler_log_level(Sink, Handlers, Level),
    set_sink_log_level(Rest, Level).

set_sink_handler_log_level(_Sink, [], _Level) ->
    ok;
set_sink_handler_log_level(Sink, [Handler|Rest], Level) when is_atom(Handler) ->
    ok = lager:set_loglevel(Sink, Handler, undefined, Level),
    set_sink_handler_log_level(Sink, Rest, Level);
set_sink_handler_log_level(Sink, [{Handler, Id}|Rest], Level) ->
    ok = lager:set_loglevel(Sink, Handler, Id, Level),
    set_sink_handler_log_level(Sink, Rest, Level);
set_sink_handler_log_level(Sink, [_|Rest], Level) ->
    set_sink_handler_log_level(Sink, Rest, Level).

log_locations() ->
    ensure_lager_configured(),
    DefaultHandlers = application:get_env(lager, handlers, []),
    Sinks = application:get_env(lager, extra_sinks, []),
    ExtraHandlers = [proplists:get_value(handlers, Props, [])
                     || {_, Props} <- Sinks],
    lists:sort(log_locations1([DefaultHandlers | ExtraHandlers], [])).

log_locations1([Handlers | Rest], Locations) ->
    Locations1 = log_locations2(Handlers, Locations),
    log_locations1(Rest, Locations1);
log_locations1([], Locations) ->
    Locations.

log_locations2([{lager_file_backend, Settings} | Rest], Locations) ->
    FileName = lager_file_name1(Settings),
    Locations1 = case lists:member(FileName, Locations) of
        false -> [FileName | Locations];
        true  -> Locations
    end,
    log_locations2(Rest, Locations1);
log_locations2([{lager_console_backend, _} | Rest], Locations) ->
    Locations1 = case lists:member("<stdout>", Locations) of
        false -> ["<stdout>" | Locations];
        true  -> Locations
    end,
    log_locations2(Rest, Locations1);
log_locations2([_ | Rest], Locations) ->
    log_locations2(Rest, Locations);
log_locations2([], Locations) ->
    Locations.

fold_sinks(Fun, Acc) ->
    Handlers = lager_config:global_get(handlers),
    Sinks = dict:to_list(lists:foldl(
        fun
            ({{lager_file_backend, F}, _, S}, Dict) ->
                dict:append(S, F, Dict);
            ({_, _, S}, Dict) ->
                case dict:is_key(S, Dict) of
                    true  -> dict:store(S, [], Dict);
                    false -> Dict
                end
        end,
        dict:new(), Handlers)),
    fold_sinks(Sinks, Fun, Acc).

fold_sinks([{SinkName, FileNames} | Rest], Fun, Acc) ->
    Acc1 = Fun(SinkName, FileNames, Acc),
    fold_sinks(Rest, Fun, Acc1);
fold_sinks([], _, Acc) ->
    Acc.

ensure_log_working() ->
    {ok, Handlers} = application:get_env(lager, handlers),
    [ ensure_lager_handler_file_exist(Handler)
      || Handler <- Handlers ],
    Sinks = application:get_env(lager, extra_sinks, []),
    ensure_extra_sinks_working(Sinks, list_expected_sinks()).

ensure_extra_sinks_working(Sinks, [SinkName | Rest]) ->
    case proplists:get_value(SinkName, Sinks) of
        undefined -> throw({error, {cannot_log_to_file, unknown,
                                    rabbit_log_lager_event_sink_undefined}});
        Sink ->
            SinkHandlers = proplists:get_value(handlers, Sink, []),
            [ ensure_lager_handler_file_exist(Handler)
              || Handler <- SinkHandlers ]
    end,
    ensure_extra_sinks_working(Sinks, Rest);
ensure_extra_sinks_working(_Sinks, []) ->
    ok.

ensure_lager_handler_file_exist(Handler) ->
    case lager_file_name(Handler) of
        false    -> ok;
        FileName -> ensure_logfile_exist(FileName)
    end.

lager_file_name({lager_file_backend, Settings}) ->
    lager_file_name1(Settings);
lager_file_name(_) ->
    false.

lager_file_name1(Settings) when is_list(Settings) ->
    {file, FileName} = proplists:lookup(file, Settings),
    lager_util:expand_path(FileName);
lager_file_name1({FileName, _}) -> lager_util:expand_path(FileName);
lager_file_name1({FileName, _, _, _, _}) -> lager_util:expand_path(FileName);
lager_file_name1(_) ->
    throw({error, {cannot_log_to_file, unknown,
                   lager_file_backend_config_invalid}}).


ensure_logfile_exist(FileName) ->
    LogFile = lager_util:expand_path(FileName),
    case rabbit_file:read_file_info(LogFile) of
        {ok,_} -> ok;
        {error, Err} -> throw({error, {cannot_log_to_file, LogFile, Err}})
    end.

ensure_lager_configured() ->
    case lager_configured() of
        false -> configure_lager();
        true -> ok
    end.

%% Lager should have handlers and sinks
%% Error logger forwarding to syslog should be disabled
lager_configured() ->
    Sinks = lager:list_all_sinks(),
    ExpectedSinks = list_expected_sinks(),
    application:get_env(lager, handlers) =/= undefined
    andalso
    lists:all(fun(S) -> lists:member(S, Sinks) end, ExpectedSinks)
    andalso
    application:get_env(syslog, syslog_error_logger) =/= undefined.

configure_lager() ->
    ok = app_utils:load_applications([lager]),
    %% Turn off reformatting for error_logger messages
    case application:get_env(lager, error_logger_redirect) of
        undefined -> application:set_env(lager, error_logger_redirect, true);
        _         -> ok
    end,
    case application:get_env(lager, error_logger_format_raw) of
        undefined -> application:set_env(lager, error_logger_format_raw, true);
        _         -> ok
    end,
    case application:get_env(lager, log_root) of
        undefined ->
            %% Setting env var to 'undefined' is different from not
            %% setting it at all, and lager is sensitive to this
            %% difference.
            case application:get_env(rabbit, lager_log_root) of
                {ok, Value} ->
                    ok = application:set_env(lager, log_root, Value);
                _ ->
                    ok
            end;
        _ -> ok
    end,
    %% Set rabbit.log config variable based on environment.
    prepare_rabbit_log_config(),
    %% Configure syslog library.
    ok = configure_syslog_error_logger(),
    %% At this point we should have rabbit.log application variable
    %% configured to generate RabbitMQ log handlers.
    GeneratedHandlers = generate_lager_handlers(),

    %% If there are lager handlers configured,
    %% both lager and generate RabbitMQ handlers are used.
    %% This is because it's hard to decide clear preference rules.
    %% RabbitMQ handlers can be set to [] to use only lager handlers.
    Handlers = case application:get_env(lager, handlers, undefined) of
        undefined -> GeneratedHandlers;
        LagerHandlers ->
            %% Remove handlers generated in previous starts
            FormerRabbitHandlers = application:get_env(lager, rabbit_handlers, []),
            GeneratedHandlers ++ remove_rabbit_handlers(LagerHandlers,
                                                        FormerRabbitHandlers)
    end,

    ok = application:set_env(lager, handlers, Handlers),
    ok = application:set_env(lager, rabbit_handlers, GeneratedHandlers),

    %% Setup extra sink/handlers. If they are not configured, redirect
    %% messages to the default sink. To know the list of expected extra
    %% sinks, we look at the 'lager_extra_sinks' compilation option.
    LogConfig = application:get_env(rabbit, log, []),
    LogLevels = application:get_env(rabbit, log_levels, []),
    Categories = proplists:get_value(categories, LogConfig, []),
    CategoriesConfig = case {Categories, LogLevels} of
        {[], []} -> [];
        {[], LogLevels} ->
            io:format("Using deprecated config parameter 'log_levels'. "
                      "Please update your configuration file according to "
                      "https://rabbitmq.com/logging.html"),
            lists:map(fun({Name, Level}) -> {Name, [{level, Level}]} end,
                      LogLevels);
        {Categories, []} ->
            Categories;
        {Categories, LogLevels} ->
            io:format("Using the deprecated config parameter 'rabbit.log_levels' together "
                      "with a new parameter for log categories."
                      " 'rabbit.log_levels' will be ignored. Please remove it from the config. More at "
                      "https://rabbitmq.com/logging.html"),
            Categories
    end,
    SinkConfigs = lists:map(
        fun({Name, Config}) ->
            {rabbit_log:make_internal_sink_name(Name), Config}
        end,
        CategoriesConfig),
    LagerSinks = application:get_env(lager, extra_sinks, []),
    GeneratedSinks = generate_lager_sinks(
        [error_logger_lager_event | list_expected_sinks()],
        SinkConfigs),
    Sinks = merge_lager_sink_handlers(LagerSinks, GeneratedSinks, []),
    ok = application:set_env(lager, extra_sinks, Sinks),

    case application:get_env(lager, error_logger_hwm) of
        undefined ->
            ok = application:set_env(lager, error_logger_hwm, 1000),
            % NB: 50 is the default value in lager.app.src
            ok = application:set_env(lager, error_logger_hwm_original, 50);
        {ok, Val} when is_integer(Val) andalso Val < 1000 ->
            ok = application:set_env(lager, error_logger_hwm, 1000),
            ok = application:set_env(lager, error_logger_hwm_original, Val);
        {ok, Val} ->
            ok = application:set_env(lager, error_logger_hwm_original, Val),
            ok
    end,
    ok.

configure_syslog_error_logger() ->
    %% Disable error_logger forwarding to syslog if it's not configured
    case application:get_env(syslog, syslog_error_logger) of
        undefined ->
            application:set_env(syslog, syslog_error_logger, false);
        _ -> ok
    end.

remove_rabbit_handlers(Handlers, FormerHandlers) ->
    lists:filter(fun(Handler) ->
        not lists:member(Handler, FormerHandlers)
    end,
    Handlers).

generate_lager_handlers() ->
    LogConfig = application:get_env(rabbit, log, []),
    LogHandlersConfig = lists:keydelete(categories, 1, LogConfig),
    generate_lager_handlers(LogHandlersConfig).

generate_lager_handlers(LogHandlersConfig) ->
    lists:flatmap(
    fun
        ({file, HandlerConfig}) ->
            case proplists:get_value(file, HandlerConfig, false) of
                false -> [];
                FileName when is_list(FileName) ->
                    Backend = lager_backend(file),
                    generate_handler(Backend, HandlerConfig)
            end;
        ({Other, HandlerConfig}) when
              Other =:= console; Other =:= syslog; Other =:= exchange ->
            case proplists:get_value(enabled, HandlerConfig, false) of
                false -> [];
                true  ->
                    Backend = lager_backend(Other),
                    generate_handler(Backend,
                                     lists:keydelete(enabled, 1, HandlerConfig))
            end
    end,
    LogHandlersConfig).

lager_backend(file)     -> lager_file_backend;
lager_backend(console)  -> lager_console_backend;
lager_backend(syslog)   -> syslog_lager_backend;
lager_backend(exchange) -> lager_exchange_backend.

%% Syslog backend is using an old API for configuration and
%% does not support proplists.
generate_handler(syslog_lager_backend, HandlerConfig) ->
    DefaultConfigVal = default_config_value(level),
    Level = proplists:get_value(level, HandlerConfig, DefaultConfigVal),
    ok = app_utils:load_applications([syslog]),
    [{syslog_lager_backend,
     [Level,
      {},
      {lager_default_formatter, syslog_formatter_config()}]}];
generate_handler(Backend, HandlerConfig) ->
    [{Backend,
        lists:ukeymerge(1, lists:ukeysort(1, HandlerConfig),
                           lists:ukeysort(1, default_handler_config(Backend)))}].

default_handler_config(lager_console_backend) ->
    [{level, default_config_value(level)},
     {formatter_config, default_config_value(formatter_config)}];
default_handler_config(lager_exchange_backend) ->
    [{level, default_config_value(level)},
     {formatter_config, default_config_value(formatter_config)}];
default_handler_config(lager_file_backend) ->
    [{level, default_config_value(level)},
     {formatter_config, default_config_value(formatter_config)},
     {date, ""},
     {size, 0}].

default_config_value(level) -> info;
default_config_value(formatter_config) ->
    [date, " ", time, " ", color, "[", severity, "] ",
       {pid, ""},
       " ", message, "\n"].

syslog_formatter_config() ->
    [color, "[", severity, "] ",
       {pid, ""},
       " ", message, "\n"].

prepare_rabbit_log_config() ->
    %% If RABBIT_LOGS is not set, we should ignore it.
    DefaultFile = application:get_env(rabbit, lager_default_file, undefined),
    %% If RABBIT_UPGRADE_LOGS is not set, we should ignore it.
    UpgradeFile = application:get_env(rabbit, lager_upgrade_file, undefined),
    case DefaultFile of
        undefined -> ok;
        false ->
            set_env_default_log_disabled();
        tty ->
            set_env_default_log_console();
        FileName when is_list(FileName) ->
            case os:getenv("RABBITMQ_LOGS_source") of
                %% The user explicitly sets $RABBITMQ_LOGS;
                %% we should override a file location even
                %% if it's set in rabbitmq.config
                "environment" -> set_env_default_log_file(FileName, override);
                _             -> set_env_default_log_file(FileName, keep)
            end
    end,

    %% Upgrade log file never overrides the value set in rabbitmq.config
    case UpgradeFile of
        %% No special env for upgrade logs - redirect to the default sink
        undefined -> ok;
        %% Redirect logs to default output.
        DefaultFile -> ok;
        UpgradeFileName when is_list(UpgradeFileName) ->
            set_env_upgrade_log_file(UpgradeFileName)
    end.

set_env_default_log_disabled() ->
    %% Disabling all the logs.
    ok = application:set_env(rabbit, log, []).

set_env_default_log_console() ->
    LogConfig = application:get_env(rabbit, log, []),
    ConsoleConfig = proplists:get_value(console, LogConfig, []),
    LogConfigConsole =
        lists:keystore(console, 1, LogConfig,
                       {console, lists:keystore(enabled, 1, ConsoleConfig,
                                                {enabled, true})}),
    %% Remove the file handler - disable logging to file
    LogConfigConsoleNoFile = lists:keydelete(file, 1, LogConfigConsole),
    ok = application:set_env(rabbit, log, LogConfigConsoleNoFile).

set_env_default_log_file(FileName, Override) ->
    LogConfig = application:get_env(rabbit, log, []),
    FileConfig = proplists:get_value(file, LogConfig, []),
    NewLogConfig = case proplists:get_value(file, FileConfig, undefined) of
        undefined ->
            lists:keystore(file, 1, LogConfig,
                           {file, lists:keystore(file, 1, FileConfig,
                                                 {file, FileName})});
        _ConfiguredFileName ->
            case Override of
                override ->
                    lists:keystore(
                        file, 1, LogConfig,
                        {file, lists:keystore(file, 1, FileConfig,
                                              {file, FileName})});
                keep ->
                    LogConfig
            end
    end,
    ok = application:set_env(rabbit, log, NewLogConfig).

set_env_upgrade_log_file(FileName) ->
    LogConfig = application:get_env(rabbit, log, []),
    SinksConfig = proplists:get_value(categories, LogConfig, []),
    UpgradeSinkConfig = proplists:get_value(upgrade, SinksConfig, []),
    FileConfig = proplists:get_value(file, SinksConfig, []),
    NewLogConfig = case proplists:get_value(file, FileConfig, undefined) of
        undefined ->
            lists:keystore(
                categories, 1, LogConfig,
                {categories,
                    lists:keystore(
                        upgrade, 1, SinksConfig,
                        {upgrade,
                            lists:keystore(file, 1, UpgradeSinkConfig,
                                           {file, FileName})})});
        %% No cahnge. We don't want to override the configured value.
        _File -> LogConfig
    end,
    ok = application:set_env(rabbit, log, NewLogConfig).

generate_lager_sinks(SinkNames, SinkConfigs) ->
    lists:map(fun(SinkName) ->
        SinkConfig = proplists:get_value(SinkName, SinkConfigs, []),
        SinkHandlers = case proplists:get_value(file, SinkConfig, false) of
            %% If no file defined - forward everything to the default backend
            false ->
                ForwarderLevel = proplists:get_value(level, SinkConfig, inherit),
                [{lager_forwarder_backend,
                    [lager_util:make_internal_sink_name(lager), ForwarderLevel]}];
            %% If a file defined - add a file backend to handlers and remove all default file backends.
            File ->
                %% Use `debug` as a default handler to not override a handler level
                Level = proplists:get_value(level, SinkConfig, debug),
                DefaultGeneratedHandlers = application:get_env(lager, rabbit_handlers, []),
                SinkFileHandlers = case proplists:get_value(lager_file_backend, DefaultGeneratedHandlers, undefined) of
                    undefined ->
                        %% Create a new file handler.
                        %% `info` is a default level here.
                        FileLevel = proplists:get_value(level, SinkConfig, default_config_value(level)),
                        generate_lager_handlers([{file, [{file, File}, {level, FileLevel}]}]);
                    FileHandler ->
                        %% Replace a filename in the handler
                        FileHandlerChanges = case handler_level_more_verbose(FileHandler, Level) of
                            true  -> [{file, File}, {level, Level}];
                            false -> [{file, File}]
                        end,

                        [{lager_file_backend,
                            lists:ukeymerge(1, FileHandlerChanges,
                                            lists:ukeysort(1, FileHandler))}]
                end,
                %% Remove all file handlers.
                AllLagerHandlers = application:get_env(lager, handlers, []),
                HandlersWithoutFile = lists:filter(
                    fun({lager_file_backend, _}) -> false;
                       ({_, _}) -> true
                    end,
                    AllLagerHandlers),
                %% Set level for handlers which are more verbose.
                %% We don't increase verbosity in sinks so it works like forwarder backend.
                HandlersWithoutFileWithLevel = lists:map(fun({Name, Handler}) ->
                    case handler_level_more_verbose(Handler, Level) of
                        true  -> {Name, lists:keystore(level, 1, Handler, {level, Level})};
                        false -> {Name, Handler}
                    end
                end,
                HandlersWithoutFile),

                HandlersWithoutFileWithLevel ++ SinkFileHandlers
        end,
        {SinkName, [{handlers, SinkHandlers}, {rabbit_handlers, SinkHandlers}]}
    end,
    SinkNames).

handler_level_more_verbose(Handler, Level) ->
    HandlerLevel = proplists:get_value(level, Handler, default_config_value(level)),
    lager_util:level_to_num(HandlerLevel) > lager_util:level_to_num(Level).

merge_lager_sink_handlers([{Name, Sink} | RestSinks], GeneratedSinks, Agg) ->
    case lists:keytake(Name, 1, GeneratedSinks) of
        {value, {Name, GenSink}, RestGeneratedSinks} ->
            Handlers = proplists:get_value(handlers, Sink, []),
            GenHandlers = proplists:get_value(handlers, GenSink, []),
            FormerRabbitHandlers = proplists:get_value(rabbit_handlers, Sink, []),

            %% Remove handlers defined in previous starts
            ConfiguredHandlers = remove_rabbit_handlers(Handlers, FormerRabbitHandlers),
            NewHandlers = GenHandlers ++ ConfiguredHandlers,
            MergedSink = lists:keystore(rabbit_handlers, 1,
                                        lists:keystore(handlers, 1, Sink,
                                                       {handlers, NewHandlers}),
                                        {rabbit_handlers, GenHandlers}),

            merge_lager_sink_handlers(
                RestSinks,
                RestGeneratedSinks,
                [{Name, MergedSink} | Agg]);
        false ->
            merge_lager_sink_handlers(
                RestSinks,
                GeneratedSinks,
                [{Name, Sink} | Agg])
    end;
merge_lager_sink_handlers([], GeneratedSinks, Agg) -> GeneratedSinks ++ Agg.

list_expected_sinks() ->
    case application:get_env(rabbit, lager_extra_sinks) of
        {ok, List} ->
            List;
        undefined ->
            CompileOptions = proplists:get_value(options,
                                                 ?MODULE:module_info(compile),
                                                 []),
            AutoList = [lager_util:make_internal_sink_name(M)
                        || M <- proplists:get_value(lager_extra_sinks,
                                                    CompileOptions, [])],
            List = case lists:member(?LAGER_SINK, AutoList) of
                true  -> AutoList;
                false -> [?LAGER_SINK | AutoList]
            end,
            %% Store the list in the application environment. If this
            %% module is later cover-compiled, the compile option will
            %% be lost, so we will be able to retrieve the list from the
            %% application environment.
            ok = application:set_env(rabbit, lager_extra_sinks, List),
            List
    end.

maybe_remove_logger_handler() ->
    M = logger,
    F = remove_handler,
    try
        ok = erlang:apply(M, F, [default])
    catch
        error:undef ->
            % OK since the logger module only exists in OTP 21.1 or later
            ok;
        error:{badmatch, {error, {not_found, default}}} ->
            % OK - this error happens when running a CLI command
            ok;
        Err:Reason ->
            error_logger:error_msg("calling ~p:~p failed: ~p:~p~n",
                                   [M, F, Err, Reason])
    end.
