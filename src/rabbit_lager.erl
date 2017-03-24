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
%% Copyright (c) 2007-2015 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_lager).

-include("rabbit_log.hrl").

%% API
-export([start_logger/0, log_locations/0, fold_sinks/2]).

start_logger() ->
    application:stop(lager),
    ensure_lager_configured(),
    lager:start(),
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

lager_handlers(Silent) when Silent == silent; Silent == false ->
    [];
lager_handlers(tty) ->
    [{lager_console_backend, info}];
lager_handlers(FileName) when is_list(FileName) ->
    LogFile = lager_util:expand_path(FileName),
    case rabbit_file:ensure_dir(LogFile) of
        ok -> ok;
        {error, Reason} ->
            throw({error, {cannot_log_to_file, LogFile,
                           {cannot_create_parent_dirs, LogFile, Reason}}})
    end,
    [{lager_file_backend, [{file, FileName},
                           {level, info},
                           {formatter_config,
                            [date, " ", time, " ", color, "[", severity, "] ",
                             {pid, ""},
                             " ", message, "\n"]},
                           {date, ""},
                           {size, 0}]}].


ensure_lager_configured() ->
    case lager_configured() of
        false -> configure_lager();
        true -> ok
    end.

%% Lager should have handlers and sinks
lager_configured() ->
    Sinks = lager:list_all_sinks(),
    ExpectedSinks = list_expected_sinks(),
    application:get_env(lager, handlers) =/= undefined
    andalso
    lists:all(fun(S) -> lists:member(S, Sinks) end, ExpectedSinks).

configure_lager() ->
    application:load(lager),
    %% Turn off reformatting for error_logger messages
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
                    application:set_env(lager, log_root, Value);
                _ ->
                    ok
            end;
        _ -> ok
    end,

    %% Configure the default sink/handlers.
    Handlers0 = application:get_env(lager, handlers, undefined),
    DefaultHandlers = lager_handlers(application:get_env(rabbit,
                                                         lager_handler,
                                                         tty)),
    Handlers = case os:getenv("RABBITMQ_LOGS_source") of
        %% There are no default handlers in rabbitmq.config, create a
        %% configuration from $RABBITMQ_LOGS.
        false when Handlers0 =:= undefined -> DefaultHandlers;
        %% There are default handlers configure in rabbitmq.config, but
        %% the user explicitely sets $RABBITMQ_LOGS; create new handlers
        %% based on that, instead of using rabbitmq.config.
        "environment"                      -> DefaultHandlers;
        %% Use the default handlers configured in rabbitmq.com.
        _                                  -> Handlers0
    end,
    application:set_env(lager, handlers, Handlers),

    %% Setup extra sink/handlers. If they are not configured, redirect
    %% messages to the default sink. To know the list of expected extra
    %% sinks, we look at the 'lager_extra_sinks' compilation option.
    Sinks0 = application:get_env(lager, extra_sinks, []),
    Sinks1 = configure_extra_sinks(Sinks0,
                                   [error_logger | list_expected_sinks()]),
    %% TODO Waiting for basho/lager#303
    %% Sinks2 = lists:keystore(error_logger_lager_event, 1, Sinks1,
    %%                         {error_logger_lager_event,
    %%                          [{handlers, Handlers}]}),
    application:set_env(lager, extra_sinks, Sinks1),

    case application:get_env(lager, error_logger_hwm) of
        undefined ->
            application:set_env(lager, error_logger_hwm, 100);
        {ok, Val} when is_integer(Val) andalso Val =< 100 ->
            application:set_env(lager, error_logger_hwm, 100);
        {ok, _Val} ->
            ok
    end,
    ok.

configure_extra_sinks(Sinks, [SinkName | Rest]) ->
    Sink0 = proplists:get_value(SinkName, Sinks, []),
    Sink1 = case proplists:is_defined(handlers, Sink0) of
        false -> default_sink_config(SinkName, Sink0);
        true  -> Sink0
    end,
    Sinks1 = lists:keystore(SinkName, 1, Sinks, {SinkName, Sink1}),
    configure_extra_sinks(Sinks1, Rest);
configure_extra_sinks(Sinks, []) ->
    Sinks.

default_sink_config(rabbit_log_upgrade_lager_event, Sink) ->
    Handlers = lager_handlers(application:get_env(rabbit,
                                                  lager_handler_upgrade,
                                                  tty)),
    lists:keystore(handlers, 1, Sink, {handlers, Handlers});
default_sink_config(_, Sink) ->
    lists:keystore(handlers, 1, Sink,
                   {handlers,
                    [{lager_forwarder_backend,
                      lager_util:make_internal_sink_name(lager)}]}).

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
            application:set_env(rabbit, lager_extra_sinks, List),
            List
    end.
