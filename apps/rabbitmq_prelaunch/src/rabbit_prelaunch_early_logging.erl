-module(rabbit_prelaunch_early_logging).

-include_lib("rabbit_common/include/rabbit_log.hrl").

-export([setup_early_logging/2,
         enable_quick_dbg/1,
         use_colored_logging/0,
         use_colored_logging/1,
         list_expected_sinks/0]).

setup_early_logging(#{log_levels := undefined} = Context,
                         LagerEventToStdout) ->
    setup_early_logging(Context#{log_levels => get_default_log_level()},
                             LagerEventToStdout);
setup_early_logging(Context, LagerEventToStdout) ->
    Configured = lists:member(
                   lager_util:make_internal_sink_name(rabbit_log_prelaunch),
                   lager:list_all_sinks()),
    case Configured of
        true  -> ok;
        false -> do_setup_early_logging(Context, LagerEventToStdout)
    end.

get_default_log_level() ->
    #{"prelaunch" => warning}.

do_setup_early_logging(#{log_levels := LogLevels} = Context,
                       LagerEventToStdout) ->
    Colored = use_colored_logging(Context),
    application:set_env(lager, colored, Colored),
    ConsoleBackend = lager_console_backend,
    case LagerEventToStdout of
        true ->
            GLogLevel = case LogLevels of
                            #{global := Level} -> Level;
                            _                  -> warning
                        end,
            _ = lager_app:start_handler(
                  lager_event, ConsoleBackend, [{level, GLogLevel}]),
            ok;
        false ->
            ok
    end,
    lists:foreach(
      fun(Sink) ->
              CLogLevel = get_log_level(LogLevels, Sink),
              lager_app:configure_sink(
                Sink,
                [{handlers, [{ConsoleBackend, [{level, CLogLevel}]}]}])
      end, list_expected_sinks()),
    ok.

use_colored_logging() ->
    use_colored_logging(rabbit_prelaunch:get_context()).

use_colored_logging(#{log_levels := #{color := true},
                      output_supports_colors := true}) ->
    true;
use_colored_logging(_) ->
    false.

list_expected_sinks() ->
    Key = {?MODULE, lager_extra_sinks},
    case persistent_term:get(Key, undefined) of
        undefined ->
            CompileOptions = proplists:get_value(options,
                                                 module_info(compile),
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
            persistent_term:put(Key, List),
            List;
        List ->
            List
    end.

sink_to_category(Sink) when is_atom(Sink) ->
    re:replace(
      atom_to_list(Sink),
      "^rabbit_log_(.+)_lager_event$",
      "\\1",
      [{return, list}]).

get_log_level(LogLevels, Sink) ->
    Category = sink_to_category(Sink),
    case LogLevels of
        #{Category := Level} -> Level;
        #{global := Level}   -> Level;
        _                    -> warning
    end.

enable_quick_dbg(#{dbg_output := Output, dbg_mods := Mods}) ->
    case Output of
        stdout -> {ok, _} = dbg:tracer(),
                  ok;
        _      -> {ok, _} = dbg:tracer(port, dbg:trace_port(file, Output)),
                  ok
    end,
    {ok, _} = dbg:p(all, c),
    lists:foreach(fun(M) -> {ok, _} = dbg:tp(M, cx) end, Mods).
