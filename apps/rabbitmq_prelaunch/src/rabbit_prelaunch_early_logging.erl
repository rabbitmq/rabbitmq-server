-module(rabbit_prelaunch_early_logging).

-export([setup_early_logging/2,
         enable_quick_dbg/1,
         use_colored_logging/0,
         use_colored_logging/1]).

-define(SINK, rabbit_log_prelaunch_lager_event).

setup_early_logging(#{log_levels := undefined} = Context,
                         LagerEventToStdout) ->
    setup_early_logging(Context#{log_levels => get_default_log_level()},
                             LagerEventToStdout);
setup_early_logging(Context, LagerEventToStdout) ->
    case lists:member(?SINK, lager:list_all_sinks()) of
        true  -> ok;
        false -> do_setup_early_logging(Context, LagerEventToStdout)
    end.

get_default_log_level() ->
    #{"prelaunch" => warning}.

do_setup_early_logging(#{log_levels := LogLevels} = Context,
                       LagerEventToStdout) ->
    LogLevel = case LogLevels of
                   #{"prelaunch" := Level} -> Level;
                   #{global := Level}      -> Level;
                   _                       -> warning
               end,
    Colored = use_colored_logging(Context),
    ConsoleBackend = lager_console_backend,
    ConsoleOptions = [{level, LogLevel}],
    application:set_env(lager, colored, Colored),
    case LagerEventToStdout of
        true ->
            lager_app:start_handler(
              lager_event, ConsoleBackend, ConsoleOptions);
        false ->
            ok
    end,
    lager_app:configure_sink(
      ?SINK,
      [{handlers, [{ConsoleBackend, ConsoleOptions}]}]),
    ok.

use_colored_logging() ->
    use_colored_logging(rabbit_prelaunch:get_context()).

use_colored_logging(#{log_levels := #{color := true},
                      output_supports_colors := true}) ->
    true;
use_colored_logging(_) ->
    false.

enable_quick_dbg(#{dbg_output := Output, dbg_mods := Mods}) ->
    case Output of
        stdout -> {ok, _} = dbg:tracer();
        _      -> {ok, _} = dbg:tracer(port, dbg:trace_port(file, Output))
    end,
    {ok, _} = dbg:p(all, c),
    lists:foreach(fun(M) -> {ok, _} = dbg:tp(M, cx) end, Mods).
