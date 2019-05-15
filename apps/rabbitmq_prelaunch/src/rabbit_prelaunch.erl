-module(rabbit_prelaunch).

-include_lib("eunit/include/eunit.hrl").

-export([run_prelaunch_first_phase/0,
         assert_mnesia_is_stopped/0,
         get_context/0,
         get_boot_state/0,
         set_boot_state/1,
         is_boot_state_reached/1,
         wait_for_boot_state/1,
         wait_for_boot_state/2,
         get_stop_reason/0,
         set_stop_reason/1,
         clear_stop_reason/0,
         is_initial_pass/0,
         initial_pass_finished/0,
         shutdown_func/1]).

-define(PT_KEY_CONTEXT,       {?MODULE, context}).
-define(PT_KEY_BOOT_STATE,    {?MODULE, boot_state}).
-define(PT_KEY_INITIAL_PASS,  {?MODULE, initial_pass_finished}).
-define(PT_KEY_SHUTDOWN_FUNC, {?MODULE, chained_shutdown_func}).
-define(PT_KEY_STOP_REASON,   {?MODULE, stop_reason}).

run_prelaunch_first_phase() ->
    try
        do_run()
    catch
        throw:{error, _} = Error ->
            rabbit_prelaunch_errors:log_error(Error),
            set_stop_reason(Error),
            set_boot_state(stopped),
            Error;
        Class:Exception:Stacktrace ->
            rabbit_prelaunch_errors:log_exception(
              Class, Exception, Stacktrace),
            Error = {error, Exception},
            set_stop_reason(Error),
            set_boot_state(stopped),
            Error
    end.

do_run() ->
    %% Indicate RabbitMQ is booting.
    clear_stop_reason(),
    set_boot_state(booting),

    %% Configure dbg if requested.
    rabbit_prelaunch_early_logging:enable_quick_dbg(rabbit_env:dbg_config()),

    %% We assert Mnesia is stopped before we run the prelaunch
    %% phases.
    %%
    %% We need this because our cluster consistency check (in the second
    %% phase) depends on Mnesia not being started before it has a chance
    %% to run.
    %%
    %% Also, in the initial pass, we don't want Mnesia to run before
    %% Erlang distribution is configured.
    assert_mnesia_is_stopped(),

    %% Get informations to setup logging.
    Context0 = rabbit_env:get_context_before_logging_init(),
    ?assertMatch(#{}, Context0),

    %% Setup logging for the prelaunch phase.
    ok = rabbit_prelaunch_early_logging:setup_early_logging(Context0, true),

    IsInitialPass = is_initial_pass(),
    case IsInitialPass of
        true ->
            rabbit_log_prelaunch:debug(""),
            rabbit_log_prelaunch:debug(
              "== Prelaunch phase [1/2] (initial pass) =="),
            rabbit_log_prelaunch:debug("");
        false ->
            rabbit_log_prelaunch:debug(""),
            rabbit_log_prelaunch:debug("== Prelaunch phase [1/2] =="),
            rabbit_log_prelaunch:debug("")
    end,
    rabbit_env:log_process_env(),

    %% Load rabbitmq-env.conf, redo logging setup and continue.
    Context1 = rabbit_env:get_context_after_logging_init(Context0),
    ?assertMatch(#{}, Context1),
    ok = rabbit_prelaunch_early_logging:setup_early_logging(Context1, true),
    rabbit_env:log_process_env(),

    %% Complete context now that we have the final environment loaded.
    Context2 = rabbit_env:get_context_after_reloading_env(Context1),
    ?assertMatch(#{}, Context2),
    store_context(Context2),
    rabbit_env:log_context(Context2),
    ok = setup_shutdown_func(),

    Context = Context2#{initial_pass => IsInitialPass},

    rabbit_env:context_to_code_path(Context),
    rabbit_env:context_to_app_env_vars(Context),

    %% 1. Erlang/OTP compatibility check.
    ok = rabbit_prelaunch_erlang_compat:check(Context),

    %% 2. Erlang distribution check + start.
    ok = rabbit_prelaunch_dist:setup(Context),

    %% 3. Write PID file.
    rabbit_log_prelaunch:debug(""),
    _ = write_pid_file(Context),
    ignore.

assert_mnesia_is_stopped() ->
    ?assertNot(lists:keymember(mnesia, 1, application:which_applications())).

store_context(Context) when is_map(Context) ->
    persistent_term:put(?PT_KEY_CONTEXT, Context).

get_context() ->
    case persistent_term:get(?PT_KEY_CONTEXT, undefined) of
        undefined -> undefined;
        Context   -> Context#{initial_pass => is_initial_pass()}
    end.

get_boot_state() ->
    persistent_term:get(?PT_KEY_BOOT_STATE, stopped).

set_boot_state(stopped) ->
    rabbit_log_prelaunch:debug("Change boot state to `stopped`"),
    persistent_term:erase(?PT_KEY_BOOT_STATE);
set_boot_state(BootState) ->
    rabbit_log_prelaunch:debug("Change boot state to `~s`", [BootState]),
    ?assert(is_boot_state_valid(BootState)),
    persistent_term:put(?PT_KEY_BOOT_STATE, BootState).

wait_for_boot_state(BootState) ->
    wait_for_boot_state(BootState, infinity).

wait_for_boot_state(BootState, Timeout) ->
    ?assert(is_boot_state_valid(BootState)),
    wait_for_boot_state1(BootState, Timeout).

wait_for_boot_state1(BootState, infinity = Timeout) ->
    case is_boot_state_reached(BootState) of
        true  -> ok;
        false -> wait_for_boot_state1(BootState, Timeout)
    end;
wait_for_boot_state1(BootState, Timeout)
  when is_integer(Timeout) andalso Timeout >= 0 ->
    case is_boot_state_reached(BootState) of
        true  -> ok;
        false -> Wait = 200,
                 timer:sleep(Wait),
                 wait_for_boot_state1(BootState, Timeout - Wait)
    end;
wait_for_boot_state1(_, _) ->
    {error, timeout}.

boot_state_idx(stopped)  -> 0;
boot_state_idx(booting)  -> 1;
boot_state_idx(ready)    -> 2;
boot_state_idx(stopping) -> 3;
boot_state_idx(_)        -> undefined.

is_boot_state_valid(BootState) ->
    is_integer(boot_state_idx(BootState)).

is_boot_state_reached(TargetBootState) ->
    is_boot_state_reached(get_boot_state(), TargetBootState).

is_boot_state_reached(CurrentBootState, CurrentBootState) ->
    true;
is_boot_state_reached(stopping, stopped) ->
    false;
is_boot_state_reached(_CurrentBootState, stopped) ->
    true;
is_boot_state_reached(stopped, _TargetBootState) ->
    true;
is_boot_state_reached(CurrentBootState, TargetBootState) ->
    boot_state_idx(TargetBootState) =< boot_state_idx(CurrentBootState).

get_stop_reason() ->
    persistent_term:get(?PT_KEY_STOP_REASON, undefined).

set_stop_reason(Reason) ->
    case get_stop_reason() of
        undefined ->
            rabbit_log_prelaunch:debug("Set stop reason to: ~p", [Reason]),
            persistent_term:put(?PT_KEY_STOP_REASON, Reason);
        _ ->
            ok
    end.

clear_stop_reason() ->
    persistent_term:erase(?PT_KEY_STOP_REASON).

is_initial_pass() ->
    not persistent_term:get(?PT_KEY_INITIAL_PASS, false).

initial_pass_finished() ->
    persistent_term:put(?PT_KEY_INITIAL_PASS, true).

setup_shutdown_func() ->
    ThisMod = ?MODULE,
    ThisFunc = shutdown_func,
    ExistingShutdownFunc = application:get_env(kernel, shutdown_func),
    case ExistingShutdownFunc of
        {ok, {ThisMod, ThisFunc}} ->
            ok;
        {ok, {ExistingMod, ExistingFunc}} ->
            rabbit_log_prelaunch:debug(
              "Setting up kernel shutdown function: ~s:~s/1 "
              "(chained with ~s:~s/1)",
              [ThisMod, ThisFunc, ExistingMod, ExistingFunc]),
            ok = persistent_term:put(
                   ?PT_KEY_SHUTDOWN_FUNC,
                   ExistingShutdownFunc),
            ok = record_kernel_shutdown_func(ThisMod, ThisFunc);
        _ ->
            rabbit_log_prelaunch:debug(
              "Setting up kernel shutdown function: ~s:~s/1",
              [ThisMod, ThisFunc]),
            ok = record_kernel_shutdown_func(ThisMod, ThisFunc)
    end.

record_kernel_shutdown_func(Mod, Func) ->
    application:set_env(
      kernel, shutdown_func, {Mod, Func},
      [{persistent, true}]).

shutdown_func(Reason) ->
    rabbit_log_prelaunch:debug(
      "Running ~s:shutdown_func() as part of `kernel` shutdown", [?MODULE]),
    Context = get_context(),
    remove_pid_file(Context),
    ChainedShutdownFunc = persistent_term:get(
                            ?PT_KEY_SHUTDOWN_FUNC,
                            undefined),
    case ChainedShutdownFunc of
        {ChainedMod, ChainedFunc} -> ChainedMod:ChainedFunc(Reason);
        _                         -> ok
    end.

write_pid_file(#{pid_file := PidFile}) ->
    rabbit_log_prelaunch:debug("Writing PID file: ~s", [PidFile]),
    case filelib:ensure_dir(PidFile) of
        ok ->
            OSPid = os:getpid(),
            case file:write_file(PidFile, OSPid) of
                ok ->
                    ok;
                {error, Reason} = Error ->
                    rabbit_log_prelaunch:warning(
                      "Failed to write PID file \"~s\": ~s",
                      [PidFile, file:format_error(Reason)]),
                    Error
            end;
        {error, Reason} = Error ->
            rabbit_log_prelaunch:warning(
              "Failed to create PID file \"~s\" directory: ~s",
              [PidFile, file:format_error(Reason)]),
            Error
    end;
write_pid_file(_) ->
    ok.

remove_pid_file(#{pid_file := PidFile, keep_pid_file_on_exit := true}) ->
    rabbit_log_prelaunch:debug("Keeping PID file: ~s", [PidFile]),
    ok;
remove_pid_file(#{pid_file := PidFile}) ->
    rabbit_log_prelaunch:debug("Deleting PID file: ~s", [PidFile]),
    _ = file:delete(PidFile);
remove_pid_file(_) ->
    ok.
