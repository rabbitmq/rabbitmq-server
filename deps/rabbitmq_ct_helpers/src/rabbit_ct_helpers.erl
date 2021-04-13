%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2016-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_ct_helpers).

-include_lib("common_test/include/ct.hrl").

-export([
    log_environment/0,
    run_steps/2,
    run_setup_steps/1, run_setup_steps/2,
    run_teardown_steps/1, run_teardown_steps/2,
    register_teardown_step/2,
    register_teardown_steps/2,
    guess_tested_erlang_app_name/1,
    ensure_application_srcdir/3,
    ensure_application_srcdir/4,
    ensure_rabbitmqctl_cmd/1,
    ensure_rabbitmqctl_app/1,
    load_rabbitmqctl_app/1,
    ensure_rabbitmq_plugins_cmd/1,
    ensure_rabbitmq_queues_cmd/1,
    start_long_running_testsuite_monitor/1,
    stop_long_running_testsuite_monitor/1,
    config_to_testcase_name/2,
    testcases/1,
    testcase_number/3,
    testcase_absname/2, testcase_absname/3,
    testcase_started/2, testcase_finished/2,
    term_checksum/1,
    random_term_checksum/0,
    exec/1, exec/2,
    make/3, make/4,
    get_config/2, get_config/3, set_config/2, delete_config/2,
    merge_app_env/2, merge_app_env_in_erlconf/2,
    get_app_env/4,
    nodename_to_hostname/1,
    convert_to_unicode_binary/1,
    cover_work_factor/2,

    await_condition/1,
    await_condition/2,
    await_condition_with_retries/2
  ]).

-define(SSL_CERT_PASSWORD, "test").

%% -------------------------------------------------------------------
%% Testsuite internal helpers.
%% -------------------------------------------------------------------

log_environment() ->
    Vars = lists:sort(fun(A, B) -> A =< B end, os:getenv()),
    case file:native_name_encoding() of
        latin1 ->
            ct:pal(?LOW_IMPORTANCE, "Environment variables:~n~s",
                   [[io_lib:format("  ~s~n", [V]) || V <- Vars]]);
        utf8 ->
            ct:pal(?LOW_IMPORTANCE, "Environment variables:~n~ts",
                   [[io_lib:format("  ~ts~n", [V]) || V <- Vars]])
    end.

run_setup_steps(Config) ->
    run_setup_steps(Config, []).

run_setup_steps(Config, ExtraSteps) ->
    Steps = case os:getenv("RABBITMQ_RUN") of
        false ->
            [
                fun init_skip_as_error_flag/1,
                fun guess_tested_erlang_app_name/1,
                fun ensure_secondary_umbrella/1,
                fun ensure_current_srcdir/1,
                fun ensure_rabbitmq_ct_helpers_srcdir/1,
                fun ensure_erlang_mk_depsdir/1,
                fun ensure_secondary_erlang_mk_depsdir/1,
                fun ensure_secondary_current_srcdir/1,
                fun ensure_rabbit_common_srcdir/1,
                fun ensure_rabbitmq_cli_srcdir/1,
                fun ensure_rabbit_srcdir/1,
                fun ensure_make_cmd/1,
                fun ensure_erl_call_cmd/1,
                fun ensure_ssl_certs/1,
                fun start_long_running_testsuite_monitor/1,
                fun load_elixir/1
            ];
        _ ->
            [
                fun init_skip_as_error_flag/1,
                fun ensure_secondary_umbrella/1,
                fun ensure_current_srcdir/1,
                fun ensure_rabbitmq_ct_helpers_srcdir/1,
                fun maybe_rabbit_srcdir/1,
                fun ensure_make_cmd/1,
                fun ensure_rabbitmq_run_cmd/1,
                fun ensure_ssl_certs/1,
                fun start_long_running_testsuite_monitor/1
            ]
    end,
    run_steps(Config, Steps ++ ExtraSteps).

run_teardown_steps(Config) ->
    run_teardown_steps(Config, []).

run_teardown_steps(Config, ExtraSteps) ->
    RegisteredSteps = get_config(Config, teardown_steps, []),
    Steps = [
      fun stop_long_running_testsuite_monitor/1,
      fun symlink_priv_dir/1
    ],
    run_steps(Config, ExtraSteps ++ RegisteredSteps ++ Steps).

register_teardown_step(Config, Step) ->
    register_teardown_steps(Config, [Step]).

register_teardown_steps(Config, Steps) ->
    RegisteredSteps = get_config(Config, teardown_steps, []),
    set_config(Config, {teardown_steps, Steps ++ RegisteredSteps}).

run_steps(Config, [Step | Rest]) ->
    SkipAsError = case get_config(Config, skip_as_error) of
                      undefined -> false;
                      Value     -> Value
                  end,
    case Step(Config) of
        {skip, Reason} when SkipAsError ->
            run_teardown_steps(Config),
            exit(Reason);
        {skip, _} = Error ->
            run_teardown_steps(Config),
            Error;
        Config1 when is_list(Config1) ->
            run_steps(Config1, Rest);
        Other ->
            ct:pal(?LOW_IMPORTANCE,
                "~p:~p/~p failed with ~p steps remaining (Config value ~p is not a proplist)",
                [?MODULE, ?FUNCTION_NAME, ?FUNCTION_ARITY, length(Rest), Other]),
            run_teardown_steps(Config),
            exit("A setup step returned a non-proplist")
    end;
run_steps(Config, []) ->
    Config.

init_skip_as_error_flag(Config) ->
    SkipAsError = case os:getenv("RABBITMQ_CT_SKIP_AS_ERROR") of
                      false -> false;
                      Value -> REOpts = [{capture, none}, caseless],
                               case re:run(Value, "^(1|yes|true)$", REOpts) of
                                   nomatch -> false;
                                   match   -> true
                               end
                  end,
    set_config(Config, {skip_as_error, SkipAsError}).

guess_tested_erlang_app_name(Config) ->
    case os:getenv("DIALYZER_PLT") of
        false ->
            {skip,
             "plt file required, please set DIALYZER_PLT"};
        Filename ->
            AppName0 = filename:basename(Filename, ".plt"),
            AppName = string:strip(AppName0, left, $.),
            set_config(Config, {tested_erlang_app, list_to_atom(AppName)})
    end.

ensure_secondary_umbrella(Config) ->
    Path = case get_config(Config, secondary_umbrella) of
               undefined -> os:getenv("SECONDARY_UMBRELLA");
               P         -> P
           end,
    case Path =/= false andalso filelib:is_dir(Path) of
        true  -> set_config(Config, {secondary_umbrella, Path});
        false -> set_config(Config, {secondary_umbrella, false})
    end.

ensure_current_srcdir(Config) ->
    Path = case get_config(Config, current_srcdir) of
        undefined -> os:getenv("PWD");
        P         -> P
    end,
    case filelib:is_dir(Path) of
        true  -> set_config(Config, {current_srcdir, Path});
        false -> {skip,
                  "Current source directory required, " ++
                  "please set 'current_srcdir' in ct config"}
    end.

ensure_rabbitmq_ct_helpers_srcdir(Config) ->
    Path = case get_config(Config, rabbitmq_ct_helpers_srcdir) of
        undefined ->
            filename:dirname(
              filename:dirname(
                code:which(?MODULE)));
        P ->
            P
    end,
    case filelib:is_dir(Path) of
        true  -> set_config(Config, {rabbitmq_ct_helpers_srcdir, Path});
        false -> {skip,
                  "rabbitmq_ct_helpers source directory required, " ++
                  "please set 'rabbitmq_ct_helpers_srcdir' in ct config"}
    end.

ensure_erlang_mk_depsdir(Config) ->
    Path = case get_config(Config, erlang_mk_depsdir) of
        undefined ->
            case os:getenv("DEPS_DIR") of
                false ->
                    %% Try the common locations.
                    SrcDir = ?config(rabbitmq_ct_helpers_srcdir, Config),
                    Ds = [
                      filename:join(SrcDir, "deps"),
                      filename:join(SrcDir, "../../deps")
                    ],
                    case lists:filter(fun filelib:is_dir/1, Ds) of
                        [P |_] -> P;
                        []     -> false
                    end;
                P ->
                    P
            end;
        P ->
            P
    end,
    case Path =/= false andalso filelib:is_dir(Path) of
        true  -> set_config(Config, {erlang_mk_depsdir, Path});
        false -> {skip,
                  "deps directory required, " ++
                  "please set DEPS_DIR or 'erlang_mk_depsdir' " ++
                  "in ct config"}
    end.

ensure_secondary_erlang_mk_depsdir(Config) ->
    Path = case get_config(Config, secondary_erlang_mk_depsdir) of
               undefined ->
                   case ?config(secondary_umbrella, Config) of
                       false       -> ?config(erlang_mk_depsdir, Config);
                       SecUmbrella -> filename:join(SecUmbrella, "deps")
                   end;
               P ->
                   P
           end,
    case filelib:is_dir(Path) of
        true  -> set_config(Config, {secondary_erlang_mk_depsdir, Path});
        false -> {skip,
                  "Secondary deps directory required, " ++
                  "please set 'secondary_erlang_mk_depsdir' in ct config"}
    end.

ensure_secondary_current_srcdir(Config) ->
    Path = case get_config(Config, secondary_current_srcdir) of
               undefined ->
                   case ?config(secondary_umbrella, Config) of
                       false ->
                           ?config(current_srcdir, Config);
                       _ ->
                           TestedAppName = ?config(tested_erlang_app, Config),
                           filename:join(
                             ?config(secondary_erlang_mk_depsdir, Config),
                             TestedAppName)
                   end;
               P ->
                   P
           end,
    case filelib:is_dir(Path) of
        true  -> set_config(Config, {secondary_current_srcdir, Path});
        false -> {skip,
                  "Secondary current source directory required, " ++
                  "please set 'secondary_current_srcdir' in ct config"}
    end.

ensure_rabbit_common_srcdir(Config) ->
    ensure_application_srcdir(Config, rabbit_common, rabbit_misc).

ensure_rabbitmq_cli_srcdir(Config) ->
    ensure_application_srcdir(Config, rabbitmq_cli, elixir, 'Elixir.RabbitMQCtl').

ensure_rabbit_srcdir(Config) ->
    ensure_application_srcdir(Config, rabbit, rabbit).

maybe_rabbit_srcdir(Config) ->
    % Some tests under bazel use this value, others do not.
    % By allowing this config to be optional, we avoid making
    % more tests depend on rabbit
    case ensure_application_srcdir(Config, rabbit, rabbit) of
        {skip, _} -> Config;
        Config1 -> Config1
    end.

ensure_application_srcdir(Config, App, Module) ->
    ensure_application_srcdir(Config, App, erlang, Module).

ensure_application_srcdir(Config, App, Lang, Module) ->
    AppS = atom_to_list(App),
    Key = list_to_atom(AppS ++ "_srcdir"),
    SecondaryKey = list_to_atom("secondary_" ++ AppS ++ "_srcdir"),
    Path = case get_config(Config, Key) of
        undefined ->
            case code:which(Module) of
                non_existing ->
                    filename:join(?config(erlang_mk_depsdir, Config), AppS);
                P when Lang =:= erlang ->
                    %% P is $SRCDIR/ebin/$MODULE.beam.
                    filename:dirname(
                      filename:dirname(P));
                P when Lang =:= elixir ->
                    %% P is $SRCDIR/_build/$MIX_ENV/lib/$APP/ebin/$MODULE.beam.
                    filename:dirname(
                      filename:dirname(
                        filename:dirname(
                          filename:dirname(
                            filename:dirname(
                              filename:dirname(P))))))
            end;
        P ->
            P
    end,
    SecondaryPath = case ?config(secondary_umbrella, Config) of
                        false ->
                            Path;
                        _ ->
                            case get_config(Config, SecondaryKey) of
                                undefined ->
                                    filename:join(
                                      ?config(secondary_erlang_mk_depsdir,
                                              Config),
                                      AppS);
                                SP ->
                                    SP
                            end
                    end,
    case filelib:is_dir(Path) andalso filelib:is_dir(SecondaryPath) of
        true  -> set_config(Config,
                            [{Key, Path},
                             {SecondaryKey, SecondaryPath}]);
        false -> {skip,
                  AppS ++ " source directory required, " ++
                  "please set '" ++ AppS ++ "_srcdir' in ct config"}
    end.

ensure_make_cmd(Config) ->
    Make = case get_config(Config, make_cmd) of
        undefined ->
            case os:getenv("MAKE") of
                false -> "make";
                M     -> M
            end;
        M ->
            M
    end,
    Cmd = [Make, "--version"],
    case exec(Cmd, [{match_stdout, "GNU Make"}]) of
        {ok, _} -> set_config(Config, {make_cmd, Make});
        _       -> {skip,
                    "GNU Make required, " ++
                    "please set MAKE or 'make_cmd' in ct config"}
    end.

ensure_rabbitmq_run_cmd(Config) ->
    case os:getenv("RABBITMQ_RUN") of
        false ->
            {skip,
             "Bazel helper rabbitmq-run required, " ++
             "please set RABBITMQ_RUN"};
        P ->
            set_config(Config, {rabbitmq_run_cmd, P})
    end.

ensure_erl_call_cmd(Config) ->
    ErlCallDir = code:lib_dir(erl_interface, bin),
    ErlCall = filename:join(ErlCallDir, "erl_call"),
    Cmd = [ErlCall],
    case exec(Cmd, [{match_stdout, "Usage: "}]) of
        {ok, _} -> set_config(Config, {erl_call_cmd, ErlCall});
        _       -> {skip,
                    "erl_call required, " ++
                    "please set ERL_CALL or 'erl_call_cmd' in ct config"}
    end.

ensure_rabbitmqctl_cmd(Config) ->
    Rabbitmqctl = case get_config(Config, rabbitmqctl_cmd) of
        undefined ->
            case os:getenv("RABBITMQCTL") of
                false ->
                    find_script(Config, "rabbitmqctl");
                R ->
                    ct:pal(?LOW_IMPORTANCE,
                      "Using rabbitmqctl from RABBITMQCTL: ~p~n", [R]),
                    R
            end;
        R ->
            ct:pal(?LOW_IMPORTANCE,
              "Using rabbitmqctl from rabbitmqctl_cmd: ~p~n", [R]),
            R
    end,
    Error = {skip, "rabbitmqctl required, " ++
             "please set RABBITMQCTL or 'rabbitmqctl_cmd' in ct config"},
    case Rabbitmqctl of
        false ->
            Error;
        _ ->
            Cmd = [Rabbitmqctl],
            Env = [
                   {"RABBITMQ_SCRIPTS_DIR", filename:dirname(Rabbitmqctl)}
                  ],
            case exec(Cmd, [drop_stdout, {env, Env}]) of
                {error, 64, _} ->
                    set_config(Config, {rabbitmqctl_cmd, Rabbitmqctl});
                {error, Code, Reason} ->
                    ct:pal("Exec failed with exit code ~p: ~p", [Code, Reason]),
                    Error;
                _ ->
                    Error
            end
    end.

find_script(Config, Script) ->
    Locations = [File
                 || File <- [new_script_location(Config, Script),
                             old_script_location(Config, Script)],
                    filelib:is_file(File)],
    case Locations of
        [Location | _] ->
            ct:pal(?LOW_IMPORTANCE, "Using ~s at ~p~n", [Script, Location]),
            Location;
        [] ->
            false
    end.

old_script_location(Config, Script) ->
    SrcDir = ?config(rabbit_srcdir, Config),
    filename:join([SrcDir, "scripts", Script]).

new_script_location(Config, Script) ->
    SrcDir = ?config(current_srcdir, Config),
    filename:join([SrcDir, "sbin", Script]).

ensure_rabbitmqctl_app(Config) ->
    SrcDir = ?config(rabbitmq_cli_srcdir, Config),
    MixEnv = os:getenv("MIX_ENV", "dev"),
    EbinDir = filename:join(
      [SrcDir, "_build", MixEnv, "lib", "rabbitmqctl", "ebin"]),
    case filelib:is_file(filename:join(EbinDir, "rabbitmqctl.app")) of
        true ->
            true = code:add_path(EbinDir),
            case application:load(rabbitmqctl) of
                ok ->
                    Config;
                {error, {already_loaded, rabbitmqctl}} ->
                    Config;
                {error, _} ->
                    {skip, "Access to rabbitmq_cli ebin dir. required, " ++
                     "please build rabbitmq_cli and set MIX_ENV"}
            end;
        false ->
            {skip, "Access to rabbitmq_cli ebin dir. required, " ++
             "please build rabbitmq_cli and set MIX_ENV"}
    end.

load_rabbitmqctl_app(Config) ->
    case application:load(rabbitmqctl) of
        ok ->
            Config;
        {error, {already_loaded, rabbitmqctl}} ->
            Config;
        {error, _} ->
            {skip, "Application rabbitmqctl could not be loaded, " ++
                "please place compiled rabbitmq_cli on the code path"}
    end.

ensure_rabbitmq_plugins_cmd(Config) ->
    Rabbitmqplugins = case get_config(Config, rabbitmq_plugins_cmd) of
        undefined ->
            case os:getenv("RABBITMQ_PLUGINS") of
                false -> find_script(Config, "rabbitmq-plugins");
                R -> R
            end;
        R ->
            R
    end,
    Error = {skip, "rabbitmq_plugins required, " ++
             "please set RABBITMQ_PLUGINS or 'rabbitmq_plugins_cmd' in ct config"},
    case Rabbitmqplugins of
        false ->
            Error;
        _ ->
            Cmd = [Rabbitmqplugins],
            Env = [
                   {"RABBITMQ_SCRIPTS_DIR", filename:dirname(Rabbitmqplugins)}
                  ],
            case exec(Cmd, [drop_stdout, {env, Env}]) of
                {error, 64, _} ->
                    set_config(Config, {rabbitmq_plugins_cmd, Rabbitmqplugins});
                _ ->
                    Error
            end
    end.

ensure_rabbitmq_queues_cmd(Config) ->
    RabbitmqQueues = case get_config(Config, rabbitmq_queues_cmd) of
        undefined ->
            case os:getenv("RABBITMQ_QUEUES") of
                false -> find_script(Config, "rabbitmq-queues");
                R -> R
            end;
        R ->
            ct:pal(?LOW_IMPORTANCE,
              "Using rabbitmq-queues from rabbitmq_queues_cmd: ~p~n", [R]),
            R
    end,
    Error = {skip, "rabbitmq-queues required, " ++
             "please set 'rabbitmq_queues_cmd' in ct config"},
    case RabbitmqQueues of
        false ->
            Error;
        _ ->
            Cmd = [RabbitmqQueues],
            Env = [
                   {"RABBITMQ_SCRIPTS_DIR", filename:dirname(RabbitmqQueues)}
                  ],
            case exec(Cmd, [drop_stdout, {env, Env}]) of
                {error, 64, _} ->
                    set_config(Config,
                               {rabbitmq_queues_cmd,
                                RabbitmqQueues});
                {error, Code, Reason} ->
                    ct:pal("Exec failed with exit code ~p: ~p", [Code, Reason]),
                    Error;
                _ ->
                    Error
            end
    end.

ensure_ssl_certs(Config) ->
    SrcDir = ?config(rabbitmq_ct_helpers_srcdir, Config),
    CertsMakeDir = filename:join([SrcDir, "tools", "tls-certs"]),
    PrivDir = ?config(priv_dir, Config),
    CertsDir = filename:join(PrivDir, "certs"),
    CertsPwd = proplists:get_value(rmq_certspwd, Config, ?SSL_CERT_PASSWORD),
    Cmd = [
      "PASSWORD=" ++ CertsPwd,
      "DIR=" ++ CertsDir],
    case make(Config, CertsMakeDir, Cmd) of
        {ok, _} ->
            %% Add SSL certs to the broker configuration.
            Verify = get_config(Config, rabbitmq_ct_tls_verify, verify_peer),
            FailIfNoPeerCert = get_config(
                                 Config,
                                 rabbitmq_ct_tls_fail_if_no_peer_cert,
                                 true) andalso Verify =/= verify_none,
            Config1 = merge_app_env(Config,
              {rabbit, [
                  {ssl_options, [
                      {cacertfile,
                       filename:join([CertsDir, "testca", "cacert.pem"])},
                      {certfile,
                       filename:join([CertsDir, "server", "cert.pem"])},
                      {keyfile,
                       filename:join([CertsDir, "server", "key.pem"])},
                      {verify, Verify},
                      {fail_if_no_peer_cert, FailIfNoPeerCert}
                    ]}]}),
            set_config(Config1, {rmq_certsdir, CertsDir});
        _ ->
            {skip, "Failed to create SSL certificates"}
    end.

link_name(["deps", _ | Tail]) ->
    case lists:reverse(Tail) of
        ["logs" | Rest] ->
            string:join(lists:reverse(["private_log" | Rest]), ".");
        _ ->
            string:join(Tail, ".")
    end;
link_name(X) -> X.

get_selection_from_tc_logfile(["logs", _, S | _Tail]) ->
    {ok, link_name(string:tokens(S, "."))};
get_selection_from_tc_logfile([_ | Tail]) ->
    get_selection_from_tc_logfile(Tail);
get_selection_from_tc_logfile([]) -> not_found.

get_selection(Config) ->
    TcLogFile = ?config(tc_logfile, Config),
    get_selection_from_tc_logfile(filename:split(TcLogFile)).


symlink_priv_dir(Config) ->
    case os:type() of
        {win32, _} ->
            Config;
        _ ->
            SrcDir = ?config(current_srcdir, Config),
            PrivDir = ?config(priv_dir, Config),
            case get_selection(Config) of
                {ok, Name} ->
                    Target = filename:join([SrcDir, "logs", Name]),
                    case exec(["ln", "-snf", PrivDir, Target]) of
                        {ok, _} -> ok;
                        _ -> ct:pal(?LOW_IMPORTANCE,
                                    "Failed to symlink private_log directory.")
                    end,
                    Config;
                not_found ->
                    ct:pal(?LOW_IMPORTANCE,
                           "Failed to symlink private_log directory."),
                    Config
            end
    end.

%% -------------------------------------------------------------------
%% Process to log a message every minute during long testcases.
%% -------------------------------------------------------------------

-define(PING_CT_INTERVAL, 60 * 1000). %% In milliseconds.

start_long_running_testsuite_monitor(Config) ->
    Pid = spawn(
      fun() ->
          {ok, TimerRef} = timer:send_interval(?PING_CT_INTERVAL, ping_ct),
          long_running_testsuite_monitor(TimerRef, [])
      end),
    set_config(Config, {long_running_testsuite_monitor, Pid}).

load_elixir(Config) ->
    case find_elixir_home() of
        {skip, _} = Skip ->
            Skip;
        ElixirLibDir ->
            ct:pal(?LOW_IMPORTANCE, "Elixir lib dir: ~s~n", [ElixirLibDir]),
            true = code:add_pathz(ElixirLibDir),
            application:load(elixir),
            {ok, _} = application:ensure_all_started(elixir),
            Config
    end.

find_elixir_home() ->
    ElixirExe = case os:type() of
                    {unix, _}  -> "elixir";
                    {win32, _} -> "elixir.bat"
                end,
    case os:find_executable(ElixirExe) of
        false   -> {skip, "Failed to locate Elixir executable"};
        ExePath -> resolve_symlink(ExePath)
    end.

resolve_symlink(ExePath) ->
    case file:read_link_all(ExePath) of
        {error, einval} ->
            determine_elixir_home(ExePath);
        {ok, ResolvedLink} ->
            ExePath1 = filename:absname(ResolvedLink,
                                        filename:dirname(ExePath)),
            resolve_symlink(ExePath1);
        {error, Reason} ->
            Msg = rabbit_misc:format("Failed to locate Elixir home: ~p",
                                     [file:format_error(Reason)]),
            {skip, Msg}
    end.

determine_elixir_home(ExePath) ->
    LibPath = filename:join([filename:dirname(filename:dirname(ExePath)),
                             "lib",
                             "elixir",
                             "ebin"]),
    case filelib:is_dir(LibPath) of
        true  -> LibPath;
        false -> {skip, "Failed to locate Elixir lib dir"}
    end.

stop_long_running_testsuite_monitor(Config) ->
    case get_config(Config, long_running_testsuite_monitor) of
        undefined -> ok;
        Pid       -> Pid ! stop
    end,
    Config.

long_running_testsuite_monitor(TimerRef, Testcases) ->
    receive
        {started, Testcase} ->
            Testcases1 = [{Testcase, erlang:monotonic_time(seconds)}
                          | Testcases],
            long_running_testsuite_monitor(TimerRef, Testcases1);
        {finished, Testcase} ->
            Testcases1 = proplists:delete(Testcase, Testcases),
            long_running_testsuite_monitor(TimerRef, Testcases1);
        ping_ct ->
            T1 = erlang:monotonic_time(seconds),
            ct:pal(?STD_IMPORTANCE, "Testcases still in progress:~s",
              [[
                  begin
                      TDiff = format_time_diff(T1, T0),
                      rabbit_misc:format("~n - ~s (~s)", [TC, TDiff])
                  end
                  || {TC, T0} <- Testcases
                ]]),
            long_running_testsuite_monitor(TimerRef, Testcases);
        stop ->
            timer:cancel(TimerRef)
    end.

format_time_diff(T1, T0) ->
    Diff = T1 - T0,
    Hours = Diff div 3600,
    Diff1 = Diff rem 3600,
    Minutes = Diff1 div 60,
    Seconds = Diff1 rem 60,
    rabbit_misc:format("~b:~2..0b:~2..0b", [Hours, Minutes, Seconds]).

testcase_started(Config, Testcase) ->
    Testcase1 = config_to_testcase_name(Config, Testcase),
    ?config(long_running_testsuite_monitor, Config) ! {started, Testcase1},
    Config.

testcase_finished(Config, Testcase) ->
    Testcase1 = config_to_testcase_name(Config, Testcase),
    ?config(long_running_testsuite_monitor, Config) ! {finished, Testcase1},
    Config.

config_to_testcase_name(Config, Testcase) ->
    testcase_absname(Config, Testcase).

testcase_absname(Config, Testcase) ->
    testcase_absname(Config, Testcase, "/").

testcase_absname(Config, Testcase, Sep) ->
    Name = rabbit_misc:format("~s", [Testcase]),
    case get_config(Config, tc_group_properties) of
        [] ->
            Name;
        Props ->
            Name1 = case Name of
                "" ->
                    rabbit_misc:format("~s",
                      [proplists:get_value(name, Props)]);
                _ ->
                    rabbit_misc:format("~s~s~s",
                      [proplists:get_value(name, Props), Sep, Name])
            end,
            testcase_absname1(Name1,
              get_config(Config, tc_group_path), Sep)
    end.

testcase_absname1(Name, [Props | Rest], Sep) ->
    Name1 = rabbit_misc:format("~s~s~s",
      [proplists:get_value(name, Props), Sep, Name]),
    testcase_absname1(Name1, Rest, Sep);
testcase_absname1(Name, [], _) ->
    lists:flatten(Name).

testcases(Testsuite) ->
    All = Testsuite:all(),
    testcases1(Testsuite, All, [], []).

testcases1(Testsuite, [{group, GroupName} | Rest], CurrentPath, Testcases) ->
    Group = {GroupName, _, _} = lists:keyfind(GroupName, 1, Testsuite:groups()),
    testcases1(Testsuite, [Group | Rest], CurrentPath, Testcases);
testcases1(Testsuite, [{GroupName, _, Children} | Rest],
  CurrentPath, Testcases) ->
    Testcases1 = testcases1(Testsuite, Children,
      [[{name, GroupName}] | CurrentPath], Testcases),
    testcases1(Testsuite, Rest, CurrentPath, Testcases1);
testcases1(Testsuite, [Testcase | Rest], CurrentPath, Testcases)
when is_atom(Testcase) ->
    {Props, Path} = case CurrentPath of
        []      -> {[], []};
        [H | T] -> {H, T}
    end,
    Name = config_to_testcase_name([
        {tc_group_properties, Props},
        {tc_group_path, Path}
      ], Testcase),
    testcases1(Testsuite, Rest, CurrentPath, [Name | Testcases]);
testcases1(_, [], [], Testcases) ->
    lists:reverse(Testcases);
testcases1(_, [], _, Testcases) ->
    Testcases.

testcase_number(Config, TestSuite, TestName) ->
    Testcase = config_to_testcase_name(Config, TestName),
    Testcases = testcases(TestSuite),
    testcase_number1(Testcases, Testcase, 0).

testcase_number1([Testcase | _], Testcase, N) ->
    N;
testcase_number1([_ | Rest], Testcase, N) ->
    testcase_number1(Rest, Testcase, N + 1);
testcase_number1([], _, N) ->
    N.

%% -------------------------------------------------------------------
%% Helpers for helpers.
%% -------------------------------------------------------------------

term_checksum(Term) ->
    Bin = term_to_binary(Term),
    <<Checksum:128/big-unsigned-integer>> = erlang:md5(Bin),
    rabbit_misc:format("~32.16.0b", [Checksum]).

random_term_checksum() ->
    term_checksum(rabbit_misc:random(1000000)).

exec(Cmd) ->
    exec(Cmd, []).

exec([Cmd | Args], Options) when is_list(Cmd) orelse is_binary(Cmd) ->
    Cmd0 = case (lists:member($/, Cmd) orelse lists:member($\\, Cmd)) of
        true ->
            Cmd;
        false ->
            case os:find_executable(Cmd) of
                false -> Cmd;
                Path  -> Path
            end
    end,
    Cmd1 = convert_to_unicode_binary(
             string:trim(
               rabbit_data_coercion:to_list(Cmd0))),
    Args1 = [convert_to_unicode_binary(format_arg(Arg)) || Arg <- Args],
    {LocalOptions, PortOptions} = lists:partition(
      fun
          ({match_stdout, _}) -> true;
          ({timeout, _})      -> true;
          (drop_stdout)       -> true;
          (_)                 -> false
      end, Options),
    PortOptions1 = case lists:member(nouse_stdio, PortOptions) of
        true  -> PortOptions;
        false -> [use_stdio, stderr_to_stdout | PortOptions]
    end,
    Log = "+ ~s (pid ~p)",
    ExportedEnvVars = ["ERL_INETRC"],
    ExportedEnv = lists:foldl(
                    fun(Var, Env) ->
                            case os:getenv(Var) of
                                false -> Env;
                                Value -> [{Var, Value} | Env]
                            end
                    end, [], ExportedEnvVars),
    {PortOptions2, Log1} = case proplists:get_value(env, PortOptions1) of
        undefined ->
            {[{env, ExportedEnv} | PortOptions1], Log};
        Env ->
            Env1 = [
              begin
                  Key1 = format_arg(Key),
                  Value1 = format_arg(Value),
                  Value2 = case is_binary(Value1) of
                               true  -> binary_to_list(Value1);
                               false -> Value1
                           end,
                  {Key1, Value2}
              end
              || {Key, Value} <- Env
            ],
            {
              [{env, Env1 ++ ExportedEnv}
               | proplists:delete(env, PortOptions1)],
              Log ++ "~n~nEnvironment variables:~n" ++
              string:join(
                [rabbit_misc:format("  ~s=~s", [K, V]) || {K, V} <- Env1],
                "~n")
            }
    end,
    %% Because Args1 may contain binaries, we don't use string:join().
    %% Instead we do a list comprehension.
    ArgsIoList = [Cmd1, [[$\s, Arg] || Arg <- Args1]],
    ct:pal(?LOW_IMPORTANCE, Log1, [ArgsIoList, self()]),
    try
        Port = erlang:open_port(
          {spawn_executable, Cmd1}, [
            {args, Args1},
            exit_status
            | PortOptions2]),

        case lists:keytake(timeout, 1, LocalOptions) of
            false ->
                port_receive_loop(Port, "", LocalOptions, infinity);
            {value, {timeout, infinity}, LocalOptions1} ->
                port_receive_loop(Port, "", LocalOptions1, infinity);
            {value, {timeout, Timeout}, LocalOptions1} ->
                Until = erlang:system_time(millisecond) + Timeout,
                port_receive_loop(Port, "", LocalOptions1, Until)
        end
    catch
        error:Reason ->
            ct:pal(?LOW_IMPORTANCE, "~s: ~s",
              [Cmd1, file:format_error(Reason)]),
            {error, Reason, file:format_error(Reason)}
    end.

format_arg({Format, FormatArgs}) ->
    rabbit_misc:format(Format, FormatArgs);
format_arg(Arg) when is_atom(Arg) ->
    atom_to_list(Arg);
format_arg(Arg) ->
    Arg.

port_receive_loop(Port, Stdout, Options, Until) ->
    port_receive_loop(Port, Stdout, Options, Until, stdout_dump_timer()).

port_receive_loop(Port, Stdout, Options, Until, DumpTimer) ->
  Timeout = case Until of
                infinity -> infinity;
                _ -> max(0, Until - erlang:system_time(millisecond))
            end,
  receive
      {Port, {exit_status, X}} ->
          timer:cancel(DumpTimer),
          DropStdout = lists:member(drop_stdout, Options) orelse
              Stdout =:= "",
          if
              DropStdout ->
                  ct:pal(?LOW_IMPORTANCE, "Exit code: ~p (pid ~p)",
                         [X, self()]);
              true ->
                  ct:pal(?LOW_IMPORTANCE, "~ts~nExit code: ~p (pid ~p)",
                         [Stdout, X, self()])
          end,
          case proplists:get_value(match_stdout, Options) of
              undefined ->
                  case X of
                      0 -> {ok, Stdout};
                      _ -> {error, X, Stdout}
                  end;
              RE ->
                  case re:run(Stdout, RE, [{capture, none}]) of
                      match   -> {ok, Stdout};
                      nomatch -> {error, X, Stdout}
                  end
          end;
      dump_output ->
          DropStdout = lists:member(drop_stdout, Options) orelse
              Stdout =:= "",
          if
              DropStdout ->
                  ok;
              true ->
                  ct:pal(?LOW_IMPORTANCE, "~ts~n[Command still in progress] (pid ~p)",
                         [Stdout, self()])
          end,
          port_receive_loop(Port, Stdout, Options, Until, stdout_dump_timer());
      {Port, {data, Out}} ->
          port_receive_loop(Port, Stdout ++ Out, Options, Until, DumpTimer)
  after
      Timeout ->
          {error, timeout, Stdout}
  end.

stdout_dump_timer() ->
    {ok, TRef} = timer:send_after(30000, dump_output),
    TRef.

make(Config, Dir, Args) ->
    make(Config, Dir, Args, []).

make(Config, Dir, Args, Options) ->
    Make = rabbit_ct_vm_helpers:get_current_vm_config(Config, make_cmd),
    Verbosity = case os:getenv("V") of
        false -> [];
        V     -> ["V=" ++ V]
    end,
    Cmd = [Make, "-C", Dir] ++ Verbosity ++ Args,
    exec(Cmd, Options).

%% This is the same as ?config(), except this one doesn't log a warning
%% if the key is missing.
get_config(Config, Key) ->
    proplists:get_value(Key, Config).

get_config(Config, Key, Default) ->
    proplists:get_value(Key, Config, Default).

set_config(Config, Tuple) when is_tuple(Tuple) ->
    Key = element(1, Tuple),
    lists:keystore(Key, 1, Config, Tuple);
set_config(Config, [Tuple | Rest]) ->
    Config1 = set_config(Config, Tuple),
    set_config(Config1, Rest);
set_config(Config, []) ->
    Config.

delete_config(Config, Key) ->
    proplists:delete(Key, Config).

get_app_env(Config, App, Key, Default) ->
    ErlangConfig = proplists:get_value(erlang_node_config, Config, []),
    AppConfig = proplists:get_value(App, ErlangConfig, []),
    proplists:get_value(Key, AppConfig, Default).

merge_app_env(Config, Env) ->
    ErlangConfig = proplists:get_value(erlang_node_config, Config, []),
    ErlangConfig1 = merge_app_env_in_erlconf(ErlangConfig, Env),
    set_config(Config, {erlang_node_config, ErlangConfig1}).

merge_app_env_in_erlconf(ErlangConfig, {App, Env}) ->
    AppConfig = proplists:get_value(App, ErlangConfig, []),
    AppConfig1 = lists:foldl(
      fun({Key, _} = Tuple, AC) ->
          lists:keystore(Key, 1, AC, Tuple)
      end, AppConfig, Env),
    lists:keystore(App, 1, ErlangConfig, {App, AppConfig1});
merge_app_env_in_erlconf(ErlangConfig, [Env | Rest]) ->
    ErlangConfig1 = merge_app_env_in_erlconf(ErlangConfig, Env),
    merge_app_env_in_erlconf(ErlangConfig1, Rest);
merge_app_env_in_erlconf(ErlangConfig, []) ->
    ErlangConfig.

nodename_to_hostname(Nodename) when is_atom(Nodename) ->
    [_, Hostname] = string:tokens(atom_to_list(Nodename), "@"),
    Hostname.

convert_to_unicode_binary(Arg) when is_list(Arg) ->
    unicode:characters_to_binary(Arg);
convert_to_unicode_binary(Arg) when is_binary(Arg) ->
    Arg.

%% -------------------------------------------------------------------
%% Assertions that retry
%% -------------------------------------------------------------------

await_condition(ConditionFun) ->
    await_condition(ConditionFun, 10000).

await_condition(ConditionFun, Timeout) ->
    Retries = ceil(Timeout / 50),
    await_condition_with_retries(ConditionFun, Retries).

await_condition_with_retries(_ConditionFun, 0) ->
    ct:fail("Condition did not materialize in the expected period of time");
await_condition_with_retries(ConditionFun, RetriesLeft) ->
    case ConditionFun() of
        false ->
            timer:sleep(50),
            await_condition_with_retries(ConditionFun, RetriesLeft - 1);
        true ->
            ok
    end.

%% -------------------------------------------------------------------
%% Cover-related functions.
%% -------------------------------------------------------------------

%% TODO.
cover_work_factor(_Config, Without) ->
    Without.
