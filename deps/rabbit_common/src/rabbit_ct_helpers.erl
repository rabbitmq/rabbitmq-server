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
%% Copyright (c) 2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_ct_helpers).

-include_lib("common_test/include/ct.hrl").

-export([
    log_environment/0,
    run_steps/2,
    run_setup_steps/1, run_setup_steps/2,
    run_teardown_steps/1, run_teardown_steps/2,
    ensure_application_srcdir/3,
    start_long_running_testsuite_monitor/1,
    stop_long_running_testsuite_monitor/1,
    config_to_testcase_name/2,
    testcases/1,
    testcase_number/3,
    testcase_started/2, testcase_finished/2,
    exec/1, exec/2,
    make/3,
    get_config/2, set_config/2,
    merge_app_env/2, merge_app_env_in_erlconf/2
  ]).

-define(SSL_CERT_PASSWORD, "test").

%% -------------------------------------------------------------------
%% Testsuite internal helpers.
%% -------------------------------------------------------------------

log_environment() ->
    Vars = lists:sort(fun(A, B) -> A =< B end, os:getenv()),
    ct:pal("Environment variables:~n~s", [
        [io_lib:format("  ~s~n", [V]) || V <- Vars]]).

run_setup_steps(Config) ->
    run_setup_steps(Config, []).

run_setup_steps(Config, ExtraSteps) ->
    Steps = [
      fun ensure_rabbit_common_srcdir/1,
      fun ensure_erlang_mk_depsdir/1,
      fun ensure_rabbit_srcdir/1,
      fun ensure_make_cmd/1,
      fun ensure_rabbitmqctl_cmd/1,
      fun ensure_ssl_certs/1,
      fun start_long_running_testsuite_monitor/1
    ],
    run_steps(Config, Steps ++ ExtraSteps).

run_teardown_steps(Config) ->
    run_teardown_steps(Config, []).

run_teardown_steps(Config, ExtraSteps) ->
    Steps = [
      fun stop_long_running_testsuite_monitor/1
    ],
    run_steps(Config, ExtraSteps ++ Steps).

run_steps(Config, [Step | Rest]) ->
    case Step(Config) of
        {skip, _} = Error -> Error;
        Config1           -> run_steps(Config1, Rest)
    end;
run_steps(Config, []) ->
    Config.

ensure_rabbit_common_srcdir(Config) ->
    Path = case get_config(Config, rabbit_common_srcdir) of
        undefined ->
            filename:dirname(
              filename:dirname(
                code:which(?MODULE)));
        P ->
            P
    end,
    case filelib:is_dir(Path) of
        true  -> set_config(Config, {rabbit_common_srcdir, Path});
        false -> {skip,
                  "rabbit_common source directory required, " ++
                  "please set 'rabbit_common_srcdir' in ct config"}
    end.

ensure_erlang_mk_depsdir(Config) ->
    Path = case get_config(Config, erlang_mk_depsdir) of
        undefined ->
            case os:getenv("DEPS_DIR") of
                false ->
                    %% Try the common locations.
                    SrcDir = ?config(rabbit_common_srcdir, Config),
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
                  "please set DEPSD_DIR or 'erlang_mk_depsdir' " ++
                  "in ct config"}
    end.

ensure_rabbit_srcdir(Config) ->
    ensure_application_srcdir(Config, rabbit, rabbit).

ensure_application_srcdir(Config, App, Module) ->
    AppS = atom_to_list(App),
    Key = list_to_atom(AppS ++ "_srcdir"),
    Path = case get_config(Config, Key) of
        undefined ->
            case code:which(Module) of
                non_existing ->
                    filename:join(?config(rabbit_common_srcdir, Config), AppS);
                P ->
                    filename:dirname(
                      filename:dirname(P))
            end;
        P ->
            P
    end,
    case filelib:is_dir(Path) of
        true  -> set_config(Config, {Key, Path});
        false -> {skip,
                  AppS ++ "source directory required, " ++
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

ensure_rabbitmqctl_cmd(Config) ->
    Rabbitmqctl = case get_config(Config, rabbitmqctl_cmd) of
        undefined ->
            case os:getenv("RABBITMQCTL") of
                false ->
                    SrcDir = ?config(rabbit_srcdir, Config),
                    R = filename:join(SrcDir, "scripts/rabbitmqctl"),
                    case filelib:is_file(R) of
                        true  -> R;
                        false -> false
                    end;
                R ->
                    R
            end;
        R ->
            R
    end,
    Error = {skip, "rabbitmqctl required, " ++
             "please set RABBITMQCTL or 'rabbitmqctl_cmd' in ct config"},
    case Rabbitmqctl of
        false ->
            Error;
        _ ->
            Cmd = [Rabbitmqctl],
            case exec(Cmd, [drop_stdout]) of
                {error, 64, _} ->
                    set_config(Config, {rabbitmqctl_cmd, Rabbitmqctl});
                _ ->
                    Error
            end
    end.

ensure_ssl_certs(Config) ->
    SrcDir = ?config(rabbit_common_srcdir, Config),
    CertsMakeDir = filename:join([SrcDir, "tools", "tls-certs"]),
    PrivDir = ?config(priv_dir, Config),
    CertsDir = filename:join(PrivDir, "certs"),
    Cmd = [
      "PASSWORD=" ++ ?SSL_CERT_PASSWORD,
      "DIR=" ++ CertsDir],
    case make(Config, CertsMakeDir, Cmd) of
        {ok, _} ->
            %% Add SSL certs to the broker configuration.
            Config1 = merge_app_env(Config,
              {rabbit, [
                  {ssl_options, [
                      {cacertfile, filename:join([CertsDir, "testca", "cacert.pem"])},
                      {certfile, filename:join([CertsDir, "server", "cert.pem"])},
                      {keyfile, filename:join([CertsDir, "server", "key.pem"])},
                      {verify, verify_peer},
                      {fail_if_no_peer_cert, true}
                    ]}]}),
            set_config(Config1, {rmq_certsdir, CertsDir});
        _ ->
            {skip, "Failed to create SSL certificates"}
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

stop_long_running_testsuite_monitor(Config) ->
    ?config(long_running_testsuite_monitor, Config) ! stop,
    Config.

long_running_testsuite_monitor(TimerRef, Testcases) ->
    receive
        {started, Testcase} ->
            Testcases1 = [{Testcase, time_compat:monotonic_time(seconds)}
                          | Testcases],
            long_running_testsuite_monitor(TimerRef, Testcases1);
        {finished, Testcase} ->
            Testcases1 = proplists:delete(Testcase, Testcases),
            long_running_testsuite_monitor(TimerRef, Testcases1);
        ping_ct ->
            T1 = time_compat:monotonic_time(seconds),
            ct:pal("Testcases still in progress:~s",
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
    Name = rabbit_misc:format("~s", [Testcase]),
    case get_config(Config, tc_group_properties) of
        [] ->
            Name;
        Props ->
            Name1 = rabbit_misc:format("~s/~s",
              [proplists:get_value(name, Props), Name]),
            config_to_testcase_name1(Name1, get_config(Config, tc_group_path))
    end.

config_to_testcase_name1(Name, [Props | Rest]) ->
    Name1 = rabbit_misc:format("~s/~s", [proplists:get_value(name, Props), Name]),
    config_to_testcase_name1(Name1, Rest);
config_to_testcase_name1(Name, []) ->
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

exec(Cmd) ->
    exec(Cmd, []).

exec([Cmd | Args], Options) when is_list(Cmd) orelse is_binary(Cmd) ->
    Cmd1 = case (lists:member($/, Cmd) orelse lists:member($\\, Cmd)) of
        true  -> Cmd;
        false -> os:find_executable(Cmd)
    end,
    Args1 = [format_arg(Arg) || Arg <- Args],
    {LocalOptions, PortOptions} = lists:partition(
      fun
          ({match_stdout, _}) -> true;
          (drop_stdout)       -> true;
          (_)                 -> false
      end, Options),
    PortOptions1 = case lists:member(nouse_stdio, PortOptions) of
        true  -> PortOptions;
        false -> [use_stdio, stderr_to_stdout | PortOptions]
    end,
    Log = "+ ~s (pid ~p)",
    {PortOptions2, Log1} = case proplists:get_value(env, PortOptions1) of
        undefined ->
            {PortOptions1, Log};
        Env ->
            Env1 = [
              begin
                  Key1 = format_arg(Key),
                  Value1 = format_arg(Value),
                  {Key1, Value1}
              end
              || {Key, Value} <- Env
            ],
            {
              [{env, Env1} | proplists:delete(env, PortOptions1)],
              Log ++ "~n~nEnvironment variables:~n" ++
              string:join(
                [rabbit_misc:format("  ~s=~s", [K, V]) || {K, V} <- Env1],
                "~n")
            }
    end,
    ct:pal(Log1, [string:join([Cmd1 | Args1], " "), self()]),
    Port = erlang:open_port(
      {spawn_executable, Cmd1}, [
        {args, Args1},
        exit_status
        | PortOptions2]),
    port_receive_loop(Port, "", LocalOptions).

format_arg({Format, FormatArgs}) ->
    rabbit_misc:format(Format, FormatArgs);
format_arg(Arg) when is_atom(Arg) ->
    atom_to_list(Arg);
format_arg(Arg) when is_binary(Arg) ->
    binary_to_list(Arg);
format_arg(Arg) ->
    Arg.

port_receive_loop(Port, Stdout, Options) ->
    receive
        {Port, {exit_status, X}} ->
            DropStdout = lists:member(drop_stdout, Options) orelse
              Stdout =:= "",
            if
                DropStdout ->
                    ct:pal("Exit code: ~p (pid ~p)", [X, self()]);
                true ->
                    ct:pal("~s~nExit code: ~p (pid ~p)", [Stdout, X, self()])
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
        {Port, {data, Out}} ->
            port_receive_loop(Port, Stdout ++ Out, Options)
    end.

make(Config, Dir, Args) ->
    Make = ?config(make_cmd, Config),
    Verbosity = case os:getenv("V") of
        false -> [];
        V     -> ["V=" ++ V]
    end,
    Cmd = [Make, "-C", Dir] ++ Verbosity ++ Args,
    exec(Cmd).

%% This is the same as ?config(), except this one doesn't log a warning
%% if the key is missing.
get_config(Config, Key) ->
    proplists:get_value(Key, Config).

set_config(Config, Tuple) when is_tuple(Tuple) ->
    Key = element(1, Tuple),
    lists:keystore(Key, 1, Config, Tuple);
set_config(Config, [Tuple | Rest]) ->
    Config1 = set_config(Config, Tuple),
    set_config(Config1, Rest);
set_config(Config, []) ->
    Config.

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
