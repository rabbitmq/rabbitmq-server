%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(unit_log_management_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

-define(TIMEOUT, 30000).

all() ->
    [
      {group, non_parallel_tests}
    ].

groups() ->
    [
      {non_parallel_tests, [], [
          log_file_initialised_during_startup,
          log_file_fails_to_initialise_during_startup,
          externally_rotated_logs_are_automatically_reopened
        ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group},
        {rmq_nodes_count, 2}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Application management.
%% -------------------------------------------------------------------

app_management(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, app_management1, [Config]).

app_management1(_Config) ->
    wait_for_application(rabbit),
    %% Starting, stopping and diagnostics.  Note that we don't try
    %% 'report' when the rabbit app is stopped and that we enable
    %% tracing for the duration of this function.
    ok = rabbit_trace:start(<<"/">>),
    ok = rabbit:stop(),
    ok = rabbit:stop(),
    ok = no_exceptions(rabbit, status, []),
    ok = no_exceptions(rabbit, environment, []),
    ok = rabbit:start(),
    ok = rabbit:start(),
    ok = no_exceptions(rabbit, status, []),
    ok = no_exceptions(rabbit, environment, []),
    ok = rabbit_trace:stop(<<"/">>),
    passed.

no_exceptions(Mod, Fun, Args) ->
    try erlang:apply(Mod, Fun, Args) of _ -> ok
    catch Type:Ex -> {Type, Ex}
    end.

wait_for_application(Application) ->
    wait_for_application(Application, 5000).

wait_for_application(_, Time) when Time =< 0 ->
    {error, timeout};
wait_for_application(Application, Time) ->
    Interval = 100,
    case lists:keyfind(Application, 1, application:which_applications()) of
        false ->
            timer:sleep(Interval),
            wait_for_application(Application, Time - Interval);
        _ -> ok
    end.



%% -------------------------------------------------------------------
%% Log management.
%% -------------------------------------------------------------------

log_file_initialised_during_startup(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, log_file_initialised_during_startup1, [Config]).

log_file_initialised_during_startup1(_Config) ->
    [LogFile|_] = rabbit:log_locations(),
    Suffix = ".0",

    %% start application with simple tty logging
    ok = rabbit:stop(),
    ok = clean_logs([LogFile], Suffix),
    os:putenv("RABBITMQ_LOGS", "-"),
    ok = rabbit:start(),

    %% start application with logging to non-existing directory
    NonExistent = rabbit_misc:format(
                    "/tmp/non-existent/~s.log", [?FUNCTION_NAME]),
    delete_file(NonExistent),
    delete_file(filename:dirname(NonExistent)),
    ok = rabbit:stop(),
    io:format("Setting log file to \"~s\"~n", [NonExistent]),
    os:putenv("RABBITMQ_LOGS", NonExistent),
    ok = rabbit:start(),

    %% clean up
    os:unsetenv("RABBITMQ_LOGS"),
    ok = rabbit:start(),
    passed.


log_file_fails_to_initialise_during_startup(Config) ->
    NonWritableDir = case os:type() of
                         {win32, _} -> "C:/Windows";
                         _          -> "/"
                     end,
    case file:open(filename:join(NonWritableDir, "test.log"), [write]) of
        {error, eacces} ->
            passed = rabbit_ct_broker_helpers:rpc(
                       Config, 0,
                       ?MODULE, log_file_fails_to_initialise_during_startup1,
                       [Config, NonWritableDir]);
        %% macOS, "read only volume"
        {error, erofs} ->
            passed = rabbit_ct_broker_helpers:rpc(
                       Config, 0,
                       ?MODULE, log_file_fails_to_initialise_during_startup1,
                       [Config, NonWritableDir]);
        {ok, Fd} ->
            %% If the supposedly non-writable directory is writable
            %% (e.g. we are running the testsuite on Windows as
            %% Administrator), we skip this test.
            file:close(Fd),
            {skip, "Supposedly non-writable directory is writable"}
    end.

log_file_fails_to_initialise_during_startup1(_Config, NonWritableDir) ->
    [LogFile|_] = rabbit:log_locations(),
    delete_file(LogFile),
    Fn = rabbit_misc:format("~s.log", [?FUNCTION_NAME]),

    %% start application with logging to directory with no
    %% write permissions
    NoPermission1 = filename:join(NonWritableDir, Fn),
    delete_file(NoPermission1),
    delete_file(filename:dirname(NoPermission1)),

    ok = rabbit:stop(),
    io:format("Setting log file to \"~s\"~n", [NoPermission1]),
    os:putenv("RABBITMQ_LOGS", NoPermission1),
    ?assertThrow(
       {error, {rabbit, {{cannot_log_to_file, _, _}, _}}},
       rabbit:start()),

    %% start application with logging to a subdirectory which
    %% parent directory has no write permissions
    NoPermission2 = filename:join([NonWritableDir,
                                   "non-existent",
                                   Fn]),
    delete_file(NoPermission2),
    delete_file(filename:dirname(NoPermission2)),

    io:format("Setting log file to \"~s\"~n", [NoPermission2]),
    os:putenv("RABBITMQ_LOGS", NoPermission2),
    ?assertThrow(
       {error, {rabbit, {{cannot_log_to_file, _, _}, _}}},
       rabbit:start()),

    %% clean up
    os:unsetenv("RABBITMQ_LOGS"),
    ok = rabbit:start(),
    passed.

externally_rotated_logs_are_automatically_reopened(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, externally_rotated_logs_are_automatically_reopened1, [Config]).

externally_rotated_logs_are_automatically_reopened1(_Config) ->
    [LogFile|_] = rabbit:log_locations(),

    %% Make sure log file is opened
    ok = test_logs_working([LogFile]),

    %% Move it away - i.e. external log rotation happened
    file:rename(LogFile, [LogFile, ".rotation_test"]),

    %% New files should be created - test_logs_working/1 will check that
    %% LogFile is not empty after doing some logging. And it's exactly
    %% what we need to check here.
    ok = test_logs_working([LogFile]),
    passed.

empty_or_nonexist_files(Files) ->
    [case file:read_file_info(File) of
         {ok, FInfo}     -> FInfo#file_info.size == 0;
         {error, enoent} -> true;
         Error           -> Error
     end || File <- Files].

empty_files(Files) ->
    [case file:read_file_info(File) of
         {ok, FInfo} -> FInfo#file_info.size == 0;
         Error       -> Error
     end || File <- Files].

non_empty_files(Files) ->
    [case EmptyFile of
         {error, Reason} -> {error, Reason};
         _               -> not(EmptyFile)
     end || EmptyFile <- empty_files(Files)].

test_logs_working(LogFiles) ->
    ok = rabbit_log:error("Log a test message"),
    %% give the error loggers some time to catch up
    timer:sleep(1000),
    lists:all(fun(LogFile) -> [true] =:= non_empty_files([LogFile]) end, LogFiles),
    ok.

set_permissions(Path, Mode) ->
    case file:read_file_info(Path) of
        {ok, FInfo} -> file:write_file_info(
                         Path,
                         FInfo#file_info{mode=Mode});
        Error       -> Error
    end.

clean_logs(Files, Suffix) ->
    [begin
         ok = delete_file(File),
         ok = delete_file([File, Suffix])
     end || File <- Files],
    ok.

delete_file(File) ->
    case file:delete(File) of
        ok              -> ok;
        {error, enoent} -> ok;
        Error           -> Error
    end.

make_files_writable(Files) ->
    [ok = file:write_file_info(File, #file_info{mode=8#644}) ||
        File <- Files],
    ok.

make_files_non_writable(Files) ->
    [ok = file:write_file_info(File, #file_info{mode=8#444}) ||
        File <- Files],
    ok.

add_log_handlers(Handlers) ->
    [ok = error_logger:add_report_handler(Handler, Args) ||
        {Handler, Args} <- Handlers],
    ok.

%% sasl_report_file_h returns [] during terminate
%% see: https://github.com/erlang/otp/blob/maint/lib/stdlib/src/error_logger_file_h.erl#L98
%%
%% error_logger_file_h returns ok since OTP 18.1
%% see: https://github.com/erlang/otp/blob/maint/lib/stdlib/src/error_logger_file_h.erl#L98
delete_log_handlers(Handlers) ->
    [ok_or_empty_list(error_logger:delete_report_handler(Handler))
     || Handler <- Handlers],
    ok.

ok_or_empty_list([]) ->
    [];
ok_or_empty_list(ok) ->
    ok.
