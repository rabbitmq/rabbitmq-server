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
%% Copyright (c) 2011-2016 Pivotal Software, Inc.  All rights reserved.
%%

-module(unit_inbroker_non_parallel_SUITE).

-include_lib("common_test/include/ct.hrl").
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
          app_management, %% Restart RabbitMQ.
          channel_statistics, %% Expect specific statistics.
          disk_monitor, %% Replace rabbit_misc module.
          file_handle_cache, %% Change FHC limit.
          head_message_timestamp_statistics, %% Expect specific statistics.
          log_management, %% Check log files.
          log_management_during_startup, %% Check log files.
          memory_high_watermark, %% Trigger alarm.
          rotate_logs_without_suffix, %% Check log files.
          server_status %% Trigger alarm.
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
      rabbit_ct_client_helpers:setup_steps() ++ [
        fun setup_file_handle_cache/1
      ]).

setup_file_handle_cache(Config) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, setup_file_handle_cache1, []),
    Config.

setup_file_handle_cache1() ->
    %% FIXME: Why are we doing this?
    application:set_env(rabbit, file_handles_high_watermark, 10),
    ok = file_handle_cache:set_limit(10),
    ok.

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
    control_action(wait, [os:getenv("RABBITMQ_PID_FILE")]),
    %% Starting, stopping and diagnostics.  Note that we don't try
    %% 'report' when the rabbit app is stopped and that we enable
    %% tracing for the duration of this function.
    ok = control_action(trace_on, []),
    ok = control_action(stop_app, []),
    ok = control_action(stop_app, []),
    ok = control_action(status, []),
    ok = control_action(cluster_status, []),
    ok = control_action(environment, []),
    ok = control_action(start_app, []),
    ok = control_action(start_app, []),
    ok = control_action(status, []),
    ok = control_action(report, []),
    ok = control_action(cluster_status, []),
    ok = control_action(environment, []),
    ok = control_action(trace_off, []),
    passed.

%% ---------------------------------------------------------------------------
%% file_handle_cache.
%% ---------------------------------------------------------------------------

file_handle_cache(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, file_handle_cache1, [Config]).

file_handle_cache1(_Config) ->
    %% test copying when there is just one spare handle
    Limit = file_handle_cache:get_limit(),
    ok = file_handle_cache:set_limit(5), %% 1 or 2 sockets, 2 msg_stores
    TmpDir = filename:join(rabbit_mnesia:dir(), "tmp"),
    ok = filelib:ensure_dir(filename:join(TmpDir, "nothing")),
    [Src1, Dst1, Src2, Dst2] = Files =
        [filename:join(TmpDir, Str) || Str <- ["file1", "file2", "file3", "file4"]],
    Content = <<"foo">>,
    CopyFun = fun (Src, Dst) ->
                      {ok, Hdl} = prim_file:open(Src, [binary, write]),
                      ok = prim_file:write(Hdl, Content),
                      ok = prim_file:sync(Hdl),
                      prim_file:close(Hdl),

                      {ok, SrcHdl} = file_handle_cache:open(Src, [read], []),
                      {ok, DstHdl} = file_handle_cache:open(Dst, [write], []),
                      Size = size(Content),
                      {ok, Size} = file_handle_cache:copy(SrcHdl, DstHdl, Size),
                      ok = file_handle_cache:delete(SrcHdl),
                      ok = file_handle_cache:delete(DstHdl)
              end,
    Pid = spawn(fun () -> {ok, Hdl} = file_handle_cache:open(
                                        filename:join(TmpDir, "file5"),
                                        [write], []),
                          receive {next, Pid1} -> Pid1 ! {next, self()} end,
                          file_handle_cache:delete(Hdl),
                          %% This will block and never return, so we
                          %% exercise the fhc tidying up the pending
                          %% queue on the death of a process.
                          ok = CopyFun(Src1, Dst1)
                end),
    ok = CopyFun(Src1, Dst1),
    ok = file_handle_cache:set_limit(2),
    Pid ! {next, self()},
    receive {next, Pid} -> ok end,
    timer:sleep(100),
    Pid1 = spawn(fun () -> CopyFun(Src2, Dst2) end),
    timer:sleep(100),
    erlang:monitor(process, Pid),
    erlang:monitor(process, Pid1),
    exit(Pid, kill),
    exit(Pid1, kill),
    receive {'DOWN', _MRef, process, Pid, _Reason} -> ok end,
    receive {'DOWN', _MRef1, process, Pid1, _Reason1} -> ok end,
    [file:delete(File) || File <- Files],
    ok = file_handle_cache:set_limit(Limit),
    passed.

%% -------------------------------------------------------------------
%% Log management.
%% -------------------------------------------------------------------

log_management(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, log_management1, [Config]).

log_management1(_Config) ->
    override_group_leader(),

    MainLog = rabbit:log_location(kernel),
    SaslLog = rabbit:log_location(sasl),
    Suffix = ".1",

    ok = test_logs_working(MainLog, SaslLog),

    %% prepare basic logs
    file:delete([MainLog, Suffix]),
    file:delete([SaslLog, Suffix]),

    %% simple logs reopening
    ok = control_action(rotate_logs, []),
    ok = test_logs_working(MainLog, SaslLog),

    %% simple log rotation
    ok = control_action(rotate_logs, [Suffix]),
    [true, true] = non_empty_files([[MainLog, Suffix], [SaslLog, Suffix]]),
    [true, true] = empty_files([MainLog, SaslLog]),
    ok = test_logs_working(MainLog, SaslLog),

    %% reopening logs with log rotation performed first
    ok = clean_logs([MainLog, SaslLog], Suffix),
    ok = control_action(rotate_logs, []),
    ok = file:rename(MainLog, [MainLog, Suffix]),
    ok = file:rename(SaslLog, [SaslLog, Suffix]),
    ok = test_logs_working([MainLog, Suffix], [SaslLog, Suffix]),
    ok = control_action(rotate_logs, []),
    ok = test_logs_working(MainLog, SaslLog),

    %% log rotation on empty files (the main log will have a ctl action logged)
    ok = clean_logs([MainLog, SaslLog], Suffix),
    ok = control_action(rotate_logs, []),
    ok = control_action(rotate_logs, [Suffix]),
    [false, true] = empty_files([[MainLog, Suffix], [SaslLog, Suffix]]),

    %% logs with suffix are not writable
    ok = control_action(rotate_logs, [Suffix]),
    ok = make_files_non_writable([[MainLog, Suffix], [SaslLog, Suffix]]),
    ok = control_action(rotate_logs, [Suffix]),
    ok = test_logs_working(MainLog, SaslLog),

    %% logging directed to tty (first, remove handlers)
    ok = delete_log_handlers([rabbit_sasl_report_file_h,
                              rabbit_error_logger_file_h]),
    ok = clean_logs([MainLog, SaslLog], Suffix),
    ok = application:set_env(rabbit, sasl_error_logger, tty),
    ok = application:set_env(rabbit, error_logger, tty),
    ok = control_action(rotate_logs, []),
    [{error, enoent}, {error, enoent}] = empty_files([MainLog, SaslLog]),

    %% rotate logs when logging is turned off
    ok = application:set_env(rabbit, sasl_error_logger, false),
    ok = application:set_env(rabbit, error_logger, silent),
    ok = control_action(rotate_logs, []),
    [{error, enoent}, {error, enoent}] = empty_files([MainLog, SaslLog]),

    %% cleanup
    ok = application:set_env(rabbit, sasl_error_logger, {file, SaslLog}),
    ok = application:set_env(rabbit, error_logger, {file, MainLog}),
    ok = add_log_handlers([{rabbit_error_logger_file_h, MainLog},
                           {rabbit_sasl_report_file_h, SaslLog}]),
    passed.

log_management_during_startup(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, log_management_during_startup1, [Config]).

log_management_during_startup1(_Config) ->
    MainLog = rabbit:log_location(kernel),
    SaslLog = rabbit:log_location(sasl),

    %% start application with simple tty logging
    ok = control_action(stop_app, []),
    ok = application:set_env(rabbit, error_logger, tty),
    ok = application:set_env(rabbit, sasl_error_logger, tty),
    ok = add_log_handlers([{error_logger_tty_h, []},
                           {sasl_report_tty_h, []}]),
    ok = control_action(start_app, []),

    %% start application with tty logging and
    %% proper handlers not installed
    ok = control_action(stop_app, []),
    ok = error_logger:tty(false),
    ok = delete_log_handlers([sasl_report_tty_h]),
    ok = case catch control_action(start_app, []) of
             ok -> exit({got_success_but_expected_failure,
                         log_rotation_tty_no_handlers_test});
             {badrpc, {'EXIT', {error,
                                {cannot_log_to_tty, _, not_installed}}}} -> ok
         end,

    %% fix sasl logging
    ok = application:set_env(rabbit, sasl_error_logger, {file, SaslLog}),

    %% start application with logging to non-existing directory
    TmpLog = "/tmp/rabbit-tests/test.log",
    delete_file(TmpLog),
    ok = control_action(stop_app, []),
    ok = application:set_env(rabbit, error_logger, {file, TmpLog}),

    ok = delete_log_handlers([rabbit_error_logger_file_h]),
    ok = add_log_handlers([{error_logger_file_h, MainLog}]),
    ok = control_action(start_app, []),

    %% start application with logging to directory with no
    %% write permissions
    ok = control_action(stop_app, []),
    TmpDir = "/tmp/rabbit-tests",
    ok = set_permissions(TmpDir, 8#00400),
    ok = delete_log_handlers([rabbit_error_logger_file_h]),
    ok = add_log_handlers([{error_logger_file_h, MainLog}]),
    ok = case control_action(start_app, []) of
             ok -> exit({got_success_but_expected_failure,
                         log_rotation_no_write_permission_dir_test});
             {badrpc, {'EXIT',
                       {error, {cannot_log_to_file, _, _}}}} -> ok
         end,

    %% start application with logging to a subdirectory which
    %% parent directory has no write permissions
    ok = control_action(stop_app, []),
    TmpTestDir = "/tmp/rabbit-tests/no-permission/test/log",
    ok = application:set_env(rabbit, error_logger, {file, TmpTestDir}),
    ok = add_log_handlers([{error_logger_file_h, MainLog}]),
    ok = case control_action(start_app, []) of
             ok -> exit({got_success_but_expected_failure,
                         log_rotatation_parent_dirs_test});
             {badrpc,
              {'EXIT',
               {error, {cannot_log_to_file, _,
                        {error,
                         {cannot_create_parent_dirs, _, eacces}}}}}} -> ok
         end,
    ok = set_permissions(TmpDir, 8#00700),
    ok = set_permissions(TmpLog, 8#00600),
    ok = delete_file(TmpLog),
    ok = file:del_dir(TmpDir),

    %% start application with standard error_logger_file_h
    %% handler not installed
    ok = control_action(stop_app, []),
    ok = application:set_env(rabbit, error_logger, {file, MainLog}),
    ok = control_action(start_app, []),

    %% start application with standard sasl handler not installed
    %% and rabbit main log handler installed correctly
    ok = control_action(stop_app, []),
    ok = delete_log_handlers([rabbit_sasl_report_file_h]),
    ok = control_action(start_app, []),
    passed.

%% "rabbitmqctl rotate_logs" without additional parameters
%% shouldn't truncate files.
rotate_logs_without_suffix(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, rotate_logs_without_suffix1, [Config]).

rotate_logs_without_suffix1(_Config) ->
    override_group_leader(),

    MainLog = rabbit:log_location(kernel),
    SaslLog = rabbit:log_location(sasl),
    Suffix = ".1",
    file:delete(MainLog),
    file:delete(SaslLog),

    %% Empty log-files should be created
    ok = control_action(rotate_logs, []),
    [true, true] = empty_files([MainLog, SaslLog]),

    %% Write something to log files and simulate external log rotation
    ok = test_logs_working(MainLog, SaslLog),
    ok = file:rename(MainLog, [MainLog, Suffix]),
    ok = file:rename(SaslLog, [SaslLog, Suffix]),

    %% Create non-empty files
    TestData = "test-data\n",
    file:write_file(MainLog, TestData),
    file:write_file(SaslLog, TestData),

    %% Nothing should be truncated - neither moved files which are still
    %% opened by server, nor new log files that should be just reopened.
    ok = control_action(rotate_logs, []),
    [true, true, true, true] =
        non_empty_files([MainLog, SaslLog, [MainLog, Suffix],
            [SaslLog, Suffix]]),

    %% And log files should be re-opened - new log records should go to
    %% new files.
    ok = test_logs_working(MainLog, SaslLog),
    true = (rabbit_file:file_size(MainLog) > length(TestData)),
    true = (rabbit_file:file_size(SaslLog) > length(TestData)),
    passed.

override_group_leader() ->
    %% Override group leader, otherwise SASL fake events are ignored by
    %% the error_logger local to RabbitMQ.
    {group_leader, Leader} = erlang:process_info(whereis(rabbit), group_leader),
    erlang:group_leader(Leader, self()).

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

test_logs_working(MainLogFile, SaslLogFile) ->
    ok = rabbit_log:error("Log a test message~n"),
    ok = error_logger:error_report(crash_report, [fake_crash_report, ?MODULE]),
    %% give the error loggers some time to catch up
    timer:sleep(100),
    [true, true] = non_empty_files([MainLogFile, SaslLogFile]),
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

server_status(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, server_status1, [Config]).

server_status1(Config) ->
    %% create a few things so there is some useful information to list
    {_Writer, Limiter, Ch} = rabbit_ct_broker_helpers:test_channel(),
    [Q, Q2] = [Queue || {Name, Owner} <- [{<<"server_status-q1">>, none},
                                          {<<"server_status-q2">>, self()}],
                        {new, Queue = #amqqueue{}} <-
                            [rabbit_amqqueue:declare(
                               rabbit_misc:r(<<"/">>, queue, Name),
                               false, false, [], Owner)]],
    ok = rabbit_amqqueue:basic_consume(
           Q, true, Ch, Limiter, false, 0, <<"ctag">>, true, [], undefined),

    %% list queues
    ok = info_action(list_queues,
      rabbit_amqqueue:info_keys(), true),

    %% as we have no way to collect output of
    %% info_action/3 call, the only way we
    %% can test individual queueinfoitems is by directly calling
    %% rabbit_amqqueue:info/2
    [{exclusive, false}] = rabbit_amqqueue:info(Q, [exclusive]),
    [{exclusive, true}] = rabbit_amqqueue:info(Q2, [exclusive]),

    %% list exchanges
    ok = info_action(list_exchanges,
      rabbit_exchange:info_keys(), true),

    %% list bindings
    ok = info_action(list_bindings,
      rabbit_binding:info_keys(), true),
    %% misc binding listing APIs
    [_|_] = rabbit_binding:list_for_source(
              rabbit_misc:r(<<"/">>, exchange, <<"">>)),
    [_] = rabbit_binding:list_for_destination(
            rabbit_misc:r(<<"/">>, queue, <<"server_status-q1">>)),
    [_] = rabbit_binding:list_for_source_and_destination(
            rabbit_misc:r(<<"/">>, exchange, <<"">>),
            rabbit_misc:r(<<"/">>, queue, <<"server_status-q1">>)),

    %% list connections
    H = ?config(rmq_hostname, Config),
    P = rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    {ok, C} = gen_tcp:connect(H, P, []),
    gen_tcp:send(C, <<"AMQP", 0, 0, 9, 1>>),
    timer:sleep(100),
    ok = info_action(list_connections,
      rabbit_networking:connection_info_keys(), false),
    %% close_connection
    [ConnPid] = rabbit_ct_broker_helpers:get_connection_pids([C]),
    ok = control_action(close_connection,
      [rabbit_misc:pid_to_string(ConnPid), "go away"]),

    %% list channels
    ok = info_action(list_channels, rabbit_channel:info_keys(), false),

    %% list consumers
    ok = control_action(list_consumers, []),

    %% set vm memory high watermark
    HWM = vm_memory_monitor:get_vm_memory_high_watermark(),
    ok = control_action(set_vm_memory_high_watermark, ["1"]),
    ok = control_action(set_vm_memory_high_watermark, ["1.0"]),
    %% this will trigger an alarm
    ok = control_action(set_vm_memory_high_watermark, ["0.0"]),
    %% reset
    ok = control_action(set_vm_memory_high_watermark, [float_to_list(HWM)]),

    %% eval
    {error_string, _} = control_action(eval, ["\""]),
    {error_string, _} = control_action(eval, ["a("]),
    ok = control_action(eval, ["a."]),

    %% cleanup
    [{ok, _} = rabbit_amqqueue:delete(QR, false, false) || QR <- [Q, Q2]],

    unlink(Ch),
    ok = rabbit_channel:shutdown(Ch),

    passed.

%% -------------------------------------------------------------------
%% Statistics.
%% -------------------------------------------------------------------

channel_statistics(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, channel_statistics1, [Config]).

channel_statistics1(_Config) ->
    application:set_env(rabbit, collect_statistics, fine),

    %% ATM this just tests the queue / exchange stats in channels. That's
    %% by far the most complex code though.

    %% Set up a channel and queue
    {_Writer, Ch} = test_spawn(),
    rabbit_channel:do(Ch, #'queue.declare'{}),
    QName = receive #'queue.declare_ok'{queue = Q0} -> Q0
            after ?TIMEOUT -> throw(failed_to_receive_queue_declare_ok)
            end,
    QRes = rabbit_misc:r(<<"/">>, queue, QName),
    X = rabbit_misc:r(<<"/">>, exchange, <<"">>),

    dummy_event_receiver:start(self(), [node()], [channel_stats]),

    %% Check stats empty
    Check1 = fun() ->
                 [] = ets:match(channel_queue_metrics, {Ch, QRes}),
                 [] = ets:match(channel_exchange_metrics, {Ch, X}),
                 [] = ets:match(channel_queue_exchange_metrics,
                                {Ch, {QRes, X}})
             end,
    test_ch_metrics(Check1, ?TIMEOUT),

    %% Publish and get a message
    rabbit_channel:do(Ch, #'basic.publish'{exchange = <<"">>,
                                           routing_key = QName},
                      rabbit_basic:build_content(#'P_basic'{}, <<"">>)),
    rabbit_channel:do(Ch, #'basic.get'{queue = QName}),

    %% Check the stats reflect that
    Check2 = fun() ->
                 [{{Ch, QRes}, 1, 0, 0, 0, 0, 0, 0}] = ets:lookup(
                                                         channel_queue_metrics,
                                                         {Ch, QRes}),
                 [{{Ch, X}, 1, 0, 0, 0}] = ets:lookup(
                                             channel_exchange_metrics,
                                             {Ch, X}),
                 [{{Ch, {QRes, X}}, 1, 0}] = ets:lookup(
                                               channel_queue_exchange_metrics,
                                               {Ch, {QRes, X}})
             end,
    test_ch_metrics(Check2, ?TIMEOUT),

    %% Check the stats are marked for removal on queue deletion.
    rabbit_channel:do(Ch, #'queue.delete'{queue = QName}),
    Check3 = fun() ->
                 [{{Ch, QRes}, 1, 0, 0, 0, 0, 0, 1}] = ets:lookup(
                                                         channel_queue_metrics,
                                                         {Ch, QRes}),
                 [{{Ch, X}, 1, 0, 0, 0}] = ets:lookup(
                                             channel_exchange_metrics,
                                             {Ch, X}),
                 [{{Ch, {QRes, X}}, 1, 1}] = ets:lookup(
                                               channel_queue_exchange_metrics,
                                               {Ch, {QRes, X}})
             end,
    test_ch_metrics(Check3, ?TIMEOUT),

    %% Check the garbage collection removes stuff.
    force_metric_gc(),
    Check4 = fun() ->
                 [] = ets:lookup(channel_queue_metrics, {Ch, QRes}),
                 [{{Ch, X}, 1, 0, 0, 0}] = ets:lookup(
                                             channel_exchange_metrics,
                                             {Ch, X}),
                 [] = ets:lookup(channel_queue_exchange_metrics,
                                 {Ch, {QRes, X}})
             end,
    test_ch_metrics(Check4, ?TIMEOUT),

    rabbit_channel:shutdown(Ch),
    dummy_event_receiver:stop(),
    passed.

force_metric_gc() ->
    timer:sleep(300),
    rabbit_core_metrics_gc ! start_gc,
    gen_server:call(rabbit_core_metrics_gc, test).

test_ch_metrics(Fun, Timeout) when Timeout =< 0 ->
    Fun();
test_ch_metrics(Fun, Timeout) ->
    try
        Fun()
    catch
        _:{badmatch, _} ->
            timer:sleep(1000),
            test_ch_metrics(Fun, Timeout - 1000)
    end.

head_message_timestamp_statistics(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, head_message_timestamp1, [Config]).

head_message_timestamp1(_Config) ->
    %% Can't find a way to receive the ack here so can't test pending acks status

    application:set_env(rabbit, collect_statistics, fine),

    %% Set up a channel and queue
    {_Writer, Ch} = test_spawn(),
    rabbit_channel:do(Ch, #'queue.declare'{}),
    QName = receive #'queue.declare_ok'{queue = Q0} -> Q0
            after ?TIMEOUT -> throw(failed_to_receive_queue_declare_ok)
            end,
    QRes = rabbit_misc:r(<<"/">>, queue, QName),

    {ok, Q1} = rabbit_amqqueue:lookup(QRes),
    QPid = Q1#amqqueue.pid,

    %% Set up event receiver for queue
    dummy_event_receiver:start(self(), [node()], [queue_stats]),

    %% Check timestamp is empty when queue is empty
    Event1 = test_queue_statistics_receive_event(QPid, fun (E) -> proplists:get_value(name, E) == QRes end),
    '' = proplists:get_value(head_message_timestamp, Event1),

    %% Publish two messages and check timestamp is that of first message
    rabbit_channel:do(Ch, #'basic.publish'{exchange = <<"">>,
                                           routing_key = QName},
                      rabbit_basic:build_content(#'P_basic'{timestamp = 1}, <<"">>)),
    rabbit_channel:do(Ch, #'basic.publish'{exchange = <<"">>,
                                           routing_key = QName},
                      rabbit_basic:build_content(#'P_basic'{timestamp = 2}, <<"">>)),
    Event2 = test_queue_statistics_receive_event(QPid, fun (E) -> proplists:get_value(name, E) == QRes end),
    1 = proplists:get_value(head_message_timestamp, Event2),

    %% Get first message and check timestamp is that of second message
    rabbit_channel:do(Ch, #'basic.get'{queue = QName, no_ack = true}),
    Event3 = test_queue_statistics_receive_event(QPid, fun (E) -> proplists:get_value(name, E) == QRes end),
    2 = proplists:get_value(head_message_timestamp, Event3),

    %% Get second message and check timestamp is empty again
    rabbit_channel:do(Ch, #'basic.get'{queue = QName, no_ack = true}),
    Event4 = test_queue_statistics_receive_event(QPid, fun (E) -> proplists:get_value(name, E) == QRes end),
    '' = proplists:get_value(head_message_timestamp, Event4),

    %% Teardown
    rabbit_channel:do(Ch, #'queue.delete'{queue = QName}),
    rabbit_channel:shutdown(Ch),
    dummy_event_receiver:stop(),

    passed.

test_queue_statistics_receive_event(Q, Matcher) ->
    %% Q ! emit_stats,
    test_queue_statistics_receive_event1(Q, Matcher).

test_queue_statistics_receive_event1(Q, Matcher) ->
    receive #event{type = queue_stats, props = Props} ->
            case Matcher(Props) of
                true -> Props;
                _    -> test_queue_statistics_receive_event1(Q, Matcher)
            end
    after ?TIMEOUT -> throw(failed_to_receive_event)
    end.

test_spawn() ->
    {Writer, _Limiter, Ch} = rabbit_ct_broker_helpers:test_channel(),
    ok = rabbit_channel:do(Ch, #'channel.open'{}),
    receive #'channel.open_ok'{} -> ok
    after ?TIMEOUT -> throw(failed_to_receive_channel_open_ok)
    end,
    {Writer, Ch}.

memory_high_watermark(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, memory_high_watermark1, [Config]).

memory_high_watermark1(_Config) ->
    %% set vm memory high watermark
    HWM = vm_memory_monitor:get_vm_memory_high_watermark(),
    %% this will trigger an alarm
    ok = control_action(set_vm_memory_high_watermark,
      ["absolute", "2000"]),
    [{{resource_limit,memory,_},[]}] = rabbit_alarm:get_alarms(),
    %% reset
    ok = control_action(set_vm_memory_high_watermark,
      [float_to_list(HWM)]),

    passed.

disk_monitor(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, disk_monitor1, [Config]).

disk_monitor1(_Config) ->
    %% Issue: rabbitmq-server #91
    %% os module could be mocked using 'unstick', however it may have undesired
    %% side effects in following tests. Thus, we mock at rabbit_misc level
    ok = meck:new(rabbit_misc, [passthrough]),
    ok = meck:expect(rabbit_misc, os_cmd, fun(_) -> "\n" end),
    ok = rabbit_sup:stop_child(rabbit_disk_monitor_sup),
    ok = rabbit_sup:start_delayed_restartable_child(rabbit_disk_monitor, [1000]),
    meck:unload(rabbit_misc),
    passed.

%% ---------------------------------------------------------------------------
%% rabbitmqctl helpers.
%% ---------------------------------------------------------------------------

control_action(Command, Args) ->
    control_action(Command, node(), Args, default_options()).

control_action(Command, Args, NewOpts) ->
    control_action(Command, node(), Args,
                   expand_options(default_options(), NewOpts)).

control_action(Command, Node, Args, Opts) ->
    case catch rabbit_control_main:action(
                 Command, Node, Args, Opts,
                 fun (Format, Args1) ->
                         io:format(Format ++ " ...~n", Args1)
                 end) of
        ok ->
            io:format("done.~n"),
            ok;
        {ok, Result} ->
            rabbit_control_misc:print_cmd_result(Command, Result),
            ok;
        Other ->
            io:format("failed: ~p~n", [Other]),
            Other
    end.

info_action(Command, Args, CheckVHost) ->
    ok = control_action(Command, []),
    if CheckVHost -> ok = control_action(Command, [], ["-p", "/"]);
       true       -> ok
    end,
    ok = control_action(Command, lists:map(fun atom_to_list/1, Args)),
    {bad_argument, dummy} = control_action(Command, ["dummy"]),
    ok.

default_options() -> [{"-p", "/"}, {"-q", "false"}].

expand_options(As, Bs) ->
    lists:foldl(fun({K, _}=A, R) ->
                        case proplists:is_defined(K, R) of
                            true -> R;
                            false -> [A | R]
                        end
                end, Bs, As).
