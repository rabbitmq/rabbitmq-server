%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(unit_inbroker_non_parallel_SUITE).

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
          app_management, %% Restart RabbitMQ.
          channel_statistics, %% Expect specific statistics.
          disk_monitor, %% Replace rabbit_misc module.
          disk_monitor_enable,
          file_handle_cache, %% Change FHC limit.
          head_message_timestamp_statistics, %% Expect specific statistics.
          externally_rotated_logs_are_automatically_reopened, %% Check log files.
          exchange_count,
          queue_count,
          connection_count,
          connection_lookup,
          file_handle_cache_reserve,
          file_handle_cache_reserve_release,
          file_handle_cache_reserve_above_limit,
          file_handle_cache_reserve_monitor,
          file_handle_cache_reserve_open_file_above_limit
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

file_handle_cache_reserve(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, file_handle_cache_reserve1, [Config]).

file_handle_cache_reserve1(_Config) ->
    Limit = file_handle_cache:get_limit(),
    ok = file_handle_cache:set_limit(5),
    %% Reserves are always accepted, even if above the limit
    %% These are for special processes such as quorum queues
    ok = file_handle_cache:set_reservation(7),

    Self = self(),
    spawn(fun () -> ok = file_handle_cache:obtain(),
                    Self ! obtained
          end),

    Props = file_handle_cache:info([files_reserved, sockets_used]),
    ?assertEqual(7, proplists:get_value(files_reserved, Props)),
    ?assertEqual(0, proplists:get_value(sockets_used, Props)),

    %% The obtain should still be blocked, as there are no file handles
    %% available
    receive
        obtained ->
            throw(error_file_obtained)
    after 1000 ->
            %% Let's release 5 file handles, that should leave
            %% enough free for the `obtain` to go through
            file_handle_cache:set_reservation(2),
            Props0 = file_handle_cache:info([files_reserved, sockets_used]),
            ?assertEqual(2, proplists:get_value(files_reserved, Props0)),
            ?assertEqual(1, proplists:get_value(sockets_used, Props0)),
            receive
                obtained ->
                    ok = file_handle_cache:set_limit(Limit),
                    passed
            after 5000 ->
                    throw(error_file_not_released)
            end
    end.

file_handle_cache_reserve_release(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, file_handle_cache_reserve_release1, [Config]).

file_handle_cache_reserve_release1(_Config) ->
    ok = file_handle_cache:set_reservation(7),
    ?assertEqual([{files_reserved, 7}], file_handle_cache:info([files_reserved])),
    ok = file_handle_cache:set_reservation(3),
    ?assertEqual([{files_reserved, 3}], file_handle_cache:info([files_reserved])),
    ok = file_handle_cache:release_reservation(),
    ?assertEqual([{files_reserved, 0}], file_handle_cache:info([files_reserved])),
    passed.

file_handle_cache_reserve_above_limit(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, file_handle_cache_reserve_above_limit1, [Config]).

file_handle_cache_reserve_above_limit1(_Config) ->
    Limit = file_handle_cache:get_limit(),
    ok = file_handle_cache:set_limit(5),
    %% Reserves are always accepted, even if above the limit
    %% These are for special processes such as quorum queues
    ok = file_handle_cache:obtain(5),
    ?assertEqual([{file_descriptor_limit, []}],  rabbit_alarm:get_alarms()),

    ok = file_handle_cache:set_reservation(7),

    Props = file_handle_cache:info([files_reserved, sockets_used]),
    ?assertEqual(7, proplists:get_value(files_reserved, Props)),
    ?assertEqual(5, proplists:get_value(sockets_used, Props)),

    ok = file_handle_cache:set_limit(Limit),
    passed.

file_handle_cache_reserve_open_file_above_limit(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, file_handle_cache_reserve_open_file_above_limit1, [Config]).

file_handle_cache_reserve_open_file_above_limit1(_Config) ->
    Limit = file_handle_cache:get_limit(),
    ok = file_handle_cache:set_limit(5),
    %% Reserves are always accepted, even if above the limit
    %% These are for special processes such as quorum queues
    ok = file_handle_cache:set_reservation(7),

    Self = self(),
    TmpDir = filename:join(rabbit_mnesia:dir(), "tmp"),
    spawn(fun () -> {ok, _} = file_handle_cache:open(
                                filename:join(TmpDir, "file_above_limit"),
                                [write], []),
                    Self ! opened
          end),

    Props = file_handle_cache:info([files_reserved]),
    ?assertEqual(7, proplists:get_value(files_reserved, Props)),

    %% The open should still be blocked, as there are no file handles
    %% available
    receive
        opened ->
            throw(error_file_opened)
    after 1000 ->
            %% Let's release 5 file handles, that should leave
            %% enough free for the `open` to go through
            file_handle_cache:set_reservation(2),
            Props0 = file_handle_cache:info([files_reserved, total_used]),
            ?assertEqual(2, proplists:get_value(files_reserved, Props0)),
            receive
                opened ->
                    ok = file_handle_cache:set_limit(Limit),
                    passed
            after 5000 ->
                    throw(error_file_not_released)
            end
    end.

file_handle_cache_reserve_monitor(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, file_handle_cache_reserve_monitor1, [Config]).

file_handle_cache_reserve_monitor1(_Config) ->
    %% Check that if the process that does the reserve dies, the file handlers are
    %% released by the cache
    Self = self(),
    Pid = spawn(fun () ->
                        ok = file_handle_cache:set_reservation(2),
                        Self ! done,
                        receive
                            stop -> ok
                        end
                end),
    receive
        done -> ok
    end,
    ?assertEqual([{files_reserved, 2}], file_handle_cache:info([files_reserved])),
    Pid ! stop,
    timer:sleep(500),
    ?assertEqual([{files_reserved, 0}], file_handle_cache:info([files_reserved])),
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
                     [{{Ch, QRes}, 1, 0, 0, 0, 0, 0, 0, 0}] = ets:lookup(
                                                                channel_queue_metrics,
                                                                {Ch, QRes}),
                     [{{Ch, X}, 1, 0, 0, 0, 0}] = ets:lookup(
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
                     [{{Ch, QRes}, 1, 0, 0, 0, 0, 0, 0, 1}] = ets:lookup(
                                                                channel_queue_metrics,
                                                                {Ch, QRes}),
                 [{{Ch, X}, 1, 0, 0, 0, 0}] = ets:lookup(
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
                 [{{Ch, X}, 1, 0, 0, 0, 0}] = ets:lookup(
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
    %% there is no convenient rabbit_channel API for confirms
    %% this test could use, so it relies on tx.* methods
    %% and gen_server2 flushing
    application:set_env(rabbit, collect_statistics, fine),

    %% Set up a channel and queue
    {_Writer, Ch} = test_spawn(),
    rabbit_channel:do(Ch, #'queue.declare'{}),
    QName = receive #'queue.declare_ok'{queue = Q0} -> Q0
            after ?TIMEOUT -> throw(failed_to_receive_queue_declare_ok)
            end,
    QRes = rabbit_misc:r(<<"/">>, queue, QName),

    {ok, Q1} = rabbit_amqqueue:lookup(QRes),
    QPid = amqqueue:get_pid(Q1),

    %% Set up event receiver for queue
    dummy_event_receiver:start(self(), [node()], [queue_stats]),

    %% the head timestamp field is empty when the queue is empty
    test_queue_statistics_receive_event(QPid,
                                        fun (E) ->
                                                (proplists:get_value(name, E) == QRes)
                                                    and
                                                      (proplists:get_value(head_message_timestamp, E) == '')
                                        end),

    rabbit_channel:do(Ch, #'tx.select'{}),
    receive #'tx.select_ok'{} -> ok
    after ?TIMEOUT -> throw(failed_to_receive_tx_select_ok)
    end,

    %% Publish two messages and check that the timestamp is that of the first message
    rabbit_channel:do(Ch, #'basic.publish'{exchange = <<"">>,
                                           routing_key = QName},
                      rabbit_basic:build_content(#'P_basic'{timestamp = 1}, <<"">>)),
    rabbit_channel:do(Ch, #'basic.publish'{exchange = <<"">>,
                                           routing_key = QName},
                      rabbit_basic:build_content(#'P_basic'{timestamp = 2}, <<"">>)),
    rabbit_channel:do(Ch, #'tx.commit'{}),
    rabbit_channel:flush(Ch),
    receive #'tx.commit_ok'{} -> ok
    after ?TIMEOUT -> throw(failed_to_receive_tx_commit_ok)
    end,
    test_queue_statistics_receive_event(QPid,
                                        fun (E) ->
                                                (proplists:get_value(name, E) == QRes)
                                                    and
                                                      (proplists:get_value(head_message_timestamp, E) == 1)
                                        end),

    %% Consume a message and check that the timestamp is now that of the second message
    rabbit_channel:do(Ch, #'basic.get'{queue = QName, no_ack = true}),
    test_queue_statistics_receive_event(QPid,
                                        fun (E) ->
                                                (proplists:get_value(name, E) == QRes)
                                                    and
                                                      (proplists:get_value(head_message_timestamp, E) == 2)
                                        end),

    %% Consume one more message and check again
    rabbit_channel:do(Ch, #'basic.get'{queue = QName, no_ack = true}),
    test_queue_statistics_receive_event(QPid,
                                        fun (E) ->
                                                (proplists:get_value(name, E) == QRes)
                                                    and
                                                      (proplists:get_value(head_message_timestamp, E) == '')
                                        end),

    %% Tear down
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

disk_monitor_enable(Config) ->
    passed = rabbit_ct_broker_helpers:rpc(Config, 0,
      ?MODULE, disk_monitor_enable1, [Config]).

disk_monitor_enable1(_Config) ->
    ok = meck:new(rabbit_misc, [passthrough]),
    ok = meck:expect(rabbit_misc, os_cmd, fun(_) -> "\n" end),
    application:set_env(rabbit, disk_monitor_failure_retries, 20000),
    application:set_env(rabbit, disk_monitor_failure_retry_interval, 100),
    ok = rabbit_sup:stop_child(rabbit_disk_monitor_sup),
    ok = rabbit_sup:start_delayed_restartable_child(rabbit_disk_monitor, [1000]),
    undefined = rabbit_disk_monitor:get_disk_free(),
    Cmd = case os:type() of
              {win32, _} -> " Le volume dans le lecteur C n’a pas de nom.\n"
                            " Le numéro de série du volume est 707D-5BDC\n"
                            "\n"
                            " Répertoire de C:\Users\n"
                            "\n"
                            "10/12/2015  11:01    <DIR>          .\n"
                            "10/12/2015  11:01    <DIR>          ..\n"
                            "               0 fichier(s)                0 octets\n"
                            "               2 Rép(s)  758537121792 octets libres\n";
              _          -> "Filesystem 1024-blocks      Used Available Capacity  iused     ifree %iused  Mounted on\n"
                            "/dev/disk1   975798272 234783364 740758908    25% 58759839 185189727   24%   /\n"
          end,
    ok = meck:expect(rabbit_misc, os_cmd, fun(_) -> Cmd end),
    timer:sleep(1000),
    Bytes = 740758908 * 1024,
    Bytes = rabbit_disk_monitor:get_disk_free(),
    meck:unload(rabbit_misc),
    application:set_env(rabbit, disk_monitor_failure_retries, 10),
    application:set_env(rabbit, disk_monitor_failure_retry_interval, 120000),
    passed.

%% ---------------------------------------------------------------------------
%% Count functions for management only API purposes
%% ---------------------------------------------------------------------------
exchange_count(Config) ->
    %% Default exchanges == 7
    ?assertEqual(7, rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_exchange, count, [])).

queue_count(Config) ->
    Conn = rabbit_ct_client_helpers:open_connection(Config, 0),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    amqp_channel:call(Ch, #'queue.declare'{ queue = <<"my-queue">> }),

    ?assertEqual(1, rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, count, [])),

    amqp_channel:call(Ch, #'queue.delete'{ queue = <<"my-queue">> }),
    rabbit_ct_client_helpers:close_channel(Ch),
    rabbit_ct_client_helpers:close_connection(Conn),
    ok.

connection_count(Config) ->
    Conn = rabbit_ct_client_helpers:open_connection(Config, 0),

    ?assertEqual(1, rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_connection_tracking, count, [])),

    rabbit_ct_client_helpers:close_connection(Conn),
    ok.

connection_lookup(Config) ->
    Conn = rabbit_ct_client_helpers:open_connection(Config, 0),

    [Connection] = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_connection_tracking, list, []),
    ?assertMatch(Connection, rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_connection_tracking,
                                                          lookup,
                                                          [Connection#tracked_connection.name])),

    rabbit_ct_client_helpers:close_connection(Conn),
    ok.

%% ---------------------------------------------------------------------------
%% rabbitmqctl helpers.
%% ---------------------------------------------------------------------------

default_options() -> [{"-p", "/"}, {"-q", "false"}].

expand_options(As, Bs) ->
    lists:foldl(fun({K, _}=A, R) ->
                        case proplists:is_defined(K, R) of
                            true -> R;
                            false -> [A | R]
                        end
                end, Bs, As).
