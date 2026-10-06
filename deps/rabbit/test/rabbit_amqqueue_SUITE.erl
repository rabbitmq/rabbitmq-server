-module(rabbit_amqqueue_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%%===================================================================
%%% Common Test callbacks
%%%===================================================================

all() ->
    [
     {group, rabbit_amqqueue_tests}
    ].


all_tests() ->
    [
     normal_queue_delete_with,
     internal_owner_queue_delete_with,
     internal_no_owner_queue_delete_with,
     with_fails_fast_for_pid_from_previous_run,
     with_fails_fast_for_pid_on_non_member_node,
     with_retries_for_dead_pid_from_this_run
    ].

groups() ->
    [
     {rabbit_amqqueue_tests, [], all_tests()}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),
    QName = rabbit_misc:r(<<"/">>, queue, rabbit_data_coercion:to_binary(Testcase)),
    Config2 = rabbit_ct_helpers:set_config(Config1, [{queue_name, QName}]),
    rabbit_ct_helpers:run_steps(Config2,
                                rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(
                Config,
                rabbit_ct_client_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%%%===================================================================
%%% Test cases
%%%===================================================================

normal_queue_delete_with(Config) ->
    QName = ?config(queue_name, Config),
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Queue = amqqueue:new(QName,
                         none, %% pid
                         true, %% durable
                         false, %% auto delete
                         none, %% owner,
                         [],
                         <<"/">>,
                         #{},
                         rabbit_classic_queue),

    ?assertMatch({new, _Q},  rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_queue_type, declare, [Queue, Node])),

    ?assertMatch({ok, _},  rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, delete_with, [QName, false, false, <<"dummy">>])),

    ?assertMatch({error, not_found}, rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, lookup, [QName])),

    ok.

internal_owner_queue_delete_with(Config) ->
    QName = ?config(queue_name, Config),
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Queue = amqqueue:new(QName,
                         none, %% pid
                         true, %% durable
                         false, %% auto delete
                         none, %% owner,
                         [],
                         <<"/">>,
                         #{},
                         rabbit_classic_queue),
    IQueue = amqqueue:make_internal(Queue, rabbit_misc:r(<<"/">>, exchange, <<"amq.default">>)),

    ?assertMatch({new, _Q},  rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_queue_type, declare, [IQueue, Node])),

    ?assertException(exit, {exception,
                            {amqp_error, resource_locked, _, none}},
                     rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, delete_with, [QName, false, false, <<"dummy">>])),

    ?assertMatch({ok, _}, rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, lookup, [QName])),

    ?assertMatch({ok, _},  rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, delete_with, [QName, false, false, ?INTERNAL_USER])),

    ?assertMatch({error, not_found}, rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, lookup, [QName])),

    ok.

internal_no_owner_queue_delete_with(Config) ->
    QName = ?config(queue_name, Config),
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Queue = amqqueue:new(QName,
                         none, %% pid
                         true, %% durable
                         false, %% auto delete
                         none, %% owner,
                         [],
                         <<"/">>,
                         #{},
                         rabbit_classic_queue),
    IQueue = amqqueue:make_internal(Queue),

    ?assertMatch({new, _Q},  rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_queue_type, declare, [IQueue, Node])),

    ?assertException(exit, {exception,
                            {amqp_error, resource_locked, _, none}},
                     rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, delete_with, [QName, false, false, <<"dummy">>])),

    ?assertMatch({ok, _}, rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, lookup, [QName])),

    ?assertMatch({ok, _},  rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, delete_with, [QName, false, false, ?INTERNAL_USER])),

    ?assertMatch({error, not_found}, rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, lookup, [QName])),

    ok.

with_fails_fast_for_pid_from_previous_run(Config) ->
    QName = ?config(queue_name, Config),
    LivePid = declare_classic_queue(Config, QName),
    StalePid = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, pid_from_previous_run, [LivePid]),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, set_queue_pid, [QName, StalePid]),

    {Elapsed, Result} = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, timed_stat, [QName]),
    ?assertMatch({absent, _, timeout}, Result),
    ?assert(Elapsed < 5000),

    delete_classic_queue(Config, QName, LivePid).

with_fails_fast_for_pid_on_non_member_node(Config) ->
    QName = ?config(queue_name, Config),
    LivePid = declare_classic_queue(Config, QName),
    ForeignPid = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, pid_on_node, [LivePid, 'rabbit@non-member-host']),
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, set_queue_pid, [QName, ForeignPid]),

    {Elapsed, Result} = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, timed_stat, [QName]),
    ?assertMatch({absent, _, timeout}, Result),
    ?assert(Elapsed < 5000),

    delete_classic_queue(Config, QName, LivePid).

with_retries_for_dead_pid_from_this_run(Config) ->
    QName = ?config(queue_name, Config),
    LivePid = declare_classic_queue(Config, QName),

    {Pending, Result} = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, stat_until_pid_is_restored, [QName, LivePid]),
    ?assertEqual(pending, Pending),
    ?assertMatch({ok, 0, 0}, Result),

    delete_classic_queue(Config, QName, LivePid).

declare_classic_queue(Config, QName) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Queue = amqqueue:new(QName, none, true, false, none, [], <<"/">>, #{}, rabbit_classic_queue),
    {new, Q} = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_queue_type, declare, [Queue, Node]),
    amqqueue:get_pid(Q).

delete_classic_queue(Config, QName, LivePid) ->
    ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, set_queue_pid, [QName, LivePid]),
    ?assertMatch({ok, _}, rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, delete_with, [QName, false, false, <<"dummy">>])),
    ok.

set_queue_pid(QName, Pid) ->
    {ok, Q} = rabbit_amqqueue:lookup(QName),
    rabbit_db_queue:set(amqqueue:set_pid(Q, Pid)).

pid_from_previous_run(Pid) ->
    #{creation := Creation} = Parts = rabbit_pid_codec:decompose(Pid),
    rabbit_pid_codec:recompose(Parts#{creation := Creation - 1}).

pid_on_node(Pid, Node) ->
    Parts = rabbit_pid_codec:decompose(Pid),
    rabbit_pid_codec:recompose(Parts#{node := Node}).

dead_pid() ->
    {Pid, MRef} = spawn_monitor(fun() -> ok end),
    receive {'DOWN', MRef, process, Pid, _} -> Pid end.

timed_stat(QName) ->
    timer:tc(fun() -> stat(QName) end, millisecond).

stat(QName) ->
    rabbit_amqqueue:with(QName, fun rabbit_amqqueue:stat/1, fun(E) -> E end).

stat_until_pid_is_restored(QName, LivePid) ->
    ok = set_queue_pid(QName, dead_pid()),
    Parent = self(),
    Ref = make_ref(),
    _ = spawn(fun() -> Parent ! {Ref, stat(QName)} end),
    Pending = receive {Ref, Early} -> {done, Early} after 1000 -> pending end,
    ok = set_queue_pid(QName, LivePid),
    Result = receive {Ref, Late} -> Late after 30000 -> no_result end,
    {Pending, Result}.
