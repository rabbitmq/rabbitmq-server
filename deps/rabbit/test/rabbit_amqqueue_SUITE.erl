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
     internal_no_owner_queue_delete_with
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
                            {amqp_error, resource_locked,
                             "Cannot delete protected queue 'internal_owner_queue_delete_with' in vhost '/'.",
                             none}}, rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, delete_with, [QName, false, false, <<"dummy">>])),

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
                            {amqp_error, resource_locked,
                             "Cannot delete protected queue 'internal_no_owner_queue_delete_with' in vhost '/'.",
                             none}}, rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, delete_with, [QName, false, false, <<"dummy">>])),

    ?assertMatch({ok, _}, rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, lookup, [QName])),

    ?assertMatch({ok, _},  rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, delete_with, [QName, false, false, ?INTERNAL_USER])),

    ?assertMatch({error, not_found}, rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, lookup, [QName])),

    ok.
