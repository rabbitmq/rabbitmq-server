-module(confirms_rejects_SUITE).


-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").
-include_lib("rabbit/include/amqqueue.hrl").
-compile(export_all).

-define(TIMEOUT, 30_000).

all() ->
    [
      {group, parallel_tests},
      {group, queue_deletion}
    ].

groups() ->
    OverflowTests = [
      confirms_rejects_conflict,
      policy_resets_to_default
    ],
    [
      {parallel_tests, [parallel], [
        {overflow_reject_publish_dlx, [parallel], OverflowTests},
        {overflow_reject_publish, [parallel], OverflowTests},
        dead_queue_rejects,
        mixed_dead_alive_queues_reject
      ]},
      {queue_deletion, [], [
        deleted_exclusive_classic_queue_confirms,
        deleted_remote_exclusive_classic_queue_confirms,
        deleted_classic_queue_confirms,
        crashed_exclusive_classic_queue_rejects,
        crashed_remote_exclusive_classic_queue_rejects,
        replaced_remote_exclusive_classic_queue_rejects,
        classic_queue_event_ignored_by_quorum_queue,
        restarted_classic_queue_rejects,
        deleted_quorum_queue_confirms,
        deleted_quorum_queue_with_unreachable_member_confirms,
        stopped_quorum_queue_rejects
      ]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).


init_per_group(overflow_reject_publish, Config) ->
    rabbit_ct_helpers:set_config(Config, [
        {overflow, <<"reject-publish">>}
      ]);
init_per_group(overflow_reject_publish_dlx, Config) ->
    rabbit_ct_helpers:set_config(Config, [
        {overflow, <<"reject-publish-dlx">>}
      ]);
init_per_group(Group, Config) ->
    ClusterSize = 2,
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group},
        {rmq_nodes_count, ClusterSize}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(overflow_reject_publish, _Config) ->
    ok;
end_per_group(overflow_reject_publish_dlx, _Config) ->
    ok;
end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(policy_resets_to_default = Testcase, Config) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    rabbit_ct_helpers:testcase_started(
        rabbit_ct_helpers:set_config(Config, [{conn, Conn}]), Testcase);
init_per_testcase(Testcase, Config)
        when Testcase == confirms_rejects_conflict;
             Testcase == dead_queue_rejects;
             Testcase == mixed_dead_alive_queues_reject ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    Conn1 = rabbit_ct_client_helpers:open_unmanaged_connection(Config),

    rabbit_ct_helpers:testcase_started(
        rabbit_ct_helpers:set_config(Config, [{conn, Conn}, {conn1, Conn1}]),
        Testcase);
init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(policy_resets_to_default = Testcase, Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    XOverflow = ?config(overflow, Config),
    QueueName = <<"policy_resets_to_default", "_", XOverflow/binary>>,
    amqp_channel:call(Ch, #'queue.delete'{queue = QueueName}),
    rabbit_ct_client_helpers:close_channels_and_connection(Config, 0),

    Conn = ?config(conn, Config),

    rabbit_ct_client_helpers:close_connection(Conn),

    rabbit_ct_helpers:testcase_finished(Config, Testcase);
end_per_testcase(confirms_rejects_conflict = Testcase, Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    XOverflow = ?config(overflow, Config),
    QueueName = <<"confirms_rejects_conflict", "_", XOverflow/binary>>,
    amqp_channel:call(Ch, #'queue.delete'{queue = QueueName}),
    end_per_testcase0(Testcase, Config);
end_per_testcase(dead_queue_rejects = Testcase, Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    amqp_channel:call(Ch, #'queue.delete'{queue = <<"dead_queue_rejects">>}),
    end_per_testcase0(Testcase, Config);
end_per_testcase(mixed_dead_alive_queues_reject = Testcase, Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    amqp_channel:call(Ch, #'queue.delete'{queue = <<"mixed_dead_alive_queues_reject_dead">>}),
    amqp_channel:call(Ch, #'queue.delete'{queue = <<"mixed_dead_alive_queues_reject_alive">>}),
    amqp_channel:call(Ch, #'exchange.delete'{exchange = <<"mixed_dead_alive_queues_reject">>}),
    end_per_testcase0(Testcase, Config);
end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

end_per_testcase0(Testcase, Config) ->
    rabbit_ct_client_helpers:close_channels_and_connection(Config, 0),

    Conn = ?config(conn, Config),
    Conn1 = ?config(conn1, Config),

    rabbit_ct_client_helpers:close_connection(Conn),
    rabbit_ct_client_helpers:close_connection(Conn1),

    clean_acks_mailbox(),

    rabbit_ct_helpers:testcase_finished(Config, Testcase).

dead_queue_rejects(Config) ->
    Conn = ?config(conn, Config),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    QueueName = <<"dead_queue_rejects">>,
    amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),

    amqp_channel:call(Ch, #'queue.declare'{queue = QueueName,
                                           durable = true}),

    amqp_channel:call(Ch, #'basic.publish'{routing_key = QueueName},
                                  #amqp_msg{payload = <<"HI">>}),

    receive
        {'basic.ack',_,_} -> ok
    after ?TIMEOUT ->
        error(timeout_waiting_for_initial_ack)
    end,

    BasicPublish = #'basic.publish'{routing_key = QueueName},
    AmqpMsg = #amqp_msg{payload = <<"HI">>},
    kill_queue_expect_nack(Config, Ch, QueueName, BasicPublish, AmqpMsg, 5).

mixed_dead_alive_queues_reject(Config) ->
    Conn = ?config(conn, Config),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    QueueNameDead = <<"mixed_dead_alive_queues_reject_dead">>,
    QueueNameAlive = <<"mixed_dead_alive_queues_reject_alive">>,
    ExchangeName = <<"mixed_dead_alive_queues_reject">>,

    amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),

    amqp_channel:call(Ch, #'queue.declare'{queue = QueueNameDead,
                                           durable = true}),
    amqp_channel:call(Ch, #'queue.declare'{queue = QueueNameAlive,
                                           durable = true}),

    amqp_channel:call(Ch, #'exchange.declare'{exchange = ExchangeName,
                                              durable = true}),

    amqp_channel:call(Ch, #'queue.bind'{exchange = ExchangeName,
                                        queue = QueueNameAlive,
                                        routing_key = <<"route">>}),

    amqp_channel:call(Ch, #'queue.bind'{exchange = ExchangeName,
                                        queue = QueueNameDead,
                                        routing_key = <<"route">>}),

    amqp_channel:call(Ch, #'basic.publish'{exchange = ExchangeName,
                                           routing_key = <<"route">>},
                      #amqp_msg{payload = <<"HI">>}),

    receive
        {'basic.ack',_,_} -> ok;
        {'basic.nack',_,_,_} -> error(expecting_ack_got_nack)
    after 50000 ->
        error({timeout_waiting_for_initial_ack, process_info(self(), messages)})
    end,

    BasicPublish = #'basic.publish'{exchange = ExchangeName, routing_key = <<"route">>},
    AmqpMsg = #amqp_msg{payload = <<"HI">>},
    kill_queue_expect_nack(Config, Ch, QueueNameDead, BasicPublish, AmqpMsg, 5).

kill_queue_expect_nack(_Config, _Ch, _QueueName, _BasicPublish, _AmqpMsg, 0) ->
    error(expecting_nack_got_ack);
kill_queue_expect_nack(Config, Ch, QueueName, BasicPublish, AmqpMsg, Tries) ->
    kill_the_queue(QueueName, Config),
    amqp_channel:cast(Ch, BasicPublish, AmqpMsg),
    R = receive
            {'basic.nack',_,_,_} ->
                ok;
            {'basic.ack',_,_} ->
                retry
        after ?TIMEOUT ->
                  error({timeout_waiting_for_nack, process_info(self(), messages)})
        end,
    case R of
        ok ->
            ok;
        retry ->
            kill_queue_expect_nack(Config, Ch, QueueName, BasicPublish, AmqpMsg, Tries - 1)
    end.

confirms_rejects_conflict(Config) ->
    Conn = ?config(conn, Config),
    Conn1 = ?config(conn1, Config),

    {ok, Ch} = amqp_connection:open_channel(Conn),
    {ok, Ch1} = amqp_connection:open_channel(Conn1),

    false = Conn =:= Conn1,
    false = Ch =:= Ch1,

    amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),

    XOverflow = ?config(overflow, Config),
    QueueName = <<"confirms_rejects_conflict", "_", XOverflow/binary>>,
    amqp_channel:call(Ch, #'queue.declare'{queue = QueueName,
                                           durable = true,
                                           arguments = [{<<"x-max-length">>, long, 12},
                                                        {<<"x-overflow">>, longstr, XOverflow}]
                                           }),
    %% Consume 3 messages at once. Do that often.
    Consume = fun Consume() ->
        receive
            stop -> ok
        after 1 ->
            amqp_channel:cast(Ch1, #'basic.get'{queue = QueueName, no_ack = true}),
            amqp_channel:cast(Ch1, #'basic.get'{queue = QueueName, no_ack = true}),
            amqp_channel:cast(Ch1, #'basic.get'{queue = QueueName, no_ack = true}),
            amqp_channel:cast(Ch1, #'basic.get'{queue = QueueName, no_ack = true}),
            Consume()
        end
    end,

    Produce = fun
        Produce(0) -> ok;
        Produce(N) ->
            amqp_channel:call(Ch, #'basic.publish'{routing_key = QueueName},
                                  #amqp_msg{payload = <<"HI">>}),
            Produce(N - 1)
    end,

    %% Initial queue should be full
    % Produce(20),

    %% Consumer is a separate process.
    Consumer = spawn(Consume),

    %% A long run. Should create race conditions hopefully.
    Produce(500000),

    Result = validate_acks_mailbox(),

    Consumer ! stop,
    % Result.
    case Result of
        ok -> ok;
        {error, E} -> error(E)
    end.

policy_resets_to_default(Config) ->
    Conn = ?config(conn, Config),

    {ok, Ch} = amqp_connection:open_channel(Conn),

    amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),

    XOverflow = ?config(overflow, Config),
    QueueName = <<"policy_resets_to_default", "_", XOverflow/binary>>,
    amqp_channel:call(Ch, #'queue.declare'{queue = QueueName,
                                           durable = true
                                           }),
    MaxLength = 2,
    rabbit_ct_broker_helpers:set_policy(
        Config, 0,
        QueueName, QueueName, <<"queues">>,
        [{<<"max-length">>, MaxLength}, {<<"overflow">>, XOverflow}]),

    ?awaitMatch([_, _], get_policy_definition(Config, QueueName), 30000),

    [amqp_channel:call(Ch, #'basic.publish'{routing_key = QueueName},
                           #amqp_msg{payload = <<"HI">>})
     || _ <- lists:seq(1, MaxLength)],

    assert_acks(MaxLength),

    #'queue.declare_ok'{message_count = MaxLength} =
        amqp_channel:call(Ch, #'queue.declare'{queue = QueueName,
                                               durable = true}),

    RejectedMessage = <<"HI-rejected">>,
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QueueName},
                          #amqp_msg{payload = RejectedMessage}),

    assert_nack(),

    rabbit_ct_broker_helpers:set_policy(
        Config, 0,
        QueueName, QueueName, <<"queues">>,
        [{<<"max-length">>, MaxLength}]),

    ?awaitMatch([_], get_policy_definition(Config, QueueName), 30000),

    NotRejectedMessage = <<"HI-not-rejected">>,
    amqp_channel:call(Ch, #'basic.publish'{routing_key = QueueName},
                          #amqp_msg{payload = NotRejectedMessage}),

    assert_ack(),

    #'queue.declare_ok'{message_count = MaxLength} =
        amqp_channel:call(Ch, #'queue.declare'{queue = QueueName,
                                               durable = true}),

    Msgs = consume_all_messages(Ch, QueueName),
    case {lists:member(RejectedMessage, Msgs), lists:member(NotRejectedMessage, Msgs)} of
        {true, _}  -> error({message_should_be_rejected, RejectedMessage});
        {_, false} -> error({message_should_be_enqueued, NotRejectedMessage});
        _ -> ok
    end.

get_policy_definition(Config, QueueName) ->
    {ok, Q} = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, lookup,
                                           [rabbit_misc:r(<<"/">>, queue, QueueName)]),
    case amqqueue:get_policy(Q) of
        undefined ->
            undefined;
        Policy ->
            proplists:get_value(definition, Policy, [])
    end.

consume_all_messages(Ch, QueueName) ->
    consume_all_messages(Ch, QueueName, []).

consume_all_messages(Ch, QueueName, Msgs) ->
    case amqp_channel:call(Ch, #'basic.get'{queue = QueueName, no_ack = true}) of
        {#'basic.get_ok'{}, #amqp_msg{payload = Msg}} ->
            consume_all_messages(Ch, QueueName, [Msg | Msgs]);
        #'basic.get_empty'{} -> Msgs
    end.

assert_ack() ->
    receive {'basic.ack', _, _} -> ok
    after ?TIMEOUT -> error(timeout_waiting_for_ack)
    end,
    clean_acks_mailbox().

assert_nack() ->
    receive {'basic.nack', _, _, _} -> ok
    after ?TIMEOUT -> error(timeout_waiting_for_nack)
    end,
    clean_acks_mailbox().

assert_acks(N) ->
    receive {'basic.ack', N, _} -> ok
    after ?TIMEOUT -> error({timeout_waiting_for_ack, N})
    end,
    clean_acks_mailbox().

validate_acks_mailbox() ->
    Result = validate_acks_mailbox({0, ok}),
    clean_acks_mailbox(),
    Result.

validate_acks_mailbox({LatestMultipleN, LatestMultipleAck}) ->
    Received = receive
        {'basic.ack', N, Multiple} = A -> {N, Multiple, A};
        {'basic.nack', N, Multiple, _} = A -> {N, Multiple, A}
    after
        10000 -> none
    end,
    % ct:pal("Received ~tp~n", [Received]),
    case Received of
        {LatestN, IsMultiple, AckOrNack} ->
            case LatestN < LatestMultipleN of
                true ->
                    {error, {received_ack_lower_than_latest_multiple, AckOrNack, smaller_than, LatestMultipleAck}};
                false ->
                    case IsMultiple of
                        true  -> validate_acks_mailbox({LatestN, AckOrNack});
                        false -> validate_acks_mailbox({LatestMultipleN, LatestMultipleAck})
                    end
            end;
        none -> ok
    end.

clean_acks_mailbox() ->
    receive
        {'basic.ack', _, _} -> clean_acks_mailbox();
        {'basic.nack', _, _, _} -> clean_acks_mailbox()
    after
        1000 -> done
    end.

deleted_exclusive_classic_queue_confirms(Config) ->
    ?assertEqual({noproc, eol},
                 rabbit_ct_broker_helpers:rpc(
                   Config, 0, ?MODULE, deliver_to_deleted_exclusive_queue,
                   [<<"deleted_exclusive_classic_queue_confirms">>])).

deleted_remote_exclusive_classic_queue_confirms(Config) ->
    QName = <<"deleted_remote_exclusive_classic_queue_confirms">>,
    {Owner, Q} = rabbit_ct_broker_helpers:rpc(
                   Config, 1, ?MODULE, declare_exclusive_queue, [QName]),
    Blockers = rabbit_ct_broker_helpers:rpc(Config, 1, ?MODULE,
                                            block_worker_pool, []),
    try
        Owner ! stop,
        await_exit(amqqueue:get_pid(Q)),
        ?assertMatch({ok, _}, rabbit_ct_broker_helpers:rpc(
                                Config, 0, rabbit_amqqueue, lookup,
                                [amqqueue:get_name(Q)])),
        ?assertEqual({noproc, eol},
                     rabbit_ct_broker_helpers:rpc(
                       Config, 0, ?MODULE, deliver_to_stale_record,
                       [Q, #{correlation => 1}]))
    after
        [Blocker ! release || Blocker <- Blockers]
    end.

crashed_remote_exclusive_classic_queue_rejects(Config) ->
    QName = <<"crashed_remote_exclusive_classic_queue_rejects">>,
    {Owner, Q} = rabbit_ct_broker_helpers:rpc(
                   Config, 1, ?MODULE, declare_exclusive_queue, [QName]),
    QPid = amqqueue:get_pid(Q),
    Sup = rabbit_ct_broker_helpers:rpc(Config, 1, ?MODULE, queue_sup, [QPid]),
    ok = sys:suspend(Sup),
    try
        exit(QPid, kill),
        await_exit(QPid),
        {[{rejected, _, down, SeqNos} | _], none} =
            rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE,
                                         deliver_twice_to_stale_record, [Q]),
        ?assertEqual([1, 2], lists:sort(SeqNos))
    after
        ok = sys:resume(Sup),
        Owner ! stop
    end.

replaced_remote_exclusive_classic_queue_rejects(Config) ->
    QName = <<"replaced_remote_exclusive_classic_queue_rejects">>,
    {Owner, Q} = rabbit_ct_broker_helpers:rpc(
                   Config, 1, ?MODULE, declare_exclusive_queue, [QName]),
    QRes = amqqueue:get_name(Q),
    Blockers = rabbit_ct_broker_helpers:rpc(Config, 1, ?MODULE,
                                            block_worker_pool, []),
    Owner ! stop,
    await_exit(amqqueue:get_pid(Q)),
    {QStates, OwnerResult} =
        try
            rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE,
                                         hold_owner_result, [Q])
        after
            [Blocker ! release || Blocker <- Blockers]
        end,
    {Owner2, Q2} = rabbit_ct_broker_helpers:rpc(
                     Config, 1, ?MODULE, declare_exclusive_queue_when_deleted,
                     [QName]),
    try
        ok = rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, await_pid,
                                          [QRes, amqqueue:get_pid(Q2), 100]),
        ?assertMatch({ok, _, [{rejected, _, down, [1]} | _]},
                     rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_queue_type,
                                                  handle_event,
                                                  [QRes, OwnerResult, QStates]))
    after
        Owner2 ! stop
    end.

classic_queue_event_ignored_by_quorum_queue(Config) ->
    QName = <<"classic_queue_event_ignored_by_quorum_queue">>,
    {ok, Q} = rabbit_ct_broker_helpers:rpc(
                Config, 0, ?MODULE, declare_queue, [QName, <<"quorum">>, none]),
    QRes = amqqueue:get_name(Q),
    Evt = {'$queue_type', rabbit_classic_queue,
           {owner_alive, make_ref(), self(), false}},
    try
        ?assertMatch({ok, _, []},
                     rabbit_ct_broker_helpers:rpc(
                       Config, 0, ?MODULE, handle_event_with_new_ctx,
                       [Q, QRes, Evt]))
    after
        {ok, _} = rabbit_ct_broker_helpers:rpc(
                    Config, 0, rabbit_amqqueue, delete,
                    [Q, false, false, <<"acting-user">>])
    end.

deleted_classic_queue_confirms(Config) ->
    ?assertEqual({noproc, eol},
                 rabbit_ct_broker_helpers:rpc(
                   Config, 0, ?MODULE, deliver_to_deleted_queue,
                   [<<"deleted_classic_queue_confirms">>, <<"classic">>,
                    #{correlation => 1}])).

crashed_exclusive_classic_queue_rejects(Config) ->
    ?assertMatch({noproc, [{rejected, _, down, [1]} | _]},
                 rabbit_ct_broker_helpers:rpc(
                   Config, 0, ?MODULE, deliver_to_crashed_exclusive_queue,
                   [<<"crashed_exclusive_classic_queue_rejects">>])).

restarted_classic_queue_rejects(Config) ->
    ?assertMatch({noproc, [{rejected, _, down, [1]} | _]},
                 rabbit_ct_broker_helpers:rpc(
                   Config, 0, ?MODULE, deliver_to_restarted_queue,
                   [<<"restarted_classic_queue_rejects">>])).

deleted_quorum_queue_confirms(Config) ->
    ?assertMatch([{settled, _, [1]}],
                 rabbit_ct_broker_helpers:rpc(
                   Config, 0, ?MODULE, deliver_to_deleted_queue,
                   [<<"deleted_quorum_queue_confirms">>, <<"quorum">>,
                    #{correlation => 1}])),
    ?assertMatch([{queue_deleted, _}],
                 rabbit_ct_broker_helpers:rpc(
                   Config, 0, ?MODULE, deliver_to_deleted_queue,
                   [<<"deleted_quorum_queue_confirms_amqp">>, <<"quorum">>,
                    #{correlation => 1, report_deleted_queues => true}])).

deleted_quorum_queue_with_unreachable_member_confirms(Config) ->
    ?assertMatch([{settled, _, [1]}],
                 rabbit_ct_broker_helpers:rpc(
                   Config, 0, ?MODULE,
                   deliver_to_deleted_queue_with_unreachable_member,
                   [<<"deleted_quorum_queue_with_unreachable_member_confirms">>])).

stopped_quorum_queue_rejects(Config) ->
    QName = <<"stopped_quorum_queue_rejects">>,
    {ok, Q} = rabbit_ct_broker_helpers:rpc(
                Config, 0, ?MODULE, declare_queue, [QName, <<"quorum">>, none]),
    {RaName, _} = amqqueue:get_pid(Q),
    Members = [{RaName, Node}
               || Node <- rabbit_ct_broker_helpers:rpc(
                            Config, 0, rabbit_amqqueue, get_quorum_nodes, [Q])],
    [ok = rabbit_ct_broker_helpers:rpc(Config, 0, ra, stop_server,
                                       [quorum_queues, Member])
     || Member <- Members],
    try
        ?assertMatch([{rejected, _, maxlen, [1]}],
                     rabbit_ct_broker_helpers:rpc(
                       Config, 0, ?MODULE, deliver_to_stale_record,
                       [Q, #{correlation => 1}]))
    after
        [ok = rabbit_ct_broker_helpers:rpc(Config, 0, ra, restart_server,
                                           [quorum_queues, Member])
         || Member <- Members],
        {ok, _} = rabbit_ct_broker_helpers:rpc(
                    Config, 0, rabbit_amqqueue, delete,
                    [Q, false, false, <<"acting-user">>])
    end.

declare_queue(QName, Type, Owner) ->
    case rabbit_amqqueue:declare(rabbit_misc:r(<<"/">>, queue, QName), true,
                                 false, [{<<"x-queue-type">>, longstr, Type}],
                                 Owner, <<"acting-user">>) of
        {new, Q} -> {ok, Q}
    end.

%% The record of an exclusive queue deleted with its owner is deleted through
%% `worker_pool' after the queue process exits, so blocking the pool keeps the
%% record in place.
declare_exclusive_queue(QName) ->
    Owner = spawn(fun() -> receive stop -> ok end end),
    {ok, Q} = declare_queue(QName, <<"classic">>, Owner),
    {Owner, Q}.

declare_exclusive_queue_when_deleted(QName) ->
    QRes = rabbit_misc:r(<<"/">>, queue, QName),
    await_deleted(QRes, 100),
    declare_exclusive_queue(QName).

await_deleted(_QRes, 0) ->
    error(queue_not_deleted);
await_deleted(QRes, N) ->
    case rabbit_amqqueue:lookup(QRes) of
        {error, not_found} ->
            ok;
        {ok, _} ->
            timer:sleep(50),
            await_deleted(QRes, N - 1)
    end.

await_pid(_QRes, _Pid, 0) ->
    error(pid_not_visible);
await_pid(QRes, Pid, N) ->
    case rabbit_amqqueue:lookup(QRes) of
        {ok, Q} when ?amqqueue_pid_equals(Q, Pid) ->
            ok;
        _ ->
            timer:sleep(50),
            await_pid(QRes, Pid, N - 1)
    end.

%% Returns the owner check result without handling it.
hold_owner_result(Q) ->
    QRes = amqqueue:get_name(Q),
    {ok, QStates0, []} = rabbit_queue_type:deliver([Q], message(QRes),
                                                   #{correlation => 1},
                                                   rabbit_queue_type:init()),
    QStates = handle_noproc_down(QRes, QStates0),
    receive
        {'$gen_cast', {queue_event, QRes, Evt}} -> {QStates, Evt}
    after ?TIMEOUT ->
              error(no_owner_result)
    end.

handle_event_with_new_ctx(Q, QRes, Evt) ->
    QStates = rabbit_queue_type:new(Q, rabbit_queue_type:init()),
    rabbit_queue_type:handle_event(QRes, Evt, QStates).

queue_sup(QPid) ->
    {dictionary, Dict} = process_info(QPid, dictionary),
    hd(proplists:get_value('$ancestors', Dict)).

deliver_to_deleted_exclusive_queue(QName) ->
    {Owner, Q} = declare_exclusive_queue(QName),
    Blockers = block_worker_pool(),
    try
        Owner ! stop,
        await_exit(amqqueue:get_pid(Q)),
        {ok, Q} = rabbit_amqqueue:lookup(amqqueue:get_name(Q)),
        deliver_to_stale_record(Q, #{correlation => 1})
    after
        [Blocker ! release || Blocker <- Blockers]
    end.

deliver_to_crashed_exclusive_queue(QName) ->
    {Owner, Q} = declare_exclusive_queue(QName),
    QPid = amqqueue:get_pid(Q),
    Sup = queue_sup(QPid),
    %% Keep the record pointing to the dead process.
    ok = sys:suspend(Sup),
    try
        exit(QPid, kill),
        await_exit(QPid),
        {ok, Q} = rabbit_amqqueue:lookup(amqqueue:get_name(Q)),
        deliver_to_stale_record(Q, #{correlation => 1})
    after
        ok = sys:resume(Sup),
        Owner ! stop
    end.

deliver_to_deleted_queue(QName, Type, Options) ->
    {ok, Q} = declare_queue(QName, Type, none),
    {ok, 0} = rabbit_amqqueue:delete(Q, false, false, <<"acting-user">>),
    _ = [await_exit(amqqueue:get_pid(Q)) || Type =:= <<"classic">>],
    deliver_to_stale_record(Q, Options).

deliver_to_deleted_queue_with_unreachable_member(QName) ->
    {ok, Q0} = declare_queue(QName, <<"quorum">>, none),
    {ok, 0} = rabbit_amqqueue:delete(Q0, false, false, <<"acting-user">>),
    #{nodes := Nodes} = TypeState = amqqueue:get_type_state(Q0),
    Unreachable = 'unreachable@localhost',
    Nodes1 = case Nodes of
                 L when is_list(L) -> [Unreachable | L];
                 M when is_map(M) -> M#{Unreachable => <<"uid">>}
             end,
    Q = amqqueue:set_type_state(Q0, TypeState#{nodes => Nodes1}),
    deliver_to_stale_record(Q, #{correlation => 1}).

deliver_to_restarted_queue(QName) ->
    {ok, Q} = declare_queue(QName, <<"classic">>, none),
    QRes = amqqueue:get_name(Q),
    QPid = amqqueue:get_pid(Q),
    try
        exit(QPid, kill),
        await_restart(QRes, QPid, 100),
        deliver_to_stale_record(Q, #{correlation => 1})
    after
        {ok, Q2} = rabbit_amqqueue:lookup(QRes),
        {ok, _} = rabbit_amqqueue:delete(Q2, false, false, <<"acting-user">>)
    end.

block_worker_pool() ->
    Self = self(),
    Size = worker_pool_sup:default_pool_size(),
    [worker_pool:submit_async(fun() ->
                                      Self ! {blocked, self()},
                                      receive release -> ok
                                      after ?TIMEOUT -> ok
                                      end
                              end)
     || _ <- lists:seq(1, Size)],
    [receive {blocked, Worker} -> Worker end || _ <- lists:seq(1, Size)].

await_exit(Pid) ->
    MRef = monitor(process, Pid),
    receive {'DOWN', MRef, process, Pid, _} -> ok end.

await_restart(_QRes, _OldPid, 0) ->
    error(queue_not_restarted);
await_restart(QRes, OldPid, N) ->
    {ok, Q} = rabbit_amqqueue:lookup(QRes),
    case amqqueue:get_pid(Q) of
        OldPid ->
            timer:sleep(50),
            await_restart(QRes, OldPid, N - 1);
        _ ->
            ok
    end.

message(#resource{name = RoutingKey}) ->
    {ok, Msg} = mc_amqpl:message(rabbit_misc:r(<<"/">>, exchange, <<>>),
                                 RoutingKey,
                                 #content{class_id = 60,
                                          properties = #'P_basic'{delivery_mode = 2},
                                          payload_fragments_rev = [<<"msg">>]}),
    Msg.

%% The second delivery reaches the exited process while the owner of the
%% queue is being checked, so both publishes depend on that one check.
deliver_twice_to_stale_record(Q) ->
    QRes = amqqueue:get_name(Q),
    {ok, QS1, []} = rabbit_queue_type:deliver([Q], message(QRes),
                                              #{correlation => 1},
                                              rabbit_queue_type:init()),
    QS2 = handle_noproc_down(QRes, QS1),
    {ok, QS3, []} = rabbit_queue_type:deliver([Q], message(QRes),
                                              #{correlation => 2}, QS2),
    QS4 = handle_noproc_down(QRes, QS3),
    Actions = receive
                  {'$gen_cast', {queue_event, QRes, Evt}} ->
                      {ok, _, As} = rabbit_queue_type:handle_event(QRes, Evt, QS4),
                      As
              after ?TIMEOUT ->
                        timeout
              end,
    Extra = receive
                {'$gen_cast', {queue_event, QRes, Evt2}} -> Evt2
            after 1000 ->
                      none
            end,
    {Actions, Extra}.

handle_noproc_down(QRes, QStates0) ->
    receive
        {{'DOWN', QRes}, _, process, Pid, noproc} ->
            {ok, QStates, []} = rabbit_queue_type:handle_down(Pid, QRes, noproc,
                                                              QStates0),
            QStates
    after ?TIMEOUT ->
              error(no_down)
    end.

%% Delivers using a queue record read before the queue process exited, so
%% that a classic queue monitor reports `noproc'. With a `queue_target'
%% instead, a deleted record would fail the lookup in `rabbit_queue_type'.
deliver_to_stale_record(Q, Options) ->
    QRes = amqqueue:get_name(Q),
    {ok, QStates, Actions} = rabbit_queue_type:deliver(
                               [Q], message(QRes), Options,
                               rabbit_queue_type:init()),
    case Actions of
        [] -> await_outcome(QRes, QStates, undefined);
        _ -> Actions
    end.

%% Returns the reason of the first DOWN and the outcome.
await_outcome(QRes, QStates, DownInfo) ->
    receive
        {{'DOWN', QRes}, _, process, Pid, Info} ->
            Info1 = case DownInfo of
                        undefined -> Info;
                        _ -> DownInfo
                    end,
            outcome(rabbit_queue_type:handle_down(Pid, QRes, Info, QStates),
                    QRes, Info1);
        {'$gen_cast', {queue_event, QRes, Evt}} ->
            outcome(rabbit_queue_type:handle_event(QRes, Evt, QStates),
                    QRes, DownInfo)
    after ?TIMEOUT ->
              timeout
    end.

outcome({ok, QStates, []}, QRes, DownInfo) ->
    await_outcome(QRes, QStates, DownInfo);
outcome({ok, _, Actions}, _QRes, DownInfo) ->
    {DownInfo, Actions};
outcome({eol, _, _}, _QRes, DownInfo) ->
    {DownInfo, eol};
outcome({eol, _}, _QRes, DownInfo) ->
    {DownInfo, eol}.

kill_the_queue(QueueName, Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, kill_the_queue, [QueueName]).

kill_the_queue(QueueName) ->
    [begin
        {ok, Q} = rabbit_amqqueue:lookup({resource, <<"/">>, queue, QueueName}),
        Pid = amqqueue:get_pid(Q),
        ct:pal("~w killed", [Pid]),
        timer:sleep(1),
        exit(Pid, kill)
     end
     || _ <- lists:seq(1, 50)],
    timer:sleep(1),
    {ok, Q} = rabbit_amqqueue:lookup({resource, <<"/">>, queue, QueueName}),
    Pid = amqqueue:get_pid(Q),
    case is_process_alive(Pid) of
        %% Try to kill it again
        true  -> kill_the_queue(QueueName);
        false -> ok
    end.

flush() ->
    receive
        Any ->
            ct:pal("flush ~tp", [Any]),
            flush()
    after 0 ->
              ok
    end.
