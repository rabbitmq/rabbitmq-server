-module(confirms_rejects_SUITE).


-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-compile(export_all).

all() ->
    [
      {group, parallel_tests}
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
        Testcase).

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
    end_per_testcase0(Testcase, Config).

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
    after 10000 ->
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
        after 10000 ->
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

    timer:sleep(1000),

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
    after 10000 -> error(timeout_waiting_for_ack)
    end,
    clean_acks_mailbox().

assert_nack() ->
    receive {'basic.nack', _, _, _} -> ok
    after 10000 -> error(timeout_waiting_for_nack)
    end,
    clean_acks_mailbox().

assert_acks(N) ->
    receive {'basic.ack', N, _} -> ok
    after 10000 -> error({timeout_waiting_for_ack, N})
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
    % ct:pal("Received ~p~n", [Received]),
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
            ct:pal("flush ~p", [Any]),
            flush()
    after 0 ->
              ok
    end.
