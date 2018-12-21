-module(confirms_rejects_SUITE).


-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-compile(export_all).

all() ->
    [
      {group, parallel_tests}
    ].

groups() ->
    [
      {parallel_tests, [parallel], [
          confirms_rejects_conflict
        ]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).


init_per_group(Group, Config) ->
    ClusterSize = 2,
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, Group},
        {rmq_nodes_count, ClusterSize}
      ]),
    rabbit_ct_helpers:run_steps(Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
      rabbit_ct_client_helpers:setup_steps()).

end_per_group(_Group, Config) ->
    rabbit_ct_helpers:run_steps(Config,
      rabbit_ct_client_helpers:teardown_steps() ++
      rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    Conn = rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    Conn1 = rabbit_ct_client_helpers:open_unmanaged_connection(Config),

    rabbit_ct_helpers:testcase_started(
        rabbit_ct_helpers:set_config(Config, [{conn, Conn}, {conn1, Conn1}]),
        Testcase).

end_per_testcase(confirms_rejects_conflict = Testcase, Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    amqp_channel:call(Ch, #'queue.delete'{queue = <<"confirms_rejects_conflict">>}),
    rabbit_ct_client_helpers:close_channels_and_connection(Config, 0),

    Conn = ?config(conn, Config),
    Conn1 = ?config(conn1, Config),

    rabbit_ct_client_helpers:close_connection(Conn),
    rabbit_ct_client_helpers:close_connection(Conn1),

    rabbit_ct_helpers:testcase_finished(Config, Testcase).

confirms_rejects_conflict(Config) ->
    Conn = ?config(conn, Config),
    Conn1 = ?config(conn1, Config),

    {ok, Ch} = amqp_connection:open_channel(Conn),
    {ok, Ch1} = amqp_connection:open_channel(Conn1),

    false = Conn =:= Conn1,
    false = Ch =:= Ch1,

    QueueName = <<"confirms_rejects_conflict">>,

    amqp_channel:call(Ch, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Ch, self()),

    amqp_channel:call(Ch, #'queue.declare'{queue = QueueName,
                                           durable = true,
                                           arguments = [{<<"x-max-length">>,long,12},
                                                        {<<"x-overflow">>,longstr,<<"reject-publish">>}]
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
