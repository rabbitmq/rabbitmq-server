-module(rabbit_fifo_dlx_integration_SUITE).

%% Integration tests for at-least-once dead-lettering comprising mainly
%% rabbit_fifo_dlx, rabbit_fifo_dlx_worker, rabbit_fifo_dlx_client
%% rabbit_quorum_queue, rabbit_fifo.

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-import(quorum_queue_utils, [wait_for_messages_ready/3,
                             dirty_query/3,
                             ra_name/1]).
-import(quorum_queue_SUITE, [publish/2,
                             consume/3]).

-compile([nowarn_export_all, export_all]).

all() ->
    [
     {group, single_node}
    ].

groups() ->
    [{single_node, [], [
                        expired,
                        rejected,
                        delivery_limit,
                        target_queue_not_bound,
                        dlx_missing
                       ]}].

%% TODO add tests for:
%% * overview and query functions return correct result / stats
%% * dlx_worker resends in various topology misconfigurations
%% * dlx_worker resends when target queue is down (e.g. node is down where non-mirrored classic queue resides)
%% * we comply with mandatory + publisher confirm semantics, e.g. with 3 target queues (1 classic queue, 1 quorum queue, 1 stream)
%% * there is always single leader in 3 node cluster (check via supervisor:count_children and by killing one node)
%% * fall back to at-most-once works
%% * switching between at-most-once and at-least-once works including rabbit_fifo_dlx:cleanup

init_per_suite(Config0) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:merge_app_env(
                Config0, {rabbit, [{quorum_tick_interval, 1000},
                                   {dead_letter_worker_consumer_prefetch, 2},
                                   {dead_letter_worker_publisher_confirm_timeout_ms, 1000}
                                  ]}),
    Config2 = rabbit_ct_helpers:merge_app_env(
                Config1, {aten, [{poll_interval, 1000}]}),
    rabbit_ct_helpers:run_setup_steps(Config2).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodes_count, 1},
                                            {rmq_nodename_suffix, Group},
                                            {tcp_ports_base},
                                            {net_ticktime, 10}]),
    Config2 =  rabbit_ct_helpers:run_steps(Config1,
                                           [fun merge_app_env/1 ] ++
                                           rabbit_ct_broker_helpers:setup_steps()),
    ok = rabbit_ct_broker_helpers:rpc(
           Config2, 0, application, set_env,
           [rabbit, channel_tick_interval, 100]),
    timer:sleep(1000),
    Config2.

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:teardown_steps()).

merge_app_env(Config) ->
    rabbit_ct_helpers:merge_app_env(
      rabbit_ct_helpers:merge_app_env(Config,
                                      {rabbit, [{core_metrics_gc_interval, 100}]}),
      {ra, [{min_wal_roll_over_interval, 30000}]}).

init_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),
    Q = rabbit_data_coercion:to_binary(Testcase),
    Config2 = rabbit_ct_helpers:set_config(Config1,
                                           [{source_queue, <<Q/binary, "_source">>},
                                            {target_queue_1, <<Q/binary, "_target_1">>},
                                            {target_queue_2, <<Q/binary, "_target_2">>}
                                           ]),
    rabbit_ct_helpers:run_steps(Config2, rabbit_ct_client_helpers:setup_steps()).


end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(
                Config,
                rabbit_ct_client_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% Test that at-least-once dead-lettering works for message dead-lettered due to message TTL.
expired(Config) ->
    {_Server, Ch, SourceQ, TargetQ, _DLX} = Objects = declare_topology(Config, []),
    Msg = <<"msg">>,
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = SourceQ},
                           #amqp_msg{props   = #'P_basic'{expiration = <<"0">>},
                                     payload = Msg}),
    {_, #amqp_msg{props = #'P_basic'{headers = Headers,
                                     expiration = undefined}}} =
    ?awaitMatch({#'basic.get_ok'{}, #amqp_msg{payload = Msg}},
                amqp_channel:call(Ch, #'basic.get'{queue = TargetQ}),
                1000),
    ?assertEqual({longstr, <<"expired">>}, rabbit_misc:table_lookup(Headers, <<"x-first-death-reason">>)),
    ?assertEqual({longstr, SourceQ}, rabbit_misc:table_lookup(Headers, <<"x-first-death-queue">>)),
    ?assertEqual({longstr, <<>>}, rabbit_misc:table_lookup(Headers, <<"x-first-death-exchange">>)),
    {array, [{table, Death}]} = rabbit_misc:table_lookup(Headers, <<"x-death">>),
    ?assertEqual({longstr, SourceQ}, rabbit_misc:table_lookup(Death, <<"queue">>)),
    ?assertEqual({longstr, <<"expired">>}, rabbit_misc:table_lookup(Death, <<"reason">>)),
    ?assertEqual({longstr, <<"0">>}, rabbit_misc:table_lookup(Death, <<"original-expiration">>)),
    ?assertEqual({longstr, <<>>}, rabbit_misc:table_lookup(Death, <<"exchange">>)),
    ?assertEqual({long, 1}, rabbit_misc:table_lookup(Death, <<"count">>)),
    ?assertEqual({array, [{longstr, SourceQ}]}, rabbit_misc:table_lookup(Death, <<"routing-keys">>)),
    delete_topology(Objects).

%% Test that at-least-once dead-lettering works for message dead-lettered due to rejected by consumer.
rejected(Config) ->
    {Server, Ch, SourceQ, TargetQ, _DLX} = Objects = declare_topology(Config, []),
    publish(Ch, SourceQ),
    wait_for_messages_ready([Server], ra_name(SourceQ), 1),
    DelTag = consume(Ch, SourceQ, false),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DelTag,
                                        multiple     = false,
                                        requeue      = false}),
    {_, #amqp_msg{props = #'P_basic'{headers = Headers}}} =
    ?awaitMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"msg">>}},
                amqp_channel:call(Ch, #'basic.get'{queue = TargetQ}),
                1000),
    ?assertEqual({longstr, <<"rejected">>}, rabbit_misc:table_lookup(Headers, <<"x-first-death-reason">>)),
    ?assertEqual({longstr, SourceQ}, rabbit_misc:table_lookup(Headers, <<"x-first-death-queue">>)),
    ?assertEqual({longstr, <<>>}, rabbit_misc:table_lookup(Headers, <<"x-first-death-exchange">>)),
    {array, [{table, Death}]} = rabbit_misc:table_lookup(Headers, <<"x-death">>),
    ?assertEqual({longstr, SourceQ}, rabbit_misc:table_lookup(Death, <<"queue">>)),
    ?assertEqual({longstr, <<"rejected">>}, rabbit_misc:table_lookup(Death, <<"reason">>)),
    ?assertEqual({longstr, <<>>}, rabbit_misc:table_lookup(Death, <<"exchange">>)),
    ?assertEqual({long, 1}, rabbit_misc:table_lookup(Death, <<"count">>)),
    ?assertEqual({array, [{longstr, SourceQ}]}, rabbit_misc:table_lookup(Death, <<"routing-keys">>)),
    delete_topology(Objects).

%% Test that at-least-once dead-lettering works for message dead-lettered due to delivery-limit exceeded.
delivery_limit(Config) ->
    {Server, Ch, SourceQ, TargetQ, _DLX} = Objects =
    declare_topology(Config, [{<<"x-delivery-limit">>, long, 0}]),
    publish(Ch, SourceQ),
    wait_for_messages_ready([Server], ra_name(SourceQ), 1),
    DelTag = consume(Ch, SourceQ, false),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DelTag,
                                        multiple     = false,
                                        requeue      = true}),
    {_, #amqp_msg{props = #'P_basic'{headers = Headers}}} =
    ?awaitMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"msg">>}},
                amqp_channel:call(Ch, #'basic.get'{queue = TargetQ}),
                1000),
    ?assertEqual({longstr, <<"delivery_limit">>}, rabbit_misc:table_lookup(Headers, <<"x-first-death-reason">>)),
    ?assertEqual({longstr, SourceQ}, rabbit_misc:table_lookup(Headers, <<"x-first-death-queue">>)),
    ?assertEqual({longstr, <<>>}, rabbit_misc:table_lookup(Headers, <<"x-first-death-exchange">>)),
    {array, [{table, Death}]} = rabbit_misc:table_lookup(Headers, <<"x-death">>),
    ?assertEqual({longstr, SourceQ}, rabbit_misc:table_lookup(Death, <<"queue">>)),
    ?assertEqual({longstr, <<"delivery_limit">>}, rabbit_misc:table_lookup(Death, <<"reason">>)),
    ?assertEqual({longstr, <<>>}, rabbit_misc:table_lookup(Death, <<"exchange">>)),
    ?assertEqual({long, 1}, rabbit_misc:table_lookup(Death, <<"count">>)),
    ?assertEqual({array, [{longstr, SourceQ}]}, rabbit_misc:table_lookup(Death, <<"routing-keys">>)),
    delete_topology(Objects).

%% Test that message is not lost despite no route from dead-letter exchange to target queue.
%% Once, the route becomes available, the message is delivered to the target queue
%% and acked to the source quorum queue.
target_queue_not_bound(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    SourceQ = ?config(source_queue, Config),
    TargetQ = ?config(target_queue_1, Config),
    DLX = <<"dead-ex">>,
    QArgs = [
             {<<"x-dead-letter-exchange">>, longstr, DLX},
             {<<"x-dead-letter-routing-key">>, longstr, <<"k1">>},
             {<<"x-dead-letter-strategy">>, longstr, <<"at-least-once">>},
             {<<"x-overflow">>, longstr, <<"reject-publish">>},
             {<<"x-queue-type">>, longstr, <<"quorum">>}
            ],
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{
                                                     queue     = SourceQ,
                                                     durable   = true,
                                                     auto_delete = false,
                                                     arguments = QArgs}),
    #'exchange.declare_ok'{} = amqp_channel:call(Ch, #'exchange.declare'{exchange = DLX}),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = TargetQ}),
    Msg = <<"msg">>,
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = SourceQ},
                           #amqp_msg{props   = #'P_basic'{expiration = <<"0">>},
                                     payload = Msg}),
    RaName = ra_name(SourceQ),
    %% Binding from target queue to DLX is missing.
    %% Therefore, 1 message should be kept in discards queue.
    eventually(?_assertMatch([{1, _}],
                             dirty_query([Server], RaName, fun rabbit_fifo:query_stat_dlx/1))),
    consistently(?_assertMatch([{1, _}],
                               dirty_query([Server], RaName, fun rabbit_fifo:query_stat_dlx/1))),
    %% Fix dead-letter toplology misconfiguration.
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{
                                                  queue = TargetQ,
                                                  exchange = DLX,
                                                  routing_key = <<"k1">>
                                                 }),
    %% Binding from target queue to DLX is now present.
    %% Therefore, message should be delivered to target queue and acked to source queue.
    eventually(?_assertEqual([{0, 0}],
                             dirty_query([Server], RaName, fun rabbit_fifo:query_stat_dlx/1)),
               500, 10),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{props = #'P_basic'{expiration = undefined},
                                               payload = Msg}},
                 amqp_channel:call(Ch, #'basic.get'{queue = TargetQ})),
    delete_topology({Server, Ch, SourceQ, TargetQ, DLX}).

%% Test that message is not lost when configured dead-letter exchange does not exist.
%% Once, the exchange gets declared, the message is delivered to the target queue
%% and acked to the source quorum queue.
dlx_missing(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    SourceQ = ?config(source_queue, Config),
    TargetQ = ?config(target_queue_1, Config),
    DLX = <<"dead-ex">>,
    QArgs = [
             {<<"x-dead-letter-exchange">>, longstr, DLX},
             {<<"x-dead-letter-routing-key">>, longstr, <<"k1">>},
             {<<"x-dead-letter-strategy">>, longstr, <<"at-least-once">>},
             {<<"x-overflow">>, longstr, <<"reject-publish">>},
             {<<"x-queue-type">>, longstr, <<"quorum">>}
            ],
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{
                                                     queue     = SourceQ,
                                                     durable   = true,
                                                     auto_delete = false,
                                                     arguments = QArgs}),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = TargetQ}),
    Msg = <<"msg">>,
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = SourceQ},
                           #amqp_msg{props   = #'P_basic'{expiration = <<"0">>},
                                     payload = Msg}),
    RaName = ra_name(SourceQ),
    %% DLX is missing. Therefore, 1 message should be kept in discards queue.
    eventually(?_assertMatch([{1, _}],
                             dirty_query([Server], RaName, fun rabbit_fifo:query_stat_dlx/1))),
    consistently(?_assertMatch([{1, _}],
                               dirty_query([Server], RaName, fun rabbit_fifo:query_stat_dlx/1))),
    %% Fix dead-letter toplology misconfiguration.
    #'exchange.declare_ok'{} = amqp_channel:call(Ch, #'exchange.declare'{exchange = DLX}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{
                                                  queue = TargetQ,
                                                  exchange = DLX,
                                                  routing_key = <<"k1">>
                                                 }),
    %% DLX is now present.
    %% Therefore, message should be delivered to target queue and acked to source queue.
    eventually(?_assertEqual([{0, 0}],
                             dirty_query([Server], RaName, fun rabbit_fifo:query_stat_dlx/1)),
               500, 10),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{props = #'P_basic'{expiration = undefined},
                                               payload = Msg}},
                 amqp_channel:call(Ch, #'basic.get'{queue = TargetQ})),
    delete_topology({Server, Ch, SourceQ, TargetQ, DLX}).

declare_topology(Config, AdditionalQArgs) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    SourceQ = ?config(source_queue, Config),
    TargetQ = ?config(target_queue_1, Config),
    DLX = <<"dead-ex">>,
    QArgs = [
             {<<"x-dead-letter-exchange">>, longstr, DLX},
             {<<"x-dead-letter-routing-key">>, longstr, <<"k1">>},
             {<<"x-dead-letter-strategy">>, longstr, <<"at-least-once">>},
             {<<"x-overflow">>, longstr, <<"reject-publish">>},
             {<<"x-queue-type">>, longstr, <<"quorum">>}
            ],
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{
                                                     queue     = SourceQ,
                                                     durable   = true,
                                                     auto_delete = false,
                                                     arguments = lists:keymerge(1, AdditionalQArgs, QArgs)}),
    #'exchange.declare_ok'{} = amqp_channel:call(Ch, #'exchange.declare'{exchange = DLX}),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = TargetQ}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{
                                                  queue = TargetQ,
                                                  exchange = DLX,
                                                  routing_key = <<"k1">>
                                                 }),
    {Server, Ch, SourceQ, TargetQ, DLX}.

delete_topology({_Server, Ch, SourceQ, TargetQ, DLX}) ->
    #'queue.unbind_ok'{} = amqp_channel:call(Ch, #'queue.unbind'{
                                                    queue     = TargetQ,
                                                    exchange = DLX,
                                                    routing_key = <<"k1">>
                                                   }),
    #'queue.delete_ok'{message_count = 0} = amqp_channel:call(Ch, #'queue.delete'{queue = TargetQ}),
    #'queue.delete_ok'{message_count = 0} = amqp_channel:call(Ch, #'queue.delete'{queue = SourceQ}),
    #'exchange.delete_ok'{} = amqp_channel:call(Ch, #'exchange.delete'{exchange = DLX}).

%%TODO move to rabbitmq_ct_helpers/include/rabbit_assert.hrl
consistently(TestObj) ->
    consistently(TestObj, 100, 10).

consistently(_, _, 0) ->
    ok;
consistently({_Line, Assertion} = TestObj, PollInterval, PollCount) ->
    Assertion(),
    timer:sleep(PollInterval),
    consistently(TestObj, PollInterval, PollCount - 1).

eventually(TestObj) ->
    eventually(TestObj, 100, 10).

eventually({Line, _}, _, 0) ->
    erlang:error({assert_timeout,
                  [{file, ?FILE},
                   {line, ?LINE},
                   {assertion_line, Line}
                  ]});
eventually({Line, Assertion} = TestObj, PollInterval, PollCount) ->
    try
        Assertion()
    catch error:_ = Err ->
              ct:pal(?LOW_IMPORTANCE,
                     "Retrying in ~b ms for ~b more times in file ~s, line ~b due to failed assertion in line ~b: ~p",
                     [PollInterval, PollCount - 1, ?FILE, ?LINE, Line, Err]),
              timer:sleep(PollInterval),
              eventually(TestObj, PollInterval, PollCount - 1)
    end.
