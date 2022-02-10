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
     {group, single_node},
     {group, cluster_size_3}
    ].

groups() ->
    [
     {single_node, [shuffle], [
                               expired,
                               rejected,
                               delivery_limit,
                               target_queue_not_bound,
                               target_queue_deleted,
                               dlx_missing,
                               cycle,
                               stats,
                               drop_head_falls_back_to_at_most_once,
                               switch_strategy,
                               reject_publish_source_queue_max_length,
                               reject_publish_source_queue_max_length_bytes,
                               reject_publish_target_classic_queue,
                               reject_publish_target_quorum_queue
                              ]},
     {cluster_size_3, [], [
                           many_target_queues,
                           single_dlx_worker
                          ]}
    ].

init_per_suite(Config0) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:merge_app_env(
                Config0, {rabbit, [{quorum_tick_interval, 1000},
                                   {dead_letter_worker_consumer_prefetch, 2},
                                   {dead_letter_worker_publisher_confirm_timeout, 1000}
                                  ]}),
    Config2 = rabbit_ct_helpers:merge_app_env(
                Config1, {aten, [{poll_interval, 1000}]}),
    rabbit_ct_helpers:run_setup_steps(Config2).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(single_node = Group, Config) ->
    init_per_group(Group, Config, 1);
init_per_group(cluster_size_3 = Group, Config) ->
    init_per_group(Group, Config, 3).

init_per_group(Group, Config, NodesCount) ->
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodes_count, NodesCount},
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
    T = rabbit_data_coercion:to_binary(Testcase),
    Counters = get_global_counters(Config1),
    Config2 = rabbit_ct_helpers:set_config(Config1,
                                           [{source_queue, <<T/binary, "_source">>},
                                            {dead_letter_exchange, <<T/binary, "_dlx">>},
                                            {target_queue_1, <<T/binary, "_target_1">>},
                                            {target_queue_2, <<T/binary, "_target_2">>},
                                            {target_queue_3, <<T/binary, "_target_3">>},
                                            {target_queue_4, <<T/binary, "_target_4">>},
                                            {target_queue_5, <<T/binary, "_target_5">>},
                                            {target_queue_6, <<T/binary, "_target_6">>},
                                            {policy, <<T/binary, "_policy">>},
                                            {counters, Counters}
                                           ]),
    rabbit_ct_helpers:run_steps(Config2, rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    delete_queue(Ch, ?config(source_queue, Config)),
    delete_queue(Ch, ?config(target_queue_1, Config)),
    delete_queue(Ch, ?config(target_queue_2, Config)),
    delete_queue(Ch, ?config(target_queue_3, Config)),
    delete_queue(Ch, ?config(target_queue_4, Config)),
    delete_queue(Ch, ?config(target_queue_5, Config)),
    delete_queue(Ch, ?config(target_queue_6, Config)),
    #'exchange.delete_ok'{} = amqp_channel:call(Ch, #'exchange.delete'{exchange = ?config(dead_letter_exchange, Config)}),
    Config1 = rabbit_ct_helpers:run_steps(
                Config,
                rabbit_ct_client_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

declare_topology(Config, AdditionalQArgs) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    SourceQ = ?config(source_queue, Config),
    TargetQ = ?config(target_queue_1, Config),
    DLX = ?config(dead_letter_exchange, Config),
    declare_queue(Ch, SourceQ, lists:keymerge(1, AdditionalQArgs,
                                              [{<<"x-dead-letter-exchange">>, longstr, DLX},
                                               {<<"x-dead-letter-routing-key">>, longstr, <<"k1">>},
                                               {<<"x-dead-letter-strategy">>, longstr, <<"at-least-once">>},
                                               {<<"x-overflow">>, longstr, <<"reject-publish">>},
                                               {<<"x-queue-type">>, longstr, <<"quorum">>}
                                              ])),
    #'exchange.declare_ok'{} = amqp_channel:call(Ch, #'exchange.declare'{exchange = DLX}),
    declare_queue(Ch, TargetQ, []),
    bind_queue(Ch, TargetQ, DLX, <<"k1">>),
    {Server, Ch, SourceQ, TargetQ}.

%% Test that at-least-once dead-lettering works for message dead-lettered due to message TTL.
expired(Config) ->
    {_Server, Ch, SourceQ, TargetQ} = declare_topology(Config, []),
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
    assert_dlx_headers(Headers, <<"expired">>, SourceQ),
    ?assertEqual(1, counted(messages_dead_lettered_expired_total, Config)),
    eventually(?_assertEqual(1, counted(messages_dead_lettered_confirmed_total, Config))).

%% Test that at-least-once dead-lettering works for message dead-lettered due to rejected by consumer.
rejected(Config) ->
    {Server, Ch, SourceQ, TargetQ} = declare_topology(Config, []),
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
    assert_dlx_headers(Headers, <<"rejected">>, SourceQ),
    ?assertEqual(1, counted(messages_dead_lettered_rejected_total, Config)),
    eventually(?_assertEqual(1, counted(messages_dead_lettered_confirmed_total, Config))).

%% Test that at-least-once dead-lettering works for message dead-lettered due to delivery-limit exceeded.
delivery_limit(Config) ->
    {Server, Ch, SourceQ, TargetQ} = declare_topology(Config, [{<<"x-delivery-limit">>, long, 0}]),
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
    assert_dlx_headers(Headers, <<"delivery_limit">>, SourceQ),
    ?assertEqual(1, counted(messages_dead_lettered_delivery_limit_total, Config)),
    eventually(?_assertEqual(1, counted(messages_dead_lettered_confirmed_total, Config))).

assert_dlx_headers(Headers, Reason, SourceQ) ->
    ?assertEqual({longstr, Reason}, rabbit_misc:table_lookup(Headers, <<"x-first-death-reason">>)),
    ?assertEqual({longstr, SourceQ}, rabbit_misc:table_lookup(Headers, <<"x-first-death-queue">>)),
    ?assertEqual({longstr, <<>>}, rabbit_misc:table_lookup(Headers, <<"x-first-death-exchange">>)),
    {array, [{table, Death}]} = rabbit_misc:table_lookup(Headers, <<"x-death">>),
    ?assertEqual({longstr, SourceQ}, rabbit_misc:table_lookup(Death, <<"queue">>)),
    ?assertEqual({longstr, Reason}, rabbit_misc:table_lookup(Death, <<"reason">>)),
    ?assertEqual({longstr, <<>>}, rabbit_misc:table_lookup(Death, <<"exchange">>)),
    ?assertEqual({long, 1}, rabbit_misc:table_lookup(Death, <<"count">>)),
    ?assertEqual({array, [{longstr, SourceQ}]}, rabbit_misc:table_lookup(Death, <<"routing-keys">>)),
    case Reason of
        <<"expired">> ->
            ?assertEqual({longstr, <<"0">>}, rabbit_misc:table_lookup(Death, <<"original-expiration">>));
        _ ->
            ok
    end.

%% Test that message is not lost despite no route from dead-letter exchange to target queue.
%% Once the route becomes available, the message is delivered to the target queue
%% and acked to the source quorum queue.
target_queue_not_bound(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    SourceQ = ?config(source_queue, Config),
    TargetQ = ?config(target_queue_1, Config),
    DLX = ?config(dead_letter_exchange, Config),
    declare_queue(Ch, SourceQ, [
                                {<<"x-dead-letter-exchange">>, longstr, DLX},
                                {<<"x-dead-letter-routing-key">>, longstr, <<"k1">>},
                                {<<"x-dead-letter-strategy">>, longstr, <<"at-least-once">>},
                                {<<"x-overflow">>, longstr, <<"reject-publish">>},
                                {<<"x-queue-type">>, longstr, <<"quorum">>}
                               ]),
    #'exchange.declare_ok'{} = amqp_channel:call(Ch, #'exchange.declare'{exchange = DLX}),
    declare_queue(Ch, TargetQ, []),
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
    ?assertEqual(1, counted(messages_dead_lettered_expired_total, Config)),
    ?assertEqual(0, counted(messages_dead_lettered_confirmed_total, Config)),
    %% Fix dead-letter toplology misconfiguration.
    bind_queue(Ch, TargetQ, DLX, <<"k1">>),
    %% Binding from target queue to DLX is now present.
    %% Therefore, message should be delivered to target queue and acked to source queue.
    eventually(?_assertEqual([{0, 0}],
                             dirty_query([Server], RaName, fun rabbit_fifo:query_stat_dlx/1)),
               500, 10),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{props = #'P_basic'{expiration = undefined},
                                               payload = Msg}},
                 amqp_channel:call(Ch, #'basic.get'{queue = TargetQ})),
    ?assertEqual(1, counted(messages_dead_lettered_expired_total, Config)),
    eventually(?_assertEqual(1, counted(messages_dead_lettered_confirmed_total, Config))).

%% Test that message is not lost when target queue gets deleted
%% because dead-letter routing topology should always be respected.
target_queue_deleted(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    SourceQ = ?config(source_queue, Config),
    TargetQ = ?config(target_queue_1, Config),
    DLX = ?config(dead_letter_exchange, Config),
    declare_queue(Ch, SourceQ, [
                                {<<"x-dead-letter-exchange">>, longstr, DLX},
                                {<<"x-dead-letter-routing-key">>, longstr, <<"k1">>},
                                {<<"x-dead-letter-strategy">>, longstr, <<"at-least-once">>},
                                {<<"x-overflow">>, longstr, <<"reject-publish">>},
                                {<<"x-queue-type">>, longstr, <<"quorum">>}
                               ]),
    #'exchange.declare_ok'{} = amqp_channel:call(Ch, #'exchange.declare'{exchange = DLX}),
    %% Make target queue a quorum queue to provoke sending an 'eol' message to dlx worker.
    declare_queue(Ch, TargetQ, [{<<"x-queue-type">>, longstr, <<"quorum">>}]),
    bind_queue(Ch, TargetQ, DLX, <<"k1">>),
    Msg1 = <<"m1">>,
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = SourceQ},
                           #amqp_msg{props   = #'P_basic'{expiration = <<"0">>},
                                     payload = Msg1}),
    RaName = ra_name(SourceQ),
    eventually(?_assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = Msg1}},
                             amqp_channel:call(Ch, #'basic.get'{queue = TargetQ}))),
    eventually(?_assertEqual([{0, 0}],
                             dirty_query([Server], RaName, fun rabbit_fifo:query_stat_dlx/1))),
    #'queue.delete_ok'{message_count = 0} = amqp_channel:call(Ch, #'queue.delete'{queue = TargetQ}),
    Msg2 = <<"m2">>,
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = SourceQ},
                           #amqp_msg{props   = #'P_basic'{expiration = <<"0">>},
                                     payload = Msg2}),
    %% Message should not be lost despite deleted target queue.
    eventually(?_assertMatch([{1, _}],
                             dirty_query([Server], RaName, fun rabbit_fifo:query_stat_dlx/1))),
    consistently(?_assertMatch([{1, _}],
                               dirty_query([Server], RaName, fun rabbit_fifo:query_stat_dlx/1))),
    %% Message should be delivered once target queue is recreated.
    %% (This time we simply create a classic target queue.)
    declare_queue(Ch, TargetQ, []),
    bind_queue(Ch, TargetQ, DLX, <<"k1">>),
    eventually(?_assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = Msg2}},
                             amqp_channel:call(Ch, #'basic.get'{queue = TargetQ})), 500, 5),
    eventually(?_assertEqual([{0, 0}],
                             dirty_query([Server], RaName, fun rabbit_fifo:query_stat_dlx/1))),
    ?assertEqual(2, counted(messages_dead_lettered_expired_total, Config)),
    ?assertEqual(2, counted(messages_dead_lettered_confirmed_total, Config)).

%% Test that message is not lost when configured dead-letter exchange does not exist.
%% Once the exchange gets declared, the message is delivered to the target queue
%% and acked to the source quorum queue.
dlx_missing(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    SourceQ = ?config(source_queue, Config),
    TargetQ = ?config(target_queue_1, Config),
    DLX = ?config(dead_letter_exchange, Config),
    declare_queue(Ch, SourceQ, [
                                {<<"x-dead-letter-exchange">>, longstr, DLX},
                                {<<"x-dead-letter-routing-key">>, longstr, <<"k1">>},
                                {<<"x-dead-letter-strategy">>, longstr, <<"at-least-once">>},
                                {<<"x-overflow">>, longstr, <<"reject-publish">>},
                                {<<"x-queue-type">>, longstr, <<"quorum">>}
                               ]),
    declare_queue(Ch, TargetQ, []),
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
    bind_queue(Ch, TargetQ, DLX, <<"k1">>),
    %% DLX is now present.
    %% Therefore, message should be delivered to target queue and acked to source queue.
    eventually(?_assertEqual([{0, 0}],
                             dirty_query([Server], RaName, fun rabbit_fifo:query_stat_dlx/1)),
               500, 8),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{props = #'P_basic'{expiration = undefined},
                                               payload = Msg}},
                 amqp_channel:call(Ch, #'basic.get'{queue = TargetQ})),
    ?assertEqual(1, counted(messages_dead_lettered_expired_total, Config)),
    eventually(?_assertEqual(1, counted(messages_dead_lettered_confirmed_total, Config))).

%% Test that message is not lost when it cycles.
%% Once the cycle is resolved, the message is delivered to the target queue and acked to
%% the source quorum queue.
cycle(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    SourceQ = ?config(source_queue, Config),
    TargetQ = ?config(target_queue_1, Config),
    PolicyName = ?config(policy, Config),
    declare_queue(Ch, SourceQ, [
                                {<<"x-dead-letter-exchange">>, longstr, <<"">>},
                                {<<"x-dead-letter-strategy">>, longstr, <<"at-least-once">>},
                                {<<"x-overflow">>, longstr, <<"reject-publish">>},
                                {<<"x-queue-type">>, longstr, <<"quorum">>}
                               ]),
    Msg = <<"msg">>,
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = SourceQ},
                           #amqp_msg{props   = #'P_basic'{expiration = <<"0">>},
                                     payload = Msg}),
    RaName = ra_name(SourceQ),
    %% Message cycled when it was dead-lettered:
    %% source queue -> default exchange -> source queue
    %% Therefore, 1 message should be kept in discards queue.
    eventually(?_assertMatch([{1, _}],
                             dirty_query([Server], RaName, fun rabbit_fifo:query_stat_dlx/1))),
    consistently(?_assertMatch([{1, _}],
                               dirty_query([Server], RaName, fun rabbit_fifo:query_stat_dlx/1))),
    %% Fix the cycle such that dead-lettering flows like this:
    %% source queue -> default exchange -> target queue
    declare_queue(Ch, TargetQ, []),
    ok = rabbit_ct_broker_helpers:set_policy(Config, Server, PolicyName,
                                             SourceQ, <<"queues">>,
                                             [{<<"dead-letter-routing-key">>, TargetQ}]),
    eventually(?_assertEqual([{0, 0}],
                             dirty_query([Server], RaName, fun rabbit_fifo:query_stat_dlx/1)),
               500, 8),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = Msg}},
                 amqp_channel:call(Ch, #'basic.get'{queue = TargetQ})),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, Server, PolicyName),
    ?assertEqual(1, counted(messages_dead_lettered_expired_total, Config)),
    eventually(?_assertEqual(1, counted(messages_dead_lettered_confirmed_total, Config))).

%% Test that rabbit_fifo_dlx tracks statistics correctly.
stats(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    SourceQ = ?config(source_queue, Config),
    TargetQ = ?config(target_queue_1, Config),
    DLX = ?config(dead_letter_exchange, Config),
    declare_queue(Ch, SourceQ, [
                                {<<"x-dead-letter-exchange">>, longstr, DLX},
                                {<<"x-dead-letter-routing-key">>, longstr, <<"k1">>},
                                {<<"x-dead-letter-strategy">>, longstr, <<"at-least-once">>},
                                {<<"x-overflow">>, longstr, <<"reject-publish">>},
                                {<<"x-queue-type">>, longstr, <<"quorum">>}
                               ]),
    #'exchange.declare_ok'{} = amqp_channel:call(Ch, #'exchange.declare'{exchange = DLX}),
    declare_queue(Ch, TargetQ, []),
    Msg = <<"12">>, %% 2 bytes per message
    [ok = amqp_channel:cast(Ch,
                            #'basic.publish'{routing_key = SourceQ},
                            #amqp_msg{props   = #'P_basic'{expiration = <<"0">>},
                                      payload = Msg})
     || _ <- lists:seq(1, 10)], %% 10 messages in total
    RaName = ra_name(SourceQ),
    %% Binding from target queue to DLX is missing. Therefore
    %% * 10 msgs should be discarded (i.e. in discards queue or checked out to dlx_worker)
    %% * 20 bytes (=10msgs*2bytes) should be discarded (i.e. in discards queue or checked out to dlx_worker)
    eventually(?_assertEqual([{10, 20}],
                             dirty_query([Server], RaName, fun rabbit_fifo:query_stat_dlx/1))),
    ?assertMatch([#{
                    %% 2 msgs (=Prefetch) should be checked out to dlx_worker
                    num_discard_checked_out := 2,
                    %% 4 bytes (=2msgs*2bytes) should be checked out to dlx_worker
                    discard_checkout_message_bytes := 4,
                    %% 8 msgs (=10-2) should be in discards queue
                    num_discarded := 8,
                    %% 16 bytes (=8msgs*2bytes) should be in discards queue
                    discard_message_bytes := 16,
                    %% 10 msgs in total
                    num_messages := 10
                   }],
                 dirty_query([Server], RaName, fun rabbit_fifo:overview/1)),
    ?assertEqual(10, counted(messages_dead_lettered_expired_total, Config)),
    ?assertEqual(0, counted(messages_dead_lettered_confirmed_total, Config)),
    %% Fix dead-letter toplology misconfiguration.
    bind_queue(Ch, TargetQ, DLX, <<"k1">>),
    %% Binding from target queue to DLX is now present.
    %% Therefore, all messages should be delivered to target queue and acked to source queue.
    %% Therefore, all stats should be decremented back to 0.
    eventually(?_assertEqual([{0, 0}],
                             dirty_query([Server], RaName, fun rabbit_fifo:query_stat_dlx/1)),
               500, 10),
    ?assertMatch([#{
                    num_discard_checked_out := 0,
                    discard_checkout_message_bytes := 0,
                    num_discarded := 0,
                    discard_message_bytes := 0,
                    num_messages := 0
                   }],
                 dirty_query([Server], RaName, fun rabbit_fifo:overview/1)),
    [?assertMatch({#'basic.get_ok'{}, #amqp_msg{props = #'P_basic'{expiration = undefined},
                                                payload = Msg}},
                  amqp_channel:call(Ch, #'basic.get'{queue = TargetQ})) || _ <- lists:seq(1, 10)],
    ?assertEqual(10, counted(messages_dead_lettered_confirmed_total, Config)).

%% Test that configuring overflow (default) drop-head will fall back to
%% dead-letter-strategy at-most-once despite configuring at-least-once.
drop_head_falls_back_to_at_most_once(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    SourceQ = ?config(source_queue, Config),
    DLX = ?config(dead_letter_exchange, Config),
    declare_queue(Ch, SourceQ, [
                                {<<"x-dead-letter-exchange">>, longstr, DLX},
                                {<<"x-dead-letter-strategy">>, longstr, <<"at-least-once">>},
                                {<<"x-overflow">>, longstr, <<"drop-head">>},
                                {<<"x-queue-type">>, longstr, <<"quorum">>}
                               ]),
    consistently(
      ?_assertMatch(
         [_, {active, 0}, _, _],
         rabbit_ct_broker_helpers:rpc(Config, Server, supervisor, count_children, [rabbit_fifo_dlx_sup]))).

%% Test that dynamically switching dead-letter-strategy works.
switch_strategy(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    SourceQ = ?config(source_queue, Config),
    RaName = ra_name(SourceQ),
    DLX = ?config(dead_letter_exchange, Config),
    PolicyName = ?config(policy, Config),
    declare_queue(Ch, SourceQ, [
                                {<<"x-dead-letter-exchange">>, longstr, DLX},
                                {<<"x-overflow">>, longstr, <<"reject-publish">>},
                                {<<"x-queue-type">>, longstr, <<"quorum">>}
                               ]),
    %% default strategy is at-most-once
    assert_active_dlx_workers(0, Config, Server),
    ok = rabbit_ct_broker_helpers:set_policy(Config, Server, PolicyName,
                                             SourceQ, <<"queues">>,
                                             [{<<"dead-letter-strategy">>, <<"at-least-once">>}]),
    assert_active_dlx_workers(1, Config, Server),
    [ok = amqp_channel:cast(Ch,
                            #'basic.publish'{routing_key = SourceQ},
                            #amqp_msg{props   = #'P_basic'{expiration = <<"0">>},
                                      payload = <<"m">>}) %% 1 byte per message
     || _ <- lists:seq(1, 5)],
    eventually(
      ?_assertMatch(
         [#{
            %% 2 msgs (=Prefetch) should be checked out to dlx_worker
            num_discard_checked_out := 2,
            discard_checkout_message_bytes := 2,
            %% 3 msgs (=5-2) should be in discards queue
            num_discarded := 3,
            discard_message_bytes := 3,
            num_messages := 5
           }],
         dirty_query([Server], RaName, fun rabbit_fifo:overview/1))),
    ok = rabbit_ct_broker_helpers:set_policy(Config, Server, PolicyName,
                                             SourceQ, <<"queues">>,
                                             [{<<"dead-letter-strategy">>, <<"at-most-once">>}]),
    assert_active_dlx_workers(0, Config, Server),
    ?assertMatch(
       [#{
          num_discard_checked_out := 0,
          discard_checkout_message_bytes := 0,
          num_discarded := 0,
          discard_message_bytes := 0,
          num_messages := 0
         }],
       dirty_query([Server], RaName, fun rabbit_fifo:overview/1)),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, Server, PolicyName),
    ?assertEqual(5, counted(messages_dead_lettered_expired_total, Config)),
    ?assertEqual(0, counted(messages_dead_lettered_confirmed_total, Config)).

%% Test that source quorum queue rejects messages when source quorum queue's max-length is reached.
%% max-length should also take into account dead-lettered messages.
reject_publish_source_queue_max_length(Config) ->
    reject_publish(Config, {<<"x-max-length">>, long, 1}).

%% Test that source quorum queue rejects messages when source quorum queue's max-length-bytes is reached.
%% max-length-bytes should also take into account dead-lettered messages.
reject_publish_source_queue_max_length_bytes(Config) ->
    reject_publish(Config, {<<"x-max-length-bytes">>, long, 1}).

reject_publish(Config, QArg) when is_tuple(QArg) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    SourceQ = ?config(source_queue, Config),
    TargetQ = ?config(target_queue_1, Config),
    PolicyName = ?config(policy, Config),
    %% This routing key prevents messages from being routed to target dead-letter queue.
    ok = rabbit_ct_broker_helpers:set_policy(Config, Server, PolicyName, SourceQ, <<"queues">>,
                                             [{<<"dead-letter-routing-key">>, <<"fake">>}]),
    declare_queue(Ch, SourceQ, [
                                {<<"x-dead-letter-exchange">>, longstr, <<"">>},
                                {<<"x-dead-letter-strategy">>, longstr, <<"at-least-once">>},
                                {<<"x-overflow">>, longstr, <<"reject-publish">>},
                                {<<"x-queue-type">>, longstr, <<"quorum">>},
                                {<<"x-message-ttl">>, long, 0},
                                QArg
                               ]),
    declare_queue(Ch, TargetQ, []),
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    ok = publish_confirm(Ch, SourceQ),
    ok = publish_confirm(Ch, SourceQ),
    RaName = ra_name(SourceQ),
    eventually(?_assertMatch([{2, 2}], %% 2 messages with 1 byte each
                             dirty_query([Server], RaName, fun rabbit_fifo:query_stat_dlx/1))),
    %% Now, we have 2 expired messages in the source quorum queue's discards queue.
    %% Now that we are over the limit we expect publishes to be rejected.
    ?assertEqual(fail, publish_confirm(Ch, SourceQ)),
    %% Fix the dead-letter routing topology.
    ok = rabbit_ct_broker_helpers:set_policy(Config, Server, PolicyName, SourceQ, <<"queues">>,
                                             [{<<"dead-letter-routing-key">>, TargetQ}]),
    eventually(?_assertEqual([{0, 0}],
                             dirty_query([Server], RaName, fun rabbit_fifo:query_stat_dlx/1)), 500, 6),
    %% Publish should be allowed again.
    ok = publish_confirm(Ch, SourceQ),
    %% Consume the 3 expired messages from the target dead-letter queue.
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"m">>}},
                 amqp_channel:call(Ch, #'basic.get'{queue = TargetQ})),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"m">>}},
                 amqp_channel:call(Ch, #'basic.get'{queue = TargetQ})),
    eventually(?_assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = <<"m">>}},
                             amqp_channel:call(Ch, #'basic.get'{queue = TargetQ}))),
    ok = rabbit_ct_broker_helpers:clear_policy(Config, Server, PolicyName).

%% Test that message gets eventually delivered to target quorum queue when it gets rejected initially.
reject_publish_target_quorum_queue(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    SourceQ = ?config(source_queue, Config),
    RaName = ra_name(SourceQ),
    TargetQ = ?config(target_queue_1, Config),
    declare_queue(Ch, SourceQ, [{<<"x-dead-letter-exchange">>, longstr, <<"">>},
                                {<<"x-dead-letter-routing-key">>, longstr, TargetQ},
                                {<<"x-dead-letter-strategy">>, longstr, <<"at-least-once">>},
                                {<<"x-overflow">>, longstr, <<"reject-publish">>},
                                {<<"x-queue-type">>, longstr, <<"quorum">>},
                                {<<"x-message-ttl">>, long, 1}
                               ]),
    declare_queue(Ch, TargetQ, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                {<<"x-overflow">>, longstr, <<"reject-publish">>},
                                {<<"x-max-length">>, long, 1}
                               ]),
    Msg = <<"m">>,
    [ok,ok,ok,ok] = [amqp_channel:cast(Ch, #'basic.publish'{routing_key = SourceQ}, #amqp_msg{payload = Msg})
                     || _N <- lists:seq(1,4)],
    %% Quorum queues reject publishes once the limit is already exceeded.
    %% Therefore, although max-length of target queue is configured to be 1,
    %% it will contain 2 messages before rejecting publishes.
    %% Therefore, we expect target queue confirmed 2 messages and rejected 2 messages.
    wait_for_messages_ready([Server], ra_name(TargetQ), 2),
    consistently(?_assertEqual([{2, 2}],
                               dirty_query([Server], RaName, fun rabbit_fifo:query_stat_dlx/1))),
    %% Let's make some space in the target queue for the 2 rejected messages.
    {#'basic.get_ok'{}, #amqp_msg{payload = Msg}} = amqp_channel:call(Ch, #'basic.get'{queue = TargetQ}),
    {#'basic.get_ok'{}, #amqp_msg{payload = Msg}} = amqp_channel:call(Ch, #'basic.get'{queue = TargetQ}),
    eventually(?_assertEqual([{0, 0}],
                             dirty_query([Server], RaName, fun rabbit_fifo:query_stat_dlx/1)), 500, 5),
    {#'basic.get_ok'{}, #amqp_msg{payload = Msg}} = amqp_channel:call(Ch, #'basic.get'{queue = TargetQ}),
    {#'basic.get_ok'{}, #amqp_msg{payload = Msg}} = amqp_channel:call(Ch, #'basic.get'{queue = TargetQ}),
    ?assertEqual(4, counted(messages_dead_lettered_expired_total, Config)),
    eventually(?_assertEqual(4, counted(messages_dead_lettered_confirmed_total, Config))).

%% Test that message gets eventually delivered to target classic queue when it gets rejected initially.
reject_publish_target_classic_queue(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    SourceQ = ?config(source_queue, Config),
    RaName = ra_name(SourceQ),
    TargetQ = ?config(target_queue_1, Config),
    declare_queue(Ch, SourceQ, [{<<"x-dead-letter-exchange">>, longstr, <<"">>},
                                {<<"x-dead-letter-routing-key">>, longstr, TargetQ},
                                {<<"x-dead-letter-strategy">>, longstr, <<"at-least-once">>},
                                {<<"x-overflow">>, longstr, <<"reject-publish">>},
                                {<<"x-queue-type">>, longstr, <<"quorum">>},
                                {<<"x-message-ttl">>, long, 1}
                               ]),
    declare_queue(Ch, TargetQ, [{<<"x-overflow">>, longstr, <<"reject-publish">>},
                                {<<"x-max-length">>, long, 1}
                               ]),
    Msg = <<"m">>,
    ok = amqp_channel:cast(Ch, #'basic.publish'{routing_key = SourceQ}, #amqp_msg{payload = Msg}),
    ok = amqp_channel:cast(Ch, #'basic.publish'{routing_key = SourceQ}, #amqp_msg{payload = Msg}),
    %% By now we expect target classic queue confirmed 1 message and rejected 1 message.
    eventually(?_assertEqual([{1, 1}],
                             dirty_query([Server], RaName, fun rabbit_fifo:query_stat_dlx/1))),
    consistently(?_assertEqual([{1, 1}],
                               dirty_query([Server], RaName, fun rabbit_fifo:query_stat_dlx/1))),
    ?assertEqual(2, counted(messages_dead_lettered_expired_total, Config)),
    ?assertEqual(1, counted(messages_dead_lettered_confirmed_total, Config)),
    %% Let's make space in the target queue for the rejected message.
    {#'basic.get_ok'{}, #amqp_msg{payload = Msg}} = amqp_channel:call(Ch, #'basic.get'{queue = TargetQ}),
    eventually(?_assertEqual(2, counted(messages_dead_lettered_confirmed_total, Config)), 500, 6),
    ?assertEqual([{0, 0}], dirty_query([Server], RaName, fun rabbit_fifo:query_stat_dlx/1)),
    {#'basic.get_ok'{}, #amqp_msg{payload = Msg}} = amqp_channel:call(Ch, #'basic.get'{queue = TargetQ}),
    ok.

publish_confirm(Ch, QName) ->
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = QName},
                           #amqp_msg{payload = <<"m">>}),
    amqp_channel:register_confirm_handler(Ch, self()),
    receive
        #'basic.ack'{} ->
            ok;
        #'basic.nack'{} ->
            fail
    after 2500 ->
              ct:fail(confirm_timeout)
    end.

%% Test that
%% 1. Message is only acked to source queue once publisher confirms got received from **all** target queues.
%% 2. Target queue can be classic queue, quorum queue, or stream queue.
%%
%% Lesson learnt by writing this test:
%% If there are multiple target queues, messages will not be sent to target non-mirrored classic queues
%% (even if durable) when their host node is temporarily down because these queues get (temporarily) deleted. See:
%% https://github.com/rabbitmq/rabbitmq-server/blob/cf76b479300b767b8ea450293d096cbf729ed734/deps/rabbit/src/rabbit_amqqueue.erl#L1955-L1964
many_target_queues(Config) ->
    [Server1, Server2, Server3] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server1),
    Ch2 = rabbit_ct_client_helpers:open_channel(Config, Server2),
    SourceQ = ?config(source_queue, Config),
    RaName = ra_name(SourceQ),
    TargetQ1 = ?config(target_queue_1, Config),
    TargetQ2 = ?config(target_queue_2, Config),
    TargetQ3 = ?config(target_queue_3, Config),
    TargetQ4 = ?config(target_queue_4, Config),
    TargetQ5 = ?config(target_queue_5, Config),
    TargetQ6 = ?config(target_queue_6, Config),
    DLX = ?config(dead_letter_exchange, Config),
    DLRKey = <<"k1">>,
    %% Create topology:
    %% * source quorum queue with 1 replica on node 1
    %% * target non-mirrored classic queue on node 1
    %% * target quorum queue with 3 replicas
    %% * target stream queue with 3 replicas
    %% * target mirrored classic queue with 3 replicas (leader on node 1)
    %% * target mirrored classic queue with 1 replica (leader on node 2)
    %% * target mirrored classic queue with 3 replica (leader on node 2)
    declare_queue(Ch, SourceQ, [{<<"x-dead-letter-exchange">>, longstr, DLX},
                                {<<"x-dead-letter-routing-key">>, longstr, DLRKey},
                                {<<"x-dead-letter-strategy">>, longstr, <<"at-least-once">>},
                                {<<"x-overflow">>, longstr, <<"reject-publish">>},
                                {<<"x-queue-type">>, longstr, <<"quorum">>},
                                {<<"x-quorum-initial-group-size">>, long, 1}
                               ]),
    #'exchange.declare_ok'{} = amqp_channel:call(Ch, #'exchange.declare'{exchange = DLX}),
    declare_queue(Ch, TargetQ1, []),
    bind_queue(Ch, TargetQ1, DLX, DLRKey),
    declare_queue(Ch, TargetQ2, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                                 {<<"x-quorum-initial-group-size">>, long, 3}
                                ]),
    bind_queue(Ch, TargetQ2, DLX, DLRKey),
    declare_queue(Ch, TargetQ3, [{<<"x-queue-type">>, longstr, <<"stream">>},
                                 {<<"x-initial-cluster-size">>, long, 3}
                                ]),
    bind_queue(Ch, TargetQ3, DLX, DLRKey),
    ok = rabbit_ct_broker_helpers:set_policy(Config, Server1, <<"mirror-q4">>, TargetQ4, <<"queues">>,
                                             [{<<"ha-mode">>, <<"all">>},
                                              {<<"queue-master-locator">>, <<"client-local">>}]),
    declare_queue(Ch, TargetQ4, []),
    bind_queue(Ch, TargetQ4, DLX, DLRKey),
    ok = rabbit_ct_broker_helpers:set_policy(Config, Server1, <<"mirror-q5">>, TargetQ5, <<"queues">>,
                                             [{<<"ha-mode">>, <<"exactly">>},
                                              {<<"ha-params">>, 1},
                                              {<<"queue-master-locator">>, <<"client-local">>}]),
    declare_queue(Ch2, TargetQ5, []),
    bind_queue(Ch2, TargetQ5, DLX, DLRKey),
    ok = rabbit_ct_broker_helpers:set_policy(Config, Server1, <<"mirror-q6">>, TargetQ6, <<"queues">>,
                                             [{<<"ha-mode">>, <<"all">>},
                                              {<<"queue-master-locator">>, <<"client-local">>}]),
    declare_queue(Ch2, TargetQ6, []),
    bind_queue(Ch2, TargetQ6, DLX, DLRKey),
    Msg1 = <<"m1">>,
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = SourceQ},
                           #amqp_msg{props   = #'P_basic'{expiration = <<"5">>},
                                     payload = Msg1}),
    eventually(?_assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = Msg1}},
                             amqp_channel:call(Ch, #'basic.get'{queue = TargetQ1}))),
    eventually(?_assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = Msg1}},
                             amqp_channel:call(Ch, #'basic.get'{queue = TargetQ2}))),
    %% basic.get not supported by stream queues
    #'basic.qos_ok'{} = amqp_channel:call(Ch, #'basic.qos'{prefetch_count = 2}),
    CTag = <<"ctag">>,
    amqp_channel:subscribe(
      Ch,
      #'basic.consume'{queue = TargetQ3,
                       consumer_tag = CTag,
                       arguments = [{<<"x-stream-offset">>, long, 0}]},
      self()),
    receive
        #'basic.consume_ok'{consumer_tag = CTag} ->
            ok
    after 2000 ->
              exit(consume_ok_timeout)
    end,
    receive
        {#'basic.deliver'{consumer_tag = CTag},
         #amqp_msg{payload = Msg1}} ->
            ok
    after 2000 ->
              exit(deliver_timeout)
    end,
    eventually(?_assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = Msg1}},
                             amqp_channel:call(Ch, #'basic.get'{queue = TargetQ4}))),
    eventually(?_assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = Msg1}},
                             amqp_channel:call(Ch2, #'basic.get'{queue = TargetQ5}))),
    eventually(?_assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = Msg1}},
                             amqp_channel:call(Ch2, #'basic.get'{queue = TargetQ6}))),
    eventually(?_assertEqual([{0, 0}],
                             dirty_query([Server1], RaName, fun rabbit_fifo:query_stat_dlx/1))),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Server3),
    ok = rabbit_ct_broker_helpers:stop_node(Config, Server2),
    Msg2 = <<"m2">>,
    ok = amqp_channel:cast(Ch,
                           #'basic.publish'{routing_key = SourceQ},
                           #amqp_msg{props   = #'P_basic'{expiration = <<"1">>},
                                     payload = Msg2}),
    %% Nodes 2 and 3 are down.
    %% rabbit_fifo_dlx_worker should wait until all queues confirm the message
    %% before acking it to the source queue.
    eventually(?_assertEqual([{1, 2}],
                             dirty_query([Server1], RaName, fun rabbit_fifo:query_stat_dlx/1))),
    consistently(?_assertEqual([{1, 2}],
                               dirty_query([Server1], RaName, fun rabbit_fifo:query_stat_dlx/1))),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = Msg2}},
                 amqp_channel:call(Ch, #'basic.get'{queue = TargetQ1})),
    ok = rabbit_ct_broker_helpers:start_node(Config, Server2),
    ok = rabbit_ct_broker_helpers:start_node(Config, Server3),
    eventually(?_assertEqual([{0, 0}],
                             dirty_query([Server1], RaName, fun rabbit_fifo:query_stat_dlx/1)), 500, 6),
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = Msg2}},
                 amqp_channel:call(Ch, #'basic.get'{queue = TargetQ2})),
    receive
        {#'basic.deliver'{consumer_tag = CTag},
         #amqp_msg{payload = Msg2}} ->
            ok
    after 0 ->
              exit(deliver_timeout)
    end,
    ?assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = Msg2}},
                 amqp_channel:call(Ch, #'basic.get'{queue = TargetQ4})),
    eventually(?_assertMatch({#'basic.get_ok'{}, #amqp_msg{payload = Msg2}},
                             amqp_channel:call(Ch, #'basic.get'{queue = TargetQ5}))),
    %%TODO why is the 1st message (m1) a duplicate?
    ?awaitMatch({#'basic.get_ok'{}, #amqp_msg{payload = Msg2}},
                amqp_channel:call(Ch, #'basic.get'{queue = TargetQ6}), 2, 200),
    ?assertEqual(2, counted(messages_dead_lettered_expired_total, Config)),
    ?assertEqual(2, counted(messages_dead_lettered_confirmed_total, Config)).

%% Test that there is a single active rabbit_fifo_dlx_worker that is co-located with the quorum queue leader.
single_dlx_worker(Config) ->
    [Server1, Server2, _] = Servers = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server1),
    SourceQ = ?config(source_queue, Config),
    DLX = ?config(dead_letter_exchange, Config),
    declare_queue(Ch, SourceQ, [
                                {<<"x-dead-letter-exchange">>, longstr, DLX},
                                {<<"x-dead-letter-strategy">>, longstr, <<"at-least-once">>},
                                {<<"x-overflow">>, longstr, <<"reject-publish">>},
                                {<<"x-queue-type">>, longstr, <<"quorum">>}
                               ]),
    ?assertMatch(
       [[_, {active, 1}, _, _],
        [_, {active, 0}, _, _],
        [_, {active, 0}, _, _]],
       rabbit_ct_broker_helpers:rpc_all(Config, supervisor, count_children, [rabbit_fifo_dlx_sup])),

    ok = rabbit_ct_broker_helpers:stop_node(Config, Server1),
    RaName = ra_name(SourceQ),
    {ok, _, {_, Leader0}} = ra:members({RaName, Server2}),
    ?assertNotEqual(Server1, Leader0),
    [Follower0] = Servers -- [Server1, Leader0],
    assert_active_dlx_workers(1, Config, Leader0),
    assert_active_dlx_workers(0, Config, Follower0),
    ok = rabbit_ct_broker_helpers:start_node(Config, Server1),
    consistently(
      ?_assertMatch(
         [_, {active, 0}, _, _],
         rabbit_ct_broker_helpers:rpc(Config, Server1, supervisor, count_children, [rabbit_fifo_dlx_sup], 1000))),

    Pid = rabbit_ct_broker_helpers:rpc(Config, Leader0, erlang, whereis, [RaName]),
    true = rabbit_ct_broker_helpers:rpc(Config, Leader0, erlang, exit, [Pid, kill]),
    {ok, _, {_, Leader1}} = ?awaitMatch({ok, _, _},
                                        ra:members({RaName, Follower0}),
                                        1000),
    ?assertNotEqual(Leader0, Leader1),
    [Follower1, Follower2] = Servers -- [Leader1],
    assert_active_dlx_workers(0, Config, Follower1),
    assert_active_dlx_workers(0, Config, Follower2),
    assert_active_dlx_workers(1, Config, Leader1).

assert_active_dlx_workers(N, Config, Server) ->
    ?assertMatch(
       [_, {active, N}, _, _],
       rabbit_ct_broker_helpers:rpc(Config, Server, supervisor, count_children, [rabbit_fifo_dlx_sup], 1000)).

declare_queue(Channel, Queue, Args) ->
    #'queue.declare_ok'{} = amqp_channel:call(Channel, #'queue.declare'{
                                                          queue     = Queue,
                                                          durable   = true,
                                                          arguments = Args
                                                         }).

bind_queue(Channel, Queue, Exchange, RoutingKey) ->
    #'queue.bind_ok'{} = amqp_channel:call(Channel, #'queue.bind'{
                                                       queue = Queue,
                                                       exchange = Exchange,
                                                       routing_key = RoutingKey
                                                      }).

delete_queue(Channel, Queue) ->
    %% We implicitly test here that we don't end up with duplicate messages.
    #'queue.delete_ok'{message_count = 0} = amqp_channel:call(Channel, #'queue.delete'{queue = Queue}).

%%TODO move to rabbitmq_ct_helpers/include/rabbit_assert.hrl
consistently(TestObj) ->
    consistently(TestObj, 200, 5).

consistently(_, _, 0) ->
    ok;
consistently({_Line, Assertion} = TestObj, PollInterval, PollCount) ->
    Assertion(),
    timer:sleep(PollInterval),
    consistently(TestObj, PollInterval, PollCount - 1).

eventually(TestObj) ->
    eventually(TestObj, 200, 5).

eventually({Line, _}, _, 0) ->
    erlang:error({assert_timeout,
                  [{file, ?FILE},
                   {line, ?LINE},
                   {assertion_line, Line}
                  ]});
eventually({Line, Assertion} = TestObj, PollInterval, PollCount) ->
    case catch Assertion() of
        ok ->
            ok;
        Err ->
            ct:pal(?LOW_IMPORTANCE,
                   "Retrying in ~b ms for ~b more times in file ~s, line ~b due to failed assertion in line ~b: ~p",
                   [PollInterval, PollCount - 1, ?FILE, ?LINE, Line, Err]),
            timer:sleep(PollInterval),
            eventually(TestObj, PollInterval, PollCount - 1)
    end.

get_global_counters(Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_global_counters, overview, []).

%% Returns the delta of Metric between testcase start and now.
counted(Metric, Config) ->
    OldCounters = ?config(counters, Config),
    Counters = get_global_counters(Config),
    metric(Metric, Counters) -
    metric(Metric, OldCounters).

metric(Metric, Counters) ->
    Metrics = maps:get([{queue_type, rabbit_quorum_queue}, {dead_letter_strategy, at_least_once}], Counters),
    maps:get(Metric, Metrics).
