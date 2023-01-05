%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2011-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%
%%
-module(queue_parallel_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(nowarn_export_all).
-compile(export_all).


all() ->
    [
     {group, parallel_tests}
    ].

groups() ->
    %% Don't run testcases in parallel when Bazel is used because they fail
    %% with various system errors in CI, like the inability to spawn system
    %% processes or to open a TCP port.
    UsesBazel = case os:getenv("RABBITMQ_RUN") of
                    false -> false;
                    _     -> true
                end,
    GroupOptions = case UsesBazel of
                       false -> [parallel];
                       true  -> []
                   end,
    AllTests = [publish,
                consume,
                consume_first_empty,
                consume_from_empty_queue,
                consume_and_autoack,
                subscribe,
                subscribe_consumers,
                subscribe_with_autoack,
                consume_and_ack,
                consume_and_multiple_ack,
                subscribe_and_ack,
                subscribe_and_multiple_ack,
                subscribe_and_requeue_multiple_nack,
                subscribe_and_nack,
                subscribe_and_requeue_nack,
                subscribe_and_multiple_nack,
                consume_and_requeue_nack,
                consume_and_nack,
                consume_and_requeue_multiple_nack,
                consume_and_multiple_nack,
                basic_cancel,
                purge,
                purge_no_consumer,
                basic_recover,
                delete_immediately_by_resource,
                cc_header_non_array_should_close_channel
               ],
    ExtraBccTests = [extra_bcc_option,
                     extra_bcc_option_multiple_1,
                     extra_bcc_option_multiple_2
                    ],
    [
     {parallel_tests, [], [
       {classic_queue, GroupOptions, AllTests ++ [delete_immediately_by_pid_succeeds,
                                                  trigger_message_store_compaction]},
       {mirrored_queue, GroupOptions, AllTests ++ [delete_immediately_by_pid_succeeds,
                                                   trigger_message_store_compaction]},
       {quorum_queue, GroupOptions, AllTests ++ ExtraBccTests ++ [delete_immediately_by_pid_fails]},
       {quorum_queue_in_memory_limit, GroupOptions, AllTests ++ [delete_immediately_by_pid_fails]},
       {quorum_queue_in_memory_bytes, GroupOptions, AllTests ++ [delete_immediately_by_pid_fails]},
       {stream_queue, GroupOptions, ExtraBccTests ++ [publish, subscribe]}
      ]}
    ].

suite() ->
    [
      {timetrap, {minutes, 3}}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(classic_queue, Config) ->
    rabbit_ct_helpers:set_config(
      Config,
      [{queue_args, [{<<"x-queue-type">>, longstr, <<"classic">>}]},
       {consumer_args, []},
       {queue_durable, true}]);
init_per_group(quorum_queue, Config) ->
    rabbit_ct_helpers:set_config(
      Config,
      [{queue_args, [{<<"x-queue-type">>, longstr, <<"quorum">>}]},
       {consumer_args, []},
       {queue_durable, true}]);
init_per_group(quorum_queue_in_memory_limit, Config) ->
    rabbit_ct_helpers:set_config(
      Config,
      [{queue_args, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                     {<<"x-max-in-memory-length">>, long, 1}]},
       {consumer_args, []},
       {queue_durable, true}]);
init_per_group(quorum_queue_in_memory_bytes, Config) ->
    rabbit_ct_helpers:set_config(
      Config,
      [{queue_args, [{<<"x-queue-type">>, longstr, <<"quorum">>},
                     {<<"x-max-in-memory-bytes">>, long, 1}]},
       {consumer_args, []},
       {queue_durable, true}]);
init_per_group(mirrored_queue, Config) ->
    case rabbit_ct_broker_helpers:configured_metadata_store(Config) of
        mnesia ->
            rabbit_ct_broker_helpers:set_ha_policy(
              Config, 0, <<"^max_length.*queue">>,
              <<"all">>, [{<<"ha-sync-mode">>, <<"automatic">>}]),
            Config1 = rabbit_ct_helpers:set_config(
                        Config, [{is_mirrored, true},
                                 {queue_args, [{<<"x-queue-type">>, longstr, <<"classic">>}]},
                                 {consumer_args, []},
                                 {queue_durable, true}]),
            rabbit_ct_helpers:run_steps(Config1, []);
        {khepri, _} ->
            {skip, "Classic queue mirroring not supported by Khepri"}
    end;
init_per_group(stream_queue, Config) ->
    rabbit_ct_helpers:set_config(
      Config,
      [{queue_args, [{<<"x-queue-type">>, longstr, <<"stream">>}]},
       {consumer_args, [{<<"x-stream-offset">>, long, 0}]},
       {queue_durable, true}]);
init_per_group(Group, Config0) ->
    case lists:member({group, Group}, all()) of
        true ->
            ClusterSize = 3,
            Config = rabbit_ct_helpers:merge_app_env(
                       Config0, {rabbit, [{channel_tick_interval, 1000},
                                          {quorum_tick_interval, 1000},
                                          {stream_tick_interval, 1000}]}),
            Config1 = rabbit_ct_helpers:set_config(
                        Config, [ {rmq_nodename_suffix, Group},
                                  {rmq_nodes_count, ClusterSize}
                                ]),
            rabbit_ct_helpers:run_steps(Config1,
                                        rabbit_ct_broker_helpers:setup_steps() ++
                                        rabbit_ct_client_helpers:setup_steps());
        false ->
            rabbit_ct_helpers:run_steps(Config0, [])
    end.

end_per_group(Group, Config) ->
    case lists:member({group, Group}, all()) of
        true ->
            rabbit_ct_helpers:run_steps(Config,
              rabbit_ct_client_helpers:teardown_steps() ++
              rabbit_ct_broker_helpers:teardown_steps());
        false ->
            Config
    end.

init_per_testcase(Testcase, Config) ->
    Group = proplists:get_value(name, ?config(tc_group_properties, Config)),
    Q = rabbit_data_coercion:to_binary(io_lib:format("~p_~tp", [Group, Testcase])),
    Q2 = rabbit_data_coercion:to_binary(io_lib:format("~p_~p_2", [Group, Testcase])),
    Config1 = rabbit_ct_helpers:set_config(Config, [{queue_name, Q},
                                                    {queue_name_2, Q2}]),
    rabbit_ct_helpers:testcase_started(Config1, Testcase).

end_per_testcase(Testcase, Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    amqp_channel:call(Ch, #'queue.delete'{queue = ?config(queue_name, Config)}),
    amqp_channel:call(Ch, #'queue.delete'{queue = ?config(queue_name_2, Config)}),
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

publish(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    publish(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, QName, 1, 1, 0).

consume(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    publish(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, QName, 1, 1, 0),
    consume(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, QName, 1, 0, 1),
    rabbit_ct_client_helpers:close_channel(Ch),
    wait_for_messages(Config, QName, 1, 1, 0).

consume_first_empty(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    consume_empty(Ch, QName),
    publish(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, QName, 1, 1, 0),
    consume(Ch, QName, true, [<<"msg1">>]),
    rabbit_ct_client_helpers:close_channel(Ch).

consume_from_empty_queue(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    consume_empty(Ch, QName).

consume_and_autoack(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),
    publish(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, QName, 1, 1, 0),
    consume(Ch, QName, true, [<<"msg1">>]),
    wait_for_messages(Config, QName, 0, 0, 0),
    rabbit_ct_client_helpers:close_channel(Ch),
    wait_for_messages(Config, QName, 0, 0, 0).

subscribe(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    %% Let's set consumer prefetch so it works with stream queues
    ?assertMatch(#'basic.qos_ok'{},
                 amqp_channel:call(Ch, #'basic.qos'{global = false,
                                                    prefetch_count = 10})),
    publish(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, QName, 1, 1, 0),

    CArgs = ?config(consumer_args, Config),
    subscribe(Ch, QName, false, CArgs),
    receive_basic_deliver(false),

    rabbit_ct_client_helpers:close_channel(Ch),
    wait_for_messages(Config, QName, 1, 1, 0).

subscribe_consumers(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    CArgs = ?config(consumer_args, Config),
    ?assertMatch(#'basic.qos_ok'{},
                 amqp_channel:call(Ch, #'basic.qos'{global = false,
                                                    prefetch_count = 10})),
    subscribe(Ch, QName, false, CArgs),

    %% validate we can retrieve the consumers
    Consumers = rpc:call(Server, rabbit_amqqueue, consumers_all, [<<"/">>]),
    [Consumer] = lists:filter(fun(Props) ->
                                      Resource = proplists:get_value(queue_name, Props),
                                      QName == Resource#resource.name
                              end, Consumers),
    ?assert(is_pid(proplists:get_value(channel_pid, Consumer))),
    ?assert(is_binary(proplists:get_value(consumer_tag, Consumer))),
    ?assertEqual(true, proplists:get_value(ack_required, Consumer)),
    ?assertEqual(10, proplists:get_value(prefetch_count, Consumer)),
    ?assertEqual([], proplists:get_value(arguments, Consumer)),

    rabbit_ct_client_helpers:close_channel(Ch).

subscribe_with_autoack(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    CArgs = ?config(consumer_args, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>, <<"msg2">>]),
    wait_for_messages(Config, QName, 2, 2, 0),
    subscribe(Ch, QName, true, CArgs),
    receive_basic_deliver(false),
    receive_basic_deliver(false),
    wait_for_messages(Config, QName, 0, 0, 0),
    rabbit_ct_client_helpers:close_channel(Ch),
    wait_for_messages(Config, QName, 0, 0, 0).

consume_and_ack(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, QName, 1, 1, 0),
    [DeliveryTag] = consume(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, QName, 1, 0, 1),
    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag}),
    wait_for_messages(Config, QName, 0, 0, 0),
    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

consume_and_multiple_ack(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>, <<"msg2">>, <<"msg3">>]),
    wait_for_messages(Config, QName, 3, 3, 0),
    [_, _, DeliveryTag] = consume(Ch, QName, [<<"msg1">>, <<"msg2">>, <<"msg3">>]),
    wait_for_messages(Config, QName, 3, 0, 3),
    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag,
                                       multiple     = true}),
    wait_for_messages(Config, QName, 0, 0, 0),
    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

subscribe_and_ack(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    CArgs = ?config(consumer_args, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, QName, 1, 1, 0),
    subscribe(Ch, QName, false, CArgs),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag}, _} ->
            wait_for_messages(Config, QName, 1, 0, 1),
            amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag}),
            wait_for_messages(Config, QName, 0, 0, 0)
    end,
    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

subscribe_and_multiple_ack(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    CArgs = ?config(consumer_args, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>, <<"msg2">>, <<"msg3">>]),
    wait_for_messages(Config, QName, 3, 3, 0),
    subscribe(Ch, QName, false, CArgs),
    receive_basic_deliver(false),
    receive_basic_deliver(false),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag}, _} ->
            wait_for_messages(Config, QName, 3, 0, 3),
            amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag,
                                               multiple     = true}),
            wait_for_messages(Config, QName, 0, 0, 0)
    end,
    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

trigger_message_store_compaction(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    N = 12000,
    [publish(Ch, QName, [binary:copy(<<"a">>, 5000)]) || _ <- lists:seq(1, N)],
    wait_for_messages(Config, QName, 12000, 12000, 0),

    AllDTags = rabbit_ct_client_helpers:consume_without_acknowledging(Ch, QName, N),
    ToAck = lists:filter(fun (I) -> I > 500 andalso I < 11200 end, AllDTags),

    [amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = Tag,
                                        multiple     = false}) || Tag <- ToAck],

    %% give compaction a moment to start in and finish
    timer:sleep(5000),
    amqp_channel:cast(Ch, #'queue.purge'{queue = QName}),
    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

subscribe_and_requeue_multiple_nack(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    CArgs = ?config(consumer_args, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>, <<"msg2">>, <<"msg3">>]),
    wait_for_messages(Config, QName, 3, 3, 0),
    subscribe(Ch, QName, false, CArgs),
    receive_basic_deliver(false),
    receive_basic_deliver(false),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered  = false}, _} ->
            wait_for_messages(Config, QName, 3, 0, 3),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                                multiple     = true,
                                                requeue      = true}),
            receive_basic_deliver(true),
            receive_basic_deliver(true),
            receive
                {#'basic.deliver'{delivery_tag = DeliveryTag1,
                                  redelivered  = true}, _} ->
                    wait_for_messages(Config, QName, 3, 0, 3),
                    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag1,
                                                       multiple     = true}),
                    wait_for_messages(Config, QName, 0, 0, 0)
            end
    end,
    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

consume_and_requeue_nack(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>, <<"msg2">>]),
    wait_for_messages(Config, QName, 2, 2, 0),
    [DeliveryTag] = consume(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, QName, 2, 1, 1),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                        multiple     = false,
                                        requeue      = true}),
    wait_for_messages(Config, QName, 2, 2, 0),
    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

consume_and_nack(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, QName, 1, 1, 0),
    [DeliveryTag] = consume(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, QName, 1, 0, 1),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                        multiple     = false,
                                        requeue      = false}),
    wait_for_messages(Config, QName, 0, 0, 0),
    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

consume_and_requeue_multiple_nack(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>, <<"msg2">>, <<"msg3">>]),
    wait_for_messages(Config, QName, 3, 3, 0),
    [_, _, DeliveryTag] = consume(Ch, QName, [<<"msg1">>, <<"msg2">>, <<"msg3">>]),
    wait_for_messages(Config, QName, 3, 0, 3),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                        multiple     = true,
                                        requeue      = true}),
    wait_for_messages(Config, QName, 3, 3, 0),
    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

consume_and_multiple_nack(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>, <<"msg2">>, <<"msg3">>]),
    wait_for_messages(Config, QName, 3, 3, 0),
    [_, _, DeliveryTag] = consume(Ch, QName, [<<"msg1">>, <<"msg2">>, <<"msg3">>]),
    wait_for_messages(Config, QName, 3, 0, 3),
    amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                        multiple     = true,
                                        requeue      = false}),
    wait_for_messages(Config, QName, 0, 0, 0),
    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

subscribe_and_requeue_nack(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    CArgs = ?config(consumer_args, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, QName, 1, 1, 0),
    subscribe(Ch, QName, false, CArgs),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered  = false}, _} ->
            wait_for_messages(Config, QName, 1, 0, 1),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                                multiple     = false,
                                                requeue      = true}),
            receive
                {#'basic.deliver'{delivery_tag = DeliveryTag1,
                                  redelivered  = true}, _} ->
                    wait_for_messages(Config, QName, 1, 0, 1),
                    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag1}),
                    wait_for_messages(Config, QName, 0, 0, 0)
            end
    end,
    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

subscribe_and_nack(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    CArgs = ?config(consumer_args, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, QName, 1, 1, 0),
    subscribe(Ch, QName, false, CArgs),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered  = false}, _} ->
            wait_for_messages(Config, QName, 1, 0, 1),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                                multiple     = false,
                                                requeue      = false}),
            wait_for_messages(Config, QName, 0, 0, 0)
    end,
    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

subscribe_and_multiple_nack(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    CArgs = ?config(consumer_args, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>, <<"msg2">>, <<"msg3">>]),
    wait_for_messages(Config, QName, 3, 3, 0),
    subscribe(Ch, QName, false, CArgs),
    receive_basic_deliver(false),
    receive_basic_deliver(false),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag,
                          redelivered  = false}, _} ->
            wait_for_messages(Config, QName, 3, 0, 3),
            amqp_channel:cast(Ch, #'basic.nack'{delivery_tag = DeliveryTag,
                                                multiple     = true,
                                                requeue      = false}),
            wait_for_messages(Config, QName, 0, 0, 0)
    end,
    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

%% TODO test with single active
basic_cancel(Config) ->
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    CArgs = ?config(consumer_args, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, QName, 1, 1, 0),
    CTag = atom_to_binary(?FUNCTION_NAME, utf8),

    subscribe(Ch, QName, false, CTag, CArgs),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag}, _} ->
            wait_for_messages(Config, QName, 1, 0, 1),
            amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = CTag}),
            Consumers = rpc:call(Server, rabbit_amqqueue, consumers_all, [<<"/">>]),
            wait_for_messages(Config, QName, 1, 0, 1),
            ?assertEqual([], lists:filter(fun(Props) ->
                                                  Resource = proplists:get_value(queue_name, Props),
                                                  QName == Resource#resource.name
                                          end, Consumers)),
            publish(Ch, QName, [<<"msg2">>, <<"msg3">>]),
            wait_for_messages(Config, QName, 3, 2, 1),
            amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DeliveryTag}),
            wait_for_messages(Config, QName, 2, 2, 0)
    after 5000 ->
              exit(basic_deliver_timeout)
    end,
    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

purge(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>, <<"msg2">>]),
    wait_for_messages(Config, QName, 2, 2, 0),
    [_] = consume(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, QName, 2, 1, 1),
    {'queue.purge_ok', 1} = amqp_channel:call(Ch, #'queue.purge'{queue = QName}),
    wait_for_messages(Config, QName, 1, 0, 1),
    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

purge_no_consumer(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>, <<"msg2">>]),
    wait_for_messages(Config, QName, 2, 2, 0),
    {'queue.purge_ok', 2} = amqp_channel:call(Ch, #'queue.purge'{queue = QName}),
    wait_for_messages(Config, QName, 0, 0, 0),
    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

basic_recover(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    publish(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, QName, 1, 1, 0),
    [_] = consume(Ch, QName, [<<"msg1">>]),
    wait_for_messages(Config, QName, 1, 0, 1),
    amqp_channel:cast(Ch, #'basic.recover'{requeue = true}),
    wait_for_messages(Config, QName, 1, 1, 0),
    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

delete_immediately_by_pid_fails(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Args = ?config(queue_args, Config),
    Durable = ?config(queue_durable, Config),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    Cmd = ["eval", "{ok, Q} = rabbit_amqqueue:lookup(rabbit_misc:r(<<\"/\">>, queue, <<\"" ++ binary_to_list(QName) ++ "\">>)), Pid = rabbit_amqqueue:pid_of(Q), rabbit_amqqueue:delete_immediately([Pid])."],
    {ok, Msg} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, Cmd),
    ?assertEqual(match, re:run(Msg, ".*error.*", [{capture, none}])),

    ?assertEqual({'queue.declare_ok', QName, 0, 0},
                 amqp_channel:call(Ch, #'queue.declare'{queue     = QName,
                                                        durable   = Durable,
                                                        passive   = true,
                                                        auto_delete = false,
                                                        arguments = Args})),
    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

delete_immediately_by_pid_succeeds(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Args = ?config(queue_args, Config),
    Durable = ?config(queue_durable, Config),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    Cmd = ["eval", "{ok, Q} = rabbit_amqqueue:lookup(rabbit_misc:r(<<\"/\">>, queue, <<\"" ++ binary_to_list(QName) ++ "\">>)), Pid = rabbit_amqqueue:pid_of(Q), rabbit_amqqueue:delete_immediately([Pid])."],
    {ok, Msg} = rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, Cmd),
    ?assertEqual(match, re:run(Msg, ".*ok.*", [{capture, none}])),

    ?assertExit(
       {{shutdown, {server_initiated_close, 404, _}}, _},
       amqp_channel:call(Ch, #'queue.declare'{queue     = QName,
                                              durable   = Durable,
                                              passive   = true,
                                              auto_delete = false,
                                              arguments = Args})),
    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

delete_immediately_by_resource(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Args = ?config(queue_args, Config),
    Durable = ?config(queue_durable, Config),
    QName = ?config(queue_name, Config),
    declare_queue(Ch, Config, QName),

    Cmd = ["eval", "rabbit_amqqueue:delete_immediately_by_resource([rabbit_misc:r(<<\"/\">>, queue, <<\"" ++ binary_to_list(QName) ++ "\">>)])."],
    ?assertEqual({ok, "ok\n"}, rabbit_ct_broker_helpers:rabbitmqctl(Config, 0, Cmd)),

    ?assertExit(
       {{shutdown, {server_initiated_close, 404, _}}, _},
       amqp_channel:call(Ch, #'queue.declare'{queue     = QName,
                                              durable   = Durable,
                                              passive   = true,
                                              auto_delete = false,
                                              arguments = Args})),
    rabbit_ct_client_helpers:close_channel(Ch),
    ok.

cc_header_non_array_should_close_channel(Config) ->
    {C, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Name0 = ?FUNCTION_NAME,
    Name = atom_to_binary(Name0),
    QName = <<"queue_cc_header_non_array", Name/binary>>,
    delete_queue(Ch, QName),
    declare_queue(Ch, Config, QName),
    amqp_channel:call(Ch,
                       #'basic.publish'{exchange = <<"">>,
                                        routing_key = QName},
                       #amqp_msg{
                          props = #'P_basic'{headers = [{<<"CC">>, long, 99}]},
                          payload = <<"foo">>}),

    Ref = erlang:monitor(process, Ch),
    receive
        {'DOWN', Ref, process, Ch, {shutdown, {server_initiated_close, 406, _}}} ->
            ok
    after 5000 ->
              exit(channel_closed_timeout)
    end,

    ok = rabbit_ct_client_helpers:close_connection(C).

extra_bcc_option(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Name0 = ?FUNCTION_NAME,
    Name = atom_to_binary(Name0),
    QName = <<"queue_with_extra_bcc_", Name/binary>>,
    delete_queue(Ch, QName),
    declare_queue(Ch, Config, QName),

    ExtraBCC = <<"extra_bcc_", Name/binary>>,
    delete_queue(Ch, ExtraBCC),
    declare_bcc_queue(Ch, ExtraBCC),
    set_queue_options(Config, QName, #{
        extra_bcc => ExtraBCC
    }),

    publish(Ch, QName, [<<"msg1">>, <<"msg2">>, <<"msg3">>]),
    wait_for_messages(Config, QName, 3, 3, 0),
    wait_for_messages(Config, ExtraBCC, 3, 3, 0),

    delete_queue(Ch, QName),
    delete_queue(Ch, ExtraBCC).

%% Test single message being routed to 2 target queues where 1 target queue
%% has an extra BCC.
extra_bcc_option_multiple_1(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Name0 = ?FUNCTION_NAME,
    Name = atom_to_binary(Name0),
    Exchange = <<"fanout_", Name/binary>>,
    declare_exchange(Ch, Exchange, <<"fanout">>),

    QName1 = <<"queue_with_extra_bcc_", Name/binary>>,
    declare_queue(Ch, Config, QName1),
    #'queue.bind_ok'{} = amqp_channel:call(
                           Ch, #'queue.bind'{queue = QName1,
                                             exchange = Exchange}),
    ExtraBCC = <<"extra_bcc_", Name/binary>>,
    declare_bcc_queue(Ch, ExtraBCC),
    set_queue_options(Config, QName1, #{extra_bcc => ExtraBCC}),
    QName2 = <<"queue_without_extra_bcc_", Name/binary>>,
    declare_queue(Ch, Config, QName2),
    #'queue.bind_ok'{} = amqp_channel:call(
                           Ch, #'queue.bind'{queue = QName2,
                                             exchange = Exchange}),

    publish(Ch, <<"ignore">>, [<<"msg">>], Exchange),
    wait_for_messages(Config, QName1, 1, 1, 0),
    wait_for_messages(Config, QName2, 1, 1, 0),
    wait_for_messages(Config, ExtraBCC, 1, 1, 0),

    delete_exchange(Ch, Exchange),
    delete_queue(Ch, QName1),
    delete_queue(Ch, QName2),
    delete_queue(Ch, ExtraBCC).

%% Test single message being routed to 2 target queues where both target queues
%% have the same extra BCC.
extra_bcc_option_multiple_2(Config) ->
    {_, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config, 0),
    Name0 = ?FUNCTION_NAME,
    Name = atom_to_binary(Name0),
    Exchange = <<"fanout_", Name/binary>>,
    declare_exchange(Ch, Exchange, <<"fanout">>),

    ExtraBCC = <<"extra_bcc_", Name/binary>>,
    declare_bcc_queue(Ch, ExtraBCC),

    QName1 = <<"q1_with_extra_bcc_", Name/binary>>,
    QName2 = <<"q2_with_extra_bcc_", Name/binary>>,
    lists:foreach(
      fun(QName) ->
              declare_queue(Ch, Config, QName),
              #'queue.bind_ok'{} = amqp_channel:call(
                                     Ch, #'queue.bind'{queue = QName,
                                                       exchange = Exchange}),
              set_queue_options(Config, QName, #{extra_bcc => ExtraBCC})
      end, [QName1, QName2]),

    publish(Ch, <<"ignore">>, [<<"msg">>], Exchange),
    wait_for_messages(Config, QName1, 1, 1, 0),
    wait_for_messages(Config, QName2, 1, 1, 0),
    wait_for_messages(Config, ExtraBCC, 1, 1, 0),

    delete_exchange(Ch, Exchange),
    delete_queue(Ch, QName1),
    delete_queue(Ch, QName2),
    delete_queue(Ch, ExtraBCC).

%%%%%%%%%%%%%%%%%%%%%%%%
%% Test helpers
%%%%%%%%%%%%%%%%%%%%%%%%

declare_exchange(Ch, Name, Type) ->
    #'exchange.declare_ok'{} = amqp_channel:call(
                                 Ch, #'exchange.declare'{exchange = Name,
                                                         type     = Type
                                                        }).
delete_exchange(Ch, Name) ->
    #'exchange.delete_ok'{} = amqp_channel:call(Ch, #'exchange.delete'{exchange = Name}).

declare_queue(Ch, Config, QName) ->
    Args = ?config(queue_args, Config),
    Durable = ?config(queue_durable, Config),
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName,
                                                                   arguments = Args,
                                                                   durable = Durable}).

declare_bcc_queue(Ch, QName) ->
    #'queue.declare_ok'{} = amqp_channel:call(Ch, #'queue.declare'{queue = QName,
                                                                   durable = true}).

delete_queue(Ch, QName) ->
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = QName}).

publish(Ch, QName, Payloads) ->
    publish(Ch, QName, Payloads, <<"">>).

publish(Ch, QName, Payloads, Exchange) ->
    [amqp_channel:call(Ch,
                       #'basic.publish'{exchange = Exchange,
                                        routing_key = QName},
                       #amqp_msg{payload = Payload})
     || Payload <- Payloads].

consume(Ch, QName, Payloads) ->
    consume(Ch, QName, false, Payloads).

consume(Ch, QName, NoAck, Payloads) ->
    [begin
         {#'basic.get_ok'{delivery_tag = DTag}, #amqp_msg{payload = Payload}} =
             amqp_channel:call(Ch, #'basic.get'{queue = QName,
                                                no_ack = NoAck}),
         DTag
     end || Payload <- Payloads].

consume_empty(Ch, QName) ->
    ?assertMatch(#'basic.get_empty'{},
                 amqp_channel:call(Ch, #'basic.get'{queue = QName})).

subscribe(Ch, Queue, NoAck, CArgs) ->
    subscribe(Ch, Queue, NoAck, <<"ctag">>, CArgs).

subscribe(Ch, Queue, NoAck, Ctag, CArgs) ->
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = Queue,
                                                no_ack = NoAck,
                                                consumer_tag = Ctag,
                                                arguments = CArgs},
                           self()),
    receive
        #'basic.consume_ok'{consumer_tag = Ctag} ->
             ok
    end.

receive_basic_deliver(Redelivered) ->
    receive
        {#'basic.deliver'{redelivered = R}, _} when R == Redelivered ->
            ok
    end.

flush(T) ->
    receive X ->
                ct:pal("flushed ~w", [X]),
                flush(T)
    after T ->
              ok
    end.

set_queue_options(Config, QName, Options) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, set_queue_options1, [QName, Options]).

set_queue_options1(QName, Options) ->
    rabbit_amqqueue:update(rabbit_misc:r(<<"/">>, queue, QName),
                           fun(Q) ->
                                   amqqueue:set_options(Q, Options)
                           end).

%% Some CI runs fail with `Failed to create dirty cpu scheduler thread..`
%% when running the ctl commands. Let's try with an rpc call to avoid
%% configuration changes which are less portable
wait_for_messages(Config, QName, Msgs, MsgsReady, MsgsUnack) ->
    wait_for_messages(Config, QName, Msgs, MsgsReady, MsgsUnack, 60).

wait_for_messages(Config, QName, Msgs, MsgsReady, MsgsUnack, 0) ->
    Infos = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, info_all, [<<"/">>, [name, messages, messages_ready, messages_unacknowledged]]),
    Resource = rabbit_misc:r(<<"/">>, queue, QName),
    [QInfo] = lists:filter(fun(Info) ->
                                   lists:member({name, Resource}, Info)
                           end, Infos),
    ?assertEqual([{name, Resource}, {messages, Msgs}, {messages_ready, MsgsReady},
                  {messages_unacknowledged, MsgsUnack}],
                 QInfo);
wait_for_messages(Config, QName, Msgs, MsgsReady, MsgsUnack, N) ->
    Infos = rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_amqqueue, info_all, [<<"/">>, [name, messages, messages_ready, messages_unacknowledged]]),
    Resource = rabbit_misc:r(<<"/">>, queue, QName),
    [QInfo] = lists:filter(fun(Info) ->
                                   lists:member({name, Resource}, Info)
                           end, Infos),
    case QInfo of
        [{name, Resource}, {messages, Msgs}, {messages_ready, MsgsReady},
         {messages_unacknowledged, MsgsUnack}] ->
            ok;
        _ ->
            timer:sleep(500),
            wait_for_messages(Config, QName, Msgs, MsgsReady, MsgsUnack, N - 1)
    end.
