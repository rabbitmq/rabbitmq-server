%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(single_active_consumer_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(nowarn_export_all).
-compile(export_all).

-define(TIMEOUT, 30000).

all() ->
    [
        {group, classic_queue},
        {group, quorum_queue}
    ].

groups() ->
    [
        {classic_queue, [], [
            all_messages_go_to_one_consumer,
            fallback_to_another_consumer_when_first_one_is_cancelled,
            fallback_to_another_consumer_when_first_one_is_cancelled_qos1,
            fallback_to_another_consumer_when_exclusive_consumer_channel_is_cancelled,
            fallback_to_another_consumer_when_first_one_is_cancelled_manual_acks,
            amqp_exclusive_consume_fails_on_exclusive_consumer_queue
        ]},
        {quorum_queue, [], [
            all_messages_go_to_one_consumer,
            fallback_to_another_consumer_when_first_one_is_cancelled,
            fallback_to_another_consumer_when_first_one_is_cancelled_qos1,
            fallback_to_another_consumer_when_exclusive_consumer_channel_is_cancelled,
            fallback_to_another_consumer_when_first_one_is_cancelled_manual_acks,
            basic_get_is_unsupported
            %% amqp_exclusive_consume_fails_on_exclusive_consumer_queue % Exclusive consume not implemented in QQ
        ]}
    ].

init_per_suite(Config0) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config0, [
        {rmq_nodename_suffix, ?MODULE}
    ]),
    Config = rabbit_ct_helpers:merge_app_env(
               Config1, {rabbit, [{quorum_tick_interval, 1000}]}),
    rabbit_ct_helpers:run_setup_steps(Config,
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
    rabbit_ct_client_helpers:teardown_steps() ++
    rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(classic_queue, Config) ->
    [{single_active_consumer_queue_declare,
        #'queue.declare'{arguments = [
            {<<"x-single-active-consumer">>, bool, true},
            {<<"x-queue-type">>, longstr, <<"classic">>}
        ],
            auto_delete = true}
        } | Config];
init_per_group(quorum_queue, Config) ->
    [{single_active_consumer_queue_declare,
      #'queue.declare'{
         arguments = [
                      {<<"x-single-active-consumer">>, bool, true},
                      {<<"x-queue-type">>, longstr, <<"quorum">>}
                     ],
         durable = true, exclusive = false, auto_delete = false}
     } | Config].

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config0) ->
    Config = [{queue_name, atom_to_binary(Testcase, utf8)} | Config0],
    rabbit_ct_helpers:testcase_started(Config, Testcase).
end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

all_messages_go_to_one_consumer(Config) ->
    {C, Ch} = connection_and_channel(Config),
    Q = queue_declare(Ch, Config),
    MessageCount = 5,
    ConsumerPid = spawn(?MODULE, consume, [{self(), {maps:new(), 0}, MessageCount}]),
    #'basic.consume_ok'{consumer_tag = CTag1} =
        amqp_channel:subscribe(Ch, #'basic.consume'{queue = Q, no_ack = true}, ConsumerPid),
    #'basic.consume_ok'{consumer_tag = CTag2} =
        amqp_channel:subscribe(Ch, #'basic.consume'{queue = Q, no_ack = true}, ConsumerPid),

    Publish = #'basic.publish'{exchange = <<>>, routing_key = Q},
    [amqp_channel:cast(Ch, Publish, #amqp_msg{payload = <<"foobar">>}) || _X <- lists:seq(1, MessageCount)],

    receive
        {consumer_done, {MessagesPerConsumer, MessageCount}} ->
            ?assertEqual(MessageCount, MessageCount),
            ?assertEqual(2, maps:size(MessagesPerConsumer)),
            ?assertEqual(MessageCount, maps:get(CTag1, MessagesPerConsumer)),
            ?assertEqual(0, maps:get(CTag2, MessagesPerConsumer))
    after ?TIMEOUT ->
              flush(),
              throw(failed)
    end,

    amqp_connection:close(C),
    ok.

fallback_to_another_consumer_when_first_one_is_cancelled(Config) ->
    {C, Ch} = connection_and_channel(Config),
    Q = queue_declare(Ch, Config),
    MessageCount = 10,
    ConsumerPid = spawn(?MODULE, consume, [{self(), {maps:new(), 0}, MessageCount}]),
    #'basic.consume_ok'{consumer_tag = CTag1} =
        amqp_channel:subscribe(Ch, #'basic.consume'{queue = Q, no_ack = true}, ConsumerPid),
    #'basic.consume_ok'{consumer_tag = CTag2} =
        amqp_channel:subscribe(Ch, #'basic.consume'{queue = Q, no_ack = true}, ConsumerPid),
    #'basic.consume_ok'{consumer_tag = CTag3} =
        amqp_channel:subscribe(Ch, #'basic.consume'{queue = Q, no_ack = true}, ConsumerPid),

    Publish = #'basic.publish'{exchange = <<>>, routing_key = Q},
    [amqp_channel:cast(Ch, Publish, #amqp_msg{payload = <<"foobar">>}) || _X <- lists:seq(1, MessageCount div 2)],

    {ok, {MessagesPerConsumer1, _}} = wait_for_messages(MessageCount div 2),
    FirstActiveConsumerInList = maps:keys(maps:filter(fun(_CTag, Count) -> Count > 0 end, MessagesPerConsumer1)),
    ?assertEqual(1, length(FirstActiveConsumerInList)),

    FirstActiveConsumer = lists:nth(1, FirstActiveConsumerInList),
    #'basic.cancel_ok'{} = amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = FirstActiveConsumer}),

    {cancel_ok, FirstActiveConsumer} = wait_for_cancel_ok(),

    [amqp_channel:cast(Ch, Publish, #amqp_msg{payload = <<"foobar">>}) || _X <- lists:seq(MessageCount div 2 + 1, MessageCount - 1)],

    {ok, {MessagesPerConsumer2, _}} = wait_for_messages(MessageCount div 2 - 1),
    SecondActiveConsumerInList = maps:keys(maps:filter(
        fun(CTag, Count) -> Count > 0 andalso CTag /= FirstActiveConsumer end,
        MessagesPerConsumer2)
    ),
    ?assertEqual(1, length(SecondActiveConsumerInList)),
    SecondActiveConsumer = lists:nth(1, SecondActiveConsumerInList),

    #'basic.cancel_ok'{} = amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = SecondActiveConsumer}),

    amqp_channel:cast(Ch, Publish, #amqp_msg{payload = <<"foobar">>}),
    ?assertMatch({ok, _}, wait_for_messages(1)),

    LastActiveConsumer = lists:nth(1, lists:delete(FirstActiveConsumer, lists:delete(SecondActiveConsumer, [CTag1, CTag2, CTag3]))),

    receive
        {consumer_done, {MessagesPerConsumer, MessageCount}} ->
            ?assertEqual(MessageCount, MessageCount),
            ?assertEqual(3, maps:size(MessagesPerConsumer)),
            ?assertEqual(MessageCount div 2, maps:get(FirstActiveConsumer, MessagesPerConsumer)),
            ?assertEqual(MessageCount div 2 - 1, maps:get(SecondActiveConsumer, MessagesPerConsumer)),
            ?assertEqual(1, maps:get(LastActiveConsumer, MessagesPerConsumer))
    after ?TIMEOUT ->
              flush(),
              throw(failed)
    end,

    amqp_connection:close(C),
    ok.

fallback_to_another_consumer_when_first_one_is_cancelled_qos1(Config) ->
    {C, Ch} = connection_and_channel(Config),
    Q = queue_declare(Ch, Config),
    ?assertEqual(#'basic.qos_ok'{},
                 amqp_channel:call(Ch, #'basic.qos'{prefetch_count = 1})),
    CTag1 = <<"tag1">>,
    CTag2 = <<"tag2">>,
    amqp_channel:subscribe(Ch, #'basic.consume'{queue = Q,
                                                consumer_tag = CTag1}, self()),
    receive #'basic.consume_ok'{consumer_tag = CTag1} -> ok
    after 5000 -> ct:fail(timeout_ctag1)
    end,

    amqp_channel:subscribe(Ch, #'basic.consume'{queue = Q,
                                                consumer_tag = CTag2}, self()),
    receive #'basic.consume_ok'{consumer_tag = CTag2} -> ok
    after 5000 -> ct:fail(timeout_ctag2)
    end,

    Publish = #'basic.publish'{exchange = <<>>, routing_key = Q},
    amqp_channel:cast(Ch, Publish, #amqp_msg{payload = <<"m1">>}),
    amqp_channel:cast(Ch, Publish, #amqp_msg{payload = <<"m2">>}),

    DTag1 = receive {#'basic.deliver'{consumer_tag = CTag1,
                                      delivery_tag = DTag},
                     #amqp_msg{payload = <<"m1">>}} -> DTag
            after 5000 -> ct:fail(timeout_m1)
            end,

    #'basic.cancel_ok'{consumer_tag = CTag1} = amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = CTag1}),
    receive #'basic.cancel_ok'{consumer_tag = CTag1} -> ok
    after 5000 -> ct:fail(missing_cancel)
    end,

    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DTag1}),

    receive {#'basic.deliver'{consumer_tag = CTag2},
             #amqp_msg{payload = <<"m2">>}} -> ok;
            Unexpected -> ct:fail({unexpected, Unexpected})
    after 5000 -> ct:fail(timeout_m2)
    end,
    amqp_connection:close(C).

fallback_to_another_consumer_when_first_one_is_cancelled_manual_acks(Config) ->
    %% Let's ensure that although the consumer is cancelled we still keep the unacked
    %% messages and accept acknowledgments on them.
    [Server | _] = rabbit_ct_broker_helpers:get_node_configs(Config, nodename),
    {C, Ch} = connection_and_channel(Config),
    Q = queue_declare(Ch, Config),
    #'basic.consume_ok'{} =
        amqp_channel:subscribe(Ch, #'basic.consume'{queue = Q, no_ack = false}, self()),
    #'basic.consume_ok'{} =
        amqp_channel:subscribe(Ch, #'basic.consume'{queue = Q, no_ack = false}, self()),
    #'basic.consume_ok'{} =
        amqp_channel:subscribe(Ch, #'basic.consume'{queue = Q, no_ack = false}, self()),
    Consumers0 = rpc:call(Server, rabbit_amqqueue, consumers_all, [<<"/">>]),
    ?assertMatch([_, _, _], lists:filter(fun(Props) ->
                                                 Resource = proplists:get_value(queue_name, Props),
                                                 Q == Resource#resource.name
                                         end, Consumers0)),

    Publish = #'basic.publish'{exchange = <<>>, routing_key = Q},
    [amqp_channel:cast(Ch, Publish, #amqp_msg{payload = P}) || P <- [<<"msg1">>, <<"msg2">>]],

    {CTag, DTag1} = receive_deliver(),
    {_CTag, DTag2} = receive_deliver(),

    queue_utils:wait_for_messages(Config, [[Q, <<"2">>, <<"0">>, <<"2">>]]),
    #'basic.cancel_ok'{} = amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = CTag}),

    receive
        #'basic.cancel_ok'{consumer_tag = CTag} ->
            ok
    end,
    Consumers1 = rpc:call(Server, rabbit_amqqueue, consumers_all, [<<"/">>]),
    ?assertMatch([_, _], lists:filter(fun(Props) ->
                                              Resource = proplists:get_value(queue_name, Props),
                                              Q == Resource#resource.name
                                      end, Consumers1)),
    queue_utils:wait_for_messages(Config, [[Q, <<"2">>, <<"0">>, <<"2">>]]),

    [amqp_channel:cast(Ch, Publish, #amqp_msg{payload = P}) || P <- [<<"msg3">>, <<"msg4">>]],

    queue_utils:wait_for_messages(Config, [[Q, <<"4">>, <<"0">>, <<"4">>]]),
    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DTag1}),
    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DTag2}),
    queue_utils:wait_for_messages(Config, [[Q, <<"2">>, <<"0">>, <<"2">>]]),

    amqp_connection:close(C),
    ok.

fallback_to_another_consumer_when_exclusive_consumer_channel_is_cancelled(Config) ->
    {C, Ch} = connection_and_channel(Config),
    {C1, Ch1} = connection_and_channel(Config),
    {C2, Ch2} = connection_and_channel(Config),
    {C3, Ch3} = connection_and_channel(Config),
    Q = queue_declare(Ch, Config),
    MessageCount = 10,
    Consumer1Pid = spawn(?MODULE, consume, [{self(), {maps:new(), 0}, MessageCount div 2}]),
    Consumer2Pid = spawn(?MODULE, consume, [{self(), {maps:new(), 0}, MessageCount div 2 - 1}]),
    Consumer3Pid = spawn(?MODULE, consume, [{self(), {maps:new(), 0}, MessageCount div 2 - 1}]),
    #'basic.consume_ok'{consumer_tag = CTag1} =
        amqp_channel:subscribe(Ch1, #'basic.consume'{queue = Q, no_ack = true, consumer_tag = <<"1">>}, Consumer1Pid),
    #'basic.consume_ok'{} =
        amqp_channel:subscribe(Ch2, #'basic.consume'{queue = Q, no_ack = true, consumer_tag = <<"2">>}, Consumer2Pid),
    #'basic.consume_ok'{} =
        amqp_channel:subscribe(Ch3, #'basic.consume'{queue = Q, no_ack = true, consumer_tag = <<"3">>}, Consumer3Pid),

    Publish = #'basic.publish'{exchange = <<>>, routing_key = Q},
    [amqp_channel:cast(Ch, Publish, #amqp_msg{payload = <<"foobar">>}) || _X <- lists:seq(1, MessageCount div 2)],

    {MessagesPerConsumer1, MessageCount1} = consume_results(),
    ?assertEqual(MessageCount div 2, MessageCount1),
    ?assertEqual(1, maps:size(MessagesPerConsumer1)),
    ?assertEqual(MessageCount div 2, maps:get(CTag1, MessagesPerConsumer1)),

    ok = amqp_channel:close(Ch1),

    [amqp_channel:cast(Ch, Publish, #amqp_msg{payload = <<"foobar">>}) || _X <- lists:seq(MessageCount div 2 + 1, MessageCount - 1)],

    {MessagesPerConsumer2, MessageCount2} = consume_results(),
    ?assertEqual(MessageCount div 2 - 1, MessageCount2),
    ?assertEqual(1, maps:size(MessagesPerConsumer2)),

    ok = amqp_channel:close(Ch2),

    amqp_channel:cast(Ch, Publish, #amqp_msg{payload = <<"poison">>}),

    {MessagesPerConsumer3, MessageCount3} = consume_results(),
    ?assertEqual(1, MessageCount3),
    ?assertEqual(1, maps:size(MessagesPerConsumer3)),

    [amqp_connection:close(Conn) || Conn <- [C1, C2, C3, C]],
    ok.

basic_get_is_unsupported(Config) ->
    {C, Ch} = connection_and_channel(Config),
    Q = queue_declare(Ch, Config),

    ?assertExit(
       {{shutdown, {server_initiated_close, 405, _}}, _},
       amqp_channel:call(Ch, #'basic.get'{queue = Q, no_ack = false})),

    amqp_connection:close(C),
    ok.

amqp_exclusive_consume_fails_on_exclusive_consumer_queue(Config) ->
    {C, Ch} = connection_and_channel(Config),
    Q = queue_declare(Ch, Config),
    ?assertExit(
        {{shutdown, {server_initiated_close, 403, _}}, _},
        amqp_channel:call(Ch, #'basic.consume'{queue = Q, exclusive = true})
    ),
    amqp_connection:close(C),
    ok.

connection_and_channel(Config) ->
    C = rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    {ok, Ch} = amqp_connection:open_channel(C),
    {C, Ch}.

queue_declare(Channel, Config) ->
    QueueName = ?config(queue_name, Config),
    Declare0 = ?config(single_active_consumer_queue_declare, Config),
    Declare = Declare0#'queue.declare'{queue = QueueName},
    #'queue.declare_ok'{queue = Q} = amqp_channel:call(Channel, Declare),
    Q.

consume({Parent, State, 0}) ->
    Parent ! {consumer_done, State};
consume({Parent, {MessagesPerConsumer, MessageCount}, CountDown} = Arg) ->
    receive
        #'basic.consume_ok'{consumer_tag = CTag} ->
            consume({Parent, {maps:put(CTag, 0, MessagesPerConsumer), MessageCount}, CountDown});
        {#'basic.deliver'{consumer_tag = CTag}, #amqp_msg{payload = <<"poison">>}} ->
            Parent ! {consumer_done,
                {maps:update_with(CTag, fun(V) -> V + 1 end, MessagesPerConsumer),
                    MessageCount + 1}};
        {#'basic.deliver'{consumer_tag = CTag}, _Content} ->
            NewState = {maps:update_with(CTag, fun(V) -> V + 1 end, MessagesPerConsumer),
                MessageCount + 1},
            Parent ! {message, NewState},
            consume({Parent, NewState, CountDown - 1});
        #'basic.cancel_ok'{consumer_tag = CTag} ->
            Parent ! {cancel_ok, CTag},
            consume(Arg);
        _ ->
            consume(Arg)
    after ?TIMEOUT ->
              Parent ! {consumer_timeout, {MessagesPerConsumer, MessageCount}},
              flush(),
              exit(consumer_timeout)
    end.

consume_results() ->
    receive
        {consumer_done, {MessagesPerConsumer, MessageCount}} ->
            {MessagesPerConsumer, MessageCount};
        {consumer_timeout, {MessagesPerConsumer, MessageCount}} ->
            {MessagesPerConsumer, MessageCount};
        _ ->
            consume_results()
    after ?TIMEOUT ->
              flush(),
              throw(failed)
    end.

wait_for_messages(ExpectedCount) ->
    wait_for_messages(ExpectedCount, {}).

wait_for_messages(0, State) ->
    {ok, State};
wait_for_messages(ExpectedCount, State) ->
    receive
        {message, {MessagesPerConsumer, MessageCount}} ->
            wait_for_messages(ExpectedCount - 1, {MessagesPerConsumer, MessageCount})
    after 5000 ->
            {missing, ExpectedCount, State}
    end.

wait_for_cancel_ok() ->
    receive
        {cancel_ok, CTag} ->
            {cancel_ok, CTag}
    after 5000 ->
        throw(consumer_cancel_ok_timeout)
    end.

receive_deliver() ->
    receive
        {#'basic.deliver'{consumer_tag = CTag,
                          delivery_tag = DTag}, _} ->
            {CTag, DTag}
    after 5000 ->
            exit(deliver_timeout)
    end.

flush() ->
    receive
        Msg ->
            ct:pal("flushed: ~w~n", [Msg]),
            flush()
    after 10 ->
              ok
    end.
