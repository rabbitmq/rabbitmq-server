%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2018 Pivotal Software, Inc.  All rights reserved.
%%

-module(exclusive_consumer_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("amqp_client.hrl").

-compile(export_all).

all() ->
    [
        {group, default}
    ].

groups() ->
    [
        {default, [], [
            all_messages_go_to_one_consumer,
            fallback_to_another_consumer_when_first_one_is_cancelled,
            fallback_to_another_consumer_when_exclusive_consumer_channel_is_cancelled
        ]}
    ].

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config, [
        {rmq_nodename_suffix, ?MODULE}
    ]),
    rabbit_ct_helpers:run_setup_steps(Config1,
        rabbit_ct_broker_helpers:setup_steps() ++
        rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
        rabbit_ct_client_helpers:teardown_steps() ++
        rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

all_messages_go_to_one_consumer(Config) ->
    {C, Ch} = connection_and_channel(Config),
    Declare = #'queue.declare'{arguments = [{"x-exclusive-consumer", bool, true}],
        auto_delete = true},
    NbMessages = 5,
    #'queue.declare_ok'{queue = Q} = amqp_channel:call(Ch, Declare),
    ConsumerPid = spawn(?MODULE, consume, [{self(), {maps:new(), 0}, NbMessages}]),
    #'basic.consume_ok'{consumer_tag = CTag1} =
        amqp_channel:subscribe(Ch, #'basic.consume'{queue = Q, no_ack = true}, ConsumerPid),
    #'basic.consume_ok'{consumer_tag = CTag2} =
        amqp_channel:subscribe(Ch, #'basic.consume'{queue = Q, no_ack = true}, ConsumerPid),

    Publish = #'basic.publish'{exchange = <<>>, routing_key = Q},
    [amqp_channel:cast(Ch, Publish, #amqp_msg{payload = <<"foobar">>}) || _X <- lists:seq(1, NbMessages)],

    receive
        {consumer_done, {MessagesPerConsumer, MessageCount}} ->
            ?assertEqual(NbMessages, MessageCount),
            ?assertEqual(2, maps:size(MessagesPerConsumer)),
            ?assertEqual(NbMessages, maps:get(CTag1, MessagesPerConsumer)),
            ?assertEqual(0, maps:get(CTag2, MessagesPerConsumer))
    after 1000 ->
        throw(failed)
    end,

    amqp_connection:close(C),
    ok.

fallback_to_another_consumer_when_first_one_is_cancelled(Config) ->
    {C, Ch} = connection_and_channel(Config),
    Declare = #'queue.declare'{arguments = [{"x-exclusive-consumer", bool, true}],
        auto_delete = true},
    NbMessages = 10,
    #'queue.declare_ok'{queue = Q} = amqp_channel:call(Ch, Declare),
    ConsumerPid = spawn(?MODULE, consume, [{self(), {maps:new(), 0}, NbMessages}]),
    #'basic.consume_ok'{consumer_tag = CTag1} =
        amqp_channel:subscribe(Ch, #'basic.consume'{queue = Q, no_ack = true}, ConsumerPid),
    #'basic.consume_ok'{consumer_tag = CTag2} =
        amqp_channel:subscribe(Ch, #'basic.consume'{queue = Q, no_ack = true}, ConsumerPid),
    #'basic.consume_ok'{consumer_tag = _CTag3} =
        amqp_channel:subscribe(Ch, #'basic.consume'{queue = Q, no_ack = true}, ConsumerPid),

    Publish = #'basic.publish'{exchange = <<>>, routing_key = Q},
    [amqp_channel:cast(Ch, Publish, #amqp_msg{payload = <<"foobar">>}) || _X <- lists:seq(1, NbMessages div 2)],

    #'basic.cancel_ok'{} = amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = CTag1}),

    [amqp_channel:cast(Ch, Publish, #amqp_msg{payload = <<"foobar">>}) || _X <- lists:seq(NbMessages div 2 + 1, NbMessages - 1)],

    #'basic.cancel_ok'{} = amqp_channel:call(Ch, #'basic.cancel'{consumer_tag = CTag2}),

    amqp_channel:cast(Ch, Publish, #amqp_msg{payload = <<"foobar">>}),

    receive
        {consumer_done, {MessagesPerConsumer, MessageCount}} ->
            ?assertEqual(NbMessages, MessageCount),
            ?assertEqual(3, maps:size(MessagesPerConsumer)),
            ?assertEqual(NbMessages div 2, maps:get(CTag1, MessagesPerConsumer)),
            Counts = maps:values(MessagesPerConsumer),
            ?assert(lists:member(NbMessages div 2, Counts)),
            ?assert(lists:member(NbMessages div 2 - 1, Counts)),
            ?assert(lists:member(1, Counts))
    after 1000 ->
        throw(failed)
    end,

    amqp_connection:close(C),
    ok.

fallback_to_another_consumer_when_exclusive_consumer_channel_is_cancelled(Config) ->
    {C, Ch} = connection_and_channel(Config),
    {C1, Ch1} = connection_and_channel(Config),
    {C2, Ch2} = connection_and_channel(Config),
    {C3, Ch3} = connection_and_channel(Config),
    Declare = #'queue.declare'{arguments = [{"x-exclusive-consumer", bool, true}],
        auto_delete = true},
    NbMessages = 10,
    #'queue.declare_ok'{queue = Q} = amqp_channel:call(Ch1, Declare),
    Consumer1Pid = spawn(?MODULE, consume, [{self(), {maps:new(), 0}, NbMessages div 2}]),
    Consumer2Pid = spawn(?MODULE, consume, [{self(), {maps:new(), 0}, NbMessages div 2 - 1}]),
    Consumer3Pid = spawn(?MODULE, consume, [{self(), {maps:new(), 0}, NbMessages div 2 - 1}]),
    #'basic.consume_ok'{consumer_tag = CTag1} =
        amqp_channel:subscribe(Ch1, #'basic.consume'{queue = Q, no_ack = true, consumer_tag = <<"1">>}, Consumer1Pid),
    #'basic.consume_ok'{} =
        amqp_channel:subscribe(Ch2, #'basic.consume'{queue = Q, no_ack = true, consumer_tag = <<"2">>}, Consumer2Pid),
    #'basic.consume_ok'{} =
        amqp_channel:subscribe(Ch3, #'basic.consume'{queue = Q, no_ack = true, consumer_tag = <<"3">>}, Consumer3Pid),

    Publish = #'basic.publish'{exchange = <<>>, routing_key = Q},
    [amqp_channel:cast(Ch, Publish, #amqp_msg{payload = <<"foobar">>}) || _X <- lists:seq(1, NbMessages div 2)],

    {MessagesPerConsumer1, MessageCount1} = consume_results(),
    ?assertEqual(NbMessages div 2, MessageCount1),
    ?assertEqual(1, maps:size(MessagesPerConsumer1)),
    ?assertEqual(NbMessages div 2, maps:get(CTag1, MessagesPerConsumer1)),

    ok = amqp_channel:close(Ch1),

    [amqp_channel:cast(Ch, Publish, #amqp_msg{payload = <<"foobar">>}) || _X <- lists:seq(NbMessages div 2 + 1, NbMessages - 1)],

    {MessagesPerConsumer2, MessageCount2} = consume_results(),
    ?assertEqual(NbMessages div 2 - 1, MessageCount2),
    ?assertEqual(1, maps:size(MessagesPerConsumer2)),

    ok = amqp_channel:close(Ch2),

    amqp_channel:cast(Ch, Publish, #amqp_msg{payload = <<"poison">>}),

    {MessagesPerConsumer3, MessageCount3} = consume_results(),
    ?assertEqual(1, MessageCount3),
    ?assertEqual(1, maps:size(MessagesPerConsumer3)),

    [amqp_connection:close(Conn) || Conn <- [C1, C2, C3, C]],
    ok.

connection_and_channel(Config) ->
    C = rabbit_ct_client_helpers:open_unmanaged_connection(Config),
    {ok, Ch} = amqp_connection:open_channel(C),
    {C, Ch}.

consume({Parent, State, 0}) ->
    Parent ! {consumer_done, State};
consume({Parent, {MessagesPerConsumer, MessageCount}, CountDown}) ->
    receive
        #'basic.consume_ok'{consumer_tag = CTag} ->
            consume({Parent, {maps:put(CTag, 0, MessagesPerConsumer), MessageCount}, CountDown});
        {#'basic.deliver'{consumer_tag = CTag}, #amqp_msg{payload = <<"poison">>}} ->
            Parent ! {consumer_done,
                {maps:update_with(CTag, fun(V) -> V + 1 end, MessagesPerConsumer),
                    MessageCount + 1}};
        {#'basic.deliver'{consumer_tag = CTag}, _Content} ->
            consume({Parent,
                {maps:update_with(CTag, fun(V) -> V + 1 end, MessagesPerConsumer),
                    MessageCount + 1},
                CountDown - 1});
        #'basic.cancel_ok'{} ->
            consume({Parent, {MessagesPerConsumer, MessageCount}, CountDown});
        _ ->
            consume({Parent, {MessagesPerConsumer, MessageCount}, CountDown})
    after 10000 ->
        Parent ! {consumer_timeout, {MessagesPerConsumer, MessageCount}},
        exit(consumer_timeout)
    end.

consume_results() ->
    receive
        {consumer_done, {MessagesPerConsumer, MessageCount}} ->
            {MessagesPerConsumer, MessageCount};
        {consumer_timeout, {MessagesPerConsumer, MessageCount}} ->
            {MessagesPerConsumer, MessageCount}
    after 1000 ->
        throw(failed)
    end.