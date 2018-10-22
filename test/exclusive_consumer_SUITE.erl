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
        all_messages_go_to_one_consumer,
        fallback_to_another_consumer_when_first_one_is_cancelled
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
    {C, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    Declare = #'queue.declare'{arguments = [{"x-exclusive-consumer", bool, true}]},
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
    {C, Ch} = rabbit_ct_client_helpers:open_connection_and_channel(Config),
    Declare = #'queue.declare'{arguments = [{"x-exclusive-consumer", bool, true}]},
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

consume({Parent, State, 0}) ->
    Parent ! {consumer_done, State};
consume({Parent, {MessagesPerConsumer, MessageCount}, CountDown}) ->
    receive
        #'basic.consume_ok'{consumer_tag = CTag} ->
            consume({Parent, {maps:put(CTag, 0, MessagesPerConsumer), MessageCount}, CountDown});
        {#'basic.deliver'{consumer_tag = CTag}, _Content} ->
            consume({Parent,
                {maps:update_with(CTag, fun(V) -> V + 1 end, MessagesPerConsumer),
                    MessageCount + 1},
                CountDown - 1});
        #'basic.cancel_ok'{} ->
            consume({Parent, {MessagesPerConsumer, MessageCount}, CountDown})
    after 500 ->
        exit(consumer_timeout)
    end.