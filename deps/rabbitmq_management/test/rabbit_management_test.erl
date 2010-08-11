%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Console.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.
%%
%%   Copyright (C) 2009 LShift Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_management_test).
-export([test/0]).

-include_lib("amqp_client/include/amqp_client.hrl").

-compile([export_all]).

-define(TESTS, [test_queues, test_fine_types]).

test() ->
    SecondHalves = [first_half(Test) || Test <- ?TESTS],
    io:format("Waiting for statistics...~n", []),
    timer:sleep(?STATS_INTERVAL + 1000),
    [second_half(Half) || Half <- SecondHalves].

first_half(Test) ->
    Conn = amqp_connection:start_network(),
    Chan = amqp_connection:open_channel(Conn),
    Continuation = apply(rabbit_management_test, Test, [Chan]),
    {Continuation, Conn, Chan}.

second_half({Continuation, Conn, Chan}) ->
    Continuation(),
    amqp_channel:close(Chan),
    amqp_connection:close(Conn).

%%---------------------------------------------------------------------------

test_queues(Chan) ->
    Queue1 = declare_queue(Chan),
    Queue2 = declare_queue(Chan),
    publish(Chan, Queue1, 4),
    basic_get(Chan, Queue1, true, false),
    basic_get(Chan, Queue1, false, false),

    fun() ->
            Queues = rabbit_management_stats:get_queues(),
            Queue1Info = find_by_name(Queue1, Queues),
            Queue2Info = find_by_name(Queue2, Queues),

            3 = pget(messages, Queue1Info),
            2 = pget(messages_ready, Queue1Info),
            1 = pget(messages_unacknowledged, Queue1Info),

            0 = pget(messages, Queue2Info),
            0 = pget(messages_ready, Queue2Info),
            0 = pget(messages_unacknowledged, Queue2Info)
    end.

test_fine_types(Chan) ->
    Queue = declare_queue(Chan),
    publish(Chan, Queue, 10),
    basic_get(Chan, Queue, true, false),
    basic_get(Chan, Queue, false, true),
    consume(Chan, Queue, 1, true, false),
    consume(Chan, Queue, 1, false, true),

    fun() ->
            QStats = rabbit_management_stats:get_msg_stats(channel_queue_stats,
                                                           undefined,
                                                           ignored,
                                                           ignored),
            {_, MsgQStats} = find_by_sub_name(queue_details, Queue, QStats),
            1 = pget(get, MsgQStats),
            1 = pget(get_no_ack, MsgQStats),
            2 = pget(ack, MsgQStats),
            1 = pget(deliver, MsgQStats),
            7 = pget(deliver_no_ack, MsgQStats), % Since 2nd consume ate
                                                 % everything

            QXStats = rabbit_management_stats:get_msg_stats(
                        channel_queue_exchange_stats,
                        undefined,
                        ignored,
                        ignored),
            {_, MsgQXStats} =
                find_by_sub_name(queue_details, Queue, QXStats),
            10 = pget(publish, MsgQXStats)
    end.

%%---------------------------------------------------------------------------

find_by_name(Name, Items) ->
    [Thing] = lists:filter(fun(Item) -> pget(name, Item) == Name end, Items),
    Thing.

find_by_sub_name(Key, Name, Items) ->
    [Thing] = lists:filter(fun({Ids, _Stats}) ->
                                   pget(name, pget(Key, Ids)) == Name
                           end, Items),
    Thing.

declare_queue(Chan) ->
    #'queue.declare_ok'{ queue = Q } =
        amqp_channel:call(Chan, #'queue.declare'{ exclusive = true }),
    Q.

publish(Chan, Queue) ->
    amqp_channel:call(Chan, #'basic.publish' { exchange    = <<"">>,
                                               routing_key = Queue },
                      #amqp_msg { payload = <<"">> }).

publish(Chan, Queue, Count) ->
    [publish(Chan, Queue) || _ <- lists:seq(1, Count)].

basic_get(Chan, Queue, ExplicitAck, AutoAck) ->
    {#'basic.get_ok'{delivery_tag = Tag}, _} =
        amqp_channel:call(Chan, #'basic.get' { queue = Queue,
                                               no_ack = AutoAck }),
    case ExplicitAck of
        true  -> amqp_channel:call(Chan, #'basic.ack' { delivery_tag = Tag });
        false -> ok
    end.

%% NB: Using AutoAck will actually consume everything.
consume(Chan, Queue, Count, ExplicitAck, AutoAck) ->
    amqp_channel:call(Chan, #'basic.qos' { prefetch_count = 1 }),
    amqp_channel:subscribe(Chan, #'basic.consume' { queue = Queue,
                                                    no_ack = AutoAck },
                           self()),
    receive
        #'basic.consume_ok'{consumer_tag = CTag} -> ok
    end,
    DTags = [receive
                 {#'basic.deliver'{delivery_tag = DTag}, _Msg} -> DTag
             end || _ <- lists:seq(1, Count)],
    amqp_channel:call(Chan, #'basic.cancel' { consumer_tag = CTag }),
    receive
        #'basic.cancel_ok'{consumer_tag = CTag} -> ok
    end,
    case ExplicitAck of
         true  -> [amqp_channel:call(
                     Chan,
                     #'basic.ack' { delivery_tag = DTag }) || DTag <- DTags];
         false -> ok
    end.

pget(K, L) ->
     proplists:get_value(K, L).
