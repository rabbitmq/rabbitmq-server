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
-module(rabbit_mgmt_test_db).
-export([test/0]).

-include_lib("amqp_client/include/amqp_client.hrl").

-compile([export_all]).

-define(TESTS, [test_queues, test_connections, test_fine_types,
                test_aggregation, test_overview]).

-define(X, <<"">>).

test() ->
    io:format("~n*** Statistics DB tests ***~n", []),
    SecondHalves = [first_half(Test) || Test <- ?TESTS],
    io:format("Waiting for statistics...~n", []),
    timer:sleep(?STATS_INTERVAL + 1000),
    [second_half(Half) || Half <- SecondHalves],
    io:format("All tests passed.~n", []).

first_half(Test) ->
    io:format("Set up ~p... ", [Test]),
    Conn = amqp_connection:start_network(),
    Chan = amqp_connection:open_channel(Conn),
    Continuation = apply(rabbit_mgmt_test_db, Test, [Conn, Chan]),
    io:format("done.~n", []),
    {Test, Continuation, Conn, Chan}.

second_half({Test, Continuation, Conn, Chan}) ->
    io:format("Run ~p... ", [Test]),
    Continuation(),
    io:format("passed.~n", []),
    amqp_channel:close(Chan),
    amqp_connection:close(Conn).

%%---------------------------------------------------------------------------

test_queues(_Conn, Chan) ->
    Q1 = declare_queue(Chan),
    Q2 = declare_queue(Chan),
    publish(Chan, ?X, Q1, 4),
    basic_get(Chan, Q1, true, false),
    basic_get(Chan, Q1, false, false),

    fun() ->
            Qs = rabbit_mgmt_db:get_queues(),
            Q1Info = find_by_name(Q1, Qs),
            Q2Info = find_by_name(Q2, Qs),

            3 = pget(messages, Q1Info),
            2 = pget(messages_ready, Q1Info),
            1 = pget(messages_unacknowledged, Q1Info),

            0 = pget(messages, Q2Info),
            0 = pget(messages_ready, Q2Info),
            0 = pget(messages_unacknowledged, Q2Info)
    end.

test_connections(Conn, Chan) ->
    Q = declare_queue(Chan),
    publish(Chan, ?X, Q, 10),

    fun() ->
            Port = local_port(Conn),
            Conns = rabbit_mgmt_db:get_connections(),
            ConnInfo = find_conn_by_local_port(Port, Conns),
            %% There's little we can actually test - just retrieve and check
            %% equality.
            Pid = pget(pid, ConnInfo),
            ConnInfo2 = rabbit_mgmt_db:get_connection(Pid),
            [assert_equal(Item, ConnInfo, ConnInfo2) ||
                Item <- rabbit_reader:info_keys()]
    end.

test_overview(_Conn, Chan) ->
    Q = declare_queue(Chan),
    publish(Chan, ?X, Q, 10),

    fun() ->
            %% Very noddy, but at least we test we can get it
            Overview = rabbit_mgmt_db:get_overview(),
            0 < pget(recv_oct, Overview),
            0 < pget(send_oct, Overview)
    end.

test_fine_types(_Conn, Chan) ->
    Q = declare_queue(Chan),
    publish(Chan, ?X, Q, 10),
    basic_get(Chan, Q, true, false),
    basic_get(Chan, Q, false, true),
    consume(Chan, Q, 1, true, false),
    consume(Chan, Q, 1, false, true),

    fun() ->
            AllQStats =
                rabbit_mgmt_db:get_msg_stats(
                  channel_queue_stats, undefined, ignored, ignored),
            QStats = find_by_queue(Q, AllQStats),
            1 = pget(get, QStats),
            1 = pget(get_no_ack, QStats),
            2 = pget(ack, QStats),
            1 = pget(deliver, QStats),
            7 = pget(deliver_no_ack, QStats), % Since 2nd consume ate
                                              % everything
            AllQXStats =
                rabbit_mgmt_db:get_msg_stats(
                  channel_queue_exchange_stats, undefined, ignored, ignored),
            QXStats = find_by_queue(Q, AllQXStats),
            10 = pget(publish, QXStats)
    end.

test_aggregation(Conn, Chan) ->
    Conn2 = amqp_connection:start_network(),
    Chan2 = amqp_connection:open_channel(Conn2),

    X = <<"aggregation">>,
    declare_exchange(Chan, X),
    Qs = [declare_queue(Chan) || _ <- lists:seq(1, 10)],
    [bind_queue(Chan, X, Q) || Q <- Qs],

    [publish(Chan, X, Q, 1) || Q <- Qs],
    [publish(Chan2, X, Q, 10) || Q <- Qs],
    [consume(Chan, Q, 5, true, false) || Q <- Qs],

    fun() ->
            Get = fun(Type, GroupBy) ->
                          rabbit_mgmt_db:get_msg_stats(
                            Type, GroupBy, ignored, ignored)
                  end,

            Port = local_port(Conn),
            Port2 = local_port(Conn2),

            QByC = Get(channel_queue_stats, "channel"),
            QByCStats = find_stats_by_local_port(Port, QByC),
            50 = pget(deliver, QByCStats),
            50 = pget(ack, QByCStats),

            QByQ = Get(channel_queue_stats, "queue"),
            [begin
                 QStats = find_by_queue(Q, QByQ),
                 5 = pget(deliver, QStats),
                 5 = pget(ack, QStats)
             end || Q <- Qs],

            XByC = Get(channel_exchange_stats, "channel"),
            XByCStats = find_stats_by_local_port(Port, XByC),
            XByCStats2 = find_stats_by_local_port(Port2, XByC),
            10 = pget(publish, XByCStats),
            100 = pget(publish, XByCStats2),

            XByX = Get(channel_exchange_stats, "exchange"),
            XByXStats = find_by_exchange(X, XByX),
            110 = pget(publish, XByXStats),

            QXByC = Get(channel_queue_exchange_stats, "channel"),
            QXByCStats = find_stats_by_local_port(Port, QXByC),
            QXByCStats2 = find_stats_by_local_port(Port2, QXByC),
            10 = pget(publish, QXByCStats),
            100 = pget(publish, QXByCStats2),

            QXByQ = Get(channel_queue_exchange_stats, "queue"),
            [begin
                 QStats = find_by_queue(Q, QXByQ),
                 11 = pget(publish, QStats)
             end || Q <- Qs],

            QXByX = Get(channel_queue_exchange_stats, "exchange"),
            QXByXStats = find_by_exchange(X, QXByX),
            110 = pget(publish, QXByXStats),

            amqp_channel:close(Chan2),
            amqp_connection:close(Conn2)
    end.


%%---------------------------------------------------------------------------

find_by_name(Name, Items) ->
    [Thing] = lists:filter(fun(Item) -> pget(name, Item) == Name end, Items),
    Thing.

find_by_queue(Q, Items) ->
    [{_Ids, Stats}] = lists:filter(
                        fun({Ids, _Stats}) ->
                                pget(name, pget(queue_details, Ids)) == Q
                        end, Items),
    Stats.

find_by_exchange(X, Items) ->
    [{_Ids, Stats}] = lists:filter(
                        fun({Ids, _Stats}) ->
                                pget(name, pget(exchange, Ids)) == X
                        end, Items),
    Stats.

find_conn_by_local_port(Port, Items) ->
    [Conn] = lists:filter(
               fun(Conn) ->
                       pget(peer_port, Conn) == Port andalso
                           pget(peer_address, Conn) == <<"127.0.0.1">>
               end, Items),
    Conn.

find_stats_by_local_port(Port, Items) ->
    [{_Ids, Stats}] = lists:filter(
                        fun({Ids, _Stats}) ->
                                Ch = pget(channel_details, Ids),
                                pget(peer_port, Ch) == Port andalso
                                    pget(peer_address, Ch) == <<"127.0.0.1">>
                        end, Items),
    Stats.

declare_queue(Chan) ->
    #'queue.declare_ok'{ queue = Q } =
        amqp_channel:call(Chan, #'queue.declare'{ exclusive = true }),
    Q.

declare_exchange(Chan, X) ->
    amqp_channel:call(Chan, #'exchange.declare'{ exchange = X,
                                                 type = <<"direct">>,
                                                 auto_delete = true}).
bind_queue(Chan, X, Q) ->
    amqp_channel:call(Chan, #'queue.bind'{ queue = Q,
                                           exchange = X,
                                           routing_key = Q}).

publish(Chan, X, Q) ->
    amqp_channel:call(Chan, #'basic.publish' { exchange    = X,
                                               routing_key = Q },
                      #amqp_msg { payload = <<"">> }).

publish(Chan, X, Q, Count) ->
    [publish(Chan, X, Q) || _ <- lists:seq(1, Count)].

basic_get(Chan, Q, ExplicitAck, AutoAck) ->
    {#'basic.get_ok'{delivery_tag = Tag}, _} =
        amqp_channel:call(Chan, #'basic.get' { queue = Q,
                                               no_ack = AutoAck }),
    case ExplicitAck of
        true  -> amqp_channel:call(Chan, #'basic.ack' { delivery_tag = Tag });
        false -> ok
    end.

%% NB: Using AutoAck will actually consume everything.
consume(Chan, Q, Count, ExplicitAck, AutoAck) ->
    amqp_channel:call(Chan, #'basic.qos' { prefetch_count = Count }),
    amqp_channel:subscribe(Chan, #'basic.consume' { queue = Q,
                                                    no_ack = AutoAck },
                           self()),
    receive
        #'basic.consume_ok'{consumer_tag = CTag} -> ok
    end,
    [receive {#'basic.deliver'{}, _} -> ok end || _ <- lists:seq(1, Count)],
    amqp_channel:call(Chan, #'basic.cancel' { consumer_tag = CTag }),
    receive
        #'basic.cancel_ok'{consumer_tag = CTag} -> ok
    end,
    case ExplicitAck of
        true  -> amqp_channel:call(Chan, #'basic.ack' { multiple = true });
        false -> ok
    end.

pget(K, L) ->
     proplists:get_value(K, L).

local_port(Conn) ->
    [{sock, Sock}] = amqp_connection:info(Conn, [sock]),
    {ok, Port} = inet:port(Sock),
    Port.

assert_equal(Item, PList1, PList2) ->
    Expected = pget(Item, PList1),
    Expected = pget(Item, PList2).
