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
%%   The Initial Developers of the Original Code are Rabbit Technologies Ltd.
%%
%%   Copyright (C) 2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%
-module(rabbit_mgmt_test_db).
-export([test/0]).

-include_lib("amqp_client/include/amqp_client.hrl").

-compile([export_all]).

-define(TESTS, [test_queues, test_connections, test_channels, test_overview,
                test_channel_rates, test_rate_zeroing,
                test_channel_aggregation, test_exchange_aggregation,
                test_queue_aggregation]).

-define(X, <<"">>).

test() ->
    io:format("~n*** Statistics DB tests ***~n", []),
    ContinuationLists = [setup(Test) || Test <- ?TESTS],
    apply_continuations(1, ContinuationLists),
    [teardown(Conn, Chan) || {_, _, Conn, Chan} <- ContinuationLists],
    io:format("All tests passed.~n", []).

setup(Test) ->
    io:format("Set up ~p... ", [Test]),
    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    Continuations = apply(rabbit_mgmt_test_db, Test, [Conn, Chan]),
    io:format("done.~n", []),
    {Test, Continuations, Conn, Chan}.

teardown(Conn, Chan) ->
    amqp_channel:close(Chan),
    amqp_connection:close(Conn).

apply_continuations(_, []) ->
    ok;

apply_continuations(Count, Lists) ->
    io:format("~nRound ~p, ~p tests remain...~n", [Count, length(Lists)]),
    {ok, Interval} = application:get_env(rabbit, collect_statistics_interval),
    timer:sleep(Interval + 10),
    NewLists = [New ||
                   List <- Lists,
                   New = {_, Rest, _, _} <- [apply_continuation(List)],
                   Rest =/= []],
    apply_continuations(Count + 1, NewLists).

apply_continuation({Test, [Continuation|Rest], Conn, Chan}) ->
    io:format("Run ~p... ", [Test]),
    Continuation(),
    io:format("passed.~n", []),
    {Test, Rest, Conn, Chan}.

%%---------------------------------------------------------------------------

test_queues(_Conn, Chan) ->
    Q1 = declare_queue(Chan),
    Q2 = declare_queue(Chan),
    publish(Chan, ?X, Q1, 4),
    basic_get(Chan, Q1, true, false),
    basic_get(Chan, Q1, false, false),

    [fun() ->
             Qs = rabbit_mgmt_db:annotate_queues(
                    [rabbit_mgmt_format:queue(Q) ||
                        Q <- rabbit_amqqueue:list(<<"/">>)], coarse),
             Q1Info = find_by_name(Q1, Qs),
             Q2Info = find_by_name(Q2, Qs),

             3 = pget(messages, Q1Info),
             2 = pget(messages_ready, Q1Info),
             1 = pget(messages_unacknowledged, Q1Info),

             0 = pget(messages, Q2Info),
             0 = pget(messages_ready, Q2Info),
             0 = pget(messages_unacknowledged, Q2Info)
     end].

test_connections(Conn, Chan) ->
    Q = declare_queue(Chan),
    publish(Chan, ?X, Q, 10),

    [fun() ->
             Port = local_port(Conn),
             Conns = rabbit_mgmt_db:get_annotated_connections(),
             ConnInfo = find_conn_by_local_port(Port, Conns),
             %% There's little we can actually test - just retrieve and check
             %% equality.
             Name = pget(name, ConnInfo),
             [ConnInfo2] = rabbit_mgmt_db:annotate_connections([Name]),
             [assert_equal(Item, ConnInfo, ConnInfo2) ||
                 Item <- rabbit_reader:info_keys()]
     end].

test_overview(_Conn, Chan) ->
    Q = declare_queue(Chan),
    publish(Chan, ?X, Q, 10),

    [fun() ->
             %% Very noddy, but at least we test we can get it
             Overview = rabbit_mgmt_db:get_overview(),
             Queues = pget(queue_totals, Overview),
             assert_positive(pget(messages_unacknowledged, Queues)),
             assert_positive(pget(messages_ready, Queues)),
             assert_positive(pget(messages, Queues)),
             Stats = pget(message_stats, Overview),
             assert_positive(pget(publish, Stats)),
             assert_positive(pget(deliver, Stats)),
             assert_positive(pget(ack, Stats))
     end].

test_channels(Conn, Chan) ->
    Q = declare_queue(Chan),
    publish(Chan, ?X, Q, 10),
    basic_get(Chan, Q, true, false),
    basic_get(Chan, Q, false, true),
    consume(Chan, Q, 1, true, false),
    consume(Chan, Q, 1, false, true),

    [fun() ->
             Stats = pget(message_stats, get_channel(Conn, 1)),
             1 = pget(get, Stats),
             1 = pget(get_no_ack, Stats),
             2 = pget(ack, Stats),
             1 = pget(deliver, Stats),
             7 = pget(deliver_no_ack, Stats), % Since 2nd consume ate
                                              % everything
             10 = pget(publish, Stats)
    end].

test_channel_rates(Conn, Chan) ->
    Q = declare_queue(Chan),
    X2 = <<"channel-rates-exch">>,
    declare_exchange(Chan, X2),
    bind_queue(Chan, X2, Q),
    publish(Chan, ?X, Q, 5),

    [fun() ->
             publish(Chan, ?X, Q, 5),
             publish(Chan, X2, Q, 5)
     end,
     fun() ->
             publish(Chan, ?X, Q, 5),
             publish(Chan, X2, Q, 5),
             assert_ch_rate(Conn, 1, [{publish_details, 1}])
     end,
     fun() ->
             publish(Chan, X2, Q, 5),
             assert_ch_rate(Conn, 1, [{publish_details, 2}])
     end,
     fun() ->
             assert_ch_rate(Conn, 1, [{publish_details, 1}]),
             Stats = pget(message_stats, get_channel(Conn, 1)),
             30 = pget(publish, Stats)
     end].

test_rate_zeroing(Conn, Chan) ->
    Q = declare_queue(Chan),
    publish(Chan, ?X, Q, 5),

    [fun() ->
             publish(Chan, ?X, Q, 5)
     end,
     fun() ->
             assert_ch_rate(Conn, 1, [{publish_details, 1}])
     end,
     fun() ->
             assert_ch_rate(Conn, 1, [{publish_details, 0}])
     end].

assert_ch_rate(Conn, ChNum, Rates) ->
    Ch = get_channel(Conn, ChNum),
    Stats = pget(message_stats, Ch),
    [assert_rate(Exp, pget(Type, Stats)) || {Type, Exp} <- Rates].

test_channel_aggregation(Conn, Chan) ->
    X1 = <<"channel-aggregation-exch1">>,
    X2 = <<"channel-aggregation-exch2">>,
    declare_exchange(Chan, X1),
    declare_exchange(Chan, X2),
    publish(Chan, X1, <<"">>, 10),
    publish(Chan, X2, <<"">>, 100),

    [fun() ->
             Ch = get_channel(Conn, 1),
             110 = pget(publish, pget(message_stats, Ch)),
             assert_aggregated(exchange, [{name,X1}, {vhost,<<"/">>}],
                               [{publish, 10}], pget(publishes, Ch)),
             assert_aggregated(exchange, [{name,X2}, {vhost,<<"/">>}],
                               [{publish, 100}], pget(publishes, Ch))
     end].

test_exchange_aggregation(_Conn, Chan) ->
    X1 = <<"exchange-aggregation">>,
    declare_exchange(Chan, X1),
    Q1 = declare_queue(Chan),
    Q2 = declare_queue(Chan),
    bind_queue(Chan, X1, Q1),
    bind_queue(Chan, X1, Q2),
    publish(Chan, X1, Q1, 10),
    publish(Chan, X1, Q2, 100),

    [fun() ->
             X = get_exchange(X1),
             110 = pget(publish, pget(message_stats_in, X)),
             110 = pget(publish, pget(message_stats_out, X)),
             assert_aggregated(queue_details, [{name,Q1}, {vhost,<<"/">>}],
                               [{publish, 10}], pget(outgoing, X)),
             assert_aggregated(queue_details, [{name,Q2}, {vhost,<<"/">>}],
                               [{publish, 100}], pget(outgoing, X))
     end].

test_queue_aggregation(_Conn, Chan) ->
    X1 = <<"queue-aggregation-1">>,
    X2 = <<"queue-aggregation-2">>,
    declare_exchange(Chan, X1),
    declare_exchange(Chan, X2),
    Q1 = declare_queue(Chan),
    bind_queue(Chan, X1, Q1),
    bind_queue(Chan, X2, Q1),
    publish(Chan, X1, Q1, 10),
    publish(Chan, X2, Q1, 100),

    [fun() ->
             Q = get_queue(Q1),
             110 = pget(publish, pget(message_stats, Q)),
             assert_aggregated(exchange, [{name,X1}, {vhost,<<"/">>}],
                               [{publish, 10}], pget(incoming, Q)),
             assert_aggregated(exchange, [{name,X2}, {vhost,<<"/">>}],
                               [{publish, 100}], pget(incoming, Q))
     end].

assert_aggregated(Key, Val, Exp, List) ->
    [Act] = [pget(stats, I) || I <- List, pget(Key, I) == Val],
    [ActVal = pget(Type, Exp) || {Type, ActVal} <- Act].

%%---------------------------------------------------------------------------

find_by_name(Name, Items) ->
    [Thing] = lists:filter(fun(Item) -> pget(name, Item) == Name end, Items),
    Thing.

find_conn_by_local_port(Port, Items) ->
    [Conn] = lists:filter(
               fun(Conn) ->
                       pget(peer_port, Conn) == Port andalso
                           pget(peer_address, Conn) == <<"127.0.0.1">>
               end, Items),
    Conn.

get_channel(C, Number) ->
    Port = local_port(C),
    hd(rabbit_mgmt_db:annotate_channels(
         [list_to_binary("127.0.0.1:" ++ integer_to_list(Port) ++ ":" ++
                             integer_to_list(Number))], detailed)).

get_exchange(XName) ->
    X = rabbit_mgmt_wm_exchange:exchange(<<"/">>, XName),
    hd(rabbit_mgmt_db:annotate_exchanges([X], detailed)).

get_queue(QName) ->
    Q = rabbit_mgmt_wm_queue:queue(<<"/">>, QName),
    hd(rabbit_mgmt_db:annotate_queues([Q], detailed)).

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

assert_rate(Exp, Stats) ->
    Interval = pget(interval, Stats),
    Rate = pget(rate, Stats),
    CorrectedRate = Interval / 5000000 * Rate,
    case abs(Exp - CorrectedRate) < 0.00001 of
        true -> ok;
        _    -> throw({expected, Exp, got, Rate, corrected, CorrectedRate})
    end.

assert_positive(Val) ->
    true = is_number(Val) andalso 0 < Val.
