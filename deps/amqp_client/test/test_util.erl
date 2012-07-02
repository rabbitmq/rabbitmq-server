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
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2011-2012 VMware, Inc.  All rights reserved.
%%

-module(test_util).

-include_lib("eunit/include/eunit.hrl").
-include("amqp_client.hrl").

-compile([export_all]).

-define(TEST_REPEATS, 100).

-record(publish, {q, x, routing_key, bind_key, payload,
                  mandatory = false, immediate = false}).

%% The latch constant defines how many processes are spawned in order
%% to run certain functionality in parallel. It follows the standard
%% countdown latch pattern.
-define(Latch, 100).

%% The wait constant defines how long a consumer waits before it
%% unsubscribes
-define(Wait, 200).

%% AMQP URI parsing test
amqp_uri_parse_test() ->
    %% From the spec (adapted)
    ?assertMatch({ok, #amqp_params_network{username     = <<"user">>,
                                           password     = <<"pass">>,
                                           host         = "host",
                                           port         = 10000,
                                           virtual_host = <<"vhost">>,
                                           heartbeat    = 5}},
                 amqp_uri:parse(
                   "amqp://user:pass@host:10000/vhost?heartbeat=5")),
    ?assertMatch({ok, #amqp_params_network{username     = <<"usera">>,
                                           password     = <<"apass">>,
                                           host         = "hoast",
                                           port         = 10000,
                                           virtual_host = <<"v/host">>}},
                 amqp_uri:parse(
                   "aMQp://user%61:%61pass@ho%61st:10000/v%2fhost")),
    ?assertMatch({ok, #amqp_params_direct{}}, amqp_uri:parse("amqp://")),
    ?assertMatch({ok, #amqp_params_direct{username     = <<"">>,
                                          virtual_host = <<"">>}},
                 amqp_uri:parse("amqp://:@/")),
    ?assertMatch({ok, #amqp_params_network{username     = <<"">>,
                                           password     = <<"">>,
                                           virtual_host = <<"">>,
                                           host         = "host"}},
                 amqp_uri:parse("amqp://:@host/")),
    ?assertMatch({ok, #amqp_params_direct{username = <<"user">>}},
                 amqp_uri:parse("amqp://user@")),
    ?assertMatch({ok, #amqp_params_network{username = <<"user">>,
                                           password = <<"pass">>,
                                           host     = "localhost"}},
                 amqp_uri:parse("amqp://user:pass@localhost")),
    ?assertMatch({ok, #amqp_params_network{host         = "host",
                                           virtual_host = <<"/">>}},
                 amqp_uri:parse("amqp://host")),
    ?assertMatch({ok, #amqp_params_network{port = 10000,
                                           host = "localhost"}},
                 amqp_uri:parse("amqp://localhost:10000")),
    ?assertMatch({ok, #amqp_params_direct{virtual_host = <<"vhost">>}},
                 amqp_uri:parse("amqp:///vhost")),
    ?assertMatch({ok, #amqp_params_network{host         = "host",
                                           virtual_host = <<"">>}},
                 amqp_uri:parse("amqp://host/")),
    ?assertMatch({ok, #amqp_params_network{host         = "host",
                                           virtual_host = <<"/">>}},
                 amqp_uri:parse("amqp://host/%2f")),
    ?assertMatch({ok, #amqp_params_network{host = "::1"}},
                 amqp_uri:parse("amqp://[::1]")),

    %% Varous other cases
    ?assertMatch({ok, #amqp_params_network{host = "host", port = 100}},
                 amqp_uri:parse("amqp://host:100")),
    ?assertMatch({ok, #amqp_params_network{host = "::1", port = 100}},
                 amqp_uri:parse("amqp://[::1]:100")),

    ?assertMatch({ok, #amqp_params_network{host         = "host",
                                           virtual_host = <<"blah">>}},
                 amqp_uri:parse("amqp://host/blah")),
    ?assertMatch({ok, #amqp_params_network{host         = "host",
                                           port         = 100,
                                           virtual_host = <<"blah">>}},
                 amqp_uri:parse("amqp://host:100/blah")),
    ?assertMatch({ok, #amqp_params_network{host         = "::1",
                                           virtual_host = <<"blah">>}},
                 amqp_uri:parse("amqp://[::1]/blah")),
    ?assertMatch({ok, #amqp_params_network{host         = "::1",
                                           port         = 100,
                                           virtual_host = <<"blah">>}},
                 amqp_uri:parse("amqp://[::1]:100/blah")),

    ?assertMatch({ok, #amqp_params_network{username = <<"user">>,
                                           password = <<"pass">>,
                                           host     = "host"}},
                 amqp_uri:parse("amqp://user:pass@host")),
    ?assertMatch({ok, #amqp_params_network{username = <<"user">>,
                                           password = <<"pass">>,
                                           port     = 100}},
                 amqp_uri:parse("amqp://user:pass@host:100")),
    ?assertMatch({ok, #amqp_params_network{username = <<"user">>,
                                           password = <<"pass">>,
                                           host     = "::1"}},
                 amqp_uri:parse("amqp://user:pass@[::1]")),
    ?assertMatch({ok, #amqp_params_network{username = <<"user">>,
                                           password = <<"pass">>,
                                           host     = "::1",
                                           port     = 100}},
                 amqp_uri:parse("amqp://user:pass@[::1]:100")),

    %% Various failure cases
    ?assertMatch({error, _}, amqp_uri:parse("http://www.rabbitmq.com")),
    ?assertMatch({error, _}, amqp_uri:parse("amqp://foo:bar:baz")),
    ?assertMatch({error, _}, amqp_uri:parse("amqp://foo[::1]")),
    ?assertMatch({error, _}, amqp_uri:parse("amqp://foo:[::1]")),
    ?assertMatch({error, _}, amqp_uri:parse("amqp://[::1]foo")),
    ?assertMatch({error, _}, amqp_uri:parse("amqp://foo:1000xyz")),
    ?assertMatch({error, _}, amqp_uri:parse("amqp://foo:1000000")),
    ?assertMatch({error, _}, amqp_uri:parse("amqp://foo/bar/baz")),

    ?assertMatch({error, _}, amqp_uri:parse("amqp://foo%1")),
    ?assertMatch({error, _}, amqp_uri:parse("amqp://foo%1x")),
    ?assertMatch({error, _}, amqp_uri:parse("amqp://foo%xy")),

    ok.

%%%%
%%
%% This is an example of how the client interaction should work
%%
%%   {ok, Connection} = amqp_connection:start(network),
%%   {ok, Channel} = amqp_connection:open_channel(Connection),
%%   %%...do something useful
%%   amqp_channel:close(Channel),
%%   amqp_connection:close(Connection).
%%

lifecycle_test() ->
    {ok, Connection} = new_connection(),
    X = <<"x">>,
    {ok, Channel} = amqp_connection:open_channel(Connection),
    amqp_channel:call(Channel,
                      #'exchange.declare'{exchange = X,
                                          type = <<"topic">>}),
    Parent = self(),
    [spawn(fun () -> queue_exchange_binding(Channel, X, Parent, Tag) end)
     || Tag <- lists:seq(1, ?Latch)],
    latch_loop(),
    amqp_channel:call(Channel, #'exchange.delete'{exchange = X}),
    teardown(Connection, Channel),
    ok.

queue_exchange_binding(Channel, X, Parent, Tag) ->
    receive
        nothing -> ok
    after (?Latch - Tag rem 7) * 10 ->
        ok
    end,
    Q = <<"a.b.c", Tag:32>>,
    Binding = <<"a.b.c.*">>,
    #'queue.declare_ok'{queue = Q1}
        = amqp_channel:call(Channel, #'queue.declare'{queue = Q}),
    ?assertMatch(Q, Q1),
    Route = #'queue.bind'{queue = Q,
                          exchange = X,
                          routing_key = Binding},
    amqp_channel:call(Channel, Route),
    amqp_channel:call(Channel, #'queue.delete'{queue = Q}),
    Parent ! finished.

nowait_exchange_declare_test() ->
    {ok, Connection} = new_connection(),
    X = <<"x">>,
    {ok, Channel} = amqp_connection:open_channel(Connection),
    ?assertEqual(
      ok,
      amqp_channel:call(Channel, #'exchange.declare'{exchange = X,
                                                     type = <<"topic">>,
                                                     nowait = true})),
    teardown(Connection, Channel).

channel_lifecycle_test() ->
    {ok, Connection} = new_connection(),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    amqp_channel:close(Channel),
    {ok, Channel2} = amqp_connection:open_channel(Connection),
    teardown(Connection, Channel2),
    ok.

abstract_method_serialization_test(BeforeFun, MultiOpFun, AfterFun) ->
    {ok, Connection} = new_connection(),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    X = uuid(),
    Payload = list_to_binary(["x" || _ <- lists:seq(1, 1000)]),
    OpsPerProcess = 20,
    #'exchange.declare_ok'{} =
        amqp_channel:call(Channel, #'exchange.declare'{exchange = X,
                                                       type = <<"topic">>}),
    BeforeRet = BeforeFun(Channel, X),
    Parent = self(),
    [spawn(fun () -> Ret = [MultiOpFun(Channel, X, Payload, BeforeRet)
                            || _ <- lists:seq(1, OpsPerProcess)],
                   Parent ! {finished, Ret}
           end) || _ <- lists:seq(1, ?Latch)],
    MultiOpRet = latch_loop(),
    AfterFun(Channel, X, Payload, BeforeRet, MultiOpRet),
    teardown(Connection, Channel).

%% This is designed to exercize the internal queuing mechanism
%% to ensure that sync methods are properly serialized
sync_method_serialization_test() ->
    abstract_method_serialization_test(
        fun (_, _) -> ok end,
        fun (Channel, _, _, _) ->
                Q = uuid(),
                #'queue.declare_ok'{queue = Q1} =
                    amqp_channel:call(Channel, #'queue.declare'{queue = Q}),
                ?assertMatch(Q, Q1)
        end,
        fun (_, _, _, _, _) -> ok end).

%% This is designed to exercize the internal queuing mechanism
%% to ensure that sending async methods and then a sync method is serialized
%% properly
async_sync_method_serialization_test() ->
    abstract_method_serialization_test(
        fun (Channel, _X) ->
                #'queue.declare_ok'{queue = Q} =
                    amqp_channel:call(Channel, #'queue.declare'{}),
                Q
        end,
        fun (Channel, X, Payload, _) ->
                %% The async methods
                ok = amqp_channel:call(Channel,
                                       #'basic.publish'{exchange = X,
                                                        routing_key = <<"a">>},
                                       #amqp_msg{payload = Payload})
        end,
        fun (Channel, X, _, Q, _) ->
                %% The sync method
                #'queue.bind_ok'{} =
                    amqp_channel:call(Channel,
                                      #'queue.bind'{exchange = X,
                                                    queue = Q,
                                                    routing_key = <<"a">>}),
                %% No message should have been routed
                #'queue.declare_ok'{message_count = 0} =
                    amqp_channel:call(Channel,
                                      #'queue.declare'{queue = Q,
                                                       passive = true})
        end).

%% This is designed to exercize the internal queuing mechanism
%% to ensure that sending sync methods and then an async method is serialized
%% properly
sync_async_method_serialization_test() ->
    abstract_method_serialization_test(
        fun (_, _) -> ok end,
        fun (Channel, X, _Payload, _) ->
                Q = uuid(),
                %% The sync methods (called with cast to resume immediately;
                %% the order should still be preserved)
                amqp_channel:cast(Channel, #'queue.declare'{queue = Q}),
                amqp_channel:cast(Channel, #'queue.bind'{exchange = X,
                                                         queue = Q,
                                                         routing_key= <<"a">>}),
                Q
        end,
        fun (Channel, X, Payload, _, MultiOpRet) ->
                #'confirm.select_ok'{} = amqp_channel:call(
                                           Channel, #'confirm.select'{}),
                ok = amqp_channel:call(Channel,
                                       #'basic.publish'{exchange = X,
                                                        routing_key = <<"a">>},
                                       #amqp_msg{payload = Payload}),
                %% All queues must have gotten this message
                true = amqp_channel:wait_for_confirms(Channel),
                lists:foreach(
                    fun (Q) ->
                            #'queue.purge_ok'{message_count = 1} =
                                amqp_channel:call(
                                  Channel, #'queue.purge'{queue = Q})
                    end, lists:flatten(MultiOpRet))
        end).

queue_unbind_test() ->
    {ok, Connection} = new_connection(),
    X = <<"eggs">>, Q = <<"foobar">>, Key = <<"quay">>,
    Payload = <<"foobar">>,
    {ok, Channel} = amqp_connection:open_channel(Connection),
    amqp_channel:call(Channel, #'exchange.declare'{exchange = X}),
    amqp_channel:call(Channel, #'queue.declare'{queue = Q}),
    Bind = #'queue.bind'{queue = Q,
                         exchange = X,
                         routing_key = Key},
    amqp_channel:call(Channel, Bind),
    Publish = #'basic.publish'{exchange = X, routing_key = Key},
    amqp_channel:call(Channel, Publish, Msg = #amqp_msg{payload = Payload}),
    get_and_assert_equals(Channel, Q, Payload),
    Unbind = #'queue.unbind'{queue = Q,
                             exchange = X,
                             routing_key = Key},
    amqp_channel:call(Channel, Unbind),
    amqp_channel:call(Channel, Publish, Msg),
    get_and_assert_empty(Channel, Q),
    teardown(Connection, Channel).

get_and_assert_empty(Channel, Q) ->
    #'basic.get_empty'{}
        = amqp_channel:call(Channel, #'basic.get'{queue = Q, no_ack = true}).

get_and_assert_equals(Channel, Q, Payload) ->
    get_and_assert_equals(Channel, Q, Payload, true).

get_and_assert_equals(Channel, Q, Payload, NoAck) ->
    {GetOk = #'basic.get_ok'{}, Content}
        = amqp_channel:call(Channel, #'basic.get'{queue = Q, no_ack = NoAck}),
    #amqp_msg{payload = Payload2} = Content,
    ?assertMatch(Payload, Payload2),
    GetOk.

basic_get_test() ->
    basic_get_test1(new_connection()).

basic_get_ipv6_test() ->
    basic_get_test1(new_connection(just_network, [{host, "::1"}])).

basic_get_test1({ok, Connection}) ->
    {ok, Channel} = amqp_connection:open_channel(Connection),
    {ok, Q} = setup_publish(Channel),
    get_and_assert_equals(Channel, Q, <<"foobar">>),
    get_and_assert_empty(Channel, Q),
    teardown(Connection, Channel).

basic_return_test() ->
    {ok, Connection} = new_connection(),
    X = uuid(),
    Q = uuid(),
    Key = uuid(),
    Payload = <<"qwerty">>,
    {ok, Channel} = amqp_connection:open_channel(Connection),
    amqp_channel:register_return_handler(Channel, self()),
    amqp_channel:call(Channel, #'exchange.declare'{exchange = X}),
    amqp_channel:call(Channel, #'queue.declare'{queue = Q}),
    Publish = #'basic.publish'{exchange = X, routing_key = Key,
                               mandatory = true},
    amqp_channel:call(Channel, Publish, #amqp_msg{payload = Payload}),
    receive
        {BasicReturn = #'basic.return'{}, Content} ->
            #'basic.return'{reply_code = ReplyCode,
                            exchange = X} = BasicReturn,
            ?assertMatch(?NO_ROUTE, ReplyCode),
            #amqp_msg{payload = Payload2} = Content,
            ?assertMatch(Payload, Payload2);
        WhatsThis ->
            exit({bad_message, WhatsThis})
    after 2000 ->
        exit(no_return_received)
    end,
    teardown(Connection, Channel).

channel_repeat_open_close_test() ->
    {ok, Connection} = new_connection(),
    lists:foreach(
        fun(_) ->
            {ok, Ch} = amqp_connection:open_channel(Connection),
            ok = amqp_channel:close(Ch)
        end, lists:seq(1, 50)),
    amqp_connection:close(Connection),
    wait_for_death(Connection).

channel_multi_open_close_test() ->
    {ok, Connection} = new_connection(),
    [spawn_link(
        fun() ->
            try amqp_connection:open_channel(Connection) of
                {ok, Ch}           -> try amqp_channel:close(Ch) of
                                          ok                 -> ok;
                                          closing            -> ok
                                      catch
                                          exit:{noproc, _} -> ok;
                                          exit:{normal, _} -> ok
                                      end;
                closing            -> ok
            catch
                exit:{noproc, _} -> ok;
                exit:{normal, _} -> ok
            end
        end) || _ <- lists:seq(1, 50)],
    erlang:yield(),
    amqp_connection:close(Connection),
    wait_for_death(Connection).

basic_ack_test() ->
    {ok, Connection} = new_connection(),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    {ok, Q} = setup_publish(Channel),
    {#'basic.get_ok'{delivery_tag = Tag}, _}
        = amqp_channel:call(Channel, #'basic.get'{queue = Q, no_ack = false}),
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),
    teardown(Connection, Channel).

basic_ack_call_test() ->
    {ok, Connection} = new_connection(),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    {ok, Q} = setup_publish(Channel),
    {#'basic.get_ok'{delivery_tag = Tag}, _}
        = amqp_channel:call(Channel, #'basic.get'{queue = Q, no_ack = false}),
    amqp_channel:call(Channel, #'basic.ack'{delivery_tag = Tag}),
    teardown(Connection, Channel).

basic_consume_test() ->
    {ok, Connection} = new_connection(),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    X = uuid(),
    amqp_channel:call(Channel, #'exchange.declare'{exchange = X}),
    RoutingKey = uuid(),
    Parent = self(),
    [spawn_link(fun () ->
                        consume_loop(Channel, X, RoutingKey, Parent, <<Tag:32>>)
                end) || Tag <- lists:seq(1, ?Latch)],
    timer:sleep(?Latch * 20),
    Publish = #'basic.publish'{exchange = X, routing_key = RoutingKey},
    amqp_channel:call(Channel, Publish, #amqp_msg{payload = <<"foobar">>}),
    latch_loop(),
    teardown(Connection, Channel).

consume_loop(Channel, X, RoutingKey, Parent, Tag) ->
    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Channel, #'queue.declare'{}),
    #'queue.bind_ok'{} =
        amqp_channel:call(Channel, #'queue.bind'{queue = Q,
                                                 exchange = X,
                                                 routing_key = RoutingKey}),
    #'basic.consume_ok'{} =
        amqp_channel:call(Channel,
                          #'basic.consume'{queue = Q, consumer_tag = Tag}),
    receive #'basic.consume_ok'{consumer_tag = Tag} -> ok end,
    receive {#'basic.deliver'{}, _} -> ok end,
    #'basic.cancel_ok'{} =
        amqp_channel:call(Channel, #'basic.cancel'{consumer_tag = Tag}),
    receive #'basic.cancel_ok'{consumer_tag = Tag} -> ok end,
    Parent ! finished.

consume_notification_test() ->
    {ok, Connection} = new_connection(),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    Q = uuid(),
    #'queue.declare_ok'{} =
        amqp_channel:call(Channel, #'queue.declare'{queue = Q}),
    #'basic.consume_ok'{consumer_tag = CTag} = ConsumeOk =
        amqp_channel:call(Channel, #'basic.consume'{queue = Q}),
    receive ConsumeOk -> ok end,
    #'queue.delete_ok'{} =
        amqp_channel:call(Channel, #'queue.delete'{queue = Q}),
    receive #'basic.cancel'{consumer_tag = CTag} -> ok end,
    amqp_channel:close(Channel),
    ok.

basic_recover_test() ->
    {ok, Connection} = new_connection(),
    {ok, Channel} = amqp_connection:open_channel(
                        Connection, {amqp_direct_consumer, [self()]}),
    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Channel, #'queue.declare'{}),
    #'basic.consume_ok'{consumer_tag = Tag} =
        amqp_channel:call(Channel, #'basic.consume'{queue = Q}),
    receive #'basic.consume_ok'{consumer_tag = Tag} -> ok end,
    Publish = #'basic.publish'{exchange = <<>>, routing_key = Q},
    amqp_channel:call(Channel, Publish, #amqp_msg{payload = <<"foobar">>}),
    receive
        {#'basic.deliver'{consumer_tag = Tag}, _} ->
            %% no_ack set to false, but don't send ack
            ok
    end,
    BasicRecover = #'basic.recover'{requeue = true},
    amqp_channel:cast(Channel, BasicRecover),
    receive
        {#'basic.deliver'{consumer_tag = Tag,
                          delivery_tag = DeliveryTag2}, _} ->
            amqp_channel:cast(Channel,
                              #'basic.ack'{delivery_tag = DeliveryTag2})
    end,
    teardown(Connection, Channel).

simultaneous_close_test() ->
    {ok, Connection} = new_connection(),
    ChannelNumber = 5,
    {ok, Channel1} = amqp_connection:open_channel(Connection, ChannelNumber),

    %% Publish to non-existent exchange and immediately close channel
    amqp_channel:cast(Channel1, #'basic.publish'{exchange = uuid(),
                                                routing_key = <<"a">>},
                               #amqp_msg{payload = <<"foobar">>}),
    try amqp_channel:close(Channel1) of
        closing -> wait_for_death(Channel1)
    catch
        exit:{noproc, _}                                              -> ok;
        exit:{{shutdown, {server_initiated_close, ?NOT_FOUND, _}}, _} -> ok
    end,

    %% Channel2 (opened with the exact same number as Channel1)
    %% should not receive a close_ok (which is intended for Channel1)
    {ok, Channel2} = amqp_connection:open_channel(Connection, ChannelNumber),

    %% Make sure Channel2 functions normally
    #'exchange.declare_ok'{} =
        amqp_channel:call(Channel2, #'exchange.declare'{exchange = uuid()}),

    teardown(Connection, Channel2).

channel_tune_negotiation_test() ->
    {ok, Connection} = new_connection([{channel_max, 10}]),
    amqp_connection:close(Connection).

basic_qos_test() ->
    [NoQos, Qos] = [basic_qos_test(Prefetch) || Prefetch <- [0,1]],
    ExpectedRatio = (1+1) / (1+50/5),
    FudgeFactor = 2, %% account for timing variations
    ?assertMatch(true, Qos / NoQos < ExpectedRatio * FudgeFactor).

basic_qos_test(Prefetch) ->
    {ok, Connection} = new_connection(),
    Messages = 100,
    Workers = [5, 50],
    Parent = self(),
    {ok, Chan} = amqp_connection:open_channel(Connection),
    #'queue.declare_ok'{queue = Q} =
        amqp_channel:call(Chan, #'queue.declare'{}),
    Kids = [spawn(
            fun() ->
                {ok, Channel} = amqp_connection:open_channel(Connection),
                amqp_channel:call(Channel,
                                  #'basic.qos'{prefetch_count = Prefetch}),
                amqp_channel:call(Channel,
                                  #'basic.consume'{queue = Q}),
                Parent ! finished,
                sleeping_consumer(Channel, Sleep, Parent)
            end) || Sleep <- Workers],
    latch_loop(length(Kids)),
    spawn(fun() -> {ok, Channel} = amqp_connection:open_channel(Connection),
                   producer_loop(Channel, Q, Messages)
          end),
    {Res, _} = timer:tc(erlang, apply, [fun latch_loop/1, [Messages]]),
    [Kid ! stop || Kid <- Kids],
    latch_loop(length(Kids)),
    teardown(Connection, Chan),
    Res.

sleeping_consumer(Channel, Sleep, Parent) ->
    receive
        stop ->
            do_stop(Channel, Parent);
        #'basic.consume_ok'{} ->
            sleeping_consumer(Channel, Sleep, Parent);
        #'basic.cancel_ok'{}  ->
            exit(unexpected_cancel_ok);
        {#'basic.deliver'{delivery_tag = DeliveryTag}, _Content} ->
            Parent ! finished,
            receive stop -> do_stop(Channel, Parent)
            after Sleep -> ok
            end,
            amqp_channel:cast(Channel,
                              #'basic.ack'{delivery_tag = DeliveryTag}),
            sleeping_consumer(Channel, Sleep, Parent)
    end.

do_stop(Channel, Parent) ->
    Parent ! finished,
    amqp_channel:close(Channel),
    wait_for_death(Channel),
    exit(normal).

producer_loop(Channel, _RoutingKey, 0) ->
    amqp_channel:close(Channel),
    wait_for_death(Channel),
    ok;

producer_loop(Channel, RoutingKey, N) ->
    Publish = #'basic.publish'{exchange = <<>>, routing_key = RoutingKey},
    amqp_channel:call(Channel, Publish, #amqp_msg{payload = <<>>}),
    producer_loop(Channel, RoutingKey, N - 1).

confirm_test() ->
    {ok, Connection} = new_connection(),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    #'confirm.select_ok'{} = amqp_channel:call(Channel, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(Channel, self()),
    {ok, Q} = setup_publish(Channel),
    {#'basic.get_ok'{}, _}
        = amqp_channel:call(Channel, #'basic.get'{queue = Q, no_ack = false}),
    ok = receive
             #'basic.ack'{}  -> ok;
             #'basic.nack'{} -> fail
         after 2000 ->
                 exit(did_not_receive_pub_ack)
         end,
    teardown(Connection, Channel).

confirm_barrier_test() ->
    {ok, Connection} = new_connection(),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    #'confirm.select_ok'{} = amqp_channel:call(Channel, #'confirm.select'{}),
    [amqp_channel:call(Channel, #'basic.publish'{routing_key = <<"whoosh">>},
                       #amqp_msg{payload = <<"foo">>})
     || _ <- lists:seq(1, 1000)], %% Hopefully enough to get a multi-ack
    true = amqp_channel:wait_for_confirms(Channel),
    teardown(Connection, Channel).

confirm_barrier_nop_test() ->
    {ok, Connection} = new_connection(),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    true = amqp_channel:wait_for_confirms(Channel),
    amqp_channel:call(Channel, #'basic.publish'{routing_key = <<"whoosh">>},
                      #amqp_msg{payload = <<"foo">>}),
    true = amqp_channel:wait_for_confirms(Channel),
    teardown(Connection, Channel).

confirm_barrier_timeout_test() ->
    {ok, Connection} = new_connection(),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    #'confirm.select_ok'{} = amqp_channel:call(Channel, #'confirm.select'{}),
    [amqp_channel:call(Channel, #'basic.publish'{routing_key = <<"whoosh">>},
                       #amqp_msg{payload = <<"foo">>})
     || _ <- lists:seq(1, 1000)],
    case amqp_channel:wait_for_confirms(Channel, 0) of
        true    -> ok;
        timeout -> ok
    end,
    teardown(Connection, Channel).

confirm_barrier_die_timeout_test() ->
    {ok, Connection} = new_connection(),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    #'confirm.select_ok'{} = amqp_channel:call(Channel, #'confirm.select'{}),
    [amqp_channel:call(Channel, #'basic.publish'{routing_key = <<"whoosh">>},
                       #amqp_msg{payload = <<"foo">>})
     || _ <- lists:seq(1, 1000)],
    try amqp_channel:wait_for_confirms_or_die(Channel, 0) of
        true    -> ok
    catch
        exit:timeout -> ok
    end,
    amqp_connection:close(Connection),
    wait_for_death(Connection).

default_consumer_test() ->
    {ok, Connection} = new_connection(),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    amqp_selective_consumer:register_default_consumer(Channel, self()),

    #'queue.declare_ok'{queue = Q}
        = amqp_channel:call(Channel, #'queue.declare'{}),
    Pid = spawn(fun () -> receive
                          after 10000 -> ok
                          end
                end),
    #'basic.consume_ok'{} =
        amqp_channel:subscribe(Channel, #'basic.consume'{queue = Q}, Pid),
    erlang:monitor(process, Pid),
    exit(Pid, shutdown),
    receive
        {'DOWN', _, process, _, _} ->
            io:format("little consumer died out~n")
    end,
    Payload = <<"for the default consumer">>,
    amqp_channel:call(Channel,
                      #'basic.publish'{exchange = <<>>, routing_key = Q},
                      #amqp_msg{payload = Payload}),

    receive
        {#'basic.deliver'{}, #'amqp_msg'{payload = Payload}} ->
            ok
    after 1000 ->
            exit('default_consumer_didnt_work')
    end,
    teardown(Connection, Channel).

subscribe_nowait_test() ->
    {ok, Connection} = new_connection(),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    {ok, Q} = setup_publish(Channel),
    ok = amqp_channel:call(Channel,
                           #'basic.consume'{queue = Q,
                                            consumer_tag = uuid(),
                                            nowait = true}),
    receive #'basic.consume_ok'{} -> exit(unexpected_consume_ok)
    after 0 -> ok
    end,
    receive
        {#'basic.deliver'{delivery_tag = DTag}, _Content} ->
            amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = DTag})
    end,
    teardown(Connection, Channel).

basic_nack_test() ->
    {ok, Connection} = new_connection(),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    #'queue.declare_ok'{queue = Q}
        = amqp_channel:call(Channel, #'queue.declare'{}),

    Payload = <<"m1">>,

    amqp_channel:call(Channel,
                      #'basic.publish'{exchange = <<>>, routing_key = Q},
                      #amqp_msg{payload = Payload}),

    #'basic.get_ok'{delivery_tag = Tag} =
        get_and_assert_equals(Channel, Q, Payload, false),

    amqp_channel:call(Channel, #'basic.nack'{delivery_tag = Tag,
                                             multiple     = false,
                                             requeue      = false}),

    get_and_assert_empty(Channel, Q),
    teardown(Connection, Channel).

large_content_test() ->
    {ok, Connection} = new_connection(),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    #'queue.declare_ok'{queue = Q}
        = amqp_channel:call(Channel, #'queue.declare'{}),
    {A1,A2,A3} = now(), random:seed(A1, A2, A3),
    F = list_to_binary([random:uniform(256)-1 || _ <- lists:seq(1, 1000)]),
    Payload = list_to_binary([[F || _ <- lists:seq(1, 1000)]]),
    Publish = #'basic.publish'{exchange = <<>>, routing_key = Q},
    amqp_channel:call(Channel, Publish, #amqp_msg{payload = Payload}),
    get_and_assert_equals(Channel, Q, Payload),
    teardown(Connection, Channel).

%% ----------------------------------------------------------------------------
%% Test for the network client
%% Sends a bunch of messages and immediatly closes the connection without
%% closing the channel. Then gets the messages back from the queue and expects
%% all of them to have been sent.
pub_and_close_test() ->
    {ok, Connection1} = new_connection(just_network),
    X = uuid(), Q = uuid(), Key = uuid(),
    Payload = <<"eggs">>, NMessages = 50000,
    {ok, Channel1} = amqp_connection:open_channel(Connection1),
    amqp_channel:call(Channel1, #'exchange.declare'{exchange = X}),
    amqp_channel:call(Channel1, #'queue.declare'{queue = Q}),
    Route = #'queue.bind'{queue = Q,
                          exchange = X,
                          routing_key = Key},
    amqp_channel:call(Channel1, Route),
    %% Send messages
    pc_producer_loop(Channel1, X, Key, Payload, NMessages),
    %% Close connection without closing channels
    amqp_connection:close(Connection1),
    %% Get sent messages back and count them
    {ok, Connection2} = new_connection(just_network),
    {ok, Channel2} = amqp_connection:open_channel(
                         Connection2, {amqp_direct_consumer, [self()]}),
    amqp_channel:call(Channel2, #'basic.consume'{queue = Q, no_ack = true}),
    receive #'basic.consume_ok'{} -> ok end,
    ?assert(pc_consumer_loop(Channel2, Payload, 0) == NMessages),
    %% Make sure queue is empty
    #'queue.declare_ok'{queue = Q, message_count = NRemaining} =
        amqp_channel:call(Channel2, #'queue.declare'{queue = Q}),
    ?assert(NRemaining == 0),
    teardown(Connection2, Channel2),
    ok.

pc_producer_loop(_, _, _, _, 0) -> ok;
pc_producer_loop(Channel, X, Key, Payload, NRemaining) ->
    Publish = #'basic.publish'{exchange = X, routing_key = Key},
    ok = amqp_channel:call(Channel, Publish, #amqp_msg{payload = Payload}),
    pc_producer_loop(Channel, X, Key, Payload, NRemaining - 1).

pc_consumer_loop(Channel, Payload, NReceived) ->
    receive
        {#'basic.deliver'{},
         #amqp_msg{payload = DeliveredPayload}} ->
            case DeliveredPayload of
                Payload ->
                    pc_consumer_loop(Channel, Payload, NReceived + 1);
                _ ->
                    exit(received_unexpected_content)
            end
    after 1000 ->
        NReceived
    end.


%%----------------------------------------------------------------------------
%% Unit test for the direct client
%% This just relies on the fact that a fresh Rabbit VM must consume more than
%% 0.1 pc of the system memory:
%% 0. Wait 1 minute to let memsup do stuff
%% 1. Make sure that the high watermark is set high
%% 2. Start a process to receive the pause and resume commands from the broker
%% 3. Register this as flow control notification handler
%% 4. Let the system settle for a little bit
%% 5. Set the threshold to the lowest possible value
%% 6. When the flow handler receives the pause command, it sets the watermark
%%    to a high value in order to get the broker to send the resume command
%% 7. Allow 10 secs to receive the pause and resume, otherwise timeout and
%%    fail
channel_flow_test() ->
    {ok, Connection} = new_connection(),
    X = <<"amq.direct">>,
    K = Payload = <<"x">>,
    memsup:set_sysmem_high_watermark(0.99),
    timer:sleep(1000),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    Parent = self(),
    Child = spawn_link(
              fun() ->
                      receive
                          #'channel.flow'{active = false} -> ok
                      end,
                      Publish = #'basic.publish'{exchange = X,
                                                 routing_key = K},
                      blocked =
                        amqp_channel:call(Channel, Publish,
                                          #amqp_msg{payload = Payload}),
                      memsup:set_sysmem_high_watermark(0.99),
                      receive
                          #'channel.flow'{active = true} -> ok
                      end,
                      Parent ! ok
              end),
    amqp_channel:register_flow_handler(Channel, Child),
    timer:sleep(1000),
    memsup:set_sysmem_high_watermark(0.001),
    receive
        ok -> ok
    after 10000 ->
        ?LOG_DEBUG("Are you sure that you have waited 1 minute?~n"),
        exit(did_not_receive_channel_flow)
    end.

%%---------------------------------------------------------------------------
%% This tests whether RPC over AMQP produces the same result as invoking the
%% same argument against the same underlying gen_server instance.
rpc_test() ->
    {ok, Connection} = new_connection(),
    Q = uuid(),
    Fun = fun(X) -> X + 1 end,
    RPCHandler = fun(X) -> term_to_binary(Fun(binary_to_term(X))) end,
    Server = amqp_rpc_server:start(Connection, Q, RPCHandler),
    Client = amqp_rpc_client:start(Connection, Q),
    Input = 1,
    Reply = amqp_rpc_client:call(Client, term_to_binary(Input)),
    Expected = Fun(Input),
    DecodedReply = binary_to_term(Reply),
    ?assertMatch(Expected, DecodedReply),
    amqp_rpc_client:stop(Client),
    amqp_rpc_server:stop(Server),
    amqp_connection:close(Connection),
    wait_for_death(Connection),
    ok.

%%---------------------------------------------------------------------------

setup_publish(Channel) ->
    Publish = #publish{routing_key = <<"a.b.c.d">>,
                       q = uuid(),
                       x = uuid(),
                       bind_key = <<"a.b.c.*">>,
                       payload = <<"foobar">>},
    setup_publish(Channel, Publish).

setup_publish(Channel, #publish{routing_key = RoutingKey,
                                q = Q, x = X,
                                bind_key = BindKey,
                                payload = Payload}) ->
    ok = setup_exchange(Channel, Q, X, BindKey),
    Publish = #'basic.publish'{exchange = X, routing_key = RoutingKey},
    ok = amqp_channel:call(Channel, Publish, #amqp_msg{payload = Payload}),
    {ok, Q}.

teardown(Connection, Channel) ->
    amqp_channel:close(Channel),
    wait_for_death(Channel),
    amqp_connection:close(Connection),
    wait_for_death(Connection).

teardown_test() ->
    {ok, Connection} = new_connection(),
    {ok, Channel} = amqp_connection:open_channel(Connection),
    ?assertMatch(true, is_process_alive(Channel)),
    ?assertMatch(true, is_process_alive(Connection)),
    teardown(Connection, Channel),
    ?assertMatch(false, is_process_alive(Channel)),
    ?assertMatch(false, is_process_alive(Connection)).

setup_exchange(Channel, Q, X, Binding) ->
    amqp_channel:call(Channel, #'exchange.declare'{exchange = X,
                                                   type = <<"topic">>}),
    amqp_channel:call(Channel, #'queue.declare'{queue = Q}),
    Route = #'queue.bind'{queue = Q,
                          exchange = X,
                          routing_key = Binding},
    amqp_channel:call(Channel, Route),
    ok.

wait_for_death(Pid) ->
    Ref = erlang:monitor(process, Pid),
    receive {'DOWN', Ref, process, Pid, _Reason} -> ok
    after 1000 -> exit({timed_out_waiting_for_process_death, Pid})
    end.

latch_loop() ->
    latch_loop(?Latch, []).

latch_loop(Latch) ->
    latch_loop(Latch, []).

latch_loop(0, Acc) ->
    Acc;
latch_loop(Latch, Acc) ->
    receive
        finished        -> latch_loop(Latch - 1, Acc);
        {finished, Ret} -> latch_loop(Latch - 1, [Ret | Acc])
    after ?Latch * ?Wait -> exit(waited_too_long)
    end.

uuid() ->
    {A, B, C} = now(),
    <<A:32, B:32, C:32>>.

new_connection() ->
    new_connection(both, []).

new_connection(AllowedConnectionTypes) when is_atom(AllowedConnectionTypes) ->
    new_connection(AllowedConnectionTypes, []);
new_connection(Params) when is_list(Params) ->
    new_connection(both, Params).

new_connection(AllowedConnectionTypes, Params) ->
    Params1 =
        case {AllowedConnectionTypes,
              os:getenv("AMQP_CLIENT_TEST_CONNECTION_TYPE")} of
            {just_direct, "network"} ->
                exit(normal);
            {just_direct, "network_ssl"} ->
                exit(normal);
            {just_network, "direct"} ->
                exit(normal);
            {_, "network"} ->
                make_network_params(Params);
            {_, "network_ssl"} ->
                {ok, [[CertsDir]]} = init:get_argument(erlang_client_ssl_dir),
                make_network_params(
                  [{ssl_options, [{cacertfile,
                                   CertsDir ++ "/testca/cacert.pem"},
                                  {certfile, CertsDir ++ "/client/cert.pem"},
                                  {keyfile, CertsDir ++ "/client/key.pem"},
                                  {verify, verify_peer},
                                  {fail_if_no_peer_cert, true}]}] ++ Params);
            {_, "direct"} ->
                make_direct_params([{node, rabbit_nodes:make(rabbit)}] ++
                                       Params)
        end,
    amqp_connection:start(Params1).

%% Note: not all amqp_params_network fields supported.
make_network_params(Props) ->
    Pgv = fun (Key, Default) ->
                  proplists:get_value(Key, Props, Default)
          end,
    #amqp_params_network{username     = Pgv(username, <<"guest">>),
                         password     = Pgv(password, <<"guest">>),
                         virtual_host = Pgv(virtual_host, <<"/">>),
                         channel_max  = Pgv(channel_max, 0),
                         ssl_options  = Pgv(ssl_options, none),
                         host         = Pgv(host, "localhost")}.

%% Note: not all amqp_params_direct fields supported.
make_direct_params(Props) ->
    Pgv = fun (Key, Default) ->
                  proplists:get_value(Key, Props, Default)
          end,
    #amqp_params_direct{username     = Pgv(username, <<"guest">>),
                        virtual_host = Pgv(virtual_host, <<"/">>),
                        node         = Pgv(node, node())}.
