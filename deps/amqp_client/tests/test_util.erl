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
%%   The Original Code is the RabbitMQ Erlang Client.
%%
%%   The Initial Developers of the Original Code are LShift Ltd.,
%%   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd., Cohesive Financial
%%   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C)
%%   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit
%%   Technologies Ltd.;
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): Ben Hood <0x6e6562@gmail.com>.
%%

-module(test_util).

-include_lib("rabbit.hrl").
-include_lib("rabbit_framing.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("amqp_client.hrl").

-compile([export_all]).

-record(publish, {q, x, routing_key, bind_key, payload,
                 mandatory = false, immediate = false}).

%% The latch constant defines how many processes are spawned in order
%% to run certain functionality in parallel. It follows the standard
%% countdown latch pattern.
-define(Latch, 100).

%% The wait constant defines how long a consumer waits before it
%% unsubscribes
-define(Wait, 200).

%%%%
%%
%% This is an example of how the client interaction should work
%%
%%   Connection = amqp_connection:start(User, Password, Host),
%%   Channel = amqp_connection:open_channel(Connection),
%%   %%...do something useful
%%   ChannelClose = #'channel.close'{ %% set the appropriate fields },
%%   amqp_channel:call(Channel, ChannelClose),
%%   ConnectionClose = #'connection.close'{ %% set the appropriate fields },
%%   amqp_connection:close(Connection, ConnectionClose).
%%

lifecycle_test(Connection) ->
    X = <<"x">>,
    Channel = lib_amqp:start_channel(Connection),
    lib_amqp:declare_exchange(Channel, X, <<"topic">>),
    Parent = self(),
    [spawn(
           fun() ->
                queue_exchange_binding(Channel, X, Parent, Tag) end)
            || Tag <- lists:seq(1, ?Latch)],
    latch_loop(?Latch),
    lib_amqp:delete_exchange(Channel, X),
    lib_amqp:teardown(Connection, Channel),
    ok.

queue_exchange_binding(Channel, X, Parent, Tag) ->
    receive
        nothing -> ok
    after (?Latch - Tag rem 7) * 10 ->
        ok
    end,
    Q = <<"a.b.c", Tag:32>>,
    Binding = <<"a.b.c.*">>,
    Q1 = lib_amqp:declare_queue(Channel, Q),
    ?assertMatch(Q, Q1),
    lib_amqp:bind_queue(Channel, X, Q, Binding),
    lib_amqp:delete_queue(Channel, Q),
    Parent ! finished.

channel_lifecycle_test(Connection) ->
    Channel = lib_amqp:start_channel(Connection),
    lib_amqp:close_channel(Channel),
    Channel2 = lib_amqp:start_channel(Connection),
    lib_amqp:teardown(Connection, Channel2),
    ok.

%% This is designed to exercize the internal queuing mechanism
%% to ensure that commands are properly serialized
command_serialization_test(Connection) ->
    Channel = lib_amqp:start_channel(Connection),
    Parent = self(),
    [spawn(fun() ->
                Q = uuid(),
                Q1 = lib_amqp:declare_queue(Channel, Q),
                ?assertMatch(Q, Q1),
                Parent ! finished
           end) || _ <- lists:seq(1, ?Latch)],
    latch_loop(?Latch),
    lib_amqp:teardown(Connection, Channel).

queue_unbind_test(Connection) ->
    X = <<"eggs">>, Q = <<"foobar">>, Key = <<"quay">>,
    Payload = <<"foobar">>,
    Channel = lib_amqp:start_channel(Connection),
    lib_amqp:declare_exchange(Channel, X),
    lib_amqp:declare_queue(Channel, Q),
    lib_amqp:bind_queue(Channel, X, Q, Key),
    lib_amqp:publish(Channel, X, Key, Payload),
    get_and_assert_equals(Channel, Q, Payload),
    lib_amqp:unbind_queue(Channel, X, Q, Key),
    lib_amqp:publish(Channel, X, Key, Payload),
    get_and_assert_empty(Channel, Q),
    lib_amqp:teardown(Connection, Channel).

get_and_assert_empty(Channel, Q) ->
    BasicGetEmpty = lib_amqp:get(Channel, Q, false),
    ?assertMatch('basic.get_empty', BasicGetEmpty).

get_and_assert_equals(Channel, Q, Payload) ->
    Content = lib_amqp:get(Channel, Q),
    #content{payload_fragments_rev = PayloadFragments} = Content,
    ?assertMatch([Payload], PayloadFragments).

basic_get_test(Connection) ->
    Channel = lib_amqp:start_channel(Connection),
    {ok, Q} = setup_publish(Channel),
    %% TODO: This could be refactored to use get_and_assert_equals,
    %% get_and_assert_empty .... would require another bug though :-)
    Content = lib_amqp:get(Channel, Q),
    #content{payload_fragments_rev = PayloadFragments} = Content,
    ?assertMatch([<<"foobar">>], PayloadFragments),
    BasicGetEmpty = lib_amqp:get(Channel, Q, false),
    ?assertMatch('basic.get_empty', BasicGetEmpty),
    lib_amqp:teardown(Connection, Channel).

basic_return_test(Connection) ->
    X = uuid(),
    Q = uuid(),
    Key = uuid(),
    Payload = <<"qwerty">>,
    Channel = lib_amqp:start_channel(Connection),
    amqp_channel:register_return_handler(Channel, self()),
    lib_amqp:declare_exchange(Channel, X),
    lib_amqp:declare_queue(Channel, Q),
    lib_amqp:publish(Channel, X, Key, Payload, true),
    timer:sleep(200),
    receive
        {BasicReturn = #'basic.return'{}, Content} ->
            #'basic.return'{reply_text = ReplyText,
                            exchange = X} = BasicReturn,
            ?assertMatch(<<"unroutable">>, ReplyText),
            #content{payload_fragments_rev = Payload2} = Content,
            ?assertMatch([Payload], Payload2);
        WhatsThis ->
            %% TODO investigate where this comes from
            io:format("Spurious message ~p~n", [WhatsThis])
    after 2000 ->
        exit(no_return_received)
    end,
    lib_amqp:teardown(Connection, Channel).

basic_ack_test(Connection) ->
    Channel = lib_amqp:start_channel(Connection),
    {ok, Q} = setup_publish(Channel),
    {DeliveryTag, _} = lib_amqp:get(Channel, Q, false),
    lib_amqp:ack(Channel, DeliveryTag),
    lib_amqp:teardown(Connection, Channel).

basic_consume_test(Connection) ->
    Channel = lib_amqp:start_channel(Connection),
    X = uuid(),
    lib_amqp:declare_exchange(Channel, X),
    RoutingKey = uuid(),
    Parent = self(),
    [spawn(
        fun() ->
            consume_loop(Channel, X, RoutingKey, Parent, <<Tag:32>>) end)
        || Tag <- lists:seq(1, ?Latch)],
    timer:sleep(?Latch * 20),
    lib_amqp:publish(Channel, X, RoutingKey, <<"foobar">>),
    latch_loop(?Latch),
    lib_amqp:teardown(Connection, Channel).

consume_loop(Channel, X, RoutingKey, Parent, Tag) ->
    Q = lib_amqp:declare_queue(Channel),
    lib_amqp:bind_queue(Channel, X, Q, RoutingKey),
    lib_amqp:subscribe(Channel, Q, self(), Tag),
    receive
        #'basic.consume_ok'{consumer_tag = Tag} -> ok
    end,
    receive
        {#'basic.deliver'{}, _} -> ok
    end,
    lib_amqp:unsubscribe(Channel, Tag),
    receive
        #'basic.cancel_ok'{consumer_tag = Tag} -> ok
    end,
    Parent ! finished.

basic_recover_test(Connection) ->
    Q = uuid(),
    Channel = lib_amqp:start_channel(Connection),
    lib_amqp:declare_queue(Channel, Q),
    Tag = lib_amqp:subscribe(Channel, Q, self(), false),
    receive
        #'basic.consume_ok'{consumer_tag = Tag} -> ok
    after 2000 ->
        exit(did_not_receive_subscription_message)
    end,
    lib_amqp:publish(Channel, <<>>, Q, <<"foobar">>),
    receive
        {#'basic.deliver'{}, _} ->
            %% no_ack set to false, but don't send ack
            ok
    after 2000 ->
        exit(did_not_receive_first_message)
    end,
    BasicRecover = #'basic.recover'{requeue = true},
    amqp_channel:cast(Channel, BasicRecover),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag2}, _} ->
            lib_amqp:ack(Channel, DeliveryTag2)
    after 2000 ->
        exit(did_not_receive_second_message)
    end,
    lib_amqp:teardown(Connection, Channel).

basic_qos_test(Con) ->
    [NoQos, Qos] = [basic_qos_test(Con, Prefetch) || Prefetch <- [0,1]],
    ExpectedRatio = (1+1) / (1+50/5),
    FudgeFactor = 2, %% account for timing variations
    ?assertMatch(true, Qos / NoQos < ExpectedRatio * FudgeFactor).

basic_qos_test(Connection, Prefetch) ->
    Messages = 100,
    Workers = [5, 50],
    Parent = self(),
    Chan = lib_amqp:start_channel(Connection),
    Q = lib_amqp:declare_queue(Chan),
    Kids = [spawn(fun() ->
                    Channel = lib_amqp:start_channel(Connection),
                    lib_amqp:set_prefetch_count(Channel, Prefetch),
                    lib_amqp:subscribe(Channel, Q, self(), false),
                    Parent ! finished,
                    sleeping_consumer(Channel, Sleep, Parent)
                  end) || Sleep <- Workers],
    latch_loop(length(Kids)),
    spawn(fun() -> producer_loop(lib_amqp:start_channel(Connection),
                                 Q, Messages) end),
    {Res, ok} = timer:tc(erlang, apply, [fun latch_loop/1, [Messages]]),
    [Kid ! stop || Kid <- Kids],
    latch_loop(length(Kids)),
    lib_amqp:close_channel(Chan),
    Res.

sleeping_consumer(Channel, Sleep, Parent) ->
    receive
        stop ->
            do_stop(Channel, Parent);
        #'basic.consume_ok'{} ->
            sleeping_consumer(Channel, Sleep, Parent);
        #'basic.cancel_ok'{}  ->
            ok;
        {#'basic.deliver'{delivery_tag = DeliveryTag}, _Content} ->
            Parent ! finished,
            receive stop -> do_stop(Channel, Parent)
            after Sleep -> ok
            end,
            lib_amqp:ack(Channel, DeliveryTag),
            sleeping_consumer(Channel, Sleep, Parent)
    end.

do_stop(Channel, Parent) ->
    Parent ! finished,
    lib_amqp:close_channel(Channel),
    exit(normal).

producer_loop(Channel, _RoutingKey, 0) ->
    lib_amqp:close_channel(Channel),
    ok;

producer_loop(Channel, RoutingKey, N) ->
    lib_amqp:publish(Channel, <<>>, RoutingKey, <<>>),
    producer_loop(Channel, RoutingKey, N - 1).

%% Reject is not yet implemented in RabbitMQ
basic_reject_test(Connection) ->
    lib_amqp:close_connection(Connection).

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
channel_flow_test(Connection) ->
    X = <<"amq.direct">>,
    K = Payload = <<"x">>,
    memsup:set_sysmem_high_watermark(0.99),
    timer:sleep(1000),
    Channel = lib_amqp:start_channel(Connection),
    Parent = self(),
    Child = spawn_link(
              fun() ->
                      receive
                          #'channel.flow'{active = false} -> ok
                      end,
                      blocked = lib_amqp:publish(Channel, X, K, Payload),
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
        io:format("Are you sure that you have waited 1 minute?~n"),
        exit(did_not_receive_channel_flow)
    end.

%%----------------------------------------------------------------------------
%% This is a test, albeit not a unit test, to see if the producer
%% handles the effect of being throttled.

channel_flow_sync(Connection) ->
    start_channel_flow(Connection, fun lib_amqp:publish/4).

channel_flow_async(Connection) ->
    start_channel_flow(Connection, fun lib_amqp:async_publish/4).

start_channel_flow(Connection, PublishFun) ->
    X = <<"amq.direct">>,
    Key = uuid(),
    Producer = spawn_link(
        fun() ->
            Channel = lib_amqp:start_channel(Connection),
            Parent = self(),
            FlowHandler = spawn_link(fun() -> cf_handler_loop(Parent) end),
            amqp_channel:register_flow_handler(Channel, FlowHandler),
            Payload = << <<B:8>> || B <- lists:seq(1, 10000) >>,
            cf_producer_loop(Channel, X, Key, PublishFun, Payload, 0)
        end),
    Consumer = spawn_link(
        fun() ->
            Channel = lib_amqp:start_channel(Connection),
            Q = lib_amqp:declare_queue(Channel),
            lib_amqp:bind_queue(Channel, X, Q, Key),
            Tag = lib_amqp:subscribe(Channel, Q, self()),
            cf_consumer_loop(Channel, Tag)
        end),
    {Producer, Consumer}.

cf_consumer_loop(Channel, Tag) ->
    receive
        #'basic.consume_ok'{} -> cf_consumer_loop(Channel, Tag);
        #'basic.cancel_ok'{} -> ok;
        {#'basic.deliver'{delivery_tag = DeliveryTag}, _Content} ->
             lib_amqp:ack(Channel, DeliveryTag),
             cf_consumer_loop(Channel, Tag);
        stop ->
             lib_amqp:unsubscribe(Channel, Tag),
             ok
    end.

cf_producer_loop(Channel, X, Key, PublishFun, Payload, N)
        when N rem 5000 =:= 0 ->
    io:format("Producer (~p) has sent about ~p messages since it started~n",
              [self(), N]),
    cf_producer_loop(Channel, X, Key, PublishFun, Payload, N + 1);

cf_producer_loop(Channel, X, Key, PublishFun, Payload, N) ->
    case PublishFun(Channel, X, Key, Payload) of
        blocked ->
            io:format("Producer (~p) is blocked, will go to sleep.....ZZZ~n",
                      [self()]),
            receive
                resume ->
                    io:format("Producer (~p) has woken up :-)~n", [self()]),
                    cf_producer_loop(Channel, X, Key,
                                     PublishFun, Payload, N + 1)
            end;
        _ ->
            cf_producer_loop(Channel, X, Key, PublishFun, Payload, N + 1)
    end.

cf_handler_loop(Producer) ->
    receive
        #'channel.flow'{active = false} ->
            io:format("Producer throttling ON~n"),
            cf_handler_loop(Producer);
        #'channel.flow'{active = true} ->
            io:format("Producer throttling OFF, waking up producer (~p)~n",
                      [Producer]),
            Producer ! resume,
            cf_handler_loop(Producer);
        stop -> ok
    end.

%%---------------------------------------------------------------------------
%% This tests whether RPC over AMQP produces the same result as invoking the
%% same argument against the same underlying gen_server instance.
rpc_test(Connection) ->
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
    ok.

%%---------------------------------------------------------------------------

setup_publish(Channel) ->
    Publish = #publish{routing_key = <<"a.b.c.d">>,
                       q = <<"a.b.c">>,
                       x = <<"x">>,
                       bind_key = <<"a.b.c.*">>,
                       payload = <<"foobar">>
                       },
    setup_publish(Channel, Publish).

setup_publish(Channel, #publish{routing_key = RoutingKey,
                                q = Q, x = X,
                                bind_key = BindKey,
                                payload = Payload}) ->
    ok = setup_exchange(Channel, Q, X, BindKey),
    lib_amqp:publish(Channel, X, RoutingKey, Payload),
    {ok, Q}.

teardown_test(Connection) ->
    Channel = lib_amqp:start_channel(Connection),
    ?assertMatch(true, is_process_alive(Channel)),
    ?assertMatch(true, is_process_alive(Connection)),
    lib_amqp:teardown(Connection, Channel),
    ?assertMatch(false, is_process_alive(Channel)),
    ?assertMatch(false, is_process_alive(Connection)).

setup_exchange(Channel, Q, X, Binding) ->
    lib_amqp:declare_exchange(Channel, X, <<"topic">>),
    lib_amqp:declare_queue(Channel, Q),
    lib_amqp:bind_queue(Channel, X, Q, Binding),
    ok.

latch_loop(0) ->
    ok;

latch_loop(Latch) ->
    receive
        finished ->
            latch_loop(Latch - 1)
    after ?Latch * ?Wait ->
        exit(waited_too_long)
    end.

uuid() ->
    {A, B, C} = now(),
    <<A:32, B:32, C:32>>.

