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

-include_lib("rabbitmq_server/include/rabbit.hrl").
-include_lib("rabbitmq_server/include/rabbit_framing.hrl").
-include_lib("eunit/include/eunit.hrl").
-include("amqp_client.hrl").

-compile([export_all]).

-record(publish,{q, x, routing_key, bind_key, payload,
                 mandatory = false, immediate = false}).

-define(Latch, 100).

%%%%
%
% This is an example of how the client interaction should work
%
%   Connection = amqp_connection:start(User, Password, Host),
%   Channel = amqp_connection:open_channel(Connection),
%   %%...do something useful
%   ChannelClose = #'channel.close'{ %% set the appropriate fields },
%   amqp_channel:call(Channel, ChannelClose),
%   ConnectionClose = #'connection.close'{ %% set the appropriate fields },
%   amqp_connection:close(Connection, ConnectionClose).
%

lifecycle_test(Connection) ->
    X = <<"x">>,
    Channel = setup_channel(Connection),
    ExchangeDeclare = #'exchange.declare'{exchange = X, type = <<"topic">>,
                                          passive = false, durable = false, auto_delete = false, internal = false,
                                          nowait = false, arguments = []},
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, ExchangeDeclare),
    Parent = self(),
    [spawn(fun() -> queue_exchange_binding(Channel,X,Parent,Tag) end) || Tag <- lists:seq(1,?Latch)],
    latch_loop(?Latch),
    ExchangeDelete = #'exchange.delete'{exchange = X,
                                        if_unused = false, nowait = false},
    #'exchange.delete_ok'{} = amqp_channel:call(Channel, ExchangeDelete),
    teardown(Connection, Channel).

queue_exchange_binding(Channel,X,Parent,Tag) ->
    receive
        nothing -> ok
    after (?Latch - Tag rem 7) * 10 ->
        ok
    end,
    Q = <<"a.b.c",Tag:32>>,
    BindKey = <<"a.b.c.*">>,
    RoutingKey = <<"a.b.c.d">>,
    Payload = <<"foobar">>,
    QueueDeclare = #'queue.declare'{queue = Q,
                                    passive = false, durable = false,
                                    exclusive = false, auto_delete = false,
                                    nowait = false, arguments = []},
    #'queue.declare_ok'{queue = Q1,
                        message_count = MessageCount,
                        consumer_count = ConsumerCount}
                       = amqp_channel:call(Channel,QueueDeclare),
    ?assertMatch(Q, Q1),
    QueueBind = #'queue.bind'{queue = Q, exchange = X,
                              routing_key = BindKey, nowait = false, arguments = []},
    #'queue.bind_ok'{} = amqp_channel:call(Channel, QueueBind),
    QueueDelete = #'queue.delete'{queue = Q,
                                  if_unused = true, if_empty = true, nowait = false},
    #'queue.delete_ok'{message_count = MessageCount2} = amqp_channel:call(Channel, QueueDelete),
    ?assertMatch(MessageCount, MessageCount2),
    Parent ! finished.

channel_lifecycle_test(Connection) ->
    Channel1 = setup_channel(Connection),
    ChannelClose = #'channel.close'{reply_code = 200, reply_text = <<"Goodbye">>,
                                    class_id = 0, method_id = 0},
    #'channel.close_ok'{} = amqp_channel:call(Channel1, ChannelClose),
    Channel2 = setup_channel(Connection),
    teardown(Connection, Channel2).

basic_get_test(Connection) ->
    {Channel, Q} = setup_publish(Connection),
    BasicGet = #'basic.get'{queue = Q, no_ack = true},
    {Method, Content} = amqp_channel:call(Channel, BasicGet),
    #'basic.get_ok'{delivery_tag = DeliveryTag,
                    redelivered = Redelivered,
                    exchange = X,
                    routing_key = RoutingKey,
                    message_count = MessageCount} = Method,
    #content{class_id = ClassId,
             properties = Properties,
             properties_bin = PropertiesBin,
             payload_fragments_rev = PayloadFragments} = Content,
    ?assertMatch([<<"foobar">>], PayloadFragments),
    {BasicGetEmpty, Content2} = amqp_channel:call(Channel, BasicGet),
    ?assertMatch('basic.get_empty', BasicGetEmpty),
    ?assertMatch(<<>>, Content2),
    teardown(Connection, Channel).

basic_return_test(Connection) ->
    Publish = #publish{routing_key = <<"x.b.c.d">>,
                       q = <<"a.b.c">>,
                       x = <<"x">>,
                       bind_key = <<"a.b.c.*">>,
                       payload = ExpectedPayload = <<"qwerty">>,
                       mandatory = true},
    Channel = setup_channel(Connection),
    setup_publish(Channel,  Publish),
    sleep(2000),
    amqp_channel:register_return_handler(Channel, self()),
    setup_publish(Channel,  Publish),
    receive
        {BasicReturn = #'basic.return'{}, Content} ->
            #'basic.return'{reply_code = ReplyCode,
                            reply_text = ReplyText,
                            exchange = X,
                            routing_key = RoutingKey} = BasicReturn,
            ?assertMatch(<<"unroutable">>, ReplyText),
            #content{class_id = ClassId,
                     properties = Props,
                     properties_bin = PropsBin,
                     payload_fragments_rev = Payload} = Content,
            ?assertMatch([<<"qwerty">>], Payload);
        {Whats, This} ->
            %% TODO investigate where this comes from
            io:format(">>>Rec'd ~p/~p~n",[Whats, This])
    after 2000 ->
        exit(no_return_received)
    end.

sleep(Millis) ->
    receive
        nothing -> ok
    after Millis -> ok
    end.

basic_ack_test(Connection) ->
    {Channel,  Q} = setup_publish(Connection),
    BasicGet = #'basic.get'{queue = Q, no_ack = false},
    {Method, Content} = amqp_channel:call(Channel, BasicGet),
    #'basic.get_ok'{delivery_tag = DeliveryTag,
                    redelivered = Redelivered,
                    exchange = X,
                    routing_key = RoutingKey,
                    message_count = MessageCount} = Method,
    BasicAck = #'basic.ack'{delivery_tag = DeliveryTag, multiple = false},
    ok = amqp_channel:cast(Channel, BasicAck),
    teardown(Connection, Channel).

basic_consume_test(Connection) ->
    {Channel,  Q} = setup_publish(Connection),
    Parent = self(),
    [spawn(fun() -> consume_loop(Channel,Q,Parent,<<Tag:32>>) end) || Tag <- lists:seq(1,?Latch)],
    latch_loop(?Latch),
    teardown(Connection, Channel).

consume_loop(Channel,Q,Parent,Tag) ->
    {ok, Consumer} = gen_event:start_link(),
    gen_event:add_handler(Consumer, amqp_consumer , [] ),
    BasicConsume = #'basic.consume'{queue = Q,
                                    consumer_tag = Tag,
                                    no_local = false, no_ack = true, exclusive = false, nowait = false},
    #'basic.consume_ok'{consumer_tag = ConsumerTag} = amqp_channel:call(Channel,BasicConsume, Consumer),

    receive
    after 100 ->
        BasicCancel = #'basic.cancel'{consumer_tag = ConsumerTag, nowait = false},
        #'basic.cancel_ok'{consumer_tag = ConsumerTag} = amqp_channel:call(Channel,BasicCancel),
        gen_event:stop(Consumer)
    end,
	Parent ! finished.

basic_recover_test(Connection) ->
    {Channel,  Q} = setup_publish(Connection),
    BasicConsume = #'basic.consume'{queue = Q,
                                    no_local = false, no_ack = false, exclusive = false, nowait = false},
    #'basic.consume_ok'{consumer_tag = ConsumerTag} = amqp_channel:call(Channel,BasicConsume, self()),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag}, Content} ->
            %% no_ack set to false, but don't send ack
            io:format("got msg ~p~n",[Content]),
            ok
    after 2000 ->
        exit(did_not_receive_message)
    end,
    BasicRecover = #'basic.recover'{requeue = true},
    amqp_channel:cast(Channel,BasicRecover),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag2}, Content2} ->
            BasicAck = #'basic.ack'{delivery_tag = DeliveryTag2, multiple = false},
            ok = amqp_channel:cast(Channel, BasicAck)
    after 2000 ->
        exit(did_not_receive_message)
    end,
    teardown(Connection, Channel).

basic_qos_test(Connection) ->
    Channel = setup_channel(Connection),
    BasicQos = #'basic.qos'{prefetch_size = 8,
                            prefetch_count = 1,
                            global = true},
    #'basic.qos_ok'{} = amqp_channel:call(Channel, BasicQos),
    teardown(Connection, Channel).

basic_reject_test(Connection) ->
    {Channel,  Q} = setup_publish(Connection),
    BasicConsume = #'basic.consume'{queue = Q,
                                    no_local = false, no_ack = true, exclusive = false, nowait = false},
    #'basic.consume_ok'{consumer_tag = ConsumerTag} = amqp_channel:call(Channel,BasicConsume, self()),
    receive
        {#'basic.deliver'{delivery_tag = DeliveryTag}, Content} ->
            BasicReject = #'basic.reject'{delivery_tag = DeliveryTag,
                                          requeue = false},
            amqp_channel:cast(Channel, BasicReject),
            BasicCancel = #'basic.cancel'{consumer_tag = ConsumerTag, nowait = false},
            #'basic.cancel_ok'{consumer_tag = ConsumerTag} = amqp_channel:call(Channel,BasicCancel)
    after 2000 ->
        exit(did_not_receive_message)
    end,
    receive
        Msg ->
            exit(should_not_receive_any_more_messages, Msg)
    after 2000 ->
        ok
    end.

setup_publish(Connection) ->
    Publish = #publish{routing_key = <<"a.b.c.d">>,
                       q = <<"a.b.c">>,
                       x = <<"x">>,
                       bind_key = <<"a.b.c.*">>,
                       payload = <<"foobar">>
                       },
    Channel = setup_channel(Connection),
    setup_publish(Channel,  Publish).

setup_publish(Channel, #publish{routing_key = RoutingKey,
                                        q = Q, x = X,
                                        bind_key = BindKey, payload = Payload,
                                        mandatory = Mandatory,
                                        immediate = Immediate}) ->
    ok = setup_exchange(Channel,  Q, X, BindKey),
    BasicPublish = #'basic.publish'{exchange = X,
                                    routing_key = RoutingKey,
                                    mandatory = Mandatory,
                                    immediate = Immediate},
    Content = #content{class_id = 60, %% TODO HARDCODED VALUE
         properties = amqp_util:basic_properties(), %% either 'none', or a decoded record/tuple
         properties_bin = 'none', %% either 'none', or an encoded properties amqp_util:binary
         %% Note: at most one of properties and properties_bin can be 'none' at once.
         payload_fragments_rev = [Payload] %% list of binaries, in reverse order (!)
        },
    amqp_channel:cast(Channel, BasicPublish, Content),
    {Channel,Q}.

teardown({ConnectionPid, Mode}, Channel) ->
    ?assertMatch(true, is_process_alive(Channel)),
    ?assertMatch(true, is_process_alive(ConnectionPid)),
    ChannelClose = #'channel.close'{reply_code = 200, reply_text = <<"Goodbye">>,
                                          class_id = 0, method_id = 0},
    #'channel.close_ok'{} = amqp_channel:call(Channel, ChannelClose),
    ConnectionClose = #'connection.close'{reply_code = 200, reply_text = <<"Goodbye">>,
                                          class_id = 0, method_id = 0},
    #'connection.close_ok'{} = amqp_connection:close({ConnectionPid, Mode}, ConnectionClose),

    ?assertMatch(false, is_process_alive(Channel)),
    ?assertMatch(false, is_process_alive(ConnectionPid)).

setup_exchange(Channel,  Q, X, BindKey) ->
    QueueDeclare = #'queue.declare'{queue = Q,
                                    passive = false, durable = false,
                                    exclusive = false, auto_delete = false,
                                    nowait = false, arguments = []},
    #'queue.declare_ok'{queue = Q1,
                        message_count = MessageCount,
                        consumer_count = ConsumerCount}
                        = amqp_channel:call(Channel, QueueDeclare),
    ExchangeDeclare = #'exchange.declare'{exchange = X, type = <<"topic">>,
                                          passive = false, durable = false, auto_delete = false, internal = false,
                                          nowait = false, arguments = []},
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, ExchangeDeclare),
    QueueBind = #'queue.bind'{queue = Q, exchange = X,
                              routing_key = BindKey, nowait = false, arguments = []},
    #'queue.bind_ok'{} = amqp_channel:call(Channel, QueueBind),
    ok.

setup_channel(Connection) ->
    amqp_connection:open_channel(Connection).

latch_loop(0) -> ok;
latch_loop(Latch) ->
    receive
        finished ->
            latch_loop(Latch - 1)
    after ?Latch * 200 ->
        exit(waited_too_long)
    end.

