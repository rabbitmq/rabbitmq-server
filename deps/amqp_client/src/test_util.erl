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

%%%%
%
% This is an example of how the client interaction should work
%
%   {ok, Connection} = amqp_connection:start(User, Password, Host),
%   Channel = amqp_connection:open_channel(Connection),
%   AccessRequest = #'access.request'{ %% set the appropriate fields },
%   #'access.request_ok'{ticket = Ticket} = amqp_channel:call(Channel, AccessRequest)
%   %%...do something useful
%   ChannelClose = #'channel.close'{ %% set the appropriate fields },
%   amqp_channel:call(Channel, ChannelClose),
%   ConnectionClose = #'connection.close'{ %% set the appropriate fields },
%   amqp_connection:close(Connection, ConnectionClose).
%

lifecycle_test(Connection) ->
    Realm = <<"/data">>,
    Q = <<"a.b.c">>,
    X = <<"x">>,
    BindKey = <<"a.b.c.*">>,
    RoutingKey = <<"a.b.c.d">>,
    Payload = <<"foobar">>,
    {Channel, Ticket} = setup_channel(Connection, Realm),
    QueueDeclare = #'queue.declare'{ticket = Ticket, queue = Q,
                                    passive = false, durable = false,
                                    exclusive = false, auto_delete = false,
                                    nowait = false, arguments = []},
    #'queue.declare_ok'{queue = Q1,
                        message_count = MessageCount,
                        consumer_count = ConsumerCount}
                       = amqp_channel:call(Channel,QueueDeclare),
    ?assertMatch(Q, Q1),
    ExchangeDeclare = #'exchange.declare'{ticket = Ticket, exchange = X, type = <<"topic">>,
                                          passive = false, durable = false, auto_delete = false, internal = false,
                                          nowait = false, arguments = []},
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, ExchangeDeclare),
    QueueBind = #'queue.bind'{ticket = Ticket, queue = Q, exchange = X,
                              routing_key = BindKey, nowait = false, arguments = []},
    #'queue.bind_ok'{} = amqp_channel:call(Channel, QueueBind),
    QueueDelete = #'queue.delete'{ticket = Ticket, queue = Q,
                                  if_unused = true, if_empty = true, nowait = false},
    #'queue.delete_ok'{message_count = MessageCount2} = amqp_channel:call(Channel, QueueDelete),
    ?assertMatch(MessageCount, MessageCount2),
    ExchangeDelete = #'exchange.delete'{ticket = Ticket, exchange = X,
                                        if_unused = false, nowait = false},
    #'exchange.delete_ok'{} = amqp_channel:call(Channel, ExchangeDelete),
    teardown(Connection, Channel).

basic_get_test(Connection) ->
    {Channel, Ticket, Q} = setup_publish(Connection),
    BasicGet = #'basic.get'{ticket = Ticket, queue = Q, no_ack = true},
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
    {Method2, Content2} = amqp_channel:call(Channel, BasicGet),
    ?assertMatch(<<>>, Content2),
    teardown(Connection, Channel).

basic_ack_test(Connection) ->
    {Channel, Ticket, Q} = setup_publish(Connection),
    BasicGet = #'basic.get'{ticket = Ticket, queue = Q, no_ack = false},
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
    {Channel, Ticket, Q} = setup_publish(Connection),
    {ok, Consumer} = gen_event:start_link(),
    gen_event:add_handler(Consumer, amqp_consumer , [] ),
    Tag = <<"">>,
    BasicConsume = #'basic.consume'{ticket = Ticket, queue = Q,
                                    consumer_tag = Tag,
                                    no_local = false, no_ack = true, exclusive = false, nowait = false},
    #'basic.consume_ok'{consumer_tag = ConsumerTag} = amqp_channel:call(Channel,BasicConsume, Consumer),
    receive
    after 2000 ->
        BasicCancel = #'basic.cancel'{consumer_tag = ConsumerTag, nowait = false},
        #'basic.cancel_ok'{consumer_tag = ConsumerTag} = amqp_channel:call(Channel,BasicCancel),
        gen_event:stop(Consumer)
    end,
    teardown(Connection, Channel).

setup_publish(Connection) ->
    Realm = <<"/data">>,
    Q = <<"a.b.c">>,
    X = <<"x">>,
    BindKey = <<"a.b.c.*">>,
    RoutingKey = <<"a.b.c.d">>,
    Payload = <<"foobar">>,
    {Channel, Ticket} = setup_channel(Connection, Realm),
    ok = setup_exchange(Channel, Ticket, Q, X, BindKey),
    BasicPublish = #'basic.publish'{ticket = Ticket, exchange = X,
                                    routing_key = RoutingKey,
                                    mandatory = false, immediate = false},
    Content = #content{class_id = 60, %% TODO HARDCODED VALUE
         properties = amqp_util:basic_properties(), %% either 'none', or a decoded record/tuple
         properties_bin = 'none', %% either 'none', or an encoded properties amqp_util:binary
         %% Note: at most one of properties and properties_bin can be 'none' at once.
         payload_fragments_rev = [Payload] %% list of binaries, in reverse order (!)
        },
    amqp_channel:cast(Channel, BasicPublish, Content),
    {Channel,Ticket,Q}.

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

setup_exchange(Channel, Ticket, Q, X, BindKey) ->
    QueueDeclare = #'queue.declare'{ticket = Ticket, queue = Q,
                                    passive = false, durable = false,
                                    exclusive = false, auto_delete = false,
                                    nowait = false, arguments = []},
    #'queue.declare_ok'{queue = Q1,
                        message_count = MessageCount,
                        consumer_count = ConsumerCount}
                        = amqp_channel:call(Channel, QueueDeclare),
    ExchangeDeclare = #'exchange.declare'{ticket = Ticket, exchange = X, type = <<"topic">>,
                                          passive = false, durable = false, auto_delete = false, internal = false,
                                          nowait = false, arguments = []},
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, ExchangeDeclare),
    QueueBind = #'queue.bind'{ticket = Ticket, queue = Q, exchange = X,
                              routing_key = BindKey, nowait = false, arguments = []},
    #'queue.bind_ok'{} = amqp_channel:call(Channel, QueueBind),
    ok.

setup_channel(Connection, Realm) ->
    Channel = amqp_connection:open_channel(Connection),
    Access = #'access.request'{realm = Realm,
                               exclusive = false,
                               passive = true,
                               active = true,
                               write = true,
                               read = true},
    #'access.request_ok'{ticket = Ticket} = amqp_channel:call(Channel, Access),
    {Channel, Ticket}.
