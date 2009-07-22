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

-module(lib_amqp).

-include_lib("rabbit.hrl").
-include_lib("rabbit_framing.hrl").
-include("amqp_client.hrl").

-compile(export_all).

start_connection() ->
    amqp_connection:start_direct("guest", "guest").

start_connection(Host) ->
    start_connection(Host, ?PROTOCOL_PORT).

start_connection(Host, Port) ->
    amqp_connection:start_network("guest", "guest", Host, Port).

start_channel(Connection) ->
    amqp_connection:open_channel(Connection).

declare_exchange(Channel, X) ->
    declare_exchange(Channel, X, <<"direct">>).

declare_exchange(Channel, X, Type) ->
    ExchangeDeclare = #'exchange.declare'{exchange = X,
                                          type = Type},
    amqp_channel:call(Channel, ExchangeDeclare).

delete_exchange(Channel, X) ->
    ExchangeDelete = #'exchange.delete'{exchange = X},
    #'exchange.delete_ok'{} = amqp_channel:call(Channel, ExchangeDelete).

%%---------------------------------------------------------------------------
%% TODO This whole section of optional properties and mandatory flags
%% may have to be re-thought
publish(Channel, X, RoutingKey, Payload) ->
    publish(Channel, X, RoutingKey, Payload, false).

publish(Channel, X, RoutingKey, Payload, Mandatory)
        when is_boolean(Mandatory)->
    publish(Channel, X, RoutingKey, Payload, Mandatory,
            amqp_util:basic_properties());

publish(Channel, X, RoutingKey, Payload, Properties) ->
    publish(Channel, X, RoutingKey, Payload, false, Properties).

publish(Channel, X, RoutingKey, Payload, Mandatory, Properties) ->
    publish_internal(fun amqp_channel:call/3,
                     Channel, X, RoutingKey, Payload, Mandatory, Properties).

async_publish(Channel, X, RoutingKey, Payload) ->
    async_publish(Channel, X, RoutingKey, Payload, false).

async_publish(Channel, X, RoutingKey, Payload, Mandatory) ->
    publish_internal(fun amqp_channel:cast/3, Channel, X, RoutingKey,
                      Payload, Mandatory, amqp_util:basic_properties()).

publish_internal(Fun, Channel, X, RoutingKey,
                 Payload, Mandatory, Properties) ->
    BasicPublish = #'basic.publish'{exchange = X,
                                    routing_key = RoutingKey,
                                    mandatory = Mandatory},
    {ClassId, _MethodId} = rabbit_framing:method_id('basic.publish'),
    Content = #content{class_id = ClassId,
                       properties = Properties,
                       properties_bin = none,
                       payload_fragments_rev = [Payload]},
    Fun(Channel, BasicPublish, Content).

%%---------------------------------------------------------------------------

close_channel(Channel) ->
    ChannelClose = #'channel.close'{reply_code = 200,
                                    reply_text = <<"Goodbye">>,
                                    class_id = 0,
                                    method_id = 0},
    #'channel.close_ok'{} = amqp_channel:call(Channel, ChannelClose),
    ok.

close_connection(Connection) ->
    ConnectionClose = #'connection.close'{reply_code = 200,
                                          reply_text = <<"Goodbye">>,
                                          class_id = 0,
                                          method_id = 0},
    #'connection.close_ok'{} = amqp_connection:close(Connection,
                                                     ConnectionClose),
    ok.

teardown(Connection, Channel) ->
    close_channel(Channel),
    close_connection(Connection).


get(Channel, Q) -> get(Channel, Q, true).

get(Channel, Q, NoAck) ->
    BasicGet = #'basic.get'{queue = Q, no_ack = NoAck},
    {Method, Content} = amqp_channel:call(Channel, BasicGet),
    case Method of
        'basic.get_empty' -> 'basic.get_empty';
        _ ->
            #'basic.get_ok'{delivery_tag = DeliveryTag} = Method,
            case NoAck of
                true -> Content;
                false -> {DeliveryTag, Content}
            end
    end.

ack(Channel, DeliveryTag) ->
    BasicAck = #'basic.ack'{delivery_tag = DeliveryTag, multiple = false},
    ok = amqp_channel:cast(Channel, BasicAck).

subscribe(Channel, Q, Consumer) ->
    subscribe(Channel, Q, Consumer, <<>>, true).

subscribe(Channel, Q, Consumer, NoAck) when is_boolean(NoAck) ->
    subscribe(Channel, Q, Consumer, <<>>, NoAck);

subscribe(Channel, Q, Consumer, Tag) ->
    subscribe(Channel, Q, Consumer, Tag, true).

subscribe(Channel, Q, Consumer, Tag, NoAck) ->
    BasicConsume = #'basic.consume'{queue = Q,
                                    consumer_tag = Tag,
                                    no_ack = NoAck},
    #'basic.consume_ok'{consumer_tag = ConsumerTag} =
        amqp_channel:subscribe(Channel, BasicConsume, Consumer),
    ConsumerTag.

unsubscribe(Channel, Tag) ->
    BasicCancel = #'basic.cancel'{consumer_tag = Tag},
    #'basic.cancel_ok'{} = amqp_channel:call(Channel, BasicCancel),
    ok.

%%---------------------------------------------------------------------------
%% Convenience functions for manipulating queues

%% TODO This whole part of the API needs to be refactored to reflect current
%% usage patterns in a sensible way using the defaults that are in the spec
%% file
declare_queue(Channel) ->
    declare_queue(Channel, <<>>).

declare_queue(Channel, QueueDeclare = #'queue.declare'{}) ->
    #'queue.declare_ok'{queue = QueueName}
        = amqp_channel:call(Channel, QueueDeclare),
    QueueName;

declare_queue(Channel, Q) ->
    %% TODO Specifying these defaults is unecessary - this is already taken
    %% care of in the spec file
    QueueDeclare = #'queue.declare'{queue = Q},
    declare_queue(Channel, QueueDeclare).

%% Creates a queue that is exclusive and auto-delete
declare_private_queue(Channel) ->
    declare_queue(Channel, #'queue.declare'{exclusive = true,
                                            auto_delete = true}).

declare_private_queue(Channel, QueueName) ->
    declare_queue(Channel, #'queue.declare'{queue = QueueName,
                                            exclusive = true,
                                            auto_delete = true}).

%%---------------------------------------------------------------------------
%% Basic.Qos

%% Sets the prefetch count for messages delivered on this channel
set_prefetch_count(Channel, Prefetch) ->
    amqp_channel:call(Channel, #'basic.qos'{prefetch_count = Prefetch}).

%%---------------------------------------------------------------------------

delete_queue(Channel, Q) ->
    QueueDelete = #'queue.delete'{queue = Q},
    #'queue.delete_ok'{} = amqp_channel:call(Channel, QueueDelete).

bind_queue(Channel, X, Q, Binding) ->
    QueueBind = #'queue.bind'{queue = Q, exchange = X,
                              routing_key = Binding},
    #'queue.bind_ok'{} = amqp_channel:call(Channel, QueueBind).

unbind_queue(Channel, X, Q, Binding) ->
    Unbind = #'queue.unbind'{queue = Q, exchange = X,
                             routing_key = Binding, arguments = []},
    #'queue.unbind_ok'{} = amqp_channel:call(Channel, Unbind).

