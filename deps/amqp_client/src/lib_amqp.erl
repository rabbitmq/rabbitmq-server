-module(lib_amqp).

-include_lib("rabbitmq_server/include/rabbit.hrl").
-include_lib("rabbitmq_server/include/rabbit_framing.hrl").
-include("amqp_client.hrl").

-compile(export_all).

start_connection() ->
    amqp_connection:start("guest", "guest").

start_connection(Host) ->
    amqp_connection:start("guest", "guest", Host).

start_channel(Connection) ->
    amqp_connection:open_channel(Connection).

declare_exchange(Channel, X) ->
    declare_exchange(Channel, X, <<"direct">>).

declare_exchange(Channel, X, Type) ->
    ExchangeDeclare = #'exchange.declare'{exchange = X,
                                          type = Type,
                                          passive = false, durable = false,
                                          auto_delete = false, internal = false,
                                          nowait = false, arguments = []},
    amqp_channel:call(Channel, ExchangeDeclare).

delete_exchange(Channel, X) ->
    ExchangeDelete = #'exchange.delete'{exchange = X,
                                        if_unused = false, nowait = false},
    #'exchange.delete_ok'{} = amqp_channel:call(Channel, ExchangeDelete).

publish(Channel, X, RoutingKey, Payload) ->
    publish(Channel, X, RoutingKey, Payload, false).

publish(Channel, X, RoutingKey, Payload, Mandatory) ->
    publish_internal(fun amqp_channel:call/3,
                     Channel, X, RoutingKey, Payload, Mandatory).

async_publish(Channel, X, RoutingKey, Payload) ->
    async_publish(Channel, X, RoutingKey, Payload, false).

async_publish(Channel, X, RoutingKey, Payload, Mandatory) ->
    publish_internal(fun amqp_channel:cast/3,
                     Channel, X, RoutingKey, Payload, Mandatory).

publish_internal(Fun, Channel, X, RoutingKey, Payload, Mandatory) ->
    BasicPublish = #'basic.publish'{exchange = X,
                                    routing_key = RoutingKey,
                                    mandatory = Mandatory, immediate = false},
    {ClassId, _MethodId} = rabbit_framing:method_id('basic.publish'),
    Content = #content{class_id = ClassId,
                   properties = amqp_util:basic_properties(),
                   properties_bin = none,
                   payload_fragments_rev = [Payload]},
    Fun(Channel, BasicPublish, Content).

close_channel(Channel) ->
    ChannelClose = #'channel.close'{reply_code = 200, reply_text = <<"Goodbye">>,
                                    class_id = 0, method_id = 0},
    #'channel.close_ok'{} = amqp_channel:call(Channel, ChannelClose),
    ok.

close_connection(Connection) ->
    ConnectionClose = #'connection.close'{reply_code = 200, reply_text = <<"Goodbye">>,
                                              class_id = 0, method_id = 0},
    #'connection.close_ok'{} = amqp_connection:close(Connection, ConnectionClose),
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
                                    no_local = false, no_ack = NoAck,
                                    exclusive = false, nowait = false},
    #'basic.consume_ok'{consumer_tag = ConsumerTag} = amqp_channel:call(Channel,BasicConsume, Consumer),
    ConsumerTag.

unsubscribe(Channel, Tag) ->
    BasicCancel = #'basic.cancel'{consumer_tag = Tag, nowait = false},
    #'basic.cancel_ok'{} = amqp_channel:call(Channel,BasicCancel),
    ok.

declare_queue(Channel) ->
    declare_queue(Channel, <<>>).

declare_queue(Channel, Q) ->
    QueueDeclare = #'queue.declare'{queue = Q,
                                    passive = false, durable = false,
                                    exclusive = false, auto_delete = false,
                                    nowait = false, arguments = []},
    #'queue.declare_ok'{queue = Q1}
                        = amqp_channel:call(Channel, QueueDeclare),
    Q1.

delete_queue(Channel, Q) ->
    QueueDelete = #'queue.delete'{queue = Q,
                                  if_unused = false,
                                  if_empty = false,
                                  nowait = false},
    #'queue.delete_ok'{} = amqp_channel:call(Channel, QueueDelete).

bind_queue(Channel, X, Q, Binding) ->
    QueueBind = #'queue.bind'{queue = Q, exchange = X,
                              routing_key = Binding, nowait = false, arguments = []},
    #'queue.bind_ok'{} = amqp_channel:call(Channel, QueueBind).

unbind_queue(Channel, X, Q, Binding) ->
    Unbind = #'queue.unbind'{queue = Q, exchange = X,
                             routing_key = Binding, arguments = []},
    #'queue.unbind_ok'{} = amqp_channel:call(Channel, Unbind).
