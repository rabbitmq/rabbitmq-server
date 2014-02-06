# RabbitMQ Event Exchange

This plugin exposes the internal event mechanism to AMQP. It's useful
if you want to hear when queues, exchanges, bindings, users,
connections and so on are created and deleted. It filters out stats
events, you are almost certainly going to get better results using
the management plugin for stats.

It creates a topic exchange called 'amq.rabbitmq.event' in the default
virtual host. All events are published to this exchange with routing
keys like 'exchange.created', 'binding.deleted' etc, so you can
subscribe to only the events you're interested in.

The exchange behaves similarly to 'amq.rabbitmq.log': everything gets
published there; if you don't trust a user with the information that
gets published, don't allow them access.

The plugin requires no configuration, just activate it:

    rabbitmq-plugins enable rabbitmq_event_exchange

## Building

Build it like any other plugin. See
http://www.rabbitmq.com/plugin-development.html

tl;dr:

    $ hg clone http://hg.rabbitmq.com/rabbitmq-public-umbrella
    $ cd rabbitmq-public-umbrella
    $ make co
    $ git clone https://github.com/simonmacmullen/rabbitmq-event-exchange.git
    $ cd rabbitmq-event-exchange
    $ make -j

## Event format

Each event has various properties associated with it. These are
translated into AMQPish terms and inserted in the message headers. The
message body is always blank.

## Example

There is a usage example using the Java client in `examples/java`.

## Removing

If you want to remove the exchange which this plugin creates, first
disable the plugin and restart the broker. Then invoke something like:

    rabbitmqctl eval 'rabbit_exchange:delete(rabbit_misc:r(<<"/">>, exchange, <<"amq.rabbitmq.event">>), false).'
