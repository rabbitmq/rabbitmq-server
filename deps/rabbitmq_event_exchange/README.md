# RabbitMQ Event Exchange

## Overview

This plugin exposes the internal RabbitMQ event mechanism as messages that clients
can consume. It's useful
if you want to keep track of certain events, e.g. when queues, exchanges, bindings, users,
connections, channels are created and deleted. This plugin filters out stats
events, so you are almost certainly going to get better results using
the management plugin for stats.

## How it Works

It declares a topic exchange called 'amq.rabbitmq.event' in the default
virtual host. All events are published to this exchange with routing
keys like 'exchange.created', 'binding.deleted' etc, so you can
subscribe to only the events you're interested in.

The exchange behaves similarly to 'amq.rabbitmq.log': everything gets
published there; if you don't trust a user with the information that
gets published, don't allow them access.


## Installation

### RabbitMQ 3.6.0 or later

As of RabbitMQ `3.6.0` this plugin is included into the RabbitMQ distribution.

Enable it with the following command:

```bash
rabbitmq-plugins enable rabbitmq_event_exchange
```

You'd probably want to also enable the Consistent Hash Exchange
plugin, too.

### With Earlier Versions

Install the corresponding .ez files from our
[Community Plugins archive](http://www.rabbitmq.com/community-plugins/).

Then run the following command:

```bash
rabbitmq-plugins enable rabbitmq_event_exchange
```

## Configuration

 * `rabbitmq_event_exchange.vhost`: what vhost should the `amq.rabbitmq.event` exchange be declared in. Default: `rabbit.default_vhost` (`<<"/">>`).


## Building From Source

Building is no different from [building other RabbitMQ plugins](http://www.rabbitmq.com/plugin-development.html).

TL;DR:

    git clone https://github.com/rabbitmq/rabbitmq-event-exchange.git
    cd rabbitmq-event-exchange
    make -j dist
    ls plugins/*

## Event format

Each event has various properties associated with it. These are
translated into AMQP 0-9-1 data encoding and inserted in the message headers. The
**message body is always blank**.

## Events

So far RabbitMQ and related plugins emit events with the following routing keys:

### RabbitMQ Broker

Queue, Exchange and Binding events:

 * `queue.deleted`
 * `queue.created`
 * `exchange.created`
 * `exchange.deleted`
 * `binding.created`
 * `binding.deleted`

Connection and Channel events:

 * `connection.created`
 * `connection.closed`
 * `channel.created`
 * `channel.closed`

Consumer events:

 * `consumer.created`
 * `consumer.deleted`

Policy and Parameter events:

 * `policy.set`
 * `policy.cleared`
 * `parameter.set`
 * `parameter.cleared`

Virtual host events:

 * `vhost.created`
 * `vhost.deleted`
 * `vhost.limits.set`
 * `vhost.limits.cleared`

User related events:

 * `user.authentication.success`
 * `user.authentication.failure`
 * `user.created`
 * `user.deleted`
 * `user.password.changed`
 * `user.password.cleared`
 * `user.tags.set`

Permission events:

 * `permission.created`
 * `permission.deleted`
 * `topic.permission.created`
 * `topic.permission.deleted`

### Shovel Plugin

Worker events:

 * `shovel.worker.status`
 * `shovel.worker.removed`

### Federation Plugin

Link events:

 * `federation.link.status`
 * `federation.link.removed`

## Example

There is a usage example using the Java client in `examples/java`.

## Uninstalling

If you want to remove the exchange which this plugin creates, first
disable the plugin and restart the broker. Then you can delete the exchange,
e.g. with :

    rabbitmqctl eval 'rabbit_exchange:delete(rabbit_misc:r(<<"/">>, exchange, <<"amq.rabbitmq.event">>), false).'

## License

Released under the Mozilla Public License 1.1,
the same as RabbitMQ.
