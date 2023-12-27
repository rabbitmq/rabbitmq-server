# RabbitMQ Recent History Cache

Keeps track of the last 20 messages that passed through the exchange. Every time a queue is bound to the exchange it delivers that last 20 messages to them. This is useful for implementing a very simple __Chat History__ where clients that join the conversation can get the latest messages.

Exchange yype: `x-recent-history`.

## Installation ##

This plugin ships with RabbitMQ. 

Like all other plugins, it must be enabled before it can be used:

```bash
[sudo] rabbitmq-plugins enable rabbitmq_recent_history_exchange
```

## Usage ##

### Creating an exchange

To create a _recent history exchange_, just declare an exchange providing the type `"x-recent-history"`.

```java
channel.exchangeDeclare("logs", "x-recent-history");
```

### Providing a custom history length

Typically this exchange will store the latest 20 messages sent over
the exchange. If you want to set a different cache length, then you
can pass a `"x-recent-history-length"` argument to `exchange.declare`.
The argument must be an integer greater or equal to zero.

For example in Java:

```java
Map<String, Object> args = new HashMap<String, Object>();
args.put("x-recent-history-length", 60);
channel.exchangeDeclare("rh", "x-recent-history", false, false, args);
```

### Preventing some messages from being stored

In case you would like to not store certain messages, just
add the header `"x-recent-history-no-store"` with the value `true` to
the message.

## Disabling the Plugin

A future version of RabbitMQ will allow users to disable plugins. When
you disable this plugin, it will delete all the cached messages.

## License

See LICENSE.
