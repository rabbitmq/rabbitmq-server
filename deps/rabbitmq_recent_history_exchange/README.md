# RabbitMQ Recent History Cache

Keeps track of the last 20 messages that passed through the exchange. Every time a queue is bound to the exchange it delivers that last 20 messages to them. This is useful for implementing a very simple __Chat History__ where clients that join the conversation can get the latest messages.

Exchange Type: `x-recent-history`

## Installation ##

### RabbitMQ 3.6.0 or later

As of RabbitMQ `3.6.0` this plugin is included into the RabbitMQ distribution.

Enable it with the following command:

```bash
rabbitmq-plugins enable rabbitmq_recent_history_exchange
```

### With Earlier Versions

Install the corresponding .ez files from our
[Community Plugins archive](https://www.rabbitmq.com/community-plugins/)..

Then run the following command:

```bash
rabbitmq-plugins enable rabbitmq_recent_history_exchange
```

## Building from Source

Please see [RabbitMQ Plugin Development guide](https://www.rabbitmq.com/plugin-development.html).

To build the plugin:

    git clone git://github.com/rabbitmq/rabbitmq-recent-history-exchange.git
    cd rabbitmq-recent-history-exchange
    make

Then copy all the `*.ez` files inside the `plugins` folder to the [RabbitMQ plugins directory](https://www.rabbitmq.com/relocate.html)
and enable the plugin:

    [sudo] rabbitmq-plugins enable rabbitmq_recent_history_exchange

## Usage ##

### Creating an exchange  ###

To create a _recent history exchange_, just declare an exchange providing the type `"x-recent-history"`.

```java
channel.exchangeDeclare("logs", "x-recent-history");
```

### Providing a custom history length ###

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

### Preventing some messages from being stored ###

In case you would like to not store certain messages, just
add the header `"x-recent-history-no-store"` with the value `true` to
the message.

## Disabling the Plugin ##

A future version of RabbitMQ will allow users to disable plugins. When
you disable this plugin, it will delete all the cached messages.

## License

See LICENSE.
