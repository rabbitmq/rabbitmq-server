# RabbitMQ Recent History Cache

Keeps track of the last 20 messages that passed through the exchange. Every time a queue is bound to the exchange it delivers that last 20 messages to them. This is useful for implementing a very simple __Chat History__ where clients that join the conversation can get the latest messages.

Exchange Type: `x-recent-history`

## Installation ##

Install the corresponding .ez files from our
[Community Plugins page](http://www.rabbitmq.com/community-plugins.html).

Then run the following command:

```bash
rabbitmq-plugins enable rabbitmq_recent_history_exchange
```

## Building from Source ##

Install and setup the RabbitMQ Public Umbrella as explained here: [http://www.rabbitmq.com/plugin-development.html#getting-started](http://www.rabbitmq.com/plugin-development.html#getting-started).

Then `cd` into the umbrella folder and type:

    $ git clone git://github.com/videlalvaro/rabbitmq-recent-history-exchange.git
    $ cd rabbitmq-recent-history-exchange
    $ make

Finally copy all the `*.ez` files inside the `dist` folder to the `$RABBITMQ_HOME/plugins` folder. Don't copy the file `rabbit_common-x.y.z` since it's not needed inside the broker installation.

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

See LICENSE.md
