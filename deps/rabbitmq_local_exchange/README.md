# RabbitMQ Local Exchange Type

This exchange type favours local bound queues (local to the producer's connection). It's basically
a direct exchange, with the exception that, only one queue and in particular, a queue local to the
producer's connection receives the message. If there are no local queues, the exchange behaves like
the random exchange, i.e. it randomly picks the target queue.


## Installation

Install the corresponding .ez files from our
[GitHub releases](https://github.com/rabbitmq/rabbitmq-local-exchange/releases)
Then run the following command:

```bash
rabbitmq-plugins enable rabbitmq_local_exchange
```


## Building from Source

Please see [RabbitMQ Plugin Development guide](https://www.rabbitmq.com/plugin-development.html).

To build the plugin:

    git clone git://github.com/rabbitmq/rabbitmq-server
    cd deps/rabbitmq-local-exchange
    make

Then copy all the `*.ez` files inside the `plugins` folder to the [RabbitMQ plugins directory](https://www.rabbitmq.com/relocate.html)
and enable the plugin:

    [sudo] rabbitmq-plugins enable rabbitmq_local_exchange


## Usage

To create a _local_, just declare an exchange providing the type `"x-local"`.

```java
channel.exchangeDeclare("cache-updates", "x-local");
```

and bind several queues to it preferably if each queue is created on a different node.
Routing keys will be ignored.



## License

See [LICENSE](./LICENSE).
