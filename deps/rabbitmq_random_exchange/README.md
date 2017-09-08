# RabbitMQ Random Exchange Type

This exchange type is for load-balancing among consumers. It's basically 
a direct exchange, with the exception that, instead of each consumer bound 
to that exchange with the same routing key getting a copy of the message, 
the exchange type randomly selects a queue to route to.

There is no weighting or anything, so maybe load "balancing" might be a bit 
of a misnomer. It uses Erlang's crypto:rand_uniform/2 function, if you're 
interested.

## Installation

Install the corresponding .ez files from our
[Community Plugins page](http://www.rabbitmq.com/community-plugins.html).

Then run the following command:

```bash
rabbitmq-plugins enable rabbitmq_random_exchange
```

## Building from Source

Please see [RabbitMQ Plugin Development guide](http://www.rabbitmq.com/plugin-development.html).

To build the plugin:

    git clone git://github.com/rabbitmq/rabbitmq-random-exchange.git
    cd rabbitmq-random-exchange
    make

Then copy all the `*.ez` files inside the `plugins` folder to the [RabbitMQ plugins directory](http://www.rabbitmq.com/relocate.html)
and enable the plugin:

    [sudo] rabbitmq-plugins enable rabbitmq_random_exchange


## Usage

### Creating an exchange

To create a _random_, just declare an exchange providing the type `"x-random"`.

```java
channel.exchangeDeclare("logs", "x-random");
```

### Usage

To use it, declare an exchange of type "x-random".


## License

See [LICENSE](./LICENSE).
