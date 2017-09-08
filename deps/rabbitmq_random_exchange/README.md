# RabbitMQ Random Exchange Type

This exchange type is for load-balancing among consumers. It's basically 
a direct exchange, with the exception that, instead of each consumer bound 
to that exchange with the same routing key getting a copy of the message, 
the exchange type randomly selects a queue to route to.

There is no weighting or anything, so maybe load "balancing" might be a bit 
of a misnomer. It uses Erlang's crypto:rand_uniform/2 function, if you're 
interested.

## Installation ##

Install the corresponding .ez files from our
[Community Plugins page](http://www.rabbitmq.com/community-plugins.html).

Then run the following command:

```bash
rabbitmq-plugins enable rabbitmq_random_exchange
```

## Building from Source ##

Install and setup the RabbitMQ Public Umbrella as explained here: [http://www.rabbitmq.com/plugin-development.html#getting-started](http://www.rabbitmq.com/plugin-development.html#getting-started).

Then `cd` into the umbrella folder and type:

    git clone git://github.com//rabbitmq-random-exchange.git
    cd rabbitmq-random-exchange
    make

Finally copy all the `*.ez` files inside the `dist` folder to the `$RABBITMQ_HOME/plugins` folder. Don't copy the file `rabbit_common-x.y.z` since it's not needed inside the broker installation.

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
