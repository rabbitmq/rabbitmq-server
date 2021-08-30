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
[GitHub releases](https://github.com/rabbitmq/rabbitmq-random-exchange/releases) or [Community Plugins page](https://www.rabbitmq.com/community-plugins.html).

Then run the following command:

```bash
rabbitmq-plugins enable rabbitmq_random_exchange
```

## Building from Source

Please see [RabbitMQ Plugin Development guide](https://www.rabbitmq.com/plugin-development.html).

To build the plugin:

    git clone git://github.com/rabbitmq/rabbitmq-random-exchange.git
    cd rabbitmq-random-exchange
    make

Then copy all the `*.ez` files inside the `plugins` folder to the [RabbitMQ plugins directory](https://www.rabbitmq.com/relocate.html)
and enable the plugin:

    [sudo] rabbitmq-plugins enable rabbitmq_random_exchange


## Usage

To create a _random_, just declare an exchange providing the type `"x-random"`.

```java
channel.exchangeDeclare("logs", "x-random");
```

and bind several queues to it. Routing keys will be ignored by modern releases
of this exchange plugin: the binding and its target queue (exchange) are picked
entirely randomly.

### Extended Example

This example uses [Bunny](http://rubybunny.info) and demonstrates
how the plugin is mean to be used with 3 queues:

``` ruby
#!/usr/bin/env ruby

require 'bundler'
Bundler.setup(:default)
require 'bunny'

c  = Bunny.new; c.start
ch = c.create_channel

rx  = ch.exchange("x.rnd", durable: true, type: "x-random")

q1 = ch.queue("r1").bind(rx)
q2 = ch.queue("r2").bind(rx)
q3 = ch.queue("r3").bind(rx)

ch.confirm_select

100.times do
  rx.publish(rand.to_s, routing_key: rand.to_s)
end

ch.wait_for_confirms

# the consumer part is left out: see
# management UI

puts "Done"
```


## License

See [LICENSE](./LICENSE).
