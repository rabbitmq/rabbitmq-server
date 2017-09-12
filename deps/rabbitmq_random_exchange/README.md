# RabbitMQ Random Exchange Type

This exchange plugin is a way to distribute messages among N queues to avoid the [Single Giant Queue anti-pattern](http://www.rabbitmq.com/queues.html).
It was originally developed by [Jon Brisbin](https://github.com/jbrisbin).

[rabbitmq-sharding/](https://github.com/rabbitmq/rabbitmq-sharding/) and [rabbitmq-consistent-hash-exchange](https://github.com/rabbitmq/rabbitmq-consistent-hash-exchange)
are two other plugins that approach the same problem in a different way or with
a different set of features.

Unlike the original version by Jon, this iteration of the plugin **will completely ignore routing keys**
(much like the fanout exchange) and pick a binding/destination at random. If the original
behaviour (using routing key the same way the direct exchange does but picking
target queue/exchange from N bindings is randomized) is desired, [exchange-to-exchange bindings](http://www.rabbitmq.com/e2e.html)
can be used to combine a direct exchange with this one.


## Installation

Install the corresponding .ez files from our
[GitHub releases](https://github.com/rabbitmq/rabbitmq-random-exchange/releases) or [Community Plugins page](http://www.rabbitmq.com/community-plugins.html).

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
