# RabbitMQ Consistent Hash Exchange Type

## Introduction

This plugin adds a consistent-hash exchange type to RabbitMQ. This
exchange type uses consistent hashing (intro blog posts: [one](http://www.martinbroadhurst.com/Consistent-Hash-Ring.html), [two](http://michaelnielsen.org/blog/consistent-hashing/), [three](https://akshatm.svbtle.com/consistent-hash-rings-theory-and-implementation)) to distribute
messages between the bound queues. It is recommended to get a basic understanding of the
concept before evaluating this plugin and its alternatives.

[rabbitmq-sharding](https://github.com/rabbitmq/rabbitmq-sharding) is another plugin
that provides a way to partition a stream of messages among a set of consumers
while trading off total stream ordering for processing parallelism.

## Problem Definition

In various scenarios it may be desired to ensure that messages sent to an
exchange are reasonably [uniformly distributed](https://en.wikipedia.org/wiki/Uniform_distribution_(discrete)) across a number of
queues based on the routing key of the message, a [nominated
header](#routing-on-a-header), or a [message property](#routing-on-a-header).
Technically this can be accomplished using a direct or topic exchange,
binding queues to that exchange and then publishing messages to that exchange that
match the various binding keys.

However, arranging things this way can be problematic:

1. It is difficult to ensure that all queues bound to the exchange
will receive a (roughly) equal number of messages (distribution uniformity)
without baking in to the publishers quite a lot of knowledge about the number of queues and
their bindings.

2. When the number of queues changes, it is not easy to ensure that the
new topology still distributes messages between the different queues
evenly.

[Consistent Hashing](https://en.wikipedia.org/wiki/Consistent_hashing)
is a hashing technique whereby each bucket appears at multiple points
throughout the hash space, and the bucket selected is the nearest
higher (or lower, it doesn't matter, provided it's consistent) bucket
to the computed hash (and the hash space wraps around). The effect of
this is that when a new bucket is added or an existing bucket removed,
only a very few hashes change which bucket they are routed to.

## Supported RabbitMQ Versions

This plugin ships with RabbitMQ.

## Supported Erlang Versions

This plugin supports the same [Erlang versions](https://rabbitmq.com/which-erlang.html) as RabbitMQ core.

## Enabling the Plugin

This plugin ships with RabbitMQ. Like all other [RabbitMQ plugins](https://www.rabbitmq.com/plugins.html),
it has to be enabled before it can be used:

``` sh
rabbitmq-plugins enable rabbitmq_consistent_hash_exchange
```

## Provided Exchange Type

The exchange type is `"x-consistent-hash"`.

## How It Works

In the case of Consistent Hashing as an exchange type, the hash is
calculated from a message property (most commonly the routing key).

When a queue is bound to this exchange, it is assigned one or more
partitions on the consistent hashing ring depending on its binding weight
(covered below).

For every property hash (e.g. routing key), a hash position computed
and a corresponding hash ring partition is picked. That partition corresponds
to a bound queue, and the message is routed to that queue.

Assuming a reasonably even routing key distribution of inbound messages,
routed messages should be reasonably evenly distributed across all
ring partitions, and thus queues according to their binding weights.

### Binding Weights

When a queue is bound to a Consistent Hash exchange, the binding key
is a number-as-a-string which indicates the binding weight: the number
of buckets (sections of the range) that will be associated with the
target queue.

### Consistent Hashing-based Routing

The hashing distributes *routing keys* among queues, not *message payloads*
among queues; all messages with the same routing key will go the
same queue.  So, if you wish for queue A to receive twice as many
routing keys routed to it than are routed to queue B, then you bind
the queue A with a binding key of twice the number (as a string --
binding keys are always strings) of the binding key of the binding
to queue B.  Note this is only the case if your routing keys are
evenly distributed in the hash space.  If, for example, only two
distinct routing keys are used on all the messages, there's a chance
both keys will route (consistently!) to the same queue, even though
other queues have higher values in their binding key.  With a larger
set of routing keys used, the statistical distribution of routing
keys approaches the ratios of the binding keys.

Each message gets delivered to at most one queue. On average, a
message gets delivered to exactly one queue. Concurrent binding changes
and queue primary replica failures can affect this but on average.

### Node Restart Effects

Consistent hashing ring is stored in memory and will be re-populated
from exchange bindings when the node boots. Relative positioning of queues
on the ring is not guaranteed to be the same between restarts. In practice
this means that after a restart, all queues will still receive roughly
the same number of messages routed to them (assuming routing key distribution
does not change) but a given routing key now **may route to a different queue**.

In other words, this exchange type provides consistent message distribution
between queues but cannot guarantee stable routing [queue] locality for a message
with a fixed routing key.


## Usage Example

### The Topology

In the below example the queues `q0` and `q1` get bound each with the weight of 1
in the hash space to the exchange `e` which means they'll each get
roughly the same number of routing keys. The queues `q2` and `q3`
however, get 2 buckets each (their weight is 2) which means they'll each get roughly the
same number of routing keys too, but that will be approximately twice
as many as `q0` and `q1`.

Note the `routing_key`s in the bindings are numbers-as-strings. This
is because AMQP 0-9-1 specifies the `routing_key` field must be a string.

### Choosing Appropriate Weight Values

The example uses low weight values intentionally.
Higher values will reduce throughput of the exchange, primarily for
workloads that experience a high binding churn (queues are bound to
and unbound from a consistent hash exchange frequently).
Single digit weight values are recommended (and usually sufficient).

### Inspecting Message Counts

The example then publishes 100,000 messages to our
exchange with random routing keys, the queues will get their share of
messages roughly equal to the binding keys ratios. After this has
completed, message distribution between queues can be inspected using
RabbitMQ's management UI and `rabbitmqctl list_queues`.

## Routing Keys and Uniformity of Distribution

It is important to ensure that the messages being published
to the exchange have varying routing keys: if a very
small set of routing keys are being used then there's a possibility of
messages not being evenly distributed between the bound queues. With a
large number of bound queues some queues may get no messages routed to
them at all.

If pseudo-random or unique values such as client/session/request identifiers
are used for routing keys (or another property used for hashing) then
reasonably uniform distribution should be observed.

### Executable Versions

Executable versions of some of the code examples can be found under [./examples](./examples).

### Code Example in Python

This version of the example uses [Pika](https://pika.readthedocs.io/en/stable/), the most widely used Python client for RabbitMQ:

``` python
#!/usr/bin/env python

import pika
import time

conn = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
ch   = conn.channel()

ch.exchange_declare(exchange="e", exchange_type="x-consistent-hash", durable=True)

for q in ["q1", "q2", "q3", "q4"]:
    ch.queue_declare(queue=q, durable=True)
    ch.queue_purge(queue=q)

for q in ["q1", "q2"]:
    ch.queue_bind(exchange="e", queue=q, routing_key="1")

for q in ["q3", "q4"]:
    ch.queue_bind(exchange="e", queue=q, routing_key="2")

n = 100000

for rk in list(map(lambda s: str(s), range(0, n))):
    ch.basic_publish(exchange="e", routing_key=rk, body="")
print("Done publishing.")

print("Waiting for routing to finish...")
# in order to keep this example simpler and focused,
# wait for a few seconds instead of using publisher confirms and waiting for those
time.sleep(5)

print("Done.")
conn.close()
```

### Code Example in Java

Below is a version of the example that uses
the official [RabbitMQ Java client](https://www.rabbitmq.com/api-guide.html):

``` java
package com.rabbitmq.examples;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeoutException;

public class ConsistentHashExchangeExample1 {
  private static String CONSISTENT_HASH_EXCHANGE_TYPE = "x-consistent-hash";

  public static void main(String[] argv) throws IOException, TimeoutException, InterruptedException {
    ConnectionFactory cf = new ConnectionFactory();
    Connection conn = cf.newConnection();
    Channel ch = conn.createChannel();

    for (String q : Arrays.asList("q1", "q2", "q3", "q4")) {
      ch.queueDeclare(q, true, false, false, null);
      ch.queuePurge(q);
    }

    ch.exchangeDeclare("e1", CONSISTENT_HASH_EXCHANGE_TYPE, true, false, null);

    for (String q : Arrays.asList("q1", "q2")) {
      ch.queueBind(q, "e1", "1");
    }

    for (String q : Arrays.asList("q3", "q4")) {
      ch.queueBind(q, "e1", "2");
    }

    ch.confirmSelect();

    AMQP.BasicProperties.Builder bldr = new AMQP.BasicProperties.Builder();
    for (int i = 0; i < 100000; i++) {
      ch.basicPublish("e1", String.valueOf(i), bldr.build(), "".getBytes("UTF-8"));
    }

    ch.waitForConfirmsOrDie(10000);

    System.out.println("Done publishing!");
    System.out.println("Evaluating results...");
    // wait for one stats emission interval so that queue counters
    // are up-to-date in the management UI
    Thread.sleep(5);

    System.out.println("Done.");
    conn.close();
  }
}
```

### Code Example in Ruby

Below is a version that uses [Bunny](http://rubybunny.info), the most widely used
Ruby client for RabbitMQ:

``` ruby
#!/usr/bin/env ruby

require 'bunny'

conn = Bunny.new
conn.start

ch = conn.create_channel
ch.confirm_select

q1 = ch.queue("q1", durable: true)
q2 = ch.queue("q2", durable: true)
q3 = ch.queue("q3", durable: true)
q4 = ch.queue("q4", durable: true)

[q1, q2, q3, q4]. each(&:purge)

x  = ch.exchange("chx", type: "x-consistent-hash", durable: true)

[q1, q2].each { |q| q.bind(x, routing_key: "1") }
[q3, q4].each { |q| q.bind(x, routing_key: "2") }

n = 100_000
n.times do |i|
  x.publish(i.to_s, routing_key: i.to_s)
end

ch.wait_for_confirms
puts "Done publishing!"

# wait for queue stats to be emitted so that management UI numbers
# are up-to-date
sleep 5
conn.close
puts "Done"
```


### Code Example in Erlang

Below is a version of the example that uses
the [RabbitMQ Erlang client](https://www.rabbitmq.com/erlang-client-user-guide.html):

``` erlang
-include_lib("amqp_client/include/amqp_client.hrl").

test() ->
    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    Queues = [<<"q0">>, <<"q1">>, <<"q2">>, <<"q3">>],
    amqp_channel:call(Chan,
                  #'exchange.declare'{
                    exchange = <<"e">>, type = <<"x-consistent-hash">>
                  }),
    [amqp_channel:call(Chan, #'queue.declare'{queue = Q}) || Q <- Queues],
    [amqp_channel:call(Chan, #'queue.bind'{queue = Q,
                                           exchange = <<"e">>,
                                           routing_key = <<"1">>})
        || Q <- [<<"q0">>, <<"q1">>]],
    [amqp_channel:call(Chan, #'queue.bind' {queue = Q,
                                            exchange = <<"e">>,
                                            routing_key = <<"2">>})
        || Q <- [<<"q2">>, <<"q3">>]],
    RK = list_to_binary(integer_to_list(random:uniform(1000000))),
    Msg = #amqp_msg{props = #'P_basic'{}, payload = <<>>},
    [amqp_channel:call(Chan,
                   #'basic.publish'{
                     exchange = <<"e">>,
                     routing_key = RK
                   }, Msg) || _ <- lists:seq(1, 100000)],
amqp_connection:close(Conn),
ok.
```

## Configuration

### Routing on a Header

Under most circumstances the routing key is a good choice for something to
hash. However, in some cases it is necessary to use the routing key for some other
purpose (for example with more complex routing involving exchange to
exchange bindings). In this case it is possible to configure the consistent hash
exchange to route based on a named header instead. To do this, declare the
exchange with a string argument called "hash-header" naming the header to
be used.

When a `"hash-header"` is specified, the chosen header **must be provided**.
If published messages do not contain the header, they will all get
routed to the same **arbitrarily chosen** queue.

#### Code Example in Python

``` python
#!/usr/bin/env python

import pika
import time

conn = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
ch   = conn.channel()

args = {u'hash-header': u'hash-on'}
ch.exchange_declare(exchange='e2',
                    exchange_type='x-consistent-hash',
                    arguments=args,
                    durable=True)

for q in ['q1', 'q2', 'q3', 'q4']:
    ch.queue_declare(queue=q, durable=True)
    ch.queue_purge(queue=q)

for q in ['q1', 'q2']:
    ch.queue_bind(exchange='e2', queue=q, routing_key='1')

for q in ['q3', 'q4']:
    ch.queue_bind(exchange='e2', queue=q, routing_key='2')

n = 100000

for rk in list(map(lambda s: str(s), range(0, n))):
    hdrs = {u'hash-on': rk}
    ch.basic_publish(exchange='e2',
                     routing_key='',
                     body='',
                     properties=pika.BasicProperties(content_type='text/plain',
                                                     delivery_mode=2,
                                                     headers=hdrs))
print('Done publishing.')

print('Waiting for routing to finish...')
# in order to keep this example simpler and focused,
# wait for a few seconds instead of using publisher confirms and waiting for those
time.sleep(5)

print('Done.')
conn.close()
```

#### Code Example in Java

``` java
package com.rabbitmq.examples;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class ConsistentHashExchangeExample2 {
  public static final String EXCHANGE = "e2";
  private static String EXCHANGE_TYPE = "x-consistent-hash";

  public static void main(String[] argv) throws IOException, TimeoutException, InterruptedException {
    ConnectionFactory cf = new ConnectionFactory();
    Connection conn = cf.newConnection();
    Channel ch = conn.createChannel();

    for (String q : Arrays.asList("q1", "q2", "q3", "q4")) {
      ch.queueDeclare(q, true, false, false, null);
      ch.queuePurge(q);
    }

    Map<String, Object> args = new HashMap<>();
    args.put("hash-header", "hash-on");
    ch.exchangeDeclare(EXCHANGE, EXCHANGE_TYPE, true, false, args);

    for (String q : Arrays.asList("q1", "q2")) {
      ch.queueBind(q, EXCHANGE, "1");
    }

    for (String q : Arrays.asList("q3", "q4")) {
      ch.queueBind(q, EXCHANGE, "2");
    }

    ch.confirmSelect();


    for (int i = 0; i < 100000; i++) {
      AMQP.BasicProperties.Builder bldr = new AMQP.BasicProperties.Builder();
      Map<String, Object> hdrs = new HashMap<>();
      hdrs.put("hash-on", String.valueOf(i));
      ch.basicPublish(EXCHANGE, "", bldr.headers(hdrs).build(), "".getBytes("UTF-8"));
    }

    ch.waitForConfirmsOrDie(10000);

    System.out.println("Done publishing!");
    System.out.println("Evaluating results...");
    // wait for one stats emission interval so that queue counters
    // are up-to-date in the management UI
    Thread.sleep(5);

    System.out.println("Done.");
    conn.close();
  }
}
```

#### Code Example in Ruby

``` ruby
#!/usr/bin/env ruby

require 'bundler'
Bundler.setup(:default, :test)
require 'bunny'

conn = Bunny.new
conn.start

ch = conn.create_channel
ch.confirm_select

q1 = ch.queue("q1", durable: true)
q2 = ch.queue("q2", durable: true)
q3 = ch.queue("q3", durable: true)
q4 = ch.queue("q4", durable: true)

[q1, q2, q3, q4]. each(&:purge)

x  = ch.exchange("x2", type: "x-consistent-hash", durable: true, arguments: {"hash-header" => "hash-on"})

[q1, q2].each { |q| q.bind(x, routing_key: "1") }
[q3, q4].each { |q| q.bind(x, routing_key: "2") }

n = 100_000
(0..n).map(&:to_s).each do |i|
  x.publish(i.to_s, routing_key: rand.to_s, headers: {"hash-on": i})
end

ch.wait_for_confirms
puts "Done publishing!"

# wait for queue stats to be emitted so that management UI numbers
# are up-to-date
sleep 5
conn.close
puts "Done"
```

#### Code Example in Erlang

With RabbitMQ Erlang client:

``` erlang
-include_lib("amqp_client/include/amqp_client.hrl").

test() ->
    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    Queues = [<<"q0">>, <<"q1">>, <<"q2">>, <<"q3">>],
    amqp_channel:call(
      Chan, #'exchange.declare'{
              exchange  = <<"e">>,
              type      = <<"x-consistent-hash">>,
              arguments = [{<<"hash-header">>, longstr, <<"hash-on">>}]
            }),
    [amqp_channel:call(Chan, #'queue.declare'{queue = Q}) || Q <- Queues],
    [amqp_channel:call(Chan, #'queue.bind' {queue = Q,
                                            exchange = <<"e">>,
                                            routing_key = <<"1">>})
        || Q <- [<<"q0">>, <<"q1">>]],
    [amqp_channel:call(Chan, #'queue.bind' {queue = Q,
                                            exchange = <<"e">>,
                                            routing_key = <<"2">>})
        || Q <- [<<"q2">>, <<"q3">>]],
    RK = list_to_binary(integer_to_list(random:uniform(1000000))),
    Msg = #amqp_msg {props = #'P_basic'{headers = [{<<"hash-on">>, longstr, RK}]}, payload = <<>>},
    [amqp_channel:call(Chan,
                   #'basic.publish'{
                     exchange = <<"e">>,
                     routing_key = <<"">>,
                   }, Msg) || _ <- lists:seq(1, 100000)],
amqp_connection:close(Conn),
ok.
```


### Routing on a Message Property

In addition to a value in the header property, you can also route on the
``message_id``, ``correlation_id``, or ``timestamp`` message properties. To do so,
declare the exchange with a string argument called ``"hash-property"`` naming the
property to be used.

When a `"hash-property"` is specified, the chosen property **must be provided**.
If published messages do not contain the property, they will all get
routed to the same **arbitrarily chosen** queue.

#### Code Example in Python

``` python
#!/usr/bin/env python

import pika
import time

conn = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
ch   = conn.channel()

args = {u'hash-property': u'message_id'}
ch.exchange_declare(exchange='e3',
                    exchange_type='x-consistent-hash',
                    arguments=args,
                    durable=True)

for q in ['q1', 'q2', 'q3', 'q4']:
    ch.queue_declare(queue=q, durable=True)
    ch.queue_purge(queue=q)

for q in ['q1', 'q2']:
    ch.queue_bind(exchange='e3', queue=q, routing_key='1')

for q in ['q3', 'q4']:
    ch.queue_bind(exchange='e3', queue=q, routing_key='2')

n = 100000

for rk in list(map(lambda s: str(s), range(0, n))):
    ch.basic_publish(exchange='e3',
                     routing_key='',
                     body='',
                     properties=pika.BasicProperties(content_type='text/plain',
                                                     delivery_mode=2,
                                                     message_id=rk))
print('Done publishing.')

print('Waiting for routing to finish...')
# in order to keep this example simpler and focused,
# wait for a few seconds instead of using publisher confirms and waiting for those
time.sleep(5)

print('Done.')
conn.close()
```

#### Code Example in Java

``` java
package com.rabbitmq.examples;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class ConsistentHashExchangeExample3 {
  public static final String EXCHANGE = "e3";
  private static String EXCHANGE_TYPE = "x-consistent-hash";

  public static void main(String[] argv) throws IOException, TimeoutException, InterruptedException {
    ConnectionFactory cf = new ConnectionFactory();
    Connection conn = cf.newConnection();
    Channel ch = conn.createChannel();

    for (String q : Arrays.asList("q1", "q2", "q3", "q4")) {
      ch.queueDeclare(q, true, false, false, null);
      ch.queuePurge(q);
    }

    Map<String, Object> args = new HashMap<>();
    args.put("hash-property", "message_id");
    ch.exchangeDeclare(EXCHANGE, EXCHANGE_TYPE, true, false, args);

    for (String q : Arrays.asList("q1", "q2")) {
      ch.queueBind(q, EXCHANGE, "1");
    }

    for (String q : Arrays.asList("q3", "q4")) {
      ch.queueBind(q, EXCHANGE, "2");
    }

    ch.confirmSelect();


    for (int i = 0; i < 100000; i++) {
      AMQP.BasicProperties.Builder bldr = new AMQP.BasicProperties.Builder();
      ch.basicPublish(EXCHANGE, "", bldr.messageId(String.valueOf(i)).build(), "".getBytes("UTF-8"));
    }

    ch.waitForConfirmsOrDie(10000);

    System.out.println("Done publishing!");
    System.out.println("Evaluating results...");
    // wait for one stats emission interval so that queue counters
    // are up-to-date in the management UI
    Thread.sleep(5);

    System.out.println("Done.");
    conn.close();
  }
}
```

#### Code Example in Ruby

``` ruby
#!/usr/bin/env ruby

require 'bundler'
Bundler.setup(:default, :test)
require 'bunny'

conn = Bunny.new
conn.start

ch = conn.create_channel
ch.confirm_select

q1 = ch.queue("q1", durable: true)
q2 = ch.queue("q2", durable: true)
q3 = ch.queue("q3", durable: true)
q4 = ch.queue("q4", durable: true)

[q1, q2, q3, q4].each(&:purge)

x  = ch.exchange("x3", type: "x-consistent-hash", durable: true, arguments: {"hash-property" => "message_id"})

[q1, q2].each { |q| q.bind(x, routing_key: "1") }
[q3, q4].each { |q| q.bind(x, routing_key: "2") }

n = 100_000
(0..n).map(&:to_s).each do |i|
  x.publish(i.to_s, routing_key: rand.to_s, message_id: i)
end

ch.wait_for_confirms
puts "Done publishing!"

# wait for queue stats to be emitted so that management UI numbers
# are up-to-date
sleep 5
conn.close
puts "Done"
```

#### Code Example in Erlang

``` erlang
-include_lib("amqp_client/include/amqp_client.hrl").

test() ->
    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    Queues = [<<"q0">>, <<"q1">>, <<"q2">>, <<"q3">>],
    amqp_channel:call(Chan,
                  #'exchange.declare'{
                    exchange = <<"e">>, type = <<"x-consistent-hash">>,
                    arguments = {<<"hash-property">>, longstr, <<"message_id">>}                    
                  }),
    [amqp_channel:call(Chan, #'queue.declare'{queue = Q}) || Q <- Queues],
    [amqp_channel:call(Chan, #'queue.bind'{queue = Q,
                                           exchange = <<"e">>,
                                           routing_key = <<"1">>})
        || Q <- [<<"q0">>, <<"q1">>]],
    [amqp_channel:call(Chan, #'queue.bind' {queue = Q,
                                            exchange = <<"e">>,
                                            routing_key = <<"2">>})
        || Q <- [<<"q2">>, <<"q3">>]],
    RK = list_to_binary(integer_to_list(random:uniform(1000000)),
    Msg = #amqp_msg{props = #'P_basic'{message_id = RK}, payload = <<>>},
    [amqp_channel:call(Chan,
                   #'basic.publish'{
                     exchange = <<"e">>,
                     routing_key = <<"">>,
                     )
                   }, Msg) || _ <- lists:seq(1, 100000)],
amqp_connection:close(Conn),
ok.
```


## Getting Help

If you have questions or need help, feel free to ask on the
[RabbitMQ mailing list](https://groups.google.com/forum/#!forum/rabbitmq-users).


## Implementation Details

The hash function used in this plugin as of RabbitMQ 3.7.8
is [A Fast, Minimal Memory, Consistent Hash Algorithm](https://arxiv.org/abs/1406.2294) by Lamping and Veach. Erlang's `phash2` function is used to convert non-integer values to
an integer one that can be used by the jump consistent hash function by Lamping and Veach.

### Distribution Uniformity

A Chi-squared test was used to evaluate distribution uniformity. Below are the
results for 18 bucket counts and how they compare to two commonly used `p-value`
thresholds:

|Number of buckets|Chi-squared test result|Degrees of freedom|p-value = 0.05|p-value = 0.01|
|-|-----------|------------------|--------|--------|
|2|0.5|1|3.84|6.64|
|3|0.946|2|5.99|9.21|
|4|2.939|3|7.81|11.35|
|5|2.163|4|3.49|13.28|
|6|2.592|5|11.07|15.09|
|7|4.654|6|12.59|16.81|
|8|7.566|7|14.07|18.48|
|9|5.847|8|15.51|20.09|
|10|9.790|9|16.92|21.67|
|11|13.448|10|18.31|23.21|
|12|12.432|11|19.68|24.73|
|13|12.338|12|21.02|26.22|
|14|9.898|13|22.36|27.69|
|15|8.513|14|23.69|29.14|
|16|6.997|15|24.99|30.58|
|17|6.279|16|26.30|32.00|
|18|10.373|17|28.87|34.81|
|19|12.935|18|30.14|36.19|
|20|11.895|19|31.41|37.57|

### Binding Operations and Bucket Management

When a queue is bound to a consistent hash exchange, the protocol method, `queue.bind`,
carries a weight in the routing (binding) key. The binding is given
a number of buckets on the hash ring (hash space) equal to the weight.
When a queue is unbound, the buckets added for the binding are deleted.
These two operations use linear algorithms to update the ring.

To perform routing the exchange extract the appropriate value for hashing,
hashes it and retrieves a bucket number from the ring, then the bucket and
its associated queue.

The implementation assumes there is only one binding between a consistent hash
exchange and a queue. Having more than one binding is unnecessary because
queue weight can be provided at the time of binding.

### Clustered Environments

The state of the hash space is distributed across all cluster nodes.


## Continuous Integration

[![Build Status](https://travis-ci.org/rabbitmq/rabbitmq-consistent-hash-exchange.svg?branch=master)](https://travis-ci.org/rabbitmq/rabbitmq-consistent-hash-exchange)

## Copyright and License

(c) 2013-2020 VMware, Inc. or its affiliates.

Released under the Mozilla Public License 2.0, same as RabbitMQ.
See [LICENSE](./LICENSE) for details.
