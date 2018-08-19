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

[Consistent Hashing](http://en.wikipedia.org/wiki/Consistent_hashing)
is a hashing technique whereby each bucket appears at multiple points
throughout the hash space, and the bucket selected is the nearest
higher (or lower, it doesn't matter, provided it's consistent) bucket
to the computed hash (and the hash space wraps around). The effect of
this is that when a new bucket is added or an existing bucket removed,
only a very few hashes change which bucket they are routed to.

## Supported RabbitMQ Versions

This plugin supports RabbitMQ 3.3.x and later versions.

## Supported Erlang Versions

This plugin supports the same [Erlang versions](https://rabbitmq.com/which-erlang.html) as RabbitMQ core.


## How It Works

In the case of Consistent Hashing as an exchange type, the hash is
calculated from a message property (most commonly the routing key).
Thus messages that have the same routing key will have the
same hash value computed for them, and thus will be routed to the same queue,
assuming no bindings have changed.

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

Each message gets delivered to at most one queue. Normally, each
message gets delivered to exactly one queue, but there is a race
between the determination of which queue to send a message to, and the
deletion/death of that queue that does permit the possibility of the
message being sent to a queue which then disappears before the message
is processed. Hence in general, at most one queue.

The exchange type is `"x-consistent-hash"`.


## Usage Examples

### Overview

In the below example the queues `q0` and `q1` get bound each with the weight of 1
in the hash space to the exchange `e` which means they'll each get
roughly the same number of routing keys. The queues `q2` and `q3`
however, get 2 buckets each (their weight is 2) which means they'll each get roughly the
same number of routing keys too, but that will be approximately twice
as many as `q0` and `q1`. The example then publishes 100,000 messages to our
exchange with random routing keys, the queues will get their share of
messages roughly equal to the binding keys ratios. After this has
completed, running `rabbitmqctl list_queues` should show that the
messages have been distributed approximately as desired.

Note the `routing_key`s in the bindings are numbers-as-strings. This
is because AMQP 0-9-1 specifies the `routing_key` field must be a string.

It is important to ensure that the messages being published
to the exchange have a range of different `routing_key`s: if a very
small set of routing keys are being used then there's a possibility of
messages not being evenly distributed between the various queues. If
the routing key is a pseudo-random session ID or such, then good
results should follow.

### Erlang

Here is an example using the Erlang client:

```erlang
-include_lib("amqp_client/include/amqp_client.hrl").

test() ->
    {ok, Conn} = amqp_connection:start(#amqp_params_network{}),
    {ok, Chan} = amqp_connection:open_channel(Conn),
    Queues = [<<"q0">>, <<"q1">>, <<"q2">>, <<"q3">>],
    amqp_channel:call(Chan,
                  #'exchange.declare' {
                    exchange = <<"e">>, type = <<"x-consistent-hash">>
                  }),
    [amqp_channel:call(Chan, #'queue.declare' { queue = Q }) || Q <- Queues],
    [amqp_channel:call(Chan, #'queue.bind' { queue = Q,
                                             exchange = <<"e">>,
                                             routing_key = <<"1">> })
        || Q <- [<<"q0">>, <<"q1">>]],
    [amqp_channel:call(Chan, #'queue.bind' { queue = Q,
                                             exchange = <<"e">>,
                                             routing_key = <<"2">> })
        || Q <- [<<"q2">>, <<"q3">>]],
    Msg = #amqp_msg { props = #'P_basic'{}, payload = <<>> },
    [amqp_channel:call(Chan,
                   #'basic.publish'{
                     exchange = <<"e">>,
                     routing_key = list_to_binary(
                                     integer_to_list(
                                       random:uniform(1000000)))
                   }, Msg) || _ <- lists:seq(1,100000)],
amqp_connection:close(Conn),
ok.
```

## Routing on a Header

Under most circumstances the routing key is a good choice for something to
hash. However, in some cases you need to use the routing key for some other
purpose (for example with more complex routing involving exchange to
exchange bindings). In this case you can configure the consistent hash
exchange to route based on a named header instead. To do this, declare the
exchange with a string argument called "hash-header" naming the header to
be used. For example using the Erlang client as above:

```erlang
    amqp_channel:call(
      Chan, #'exchange.declare' {
              exchange  = <<"e">>,
              type      = <<"x-consistent-hash">>,
              arguments = [{<<"hash-header">>, longstr, <<"hash-me">>}]
            }).
```

If you specify "hash-header" and then publish messages without the named
header, they will all get routed to the same (arbitrarily-chosen) queue.

## Routing on a Message Property

In addition to a value in the header property, you can also route on the
``message_id``, ``correlation_id``, or ``timestamp`` message property. To do so,
declare the exchange with a string argument called "hash-property" naming the
property to be used. For example using the Erlang client as above:

```erlang
    amqp_channel:call(
      Chan, #'exchange.declare' {
              exchange  = <<"e">>,
              type      = <<"x-consistent-hash">>,
              arguments = [{<<"hash-property">>, longstr, <<"message_id">>}]
            }).
```

Note that you declaring an exchange that routes on both "hash-header" and
"hash-property" is not allowed. When "hash-property" is specified, and a message
is published without a value in the named property, they will all get routed to the same
(arbitrarily-chosen) queue.


## Getting Help

Any comments or feedback welcome, to the
[RabbitMQ mailing list](https://groups.google.com/forum/#!forum/rabbitmq-users).


## Implementation Details

The hash function used in this plugin as of RabbitMQ 3.7.8
is [A Fast, Minimal Memory, Consistent Hash Algorithm](https://arxiv.org/abs/1406.2294) by Lamping and Veach.

When a queue is bound to a consistent hash exchange, the protocol method, `queue.bind`,
carries a weight in the routing (binding) key. The binding is given
a number of buckets on the hash ring (hash space) equal to the weight.
When a queue is unbound, the buckets added for the binding are deleted.
These two operations use linear algorithms to update the ring.

To perform routing the exchange extract the appropriate value for hashing,
hashes it and retrieves a bucket number from the ring, then the bucket and
its associated queue.

The implementation assumes there is only one binding between a consistent hash
exchange and a queue. Having more than one bunding is unnecessary because
queue weight can be provided at the time of binding.

## Continuous Integration

[![Build Status](https://travis-ci.org/rabbitmq/rabbitmq-consistent-hash-exchange.svg?branch=master)](https://travis-ci.org/rabbitmq/rabbitmq-consistent-hash-exchange)

## Copyright and License

(c) 2013-2018 Pivotal Software Inc.

Released under the Mozilla Public License 1.1, same as RabbitMQ.
See [LICENSE](https://github.com/rabbitmq/rabbitmq-consistent-hash-exchange/blob/master/LICENSE) for
details.
