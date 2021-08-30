# Additional information #

Here you can find some extra information about how the plugin works
and the reasons for it.

## Why do we need this plugin? ##

RabbitMQ queues are bound to the node where they were first
declared. This means that even if you create a cluster of RabbitMQ
brokers, at some point all message traffic will go to the node where
the queue lives. What this plugin does is to give you a centralized
place where to send your messages, plus __load balancing__ across many
nodes, by adding queues to the other nodes in the cluster.

The advantage of this setup is that the queues from where your
consumers will get messages will be local to the node where they are
connected.  On the other hand, the producers don't need to care about
what's behind the exchange.

All the plumbing to __automatically maintain__ the shard queues is
done by the plugin. If you add more nodes to the cluster, then the
plugin will __automatically create queues in those nodes__.

If you remove nodes from the cluster then RabbitMQ will take care of
taking them out of the list of bound queues. Message loss can happen
in the case where a race occurs from a node going away and your
message arriving to the shard exchange. If you can't afford to lose a
message then you can use
[publisher confirms](https://www.rabbitmq.com/confirms.html) to prevent
message loss.

## Message Ordering ##

Message order is maintained per sharded queue, but not globally. This
means that once a message entered a queue, then for that queue and the
set of consumers attached to the queue, ordering will be preserved.

If you need global ordering then stick with
[mirrored queues](https://www.rabbitmq.com/ha.html).

## What strategy is used for picking the queue name ##

When you issue a `basic.consume`, the plugin will choose the queue
with the _least amount of consumers_.  The queue will be local to the
broker your client is connected to. Of course the local sharded queue
will be part of the set of queues that belong to the chosen shard.

## Intercepted Channel Behaviour ##

This plugin works with the new `channel interceptors`. An interceptor
basically allows a plugin to modify parts of an AMQP method. For
example in this plugin case, whenever a user sends a `basic.consume`,
the plugin will map the queue name sent by the user to one of the
sharded queues.

Also a plugin can decide that a certain AMQP method can't be performed
on a queue that's managed by the plugin. In this case declaring a queue
called `my_shard` doesn't make much sense when there's actually a
sharded queue by that name. In this case the plugin will return a
channel error to the user.

These are the AMQP methods intercepted by the plugin, and the
respective behaviour:

- `'basic.consume', QueueName`: The plugin will pick the sharded queue
  with the least amount of consumers from the `QueueName` shard.
- `'basic.get', QueueName`: The plugin will pick the sharded queue
  with the least amount of consumers from the `QueueName` shard.
- `'queue.declare', QueueName`: The plugin rewrites `QueueName` to be
  the first queue in the shard, so `queue.declare_ok` returns the stats
  for that queue.
- `'queue.bind', QueueName`: since there isn't an actual `QueueName`
  queue, this method returns a channel error.
- `'queue.unbind', QueueName`: since there isn't an actual `QueueName`
  queue, this method returns a channel error.
- `'queue.purge', QueueName`: since there isn't an actual `QueueName`
  queue, this method returns a channel error.
- `'queue.delete', QueueName`: since there isn't an actual `QueueName`
  queue, this method returns a channel error.
