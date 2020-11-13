# RabbitMQ Consistent Hash Exchange Examples in Python

This directory contains runnable Python examples for the [RabbitMQ Consistent Hash Exchange plugin](https://github.com/rabbitmq/rabbitmq-consistent-hash-exchange/).
They are the same examples as in the [plugin's README](../../README.md) file.

## Prerequisites

The examples assume a RabbitMQ node with the `rabbitmq_consistent_hash_exchange` plugin
enabled is running on `localhost` and that default user credentials and virtual host
were not deleted.

## Dependency Installation

``` sh
pip install -r ./requirements.txt
```

## Workload Details

This example uses four queues: `q1` through `q4`. The first two of them are bound to a consistent hashing
exchange with a weight of `1`, while the last two of them use a weight of `2`. This means
that `q3` and `q4` will get roughly twice as many published messages routed to them
compared to either `q1` or `q2`.

## Running the Example

``` sh
# hashing on the routing key
python ./example1.py

# hashing on a custom header
python ./example2.py

# hashing on a message property
python ./example3.py
```

## Inspecting Queue States

To list bindings to the exchange and their weights, use

``` shell
rabbitmqctl list_bindings | grep chx
```

To list queues and the number of ready messages in them:

``` shell
rabbitmqctl list_queues name messages_ready
```