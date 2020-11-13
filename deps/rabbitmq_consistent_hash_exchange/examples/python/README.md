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

## Running the Example

``` sh
# hashing on the routing key
python ./example1.py

# hashing on a custom header
python ./example2.py

# hashing on a message property
python ./example3.py
```
