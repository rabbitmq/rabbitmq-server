# RabbitMQ Consistent Hash Exchange Examples in Ruby

This directory contains runnable Ruby examples for the [RabbitMQ Consistent Hash Exchange plugin](https://github.com/rabbitmq/rabbitmq-consistent-hash-exchange/).
They are the same examples as in the [plugin's README](../../README.md) file.

## Prerequisites

The examples assume a RabbitMQ node with the `rabbitmq_consistent_hash_exchange` plugin
enabled is running on `localhost` and that default user credentials and virtual host
were not deleted.

## Dependency Installation

``` sh
bundle install
```

## Running the Example

``` sh
# hashing on the routing key
bundle exec ruby ./example1.rb

# hashing on a custom header
bundle exec ruby ./example2.rb

# hashing on a message property
bundle exec ruby ./example3.rb
```
