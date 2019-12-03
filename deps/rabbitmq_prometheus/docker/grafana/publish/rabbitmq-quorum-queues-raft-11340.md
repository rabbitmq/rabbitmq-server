# RabbitMQ-Quorum-Queues-Raft

Raft state for all Quorum Queues running in a RabbitMQ cluster

## Categories

* RabbitMQ

## README

Helps understand the state of all Raft members running the Quorum Queues in a RabbitMQ 3.8.x cluster.

Metrics displayed:

* Log entries committed / s
* Log entry commit latency
* Uncommitted log entries
* Leader elections / s
* Raft members with >5k entries in the log

Filter by:

* RabbitMQ Cluster

Depends on `rabbitmq-prometheus` plugin, built-in since [RabbitMQ v3.8.0](https://github.com/rabbitmq/rabbitmq-server/releases/tag/v3.8.0)

Learn more about [RabbitMQ built-in Prometheus support](https://www.rabbitmq.com/prometheus.html)

To get it working locally with RabbitMQ in 3 simple steps, follow this [Quick Start guide](https://www.rabbitmq.com/prometheus.html#quick-start)
