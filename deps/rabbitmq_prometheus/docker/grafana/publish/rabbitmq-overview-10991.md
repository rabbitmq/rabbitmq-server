# RabbitMQ-Overview

An alternative to RabbitMQ Management Overview

## Categories

* RabbitMQ

## README

Understand the state of any RabbitMQ cluster at a glance. Includes all metrics displayed on RabbitMQ Management Overview page.

This dashboard includes detailed explanation for all metrics displayed, with links to relevant official docs and guides.

All metrics are node-specific making it trivial to visualise cluster imbalances (a.k.a. cluster hotspots).

Some graph panels include sensible default thresholds.

Metrics displayed:

* Node identity, including RabbitMQ & Erlang/OTP version
* Node memory & disk available before publishers blocked (alarm triggers)
* Node file descriptors & TCP sockets available
* Ready & pending messages
* Incoming message rates: published  / routed to queues / confirmed / unconfirmed / returned / dropped
* Outgoing message rated: delivered with auto or manual acks / acknowledged / redelivered
* Polling operation with auto or manual acks, as well as empty ops
* Queues, including declaration & deletion rates
* Channels, including open & close rates
* Connections, including open & close rates

Filter by:

* RabbitMQ Cluster

Requires `rabbitmq-prometheus` to be enabled, a built-in plugin since [RabbitMQ v3.8.0](https://github.com/rabbitmq/rabbitmq-server/releases/tag/v3.8.0)

Learn more about [RabbitMQ built-in Prometheus support](https://www.rabbitmq.com/prometheus.html)

To get it working locally with RabbitMQ in 3 simple steps, follow this [Quick Start guide](https://www.rabbitmq.com/prometheus.html#quick-start)
