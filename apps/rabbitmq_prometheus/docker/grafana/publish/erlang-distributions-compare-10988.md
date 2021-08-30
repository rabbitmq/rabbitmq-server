# Erlang-Distributions-Compare

Erlang Distribution traffic, node network traffic and CPU + PerfTest message throughput and latency

## Categories

* RabbitMQ

## README

Compare the effects of running Erlang Distribution with different compression algorithms (deflate, lz4, zstd, etc.)

Metrics displayed:

* Erlang Distribution outgoing link traffic / s
* Network incoming & outgoing traffic / s
* CPU utilisation
* Messages published & consumed / s
* End-to-end message latency

Filter by:

* RabbitMQ Cluster
* PerfTest Instance & message latency percentile
* Host when using [node_exporter](https://github.com/prometheus/node_exporter)
* Container when using [cadvisor](https://github.com/google/cadvisor)

Depends on `rabbitmq-prometheus` plugin, built-in since [RabbitMQ v3.8.0](https://github.com/rabbitmq/rabbitmq-server/releases/tag/v3.8.0)

Learn more about [RabbitMQ built-in Prometheus support](https://www.rabbitmq.com/prometheus.html)

To get it working locally with RabbitMQ in 3 simple steps, follow this [Quick Start guide](https://www.rabbitmq.com/prometheus.html#quick-start)
