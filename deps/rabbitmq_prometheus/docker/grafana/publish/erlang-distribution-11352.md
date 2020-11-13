# Erlang-Distribution

Erlang Distribution links, inet socket, port driver, dist process + tls_connection & tls_sender

## Categories

* RabbitMQ

## README

Understand the behaviour of Erlang clustering via Erlang Distribution links, inet socket, port driver & dist process.

If the Erlang Distribution is using TLS, the state of tls_connection & tls_sender processes will be shown as well.

Metrics displayed:

* Distribution link
  * State: established / connecting / waiting
  * Data buffered
  * Data sent to peer node / s
  * Data received from peer node / s
  * Messages sent to peer node / s
  * Messages received from peer node / s
  * Average inet packet size sent to peer node
  * Average inet packet size received from peer node

* Port driver
  * Memory used
  * Data buffered

* Dist process
    * State: waiting / running / garbage_collecting / runnable / suspended / exiting
    * Queued messages
    * Memory used
    * Process reductions / s

The last set of metrics are repeated for the `tls_connection` and `tls_sender` processes if the Erlang Distribution is using TLS.

Filter by:

* RabbitMQ Cluster
* Process type

Depends on `rabbitmq-prometheus` plugin, built-in since [RabbitMQ v3.8.0](https://github.com/rabbitmq/rabbitmq-server/releases/tag/v3.8.0)

Learn more about [RabbitMQ built-in Prometheus support](https://www.rabbitmq.com/prometheus.html)

To get it working locally with RabbitMQ in 3 simple steps, follow this [Quick Start guide](https://www.rabbitmq.com/prometheus.html#quick-start)
