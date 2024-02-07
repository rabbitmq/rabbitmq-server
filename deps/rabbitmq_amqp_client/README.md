# Erlang RabbitMQ AMQP 1.0 Client

The [Erlang AMQP 1.0 client](../amqp10_client/) is a client that can communicate with any AMQP 1.0 broker.
In contrast, this project (Erlang **RabbitMQ** AMQP 1.0 Client) can only communicate with RabbitMQ.
This project wraps (i.e. depends on) the Erlang AMQP 1.0 client providing additionally the following RabbitMQ management operations:
* declare queue
* get queue
* delete queue
* purge queue
* bind queue to exchange
* unbind queue from exchange
* declare exchange
* delete exchange
* bind exchange to exchange
* unbind exchange from exchange

Except for `get queue`, these management operations are defined in the [AMQP 0.9.1 protocol](https://www.rabbitmq.com/amqp-0-9-1-reference.html).
To support these AMQP 0.9.1 / RabbitMQ specific operations over AMQP 1.0, this project implements a subset of the following (most recent) AMQP 1.0 extension specifications:
* [AMQP Request-Response Messaging with Link Pairing Version 1.0 - Committee Specification 01](https://docs.oasis-open.org/amqp/linkpair/v1.0/cs01/linkpair-v1.0-cs01.html) (February 2021)
* [HTTP Semantics and Content over AMQP Version 1.0 - Working Draft 06](https://groups.oasis-open.org/higherlogic/ws/public/document?document_id=65571) (July 2019)
* [AMQP Management Version 1.0 - Working Draft 16](https://groups.oasis-open.org/higherlogic/ws/public/document?document_id=65575) (July 2019)

This project might support more (non AMQP 0.9.1) RabbitMQ operations via AMQP 1.0 in the future.

Topologies (exchanges, bindings, queues) in RabbitMQ can be created via
* [Management HTTP API](https://www.rabbitmq.com/docs/management#http-api)
* [Definition Import](https://www.rabbitmq.com/docs/definitions#import)
* AMQP 0.9.1 clients
* RabbitMQ AMQP 1.0 clients, such as this project
