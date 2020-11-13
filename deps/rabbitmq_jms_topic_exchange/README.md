# RabbitMQ JMS Topic Exchange Plugin

## Overview

This plugin adds server-side support for [RabbitMQ JMS client](https://github.com/rabbitmq/rabbitmq-jms-client).
This plugin provides support for JMS topic routing and selection based on JMS SQL selection
rules.

This implementation is based upon the [Java Messaging Service
Specification Version 1.1](https://www.oracle.com/technetwork/java/docs-136352.html).

## Project Maturity

RabbitMQ JMS-related projects are several years old and can be considered
reasonably mature. They have been first open sourced in June 2016.
Some related projects (e.g. a compliance test suite) and documentation are yet to be open sourced.

## Supported RabbitMQ Versions

This plugin targets RabbitMQ `3.6.0` and later versions.

## Installation

This plugin ships with RabbitMQ starting with `3.6.3`. Enable it with

```
[sudo] rabbitmq-plugins enable rabbitmq_jms_topic_exchange
```


## Design

The plugin this generates is a user-written exchange type for RabbitMQ
client use. The exchange type name is "`x_jms_topic`" but this is _not_
a topic exchange. Instead it works together with a standard topic
exchange to provide the JMS topic selection function.

When JMS Selectors are used on a Topic Destination consumer, the
destination (queue) is bound to an exchange of type `x_jms_topic`, with
arguments that indicate what the selection criteria are. The
`x_jms_topic` exchange is, in turn, bound to the standard Topic Exchange
used by JMS messaging (this uses the RabbitMQ exchange-to-exchange
binding extension to the AMQP 0-9-1 protocol).

In this way, normal topic routing can occur, with the overhead of
selection only applying when selection is used, and _after_ the routing
and filtering implied by the topic name.

## Building From Source

Building is no different from [building other RabbitMQ plugins](https://www.rabbitmq.com/plugin-development.html).

TL;DR:

    git clone https://github.com/rabbitmq/rabbitmq-jms-topic-exchange.git
    cd rabbitmq-jms-topic-exchange
    make -j dist
    ls plugins/*
    
## Copyright and License

(c) 2007-2020 VMware, Inc. or its affiliates.

See [LICENSE](./LICENSE) for license information.
