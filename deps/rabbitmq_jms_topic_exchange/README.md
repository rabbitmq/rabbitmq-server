# RabbitMQ JMS Topic Exchange Plugin

## Overview

This plugin adds server-side support for RabbitMQ JMS client. All JMS-related
projects are in the process of being open sourced by the RabbitMQ team
and **should not be used unless the process is complete and announced**.

This plugin is designed to work with the JMS Client for RabbitMQ. It
supports JMS topic routing and selection based on JMS SQL selection
rules.

This implementation is based upon the Java Messaging Service
Specification Version 1.1, see [The JMS
Specs](http://www.oracle.com/technetwork/java/docs-136352.html) for a
copy of that specification.

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

Building is no different from [building other RabbitMQ plugins](http://www.rabbitmq.com/plugin-development.html).

TL;DR:

    git clone https://github.com/rabbitmq/rabbitmq-jms-topic-exchange.git
    cd rabbitmq-jms-topic-exchange
    make -j dist
    ls plugins/*
    
## Copyright and License

(c) Pivotal Software Inc., 2007-2017.

See [LICENSE](./LICENSE) for license information.
