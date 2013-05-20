RabbitMQ JMS Topic Exchange Plugin
==================================

Overview
--------

This plugin is designed to work with the JMS Client for RabbitMQ. It
supports JMS Topic routing and selection based on JMS SQL selection
rules.

This implementation is based upon the Java Messaging Service
Specification Version 1.1, see [The Jms
Specs](http://www.oracle.com/technetwork/java/docs-136352.html) for a
copy of this specification.

Design
------

The plugin this generates is a user-written exchange type for RabbitMQ
client use. The exchange type name is "`x_jms_topic`" but this is _not_
a topic exchange. Instead it works together with a standard topic
exchange to provide the JMS topic selection function.

When JMS Selectors are used on a Topic Destination consumer, the
destination (queue) is bound to an exchange of type `x_jms_topic`, with
arguments that indicate what the selection criteria are. The
`x_jms_topic` exchange is, in turn, bound to the standard Topic Exchange
used by JMS messaging (this uses the RabbitMQ exchange-to-exchange
binding extension to the AMQP protocol).

In this way, normal topic routing can occur, with the overhead of
selection only applying when selection is used, and _after_ the routing
and filtering implied by the topic name.

Build
-----

This plugin is _not_ a standard RabbitMQ plugin repository, but contains
one wrapped in a customised Maven `pom` project. The standard RabbitMQ
plugin directory is in a sub-directory of the same name.

To build it, and install the `*.ez` artefact in the local Maven
repository, issue the command:

    mvn clean install

This will get the relevant parts of the `rabbitmq-public-umbrella`
repositories, at the specified version of RabbitMQ (stored in the Maven
`pom`), copy the plugin source subdirectory into the right position in
the umbrella tree, and issue the standard `make` commands to build a
RabbitMQ plugin. After these finish (successfully) the artefact
generated (`rabbitmq_jms_topic_exchange.ez`) is copied into the outer
`target/plugins` directory and thence pushed to the Maven repository.

All the other dependencies are pulled in by the RabbitMQ plugin `make`
process.

This rather indirect build procedure is used to tie in with the rest of
the project, residing in and published to Maven repositories.

Alternatively, the copy of the subdirectory
`rabbitmq-jms-topic-exchange` can be modified and re-built in the
`rabbitmq-public-umbrella`, using the standard RabbitMQ plugin `make`
commands (see [Plugin
Development](http://www.rabbitmq.com/plugin-development.html) on the
RabbitMQ site). **Beware**: Re-issuing the Maven `clean` goal will
delete the umbrella copy, along with any changes made in the copy of
the plugin directory.
