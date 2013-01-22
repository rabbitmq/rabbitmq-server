RabbitMQ JMS Topic Exchange Plugin
==================================


Overview
--------

This plugin is designed to work with the JMS Client for RabbitMQ. It supports
JMS Topic routing and selection based on JMS SQL selection rules.

This implementation is based upon the Java Messaging Service Specification
Version 1.1, see [The Jms
Specs](http://www.oracle.com/technetwork/java/docs-136352.html) for a copy of
this specification.

Build
-----

Currently this is a RabbitMQ plugin repository which is built by embedding it
in the `rabbitmq-public-umbrella` repository (available from
`www.rabbitmq.com`) and then running `make`. This is a manual process.

There is now a `Makefile2` which will build this at a RABBIT_BRANCH version
using

    make -f Makefile2 dist

This clones a copy of `rabbitmq-public-umbrella`, cloning and adjusting only
those dependencies of `rabbitmq-jms-topic-exchange` as are necessary to build
the plugin, and then executing the build.
