# RabbitMQ Web STOMP plugin

This project is a minimalistic bridge between the [RabbitMQ STOMP plugin](http://rabbitmq.com/stomp.html) and
WebSockets (directly or via SockJS emulation).

## RabbitMQ Version Requirements

The most recent version of this plugin requires RabbitMQ `3.6.1` or later.

## Installation and Binary Builds

This plugin is now available from the [RabbitMQ community plugins page](http://www.rabbitmq.com/community-plugins.html).
Please consult the docs on [how to install RabbitMQ plugins](http://www.rabbitmq.com/plugins.html#installing-plugins).

## Documentation

Please refer to the [RabbitMQ Web STOMP guide](http://www.rabbitmq.com/web-stomp.html).

## Building from Source

See [Plugin Development guide](http://www.rabbitmq.com/plugin-development.html).

TL;DR: running

    make dist

will build the plugin and put build artifacts under the `./plugins` directory.


## Copyright and License

(c) Pivotal Software Inc, 2007-2017

Released under the MPL, the same license as RabbitMQ.
