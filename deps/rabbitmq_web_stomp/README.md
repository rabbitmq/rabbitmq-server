# RabbitMQ Web STOMP plugin

This project is a minimalistic bridge between the [RabbitMQ STOMP plugin](https://rabbitmq.com/stomp.html) and
WebSockets.

## RabbitMQ Version Requirements

The most recent version of this plugin requires RabbitMQ `3.7.0` or later.

Since version `3.7.0` this plugin does not support SockJS anymore.
SockJS URL path was removed.

## Installation and Binary Builds

This plugin is now available from the [RabbitMQ community plugins page](https://www.rabbitmq.com/community-plugins.html).
Please consult the docs on [how to install RabbitMQ plugins](https://www.rabbitmq.com/plugins.html#installing-plugins).

## Documentation

Please refer to the [RabbitMQ Web STOMP guide](https://www.rabbitmq.com/web-stomp.html).

## Building from Source

See [Plugin Development guide](https://www.rabbitmq.com/plugin-development.html).

TL;DR: running

    make dist

will build the plugin and put build artifacts under the `./plugins` directory.


## Copyright and License

(c) Pivotal Software Inc, 2007-2017

Released under the MPL, the same license as RabbitMQ.
