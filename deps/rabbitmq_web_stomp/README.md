# RabbitMQ Web STOMP plugin

This project is a minimalistic bridge between the [RabbitMQ STOMP plugin](http://rabbitmq.com/stomp.html) and
WebSockets.

## RabbitMQ Version Requirements

The most recent version of this plugin requires RabbitMQ `3.7.0` or later.

Since version `3.7.0` this plugin no longer supports WebSocket emulation with SockJS.
The SockJS endpoint is no longer available.

## Installation and Binary Builds

This plugin ships with modern versions of RabbitMQ.
Like all plugins, it [must be enabled](https://www.rabbitmq.com/plugins.html) before it can be used:

``` bash
# this might require sudo
rabbitmq-plugins enable rabbitmq_web_stomp
```

## Documentation

Please refer to the [RabbitMQ Web STOMP guide](http://www.rabbitmq.com/web-stomp.html).

## Building from Source

See [Plugin Development guide](http://www.rabbitmq.com/plugin-development.html).

TL;DR: running

    make dist

will build the plugin and put build artifacts under the `./plugins` directory.


## Copyright and License

(c) Pivotal Software Inc, 2007-2019

Released under the MPL, the same license as RabbitMQ.
