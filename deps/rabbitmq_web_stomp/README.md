# RabbitMQ Web STOMP plugin

This plugin provides support for STOMP-over-WebSockets to RabbitMQ.

## Installation

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
