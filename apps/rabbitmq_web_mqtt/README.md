# RabbitMQ Web MQTT plugin

This plugin provides support for MQTT-over-WebSockets to RabbitMQ.

## Installation

This plugin ships with modern versions of RabbitMQ.
Like all plugins, it [must be enabled](https://www.rabbitmq.com/plugins.html) before it can be used:

``` bash
# this might require sudo
rabbitmq-plugins enable rabbitmq_web_mqtt
```

## Documentation

Please refer to the [RabbitMQ Web MQTT guide](https://www.rabbitmq.com/web-mqtt.html).


## Building From Source

 * [Generic plugin build instructions](https://www.rabbitmq.com/plugin-development.html)
 * Instructions on [how to install a plugin into RabbitMQ broker](https://www.rabbitmq.com/plugins.html#installing-plugins)

Note that release branches (`stable` vs. `master`) and target RabbitMQ version need to be taken into account
when building plugins from source.


## Copyright and License

(c) 2007-2020 VMware, Inc. or its affiliates.

Released under the same license as RabbitMQ. See LICENSE for details.
