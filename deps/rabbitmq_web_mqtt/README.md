# RabbitMQ Web MQTT plugin


This plugin provides a support for MQTT-over-WebSockets to
RabbitMQ.


## Supported RabbitMQ Versions

This plugin supports RabbitMQ `3.6.x` and later releases starting with `3.6.1`.


## Installation

### Binary Builds

Binary build is available from the [RabbitMQ Community Plugins page](http://www.rabbitmq.com/community-plugins.html).

### From Source

 * [Generic plugin build instructions](http://www.rabbitmq.com/plugin-development.html)
 * Instructions on [how to install a plugin into RabbitMQ broker](http://www.rabbitmq.com/plugins.html#installing-plugins)

Note that release branches (`stable` vs. `master`) and target RabbitMQ version need to be taken into account
when building plugins from source.


## Usage

Once the server is started you should be able to establish a Websocket
connection to `ws://127.0.0.1:15675/ws`. You will be able to communicate using the
usual MQTT protocol over it.


## Copyright and License

(c) Pivotal Software Inc, 2007-2016.

Released under the same license as RabbitMQ. See LICENSE for details.
