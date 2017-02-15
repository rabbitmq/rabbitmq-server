# RabbitMQ Web MQTT Examples

This RabbitMQ plugin contains few basic examples of [RabbitMQ Web MQTT plugin](https://github.com/rabbitmq/rabbitmq-web-mqtt)
usage.

It starts a server that binds to port 15670 and serves a few static
HTML files on port 15670 (e.g. [http://127.0.0.1:15670](http://127.0.0.1:15670/)).
Note that Web STOMP examples use the same port, so these plugins cannot be enabled
at the same time unless they are configured to use different ports.

## Installation

Once installed, this plugin needs to be enabled like any other:

    rabbitmq-plugins enable rabbitmq_web_mqtt_examples

## Building from Source

 * [RabbitMQ plugin build instructions](http://www.rabbitmq.com/plugin-development.html).

 * [RabbitMQ plugin installation](http://www.rabbitmq.com/plugins.html#installing-plugins).
