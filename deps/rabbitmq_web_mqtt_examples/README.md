# RabbitMQ Web MQTT Examples

This RabbitMQ plugin contains few basic examples of [RabbitMQ Web MQTT plugin](https://github.com/rabbitmq/rabbitmq-web-mqtt)
usage.

It starts a server that binds to port 15670 and serves a few static
HTML files on port 15670 (e.g. [http://127.0.0.1:15670](http://127.0.0.1:15670/)).
Note that Web STOMP examples use the same port, so these plugins cannot be enabled
at the same time unless they are configured to use different ports.

## Installation

This plugin ships with RabbitMQ. Enabled it using [CLI tools](https://www.rabbitmq.com/cli.html):

    rabbitmq-plugins enable rabbitmq_web_mqtt_examples
