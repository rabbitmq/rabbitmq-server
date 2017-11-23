# RabbitMQ Web MQTT Examples

This project contains few basic examples of [RabbitMQ Web MQTT plugin](https://github.com/rabbitmq/rabbitmq-web-mqtt)
usage.

Once installed the server will bind to port 15670 and serve few static
HTML files on port 15670 (e.g. [http://127.0.0.1:15670](http://127.0.0.1:15670/)).

## Installation

This plugin ships with RabbitMQ. Enabled it using [CLI tools](http://www.rabbitmq.com/cli.html):

    rabbitmq-plugins enable rabbitmq_web_mqtt_examples
