# RabbitMQ Web MQTT plugin


This plugin provides a support for MQTT-over-WebSockets to
RabbitMQ.


## Installation

Generic build instructions are at:

 * http://www.rabbitmq.com/plugin-development.html

Instructions on how to install a plugin into RabbitMQ broker:

 * http://www.rabbitmq.com/plugins.html#installing-plugins


## Usage

Once the server is started you should be able to establish a Websocket
connection to `ws://127.0.0.1:15675/ws`. You will be able to communicate using the
usual MQTT protocol over it.


## Copyright and License

(c) Pivotal Software Inc, 2007-2016.

Released under the same license as RabbitMQ. See LICENSE for details.
