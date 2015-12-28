RabbitMQ Web MQTT plugin
========================

This project is a simple bridge between "RabbitMQ-MQTT" plugin and
Websocket.

Once started the plugin opens a SockJS endpoint on prefix "/ws" on
port 15675. For example, a valid SockJS endpoint URL may look like:
"ws://127.0.0.1:15675/ws".

Once the server is started you should be able to establish a Websocket
connection to this URL. You will be able to communicate using the
usual MQTT protocol over it.

Installation
------------

Generic build instructions are at:

 * http://www.rabbitmq.com/plugin-development.html

Instructions on how to install a plugin into RabbitMQ broker:

 * http://www.rabbitmq.com/plugins.html#installing-plugins
