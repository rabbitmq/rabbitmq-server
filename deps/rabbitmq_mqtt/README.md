# RabbitMQ MQTT Plugin

## Getting Started

This is an MQTT plugin for RabbitMQ.

The plugin is included in the RabbitMQ distribution.  To enable
it, use <href="http://www.rabbitmq.com/man/rabbitmq-plugins.1.man.html">rabbitmq-plugins</a>:

    rabbitmq-plugins enable rabbitmq_mqtt

Default port used by the plugin is `1883`.

## Documentation

[MQTT plugin documentation](http://www.rabbitmq.com/mqtt.html) is available
from rabbitmq.com.

## Contributing

See [CONTRIBUTING.md](https://github.com/rabbitmq/rabbitmq-mqtt/blob/master/CONTRIBUTING.md).

### Running Tests

After cloning RabbitMQ umbrella repository, change into the `rabbitmq-mqtt` directory
and run

    make tests

This will bring up a RabbitMQ node with the plugin enabled and run integration tests
against it. Note that there must be no other MQTT server running on ports `1883` and `8883`.

## Copyright and License

(c) 2007 — 2016 Pivotal Software, Inc.

Released under the [Mozilla Public License](http://www.rabbitmq.com/mpl.html),
the same as RabbitMQ.
