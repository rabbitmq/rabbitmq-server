# RabbitMQ MQTT Plugin

## Getting Started

This is an MQTT plugin for RabbitMQ.

The plugin is included in the RabbitMQ distribution.  To enable
it, use [rabbitmq-plugins](https://www.rabbitmq.com/man/rabbitmq-plugins.1.man.html):

    rabbitmq-plugins enable rabbitmq_mqtt

Default port used by the plugin is `1883`.

## Documentation

[MQTT plugin documentation](https://www.rabbitmq.com/mqtt.html) is available
from rabbitmq.com.

## Contributing

See [CONTRIBUTING.md](https://github.com/rabbitmq/rabbitmq-mqtt/blob/master/CONTRIBUTING.md).

### Running Tests

After cloning RabbitMQ umbrella repository, change into the `rabbitmq-mqtt` directory
and run

    make tests

This will bring up a RabbitMQ node with the plugin enabled and run integration tests
against it. Note that there must be no other MQTT server running on ports `1883` and `8883`.

## Virtual host deletion

Retainers and local retained-store files are bound to the virtual host id.
Deleting and recreating a virtual host with the same name starts an empty store.
Deletion callbacks target the old id, and startup cleanup rechecks metadata
before removing captured files. Metadata errors leave files on disk.

Enabling `rabbitmq_5.0.0` assigns identities to existing virtual hosts and records
which ones own legacy storage. Their retainers and files remain in use. Virtual
hosts created after the flag is enabled use separate storage for each incarnation.
Cleanup retains files whose metadata-store identity cannot be verified, including
files left behind by a node reset. Legacy files are bound locally to their original
metadata store; resetting or joining another cluster uses a separate legacy directory.

Custom backends can implement the optional `open/3`, `delete_vhost/2` and
`remove_leftovers/2` callbacks. Backends without them receive a separate directory
for each new incarnation through the legacy API. They must respect that directory
to provide storage isolation.

## Copyright and License

(c) 2007-2024 Broadcom. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

Released under the [Mozilla Public License](https://www.rabbitmq.com/mpl.html),
the same as RabbitMQ.
