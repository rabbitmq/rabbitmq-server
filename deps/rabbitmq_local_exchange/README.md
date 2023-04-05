# RabbitMQ Local Exchange Type

TODO

## Installation

TODO

## Building from Source

Please see [RabbitMQ Plugin Development guide](https://www.rabbitmq.com/plugin-development.html).

To build the plugin:

    git clone git://github.com/rabbitmq/rabbitmq-server
    cd deps/rabbitmq-local-exchange
    make

Then copy all the `*.ez` files inside the `plugins` folder to the [RabbitMQ plugins directory](https://www.rabbitmq.com/relocate.html)
and enable the plugin:

    [sudo] rabbitmq-plugins enable rabbitmq_local_exchange


## Usage

TODO


## License

See [LICENSE](./LICENSE).
