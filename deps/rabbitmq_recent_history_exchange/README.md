# RabbitMQ Recent History Cache

Keeps track of the last 20 messages that passed through the exchange. Every time a queue is bound to the exchange it delivers that last 20 messages to them. This is useful for implementing a very simple __Chat History__ where clients that join the conversation can get the latest messages.

Exchange Type: `x-recent-history`

## Installation

Install and setup the RabbitMQ Public Umbrella as explained here: [http://www.rabbitmq.com/plugin-development.html#getting-started](http://www.rabbitmq.com/plugin-development.html#getting-started).

Then `cd` into the umbrella folder and type:

    $ git clone git://github.com/videlalvaro/rabbitmq-recent-history-exchange.git
    $ cd rabbitmq-recent-history-exchange
    $ make

Finally copu all the `*.ez` files inside the `dist` folder to the `$RABBITMQ_HOME/plugins` folder. Don't copy the file `rabbit_common-x.y.z` since it's not needed inside the broker installation.

## License

See LICENSE.md
