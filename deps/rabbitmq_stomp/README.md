# RabbitMQ STOMP adapter

## Introduction

This is a STOMP adapter plugin for use with RabbitMQ.

 - <http://stomp.github.com/>
 - <http://www.rabbitmq.com/>

Announcements regarding the adapter are periodically made on the
RabbitMQ blog and mailing list.

 - <http://lists.rabbitmq.com/cgi-bin/mailman/listinfo/rabbitmq-discuss>
 - <http://www.rabbitmq.com/blog/>

## Installing from binary

Binary packages for the STOMP adapter can be found on the
[plugins page](http://www.rabbitmq.com/plugins.html).

Instructions for installing binary plugins can be found in the
[Admin Guide](http://www.rabbitmq.com/admin-guide.html#plugins).


## Compiling and installing from source

To build the STOMP adapter from source, follow the instructions for
building the umbrella repository contained in the
[Plugin Development Guide](http://www.rabbitmq.com/plugin-development.html).

You need to install the rabbit\_stomp.ez and amqp\_client.ez packages.

## Running the STOMP adapter

When no configuration is specified the STOMP Adapter will listen on
all interfaces on port 61613.

To change this, edit your [Configuration file](http://www.rabbitmq.com/install.html#configfile),
to contain a tcp_listeners variable for the rabbit_stomp application.

For example, a complete configuration file which changes the listener
port to 12345 would look like:

[
  {rabbit_stomp, [{tcp_listeners, [12345]} ]}
].

while one which changes the listener to listen only on localhost (for
both IPv4 and IPv6) would look like:

[
  {rabbit_stomp, [{tcp_listeners, [{"127.0.0.1", 61613},
                                   {"::1",       61613} ]} ]}
].

### Checking that the adapter is running

If the adapter is running, you should be able to connect to port 61613
using a STOMP client of your choice. In a pinch, `telnet` or netcat
(`nc`) will do nicely:

    $ nc localhost 61613
    dummy
    dummy
    ERROR
    message:Invalid frame
    content-type:text/plain
    content-length:22

    Could not parse frame
    $

That `ERROR` message indicates that the adapter is listening and
attempting to parse STOMP frames.

Another option is to try out the examples that come with the STOMP
adapter -- see below.

### Running the adapter during development

If you checked out and built the `rabbitmq-public-umbrella` tree as
per the instructions in the Plugin Development Guide, then you can run
RabbitMQ with the STOMP adapter directly from the source tree:

    cd rabbitmq-public-umbrella/rabbitmq-stomp
    make run

If this is successful, you should end up with `starting
STOMP Adapter ...done` and `broker running` in your terminal.


## Running tests and code coverage

To run a simplistic test suite and see the code coverage type:

    make cover

After a successful run, you should see the `OK` message followed by
the code coverage summary.

The view the code coverage details, see the html files in the `cover` directory.

## Usage

The STOMP adapter currently supports the 1.0 version of the STOMP
specification which can be found
[here](http://stomp.github.com/stomp-specification-1.0.html).

The STOMP specification does not prescribe what kinds of destinations
a broker must support, instead the value of the `destination` header
in `SEND` and `MESSAGE` frames is broker-specific. The RabbitMQ STOMP
adapter supports three kinds of destination: `/exchange`, `/queue` and
`/topic`.

### Exchange Destinations

Any exchange/queue or exchange/routing key combination can be accessed
using destinations prefixed with `/exchange`.

For `SUBSCRIBE` frames, a destination of the form
`/exchange/<name>[/<pattern>]` can be used. This destination:

1. creates an exclusive, auto-delete queue on `<name>` exchange;
2. if `<pattern>` is supplied, binds the queue to `<name>` exchange
   using `<pattern>`; and
3. registers a subscription against the queue, for the current STOMP session.

For `SEND` frames, a destination of the form
`/exchange/<name>[/<routing-key>]` can be used. This destination:

1. sends to exchange `<name>` with the routing key `<routing-key>`.

### Queue Destinations

For simple queues destinations of the form `/queue/<name>` can be
used.

For both `SEND` and `SUBSCRIBE` frames, these destinations create
the queue `<name>`.

For `SEND` frames, the message is sent to the default exchange
with the routing key `<name>`. For `SUBSCRIBE` frames, a subscription
against the queue `<name>` is created for the current STOMP
session.

### Topic Destinations

For simple topic destinations which deliver a copy of each message to
all active subscribers, destinations of the form `/topic/<name>` can
be used. Topic destinations support all the routing patterns of AMQP
topic exchanges.

For `SEND` frames, the message is sent to the `amq.topic` exchange
with the routing key `<name>`.

For `SUBSCRIBE` frames, an exclusive queue is created and bound to the
`amq.topic` exchange with routing key `<name>`. A subscription is
created against the exclusive queue.

## Running the examples

### Ruby

At this point you can try out the service - for instance, you can run
the Ruby examples if you have Ruby and rubygems handy:

    sudo apt-get install ruby
    sudo apt-get install rubygems
    sudo gem install stomp
    ruby examples/ruby/cb-receiver.rb

and in another window

    ruby examples/ruby/cb-sender.rb

It will transfer 10,000 short messages, and end up displaying

    ...
    Test Message number 9998
    Test Message number 9999
    All Done!

in the receiver-side terminal.

### Ruby Topic Examples

You can test topic publishing using the `topic-sender.rb` and
`topic-broadcast-receiver.rb` scripts.

The `topic-sender.rb` script sends one message to each of the
`/topic/x.y`, `/topic/x.z` and `/topic/x` destinations. The
`topic-broadcast-receiver.rb` script subscribes to a configurable
topic, defaulting to `/topic/x`.

Start the receiver with no extra arguments, and you'll see it bind to
the default topic:

    ruby examples/ruby/topic-broadcast-receiver.rb
    Binding to /topic/x

Now start the sender:

    ruby examples/ruby/topic-sender.rb

In the receiver-side terminal, you'll see that one message comes
through, the one sent to `/topic/x`.

Stop the receiver and start it, specifying an argument of `x.*`:

    ruby examples/ruby/topic-broadcast-receiver.rb x.*
    Binding to /topic/x.*

Run the sender again, and this time the receiver-side terminal will
show two messages: the ones sent to `/topic/x.y` and
`/topic/x.z`. Restart the receiver again, this time specifying an
argument of `x.#`:

    ruby topic-broadcast-receiver.rb x.#
    Binding to /topic/x.#

Run the sender one more time, and this time the receiver-side terminal
will show all three messages.

### Perl

    $ sudo cpan -i Net::Stomp

The examples are those from the `Net::Stomp` documentation - run
`perldoc Net::Stomp` to read the originals.

Run the receiver before the sender to make sure the queue exists at
the moment the send takes place. In one terminal window, start the
receiver:

    $ perl examples/perl/rabbitmq_stomp_recv.pl

In another terminal window, run the sender:

    $ perl examples/perl/rabbitmq_stomp_send.pl
    $ perl examples/perl/rabbitmq_stomp_send.pl "hello world"
    $ perl examples/perl/rabbitmq_stomp_send.pl QUIT

The receiver's window should contain the received messages:

    $ perl examples/perl/rabbitmq_stomp_recv.pl
    test message
    hello
    QUIT
    $
