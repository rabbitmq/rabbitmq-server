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

Binary packages for the STOMP adapter can be found on the [plugins page](http://www.rabbitmq.com/plugins.html).

Instructions for installing binary plugins can be in the [Admin Guide](http://www.rabbitmq.com/admin-guide.html#plugins).


## Compiling and installing from source

To build the STOMP adapter from source, follow the instructions for building the umbrella repository contained in the [Plugin Development Guide](http://www.rabbitmq.com/plugin-development.html).

You need to install the rabbit\_stomp.ez and amqp\_client.ez packages.

## Running the STOMP adapter

### Configuring the server to start the plugin automatically

Most RabbitMQ server packages are set up to cause the server to pick
up configuration from `/etc/rabbitmq/rabbitmq.conf`. To tell the
server to start your plugin, first make sure it is compiled, and then
add the following text to `/etc/rabbitmq/rabbitmq.conf`:

    SERVER_START_ARGS='-rabbit_stomp listeners [{"0.0.0.0",61613}]'

Then restart the server with

    sudo /etc/init.d/rabbitmq-server restart


When no configuration is specified STOMP Adapter will listen on localhost by
default.

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

If you checked out and built the `rabbitmq-public-umbrella` tree as per the instructions in the Plugin Development Guide, then you can run RabbitMQ with STOMP adapter directly from the source tree:

    cd rabbitmq-public-umbrella/rabbitmq-stomp
    make run

If this is successful, you should end up with `starting
STOMP Adapter ...done` and `broker running` in your terminal.


## Running tests and code coverage

To run simplistic test suite and see the code coverage type:

    make cover

After successful run, you should be able to see output similar to:

    ............
    ----------------------------------------------------------------------
    Ran 32 tests in 38.309s

    OK

    **** Code coverage ****
     54.55 rabbit_stomp
     75.00 rabbit_stomp_frame
     84.24 rabbit_stomp_server
    100.00 rabbit_stomp_sup
     89.29 rabbit_stomp_util
     81.73 'TOTAL'

The view the code coverage, see html files in .cover:

    ls ./cover

    rabbit_stomp_frame.html
    rabbit_stomp.html
    rabbit_stomp_server.html
    rabbit_stomp_sup.html
    summary.txt


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
