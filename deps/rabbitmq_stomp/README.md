# RabbitMQ STOMP adapter

## Introduction

This is a STOMP adapter plugin for use with RabbitMQ.

 - <http://stomp.codehaus.org/>
 - <http://www.rabbitmq.com/>
 - <https://dev.rabbitmq.com/wiki/StompGateway>

You can get the code by checking it out from our repository with

    hg clone http://hg.rabbitmq.com/rabbitmq-stomp/

Please make sure that after you have cloned the repository you update
it to the correct tag for your RabbitMQ server version -- see below
for details.

Announcements regarding the adapter are periodically made on the
RabbitMQ mailing list and on LShift's blog.

 - <http://lists.rabbitmq.com/cgi-bin/mailman/listinfo/rabbitmq-discuss>
 - <http://www.lshift.net/blog/>
 - <http://www.lshift.net/blog/category/lshift-sw/rabbitmq/>


### Compiling from a Mercurial checkout

(This instructions work only for RabbitMQ 1.7.0 or newer.)

To compile RabbitMQ STOMP adapter plugin, you will need to download
rabbitmq-public-umbrella package:

    hg clone http://hg.rabbitmq.com/rabbitmq-public-umbrella

Umbrella is a placeholder for various packages. You need to actually
download and compile the dependencies. The simplest way is to run:

    cd rabbitmq-public-umbrella
    make co
    make

This will download and compile all the rabbitmq related packages. Actually
you don't have to compile everything, the required packages are only
rabbitmq-codegen, rabbitmq-server and rabbitmq-stomp.


If you want to compile a plugin for a specific release of the broker,
you just need to update mercurial repository to a proper tag. To do
that you can say from the umbrella directory:

    hg -R rabbitmq-codegen  up rabbitmq_v1_X_X
    hg -R rabbitmq-server   up rabbitmq_v1_X_X
    hg -R rabbitmq-stomp    up rabbitmq_v1_X_X


### Building plugin package

To build a plugin package (*.ez file), run 'make package' from the
rabbitmq-stomp directory. Package should appear in 'dist' directory.

    cd rabbitmq-stomp
    make package
    ls dist/rabbitmq_stomp.ez


To install and activate package, please follow the instructions from
Plugin Development Guide:
    http://www.rabbitmq.com/plugin-development.html#activating-a-plugin

You need to install rabbit_stomp.ez package.

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

If you are working with the full source code for the RabbitMQ server,
and you have the `../rabbitmq-server` directory you can simply say `make run`:

    make run

If this is successful, you should end up with `starting
STOMP Adapter ...done` and `broker running` in your terminal.


## Running tests and code coverage

To run simplistic test suite and see the code coverage type:

    make cover

After successfull run, you should be able to see output similar to:

    ............
    ----------------------------------------------------------------------
    Ran 12 tests in 0.300s
    [...]
    **** Code coverage ****
     54.55 rabbit_stomp
     80.88 rabbit_stomp_frame
     74.87 rabbit_stomp_server
    100.00 rabbit_stomp_sup
     75.72 'TOTAL'

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
