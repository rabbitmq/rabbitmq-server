# AMQP 1.0 support for RabbitMQ

This plugin adds AMQP 1.0 support to RabbitMQ.  It can be swapped in
for RabbitMQ's standard TCP socket server; in other words, you can
configure it to listen on port 5672 and continue to use 0-8 or 0-9-1
clients as before, as well as 1.0 clients.

# Status

This is a prototype.  You can send and receive messages between 0-9-1
or 0-8 clients and 1.0 clients (all those 1.0 clients that there
are), with broadly the same semantics as you would get with 0-9-1.

# Building and configuring

The plugin uses the standard RabbitMQ plugin build environment; see <http://www.rabbitmq.com/plugin-development.html>.

Currently you need bug23749 of rabbitmq-server and rabbitmq-codegen.

By default, it will listen on port 5673.  However, you may wish to
listen on the standard AMQP port, 5672.  To do this, give RabbitMQ a
configuration that looks like this:

    [{rabbit, [{tcp_listeners, []}]},
     {rabbitmq_amqp1_0, [{tcp_listeners, [{"0.0.0.0", 5672}]}]}].

It will then serve AMQP 0-8, 0-9-1, and 1.0 on the socket.
