Release: RabbitMQ 1.1.1
Status : beta

Release Highlights
==================

RabbitMQ server
---------------
- improved interoperability with Qpid M1 clients
- fixed a bug in persistent message re-delivery that caused RabbitMQ
  to fail when attempting to re-deliver messages after a restart
- fixed a performance problem that caused throughput to drop
  significantly for larger message sizes
- fixed a bug in amqqueue:stat_all/0 that caused it to fail
- refactored some internals in order to support additional transports
  more easily

Java client
-----------
- improved interoperability with Qpid M1 Java server
- changed threading model to stop clients from exiting when there are
  open AMQP connections
- extended API to allow setting of frameMax and channelMax

Packaging
---------
- included main test suite runner in source distribution
- dropped version status (i.e. alpha, beta, etc) from file and dir
  names
- renamed server erlang package dir to "rabbitmq_server-<version>", to
  comply with Erlang/OTP conventions


Upgrading
=========

Upgrading to this release from RabbitMQ 1.1.0 requires no special
steps at the server end. There have been some minor changes to the
Java client API. Most client code is unlikely to be affected by these,
and is easily changed if it is.

If you are upgrading from an earlier release of RabbitMQ, note that
RabbitMQ's internal database schema has changed, and hence the
MNESIA_BASE directory should be cleared before starting the new
server. This wipes out any configuration information and persisted
messages.


Credits
=======

We would like to thank the following individuals for submitting bug
reports and feedback that we incorporated into this release:

Ben Hood
James Wolstenholme
Jeff Rogers
Maximillian Dornseif
Michael Arnoldus
Steve Jenson
Tanmay Goel
