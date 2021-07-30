Release: RabbitMQ 1.2.0
Status : beta

Release Highlights
==================

RabbitMQ server
---------------
- introduced internal flow control to prevent performance drops when
  running a server near capacity
- simplified cluster configuration and added "automatic" clustering
- made rabbitmqctl command line syntax less confusing
- fixed a couple of race conditions that could result in a client
  receiving unexpected sequences of command replies and messages
- refactored internals to make it easier to construct handlers for
  transports other than raw TCP/IP

Java client
-----------
- fixed a race condition between invocation of a Consumer's
  handle{Consume,Cancel}Ok and handleDelivery methods; the former are
  now called inside the connection's thread, just like the latter

Packaging
---------
- simplified rabbitmqctl invocation under Debian
- moved default location of the log and mnesia dirs under Windows to
  sub directories of the RABBITMQ_BASE directory
- changed startup scripts to allow the rabbitmq_server package to
  reside outside the OTP library directory


Upgrading
=========

Under Windows the default location of the mnesia directory has changed
from %RABBITMQ_BASE% to %RABBITMQ_BASE%\db. If you have an existing
installation that uses the old default location and you would like to
retain the server state (including persisted messages) then just move
the *.DAT, *.DCD, *.DCL and *.LOG files from that directory to the new
location.

There have been some minor changes to the Java client API. Most client
code is unlikely to be affected by these, and is easily changed if it
is.

If you are upgrading from RabbitMQ-1.0.0 note that RabbitMQ's internal
database schema has changed, and hence the MNESIA_BASE directory
should be cleared before starting the new server. This wipes out any
configuration information and persisted messages.


Credits
=======

We would like to thank the following individuals for submitting bug
reports and feedback that we incorporated into this release:

Ben Hood
Emmanuel Okyere
Holger Hoffstätte
Jodi Moran
Robert Greig
