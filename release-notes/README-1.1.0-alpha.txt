Release   : RabbitMQ 1.1.0-alpha

Release Highlights
==================

RabbitMQ server
---------------
- support for clustering and load balancing
- near-linear scaling across multiple nodes on fanout
- various performance improvements
- more resilient startup sequence
- improved error reporting and logging, particularly during startup
- improved test coverage
- improved protocol conformance, particluarly in the area of
  connection and channel management
- fixed a number of race conditions in corner cases of the protocol
- made several parameters, such as tcp buffer size, more easily
  adjustable
- supervision tree for most of the processes, for improved resilience
- better support for building under Windows
- new rabbmitqctl commands, for clustering, broker status, and more
- improved rabbitmqctl success and failure reporting 
- improved documentation for build, install, administration
- tested against latest Erlang/OTP release - R11B-5

Java client
-----------
- support for clustering and load balancing
- improved and better documented API
- improved error handling and reporting
- new QueuingConsumer class for easy asynchronous message processing
- restructured internals
- fixed several race conditions and deadlocks, and some other bugs
- support for build under Java 6 and under Windows

Packaging
---------
- more resilient and easier configurable startup scripts
- fixed several bugs in Debian packaging
- RabbitMQ now runs as a separate user
- new Fedora and Suse RPM packages
- new Debian and RPM packages for Java client
- "binary" Java client packages for Java >=1.5 and <1.5
- streamlined packaging process


Upgrading
=========

If you are upgrading from an earlier release of RabbitMQ, note that
RabbitMQ's internal database schema has changed, and hence the
MNESIA_BASE directory should be cleared before starting the new
server. This wipes out any configuration information and persisted
messages.

The upgrade process will be much improved in future releases, to the
point where a running RabbitMQ cluster can be upgraded without service
interruption. Meanwhile, if you need assistance in migration please
contact the RabbitMQ team at rabbitmq-sales@pivotal.io.


Credits
=======

We would like to thank the following individuals for submitting bug
reports and feedback that we incorporated into this release:

Antonio Cordova
Carl Bourne
David Pollack
David MacIver
Francesco Cesarini
Gerald Loeffler
Hunter Morris
Jason Pellerin
Jeff Rogers
Martin Logan
Matt Darling
Michael Newton
Neil Bartlett
Norbert Klamann
Robert Greig
Wannes Sels
Warren Pfeffer
