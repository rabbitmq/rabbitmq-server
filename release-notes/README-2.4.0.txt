Release: RabbitMQ 2.4.0

Release Highlights
==================

server
------
bug fixes
- in a cluster, don't fail with an internal-error when re-declaring a
  queue on a connection to a node other than the queue's "home" node
- in a cluster, report a not-found error instead of internal-error
  when attempting to re-declare a durable queue whose node is
  unavailable
- do not ignore the RABBITMQ_LOG_BASE variable on Windows
- fix a bug causing SSL connections to die on Erlang prior to R14
  when using "rabbitmqctl list_connections" with the SSL options
- various minor fixes

enhancements
- greatly speed up routing for topic exchanges with many bindings
- propagate memory alarms across cluster, thus reacting better to
  memory pressure on individual nodes.
- sender-selected distribution (i.e. add support for the CC and BCC
  headers).  See
    https://www.rabbitmq.com/extensions.html#sender-selected-distribution
  for more information.
- server-side consumer cancellation notifications.  See
    https://www.rabbitmq.com/extensions.html#consumer-cancel-notify
  for more information.
- have the server present its AMQP extensions in a "capabilities"
  field in server-properties.  See
    https://www.rabbitmq.com/extensions.html#capabilities
  for more information.
- determine file descriptor limits accurately on Windows, usually
  resulting in much higher limits than previously, which allows more
  connections and improves performance
- indicate in the logs when the file descriptor limit has been reached
  (causing the server to not accept any further connections)
- allow SASL mechanisms to veto themselves based on socket type
- rename rabbitmq.conf to rabbitmq-env.conf, to avoid confusion with
  rabbitmq.config
- improve performance of publisher confirms
- various other minor enhancements and performance improvements


java client
-----------
bug fixes
- prevent stack overflow when connections have large numbers channels
- do not require a working reverse DNS when establishing connections

enhancements
- ConnectionFactory accepts a connection timeout parameter
- allow prioritisation of SASL mechanisms
- support for server-side consumer cancellation notifications
- have the client present its AMQP extensions in a "capabilities"
  field in client-properties
- rename ReturnListener.handleBasicReturn to handleReturn


.net client
-----------
bug fixes
- WCF bindings specified in configuration files are no longer ignored

enhancements
- support for server-side consumer cancellation notifications
- have the client present its AMQP extensions in a "capabilities"
  field in client-properties
- support IPv6


management plugin
-----------------
bug fixes
- hide passwords in the web UI
- fix rabbitmqadmin's handling of Unicode strings

enhancements
- present the managed socket and open file counts and respective limits
- better memory usage reporting for hibernating queues
- better support for serving the web interface through a proxy
- allow users to choose which node a queue is declared on
- show memory alarm states for nodes
- show statistics for basic.returns
- publish/receive messages via HTTP; this is intended for testing /
  learning / debugging, not as a general solution for HTTP messaging


STOMP plugin
------------
bug fixes
- prevent crash when publishing from STOMP, but subscribing from
  non-STOMP
- correctly process publishes spanning multiple network packets
- do not crash when publishing with undefined headers
- receipts for SEND frames wait on confirms
- do not issue a DISCONNECT with receipt when a clean shutdown has
  *not* occurred

enhancements
- add documentation. See https://www.rabbitmq.com/stomp.html
- significant performance improvements
- extend flow-control on back pressure through the STOMP gateway
  preventing the STOMP from overloading the server
- support for the "persistent" header
- support for multiple NACK


SSL authentication mechanism plugin
-----------------------------------
enhancements
- only offer this mechanism on SSL connections


build and packaging
-------------------
enhancements
- Windows installer for the broker
- remove the rabbitmq-multi script in order to simplify startup and
  improve error reporting
- add the "cond-restart" and "try-restart" options to the init script
- specify runlevels in the rabbitmq-server.init script
- make the java client jar an OSGi bundle
- Debian package only depends on erlang-nox

Upgrading
=========
To upgrade a non-clustered RabbitMQ from release 2.1.1 or later, simply
install the new version. All configuration and persistent message data
is retained.

To upgrade a non-clustered RabbitMQ from release 2.1.0, first upgrade
to 2.1.1 (which retains all data), and then to the current version as
described above.

To upgrade a clustered RabbitMQ or from releases prior to 2.1.0, if
the RabbitMQ installation does not contain any important data then
simply install the new version. RabbitMQ will move the existing data
to a backup location before creating a fresh, empty database. A
warning is recorded in the logs. If your RabbitMQ installation
contains important data then we recommend you contact
rabbitmq-sales@pivotal.io for assistance with the upgrade.
