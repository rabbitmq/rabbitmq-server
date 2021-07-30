Release: RabbitMQ 2.7.1

Release Highlights
==================

server
------
bug fixes
- long-running brokers could crash due to global unique identifiers not being
  unique enough
- leader election of mirrored queues could fail when using confirms
- there was a slow memory leak in HA queues with persistent and confirmed
  messages
- when using HA queues with policy of 'nodes', leader replica didn't recover
  properly
- HA queues could fail when nodes were restarting frequently
- broker sometimes hung when closing channels and connection from multiple
  threads
- queue equivalence check did not properly detect different arguments under
  some circumstances
- the broker sometimes hung when recovering queues on startup
- 'rabbitmqctl list_connections' could return incomplete information
- broker-generated queue names did not conform to AMQP syntax rules
- a (harmless) warning was emitted when running under Erlang R15B

enhancements
- deletion of exchanges or queues with many bindings is more efficient
- 'rabbitmqctl eval <expr>' evaluates arbitrary Erlang expressions in the
  broker node

java client
-----------
bug fixes
- resources were not recovered if ConnectionFactory failed to connect
- defaults for the ConnectionFactory class were not public
- part of the Java client API was hidden, causing application build errors
- interrupts were mishandled in the Java threading logic

.net client
-----------
bug fixes
- session autoclose could fail with AlreadyClosedException

plugins
-------
bug fixes
- consistent-hash-exchange mis-routed messages when handling multiple exchanges

management plugin
-----------------
bug fixes
- statistics database could remain down after nodes were restarted
- broker could fail to start if clients attempt to connect before the
  management plugin is fully started
- management plugin could fail to start if there were strange permissions
  in /proc
- overview could sometimes crash when another node starts up or shuts down
- HA mirror synchronisation could sometimes be misrepresented on the
  management UI
- encoding of underscore in URL properties was incomplete
- management interface could break if there were html syntax characters in names
- shovels were not displayed if they were in an undefined state

enhancements
- rate of change of queue lengths added to the management API and UI
- improvements to shovel information formatting

auth-backend-ldap plugin
------------------------
bug fixes
- made compatible with Erlang R15B
enhancements
- accept a broader class of group objects on in_group filter

STOMP adapter
-------------
bug fixes
- duplicate headers were generated in some MESSAGE frames
- temporary reply-to queues were not re-usable
- made compatible with Erlang R15B

build and packaging
-------------------
bug fixes
- rabbitmq-server Mac OS X portfile was incorrectly built
- maven bundle for Java client was not published to maven central


Upgrading
=========
To upgrade a non-clustered RabbitMQ from release 2.1.1 or later, simply install
the new version. All configuration and persistent message data is retained.

To upgrade a clustered RabbitMQ from release 2.1.1 or later, install the new
version on all the nodes and follow the instructions at
https://www.rabbitmq.com/clustering.html#upgrading .

To upgrade RabbitMQ from release 2.1.0, first upgrade to 2.1.1 (all data will be
retained), and then to the current version as described above.

When upgrading from RabbitMQ versions prior to 2.1.0, the existing data will be
moved to a backup location and a fresh, empty database will be created. A
warning is recorded in the logs. If your RabbitMQ installation contains
important data then we recommend you contact <support at rabbitmq.com> for
assistance with the upgrade.
