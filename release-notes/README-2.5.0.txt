Release: RabbitMQ 2.5.0

Release Highlights
==================

server
------
bug fixes
- reduce complexity of recovery, significantly improving startup times
  when there are large numbers of exchanges or bindings
- recover bindings between durable queues and non-durable exchanges
  on restart of individual cluster nodes
- do not read messages off disk in the x-message-ttl logic. This could
  severely impact performance when many queues expired messages
  (near)simultaneously.
- resolve a timer issue that could impact performance when under high
  load and memory pressure
- make source code compilable with latest Erlang release (R14B03)
- assert x-message-ttl equivalence on queue redeclaration

enhancements
- tracing facility for incoming and outgoing messages - see
  https://www.rabbitmq.com/firehose.html
- optionally serialise events for exchange types
- detect available memory on OpenBSD
- add Windows service description
- improve inbound network performance
- improve routing performance
- new rabbitmqctl commands:
  report - comprehensive report of server status for support purposes
  environment - display application environment (such as config vars)
  cluster_status - display cluster status (formerly part of 'status')

java client
-----------
bug fixes
- compile under Java 1.5 (again)

enhancements
- experimental API employing command objects and builders. See
  http://hg.rabbitmq.com/rabbitmq-java-client/file/default/test/src/com/rabbitmq/client/test/AMQBuilderApiTest.java
  for some examples. Feedback welcome!

.net client
-----------
bug fixes
- make method id of 'exchange.unbind-ok' match definition in the
  broker, so the client lib can recognise that command.
- WCF bindings specified in configuration files are no longer ignored

enhancements
- allow larger than default message sizes in WCF
- updated documentation

management plugin
-----------------
bug fixes
- handle race between queue creation/deletion and stats reporting that
  could result in errors in the latter, particularly when there are
  large numbers of queues and/or high churn
- handle race when starting the management plug-in on multiple cluster
  nodes, which in some rare (but quite reproducible) circumstances
  could cause some of the brokers to crash
- remove duplicate 'messages' entry from queue stats JSON
- make binding arguments optional in the HTTP API for binding creation
- correct error handling in the HTTP API for binding creation
- prevent spurious failures of aliveness test

enhancements
- performance improvements which significantly reduce the cost of
  stats reporting, allowing the management plug-in to cope with much
  higher numbers of queues, bindings, etc.
- issue an alert when a configured user cannot access any vhost or a
  vhost has no users
- allow choice of which stats/info items to return in the HTTP API
- include protocol adapter and direct connections in API and UI
- full STOMP SSL information displayed

rabbitmq-mochiweb
-----------------

enhancements
- more flexible configuration permitting different services to run on
  different ports, SSL support and interface-based restrictions. See
  https://www.rabbitmq.com/mochiweb.html for more details. Note that by
  default the JSON-RPC channel plugin will now listen on port 55670.

STOMP plugin
------------
enhancements
- support connections over SSL
bug fixes
- correct spelling of 'heart-beat' header
- don't drop messages if producer hangs up quickly

build and packaging
-------------------
bug fixes
- fix breakage in /etc/init.d/rabbitmq-server rotate-logs command

enhancements
- plug-in build system: support the declaration of inter-plugin
  dependencies, making development of plugins much easier.
  Inter-module dependencies are calculated automatically for all
  plugins. Note that some plugins and applications have been
  renamed for  consistency, which may require changes to any existing
  `rabbitmq.config` to match.
- do not require access to www.docbook.org when building the server
  w/o docbook installed
- get rid of some warnings in the .net client build


Upgrading
=========
To upgrade a non-clustered RabbitMQ from release 2.1.1 or later, simply
install the new version. All configuration and persistent message data
is retained.

To upgrade a clustered RabbitMQ from release 2.1.1 or later, install
the new version on all the nodes and follow these instructions:
    https://www.rabbitmq.com/clustering.html#upgrading
All configuration and persistent message data is retained.

To upgrade a non-clustered RabbitMQ from release 2.1.0, first upgrade
to 2.1.1 (which retains all data), and then to the current version as
described above.

To upgrade a clustered RabbitMQ prior to 2.1.1 or a stand-alone broker
from releases prior to 2.1.0, if the RabbitMQ installation does not
contain any important data then simply install the new
version. RabbitMQ will move the existing data to a backup location
before creating a fresh, empty database. A warning is recorded in the
logs. If your RabbitMQ installation contains important data then we
recommend you contact rabbitmq-sales@pivotal.io for assistance with the
upgrade.
