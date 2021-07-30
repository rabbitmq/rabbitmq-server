Release: RabbitMQ 2.7.0

Release Highlights
==================

server
------
bug fixes
- acknowledgements were not properly handled on transaction rollback
- could not declare a mirrored queue with a policy of "nodes" and an explicit
  list of node names
- queues created by different client libraries could look inequivalent to the
  broker, though they had equivalent properties
- queue process monitors were not removed correctly
- server start up could hang when trying to contact other Erlang nodes in some
  network configurations
- on Windows some batch file variables might pass unescaped backslashes to the
  broker, causing it to crash

enhancements
- messages re-queued (as a result of a consumer dying, for example) have their
  original order preserved
- in large queues under load, reduce length of time messages already on disk are
  retained in memory
- on platforms which support the High Performance Erlang Compiler (HiPE), the
  server can optionally (re)compile selected modules on startup for increased
  run-time performance; see https://www.rabbitmq.com/configure.html
- the server automatically adapts to changes to virtual memory resources, and to
  the memory high-watermark
- the rabbit logs are appended to on restart; log rotation is simplified
- improved synchronisation between rabbitmqctl and the server when stopping
- non-query actions initiated by rabbitmqctl are logged
- creating a connection is faster
- shutdown is more efficient, especially when there are many queues to delete
- concurrent message storage operations for many queues are more efficient
- durable queues are faster on first use, and faster to recover
- messages removed before being written to disk have the writes eliminated,
  increasing message throughput under load
- performance improvements to queues with large numbers of consumers with
  low prefetch counts
- internal flow control is more consistent
- various other general performance improvements

clients
-------
bug fixes
- connection and channel closes in the clients had internal timeouts which
  could expire prematurely and spoil the client's view of the channel state

enhancements
- clients accept a new "amqp" URI scheme, which can describe all of the
  information required to connect to an AMQP server in one URI; see
  https://www.rabbitmq.com/uri-spec.html

erlang client
-------------
bug fixes
- under some circumstances wait_for_confirms/1 could fail to return

enhancements
- a connection timeout value can be set for Erlang client connections
- socket options may be specified on connection start

java client
-----------
enhancements
- consumer callbacks, and channel operations are threadsafe; calls to channel
  operations can be safely made from a Consumer method call; Consumer callback
  work threads can be user-supplied
- channel or connection errors that refer to another method frame provide the
  method's AMQP name (if it has one) in the error message

.net client
-----------
bug fixes
- some client methods were not documented correctly

plugins
-------
bug fixes
- HTTP-based plugins did not shut down correctly when stopped independently of
  the Erlang VM

enhancements
- plugins are included in the main rabbitmq-server release, simplifying server
  configuration and upgrades; a new tool, rabbitmq-plugins, enables and
  disables plugins; see https://www.rabbitmq.com/plugins.html
- rabbitmq_federation is no longer considered experimental
- new experimental plugin: rabbitmq_consistent_hash_exchange, useful for load
  balancing very high message rates across multiple queues
- new experimental plugin: rabbitmq_tracing, a management UI for the firehose

management plugin
-----------------
bug fixes
- queue details page failed to display on recent browsers (e.g. Firefox 6) for
  High Availability queues

enhancements
- more detailed global memory statistics shown
- "all configuration" is renamed to "definitions" to reduce confusion with
  rabbitmq.config

auth-backend-ldap plugin
------------------------
enhancements
- the queries are extended to include attributes and allow pattern-matching

mochiweb plugin
---------------
enhancements
- the limit on upload size is increased to 100MB so that JSON-RPC channel can
  publish larger messages

STOMP adapter
-------------
bug fixes
- the STOMP adapter could crash when exceeding the memory high watermark

build and packaging
-------------------
bug fixes
- on non-Windows platforms invoking rabbitmq as a daemon could leave standard
  input and output streams permanently open

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
important data then we recommend you contact support at rabbitmq.com for
assistance with the upgrade.
