Release: RabbitMQ 2.1.1

Release Highlights
==================

server
------
enhancements
 - add exchange to exchange bindings. See
   www.rabbitmq.com/extensions.html#exchange-bindings. Blog post forthcoming.
 - reduce disk use when creating and deleting queues
 - faster connection termination and queue deletion for connections
   that use exclusive queues
 - miscellaneous persister performance improvements
 - extend queue leases on declaration
 - add 'client_flow' channel info item for 'rabbitmqctl list_channels'
 - add SSL information for 'rabbitmqctl list_connections'
 - enforce restrictions regarding the default exchange
 - add version information to database - for future upgrades
 - better memory detection on AIX

bug fixes
 - fix a bug that could kill rabbit after a queue.purge
 - fix a bug which could cause 'rabbitmqctl list_connections' to crash
   some of the connection handlers
 - reduce per-queue memory back to expected levels
 - don't ignore channel.flow when there were no consumers
 - fix some bugs that caused too few or too many stats to be emitted

java client
-----------
bug fixes
 - eliminate the possibility of deadlock when opening channels at the
   same times as others are being closed
 - move heartbeat sender into a separate thread to ensure that missing
   heartbeats are detected promptly in all cases

.net client
-----------
enhancements
 - added a means to detect when channel.flow is active

building & packaging
--------------------
enhancements
 - better use of dialyzer: report more warnings
 - better dependency handling in server build, reducing rebuilds


Upgrading
=========
The database schema has not changed since version 2.1.0, so user accounts,
durable exchanges and queues, and persistent messages will all be retained
during the upgrade.

If, however, you are upgrading from a release prior to 2.1.0, when the
RabbitMQ server detects the presence of an old database, it moves it to a
backup location, creates a fresh, empty database, and logs a warning. If
your RabbitMQ installation contains important data then we recommend you
contact rabbitmq-sales@pivotal.io for assistance with the upgrade.
