Release: RabbitMQ 3.1.1

Release Highlights
==================

server
------
bug fixes
25545 relax validation of x-match binding to headers exchange for
      compatibility with brokers < 3.1.0
25561 fix bug in ack handling for transactional channels that could
      cause queues to crash
25560 fix race condition in cluster autoheal that could lead to nodes
      failing to re-join the cluster
25546 fix crash when setting a prefetch count multiple times on the
      same channel
25548 fix vhost validation when setting policies and/or parameters
25549 fix x-expires handling after last consumer disconnects
25555 tighten up validation of HA-related policies


shovel plugin
-------------
bug fixes
25542 fix handling of default reconnect_delay


management plugin
-----------------
bug fixes
25536 set auth header correctly when downloading definitions
25543 set message_stats to the correct value when empty


federation-management-plugin
----------------------------
bug fixes
25556 allow multiple URIs to be specified against an upstream


.NET client
-------------
bug fixes
25558 fix a bug that could lead to duplicate channel IDs being allocated,
      causing a connection failure


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
