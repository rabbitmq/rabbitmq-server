Release: RabbitMQ 2.8.7

Release Highlights
==================

server
------
bug fixes
- fix race condition that could stop mirrored queue from sending further
  confirms, and cause it to leak memory
- fix bug that prevented confirms from mirrored queues when x-message-ttl
  was set to zero
- fix mirror synchronisation detection logic in mirrored queues
- fix possible deadlock during broker shutdown
- fix resource leak when declaring many short-lived mirrored queues with
  different names
- fix DOS vulnerability possible by malicious SSL clients
- make disk free space reporting more intelligible

performance improvements
- reduce unnecessary fsync operations when deleting non-durable resources
- mirror nodes of mirrored queues now pro-actively persist acks and messages on
  a timer with a sensible interval


packaging
---------
bug fixes
- ensure source packages can be built without network access


erlang client
-------------
bug fixes
- ensure management plugin is notified when connections fail as soon as
  they are opened

enhancements
- offer configuration flag for ipv4 / ipv6 preference


management plugin
-----------------
bug fixes
- prevent management plugin from crashing nodes when failing over


STOMP plugin
------------
bug fixes
- fix bug that caused alarms (e.g. disk free space checking) to turn off


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
