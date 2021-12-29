Release: RabbitMQ 2.8.2

Release Highlights
==================

server
------
bug fixes
- memory leak on basic.reject{requeue = false}
- queue failure when dead-lettering a message which has been paged to disc
- memory leak with dead-letter cycles and HA queues
- possible message loss with dead-lettered messages and HA queues at broker
  shutdown
- HA mirror queue crash on basic.reject{requeue = false}
- HA mirror queue crash with dead-letter cycle
- messages might incorrectly not expire if consumed immediately and then
  requeued
- message acks might be lost if sent immediately before channel closure
- dead-lettering a large number of messages at once was very slow
- memory leak when queue references a DLX that does not exist
- fix startup in FreeBSD jail without IPv6 support (thanks to Mattias Ahlbäck)
- error logged by mirrored queue supervisor on shutdown

enhancements
- disc space monitoring and blocking, similar to the existing memory monitoring
- substantially improve performance publishing large messages
- improve performance delivering small messages
- improve performance of routing via the default exchange
- allow x-message-ttl to be set to 0 (useful as an alternative to
  immediate-mode publish)
- ensure unacked messages have been requeued by the time channel.close_ok is
  received
- remove non-free RFC from source package (Debian bug #665793)


packaging
---------
bug fixes
- Debian: provide a mechanism to change the FD limit
- Debian: don't delete the rabbitmq user when removing package (#663503,
          #620799, #646175)
- Debian: use lsb-base functions in init script (#663434)


java client
-----------
bug fixes
- RpcClient used the platform default encoding instead of UTF-8
- waitForConfirms returned true even if basic.nack received

enhancements
- MulticastMain supports option to multi-ack every N messages


erlang client
-------------
bug fixes
- crash on shutdown of AMQP connection


management plugin
-----------------
bug fixes
- management plugin could miss queue deletion events in some crash scenarios
- dependency on xmerl was not declared

enhancements
- show a large warning when running a cluster with mixed RabbitMQ / Erlang
  versions


federation plugin
-----------------
bug fixes
- leak of direct connections when federation links failed to start up

enhancements
- prevent messages which have reached max_hops from being transmitted one
  additional time, using a custom upstream exchange
- link status reporting, similar to that provided by rabbitmq-shovel, from
  the command line and management plugin


STOMP plugin
------------
bug fixes
- "destination" header on MESSAGE did not match that used on SEND


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
