Release: RabbitMQ 3.1.2

Release Highlights
==================

server
------
bug fixes

25636 fix startup failure when using SSL with Erlang/OTP R16B01
25571 fix potential deadlock in application shutdown handling (since 2.1.0)
25567 fix queue crash requeuing in-memory messages (since 2.7.0)
25599 fix queue record leak of exclusive durable queues after forceful
      shutdown (since 3.0.1)
25576 fix bug in queue index where a broker crash between segment
      deletion and journal truncation could prevent the broker from
      subsequently starting (since 2.0.0)
25615 fix duplicate mirror queue mirrors starting on a single node (since 2.6.0)
25588 ensure per-message-TTL is removed when messages are dead-lettered
      (since 3.0.0)
25575 fix bug handling empty rabbit_serial leading to startup failure
      (since 1.7.0)
25640 fix channel crash with a race between basic.ack and basic.cancel
      when prefetch >= 1 (since 3.1.0)
25638 fix leak affecting HA/mirrored queues (since 3.0.0)
25611 improve stack traces when message store crash occurs
25612 fix crashing processes when stopping node as part of a cluster
      (since 2.4.0)


stomp plugin
-------------
bug fixes
25564 fix handling of reply-to for non-temporary queue destinations
      (since 3.1.0)
25566 allow unescaped colons in header values for STOMP 1.0 compatibility
      (since 3.0.0)


management plugin
-----------------
bug fixes
25592 fix bug allowing unprivileged users to see stats for all vhosts
      (since 3.1.0)
25600 fix consumer record leak in the management database (since 2.2.0)
25629 fix memory leak in the presence of long-lived channels and
      short-lived queues (since 3.1.0)
25580 fix bug preventing definitions file from loading if it contained
      a policy from a non default vhost (since 3.1.1)


LDAP plugin
-----------
bug fixes
25573 fix garbled login failure errors (since 2.3.0)


Java client
-----------
bug fixes
25633 fix quoting and escaping in json parser, avoid a potentially
      non-terminating loop and improve error handling (since 2.8.2)
      (thanks to Bradley Peabody)


Erlang client
-------------
bug fixes
25521 fix negotiated frame-max handling, which was being ignored (since 2.0.0)
25489 fix rpc client/server to ensure correlation-ids are valid UTF-8 strings
      (since 2.0.0) (thanks to Daniel White)


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
