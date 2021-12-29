Release: RabbitMQ 3.3.1

Security Fixes
==============

MQTT plugin
-----------
26109 prevent potential DOS attack on SSL handshake failure (since 3.0.0)


shovel plugin
-------------
26100 prevent dynamic shovels from allowing policymaker users to access vhosts
      they should not be able to (since 3.3.0)


Release Highlights
==================

server
------
bug fixes
26084 fix race condition causing queue mirrors to occasionally not be promoted
      on clean leader replica shutdown (since 3.0.0)
26115 prevent badmatch crash on mirror queue init during rapid restarts
      (since 3.2.0)
26117 prevent mirror being incorrectly considered dead when added at the same
      time as another mirror dies (since 3.2.0)
26118 prevent mirror queue crash if a queue hibernates before it has fully
      started (since 3.2.2)
26125 prevent possible deadlock when mirror becomes synchronised
      simultaneously with another mirror starting (since 3.1.0)
26103 ensure dead-letter cycle detection works when dead-lettering messages due
      to exceeding the queue's max-length (since 3.1.0)
26123 ensure worker pool state does not become corrupt if a worker process
      crashes while idle (since 1.8.0)
25855 ensure disk monitor does not crash the broker on platforms where
      intermediate OS processes may be killed such as Google Compute Engine
      (since 2.8.2)
26096 clarify rabbitmqctl diagnostic messages (since 3.3.0)
26102 prevent cosmetic GM crash on clean shutdown (since 2.8.7)
26104 fix format of queue synchronisation log messages (since 3.3.0)
26114 ensure crash report shrinking does not shrink reports too much
      (since 3.3.0)

enhancements
26098 bring back 'impersonator' tag removed in 3.3.0
26113 add a capability to allow clients to detect the new qos semantics
      introduced in 3.3.0 without requiring an explicit version check


management plugin
-----------------
bug fixes
26140 prevent malformed message being created when publishing with priority
      or timestamp properties set (since 2.4.0)
26110 ensure statistics database GC works in a timely manner when the number
      of objects tracked grows rapidly (since 3.1.0)
26124 prevent "" being added as the last element of an array when adding
      an array to queue or exchange arguments via the web UI (since 3.2.0)
26127 ensure that statistics database startup does not block broker startup
      for O(queues) time (since 2.8.0)
26134 improve diagnostics when failing to count used FDs on {Free,Open,Net}BSD
      (since 2.8.0)


shovel-management plugin
------------------------
bug fixes
26105 allow adding dynamic shovels when there are multiple virtual hosts
      and show correct UI to users with policymaker and monitoring tags
      (since 3.3.0)


STOMP plugin
------------
bug fixes
26061 reject publishes to destination "", rather than creating a server-named
      queue (since 2.0.0)


Java client
-----------
bug fixes
26111 prevent connection crash on recovery when a connection consumes from
      many queues (since 3.3.0)
26099 clarify AlreadyClosedException.tostring() in the case when the
      connection closed for a non-AMQP reason (since 3.3.0)


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
