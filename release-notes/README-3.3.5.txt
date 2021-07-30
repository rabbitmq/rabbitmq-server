Release: RabbitMQ 3.3.5

Release Highlights
==================

server
------
bug fixes
25921 prevent long delays in publishing after a node goes down and network
      connections to it time out (since 2.8.3)
26225 26293 greatly reduce the length of time between pause_minority mode
      detecting a minority and refusing to accept further publishes
      (since 3.1.0)
26313 do not allow clients to override server-configured channel_max
      (since 3.3.0)
26159 prevent failure to start if memory monitor cannot determine total
      system memory (since 1.7.1)
26290 correctly read /proc/meminfo on Linux even if rows do not contain
      colons (issue with certain vendor kernels) (since 1.7.1)

enhancements
26311 provide a mechanism for diagnosing stuck processes


building & packaging
--------------------
bug fixes
26322 add loopback_users to the sample configuration file (since 3.3.0)


management plugin
-----------------
bug fixes
26072 provide unminimised versions of all bundled Javascript libraries.
      Fixes Debian bug #736781. (since 2.1.0)


management visualiser plugin
----------------------------
bug fixes
26072 provide unminimised versions of all bundled Javascript libraries.
      Fixes Debian bug #736781. (since 2.1.0)


federation plugin
-----------------
bug fixes
26272 ensure changes to cluster name are picked up promptly and thus fix
      cycle detection on cluster name change (since 3.3.0)
26292 ensure that federation links apply the defined reconnect delay under
      all circumstances (since 2.6.0)
26299 fix leak when shrinking upstream-set immediately after federation
      starts (since 3.0.0)


shovel plugin
-------------
bug fixes
26318 prevent dynamic shovel crash using add-forwarding-headers=true
      without setting dest-queue or dest-exchange-key (since 3.3.0)
26292 ensure that shovel workers apply the defined reconnect delay under
      all circumstances (since 2.0.0)
26328 prevent dynamic shovels from failing over to the new node whenever
      a node comes up in a cluster (since 3.3.0)


MQTT plugin
-----------
bug fixes
26270 improve error messages on TLS/TCP connection failure (since 3.0.0)
26281 log cleanly closed MQTT connections as we do for AMQP (since 3.0.0)


AMQP 1.0 plugin
---------------
bug fixes
26288 fix handling of the symbol type in message content (as used in
      content_type and content_encoding) (since 3.1.0)
26288 (also) fix mapping of AMQP 1.0 ttl and creation_time fields to
      AMQP 0-9-1 timestamp and expiration fields (since 3.1.0)


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
