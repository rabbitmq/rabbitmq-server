Release: RabbitMQ 3.4.4

You can find RabbitMQ change log at https://www.rabbitmq.com/changelog.html.

Release Highlights
==================

server
------
bug fixes
26564 ensure that a mirrored queue declaration only returns when all mirrors
      are running (since 2.6.0)
26549 Failure to start if AMQP port > 45535 (since 3.3.0)
26570 policy change on idle queue might not be reported in a timely manner
      (since 3.0.0)
26558 rabbitmq-plugins should respect RABBITMQ_CTL_ERL_ARGS (since 3.4.0)
26562 rabbitmq-env uses "--fqdn" which is specific to net-tools' (i.e. Linux)
      hostname(1) (since 3.4.0)
26144 Windows scripts should respect RABBITMQ_NODE_ONLY (since 1.0.0)


building & packaging
--------------------
bug fixes
26443 Mnesia directory world-readable on deb / RPM (since 1.0.0)
26584 Windows installer should install new version after uninstalling
      the existing one (since 1.0.0)


management plugin
-----------------
bug fixes
26533 Specifying SSL version list for Mochiweb causes
      rabbit_mgmt_external_stats to crash (since 2.6.0)
26541 Overview page doesn't work with IE <= 8 (since 3.4.0)


Java client
-----------
bug fixes
26552 Bindings for non-durable queues are not recovered (since 3.3.0)
26580 WorkPool.WorkQueue still deadlock-prone (since 3.4.0)
26523 PerfTest --cmessages off-by-one error (fail to ack last message)
      (since 3.4.3)


.NET client
-----------
bug fixes
26588 API reference generator should work with .NET 4.0+ (since 1.0.0)
26590 .NET client .msi installer should work with WiX 4.0 (since 1.0.0)

dependency change
The client now requires .NET 4.0.


MQTT plugin
-----------
bug fixes
26567 Last Will and Testament should be sent in case of keep-alive timeout
      (since 3.0.0)
26589 MQTT processor should link its channel (since 3.0.0)


STOMP plugin
------------
bug fixes
26553 Unexpected authorisation errors may result in client connections staying open
26282 Improve error messages for STOMP connection failures
      (TLS issues, abrupt TCP connection closures) (since 3.3.3)
26559 STOMP reader should handle system messages (since 1.4.0)


AMQP 1.0 plugin
---------------
bug fixes
26587 Failure to create resources when producing / consuming not well
      handled (since 3.1.0)


LDAP plugin
-----------
bug fixes
26528 [LDAP] template replacement should escape \ and & (since 2.3.0)



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
