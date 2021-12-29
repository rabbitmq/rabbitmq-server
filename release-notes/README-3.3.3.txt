Release: RabbitMQ 3.3.3

Release Highlights
==================

server
------
bug fixes
26236 prevent log files from being silenced if certain processes crash
      (since 3.3.2)
26241 fix disk space monitor crash when using {mem_relative, Ratio}
      configuration (since 3.2.0)
24759 run shell scripts with '-e' (since 1.0.0)


STOMP plugin
------------
bug fixes
26238 fix queue leak on subscription to /exchange/<name>/<binding> when the
      exchange does not exist (since 2.0.0)


Java client
-----------
bug fixes
26232 ensure channel shutdown listeners are not lost on connection recovery
      (since 3.3.0)


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
