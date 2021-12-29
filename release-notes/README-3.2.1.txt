Release: RabbitMQ 3.2.1

Release Highlights
==================

server
------
bug fixes
25849 fix crash with {down_from_gm,down_from_gm} with multiply-mirrored queues
      (since 3.2.0)
25846 fix queue crashes when changing multiple HA policies simultaneously
      (since 3.0.0)
25618 ensure a mirrored queue mirror which crashes does not cause the leader replica to
      crash (since 2.6.0)
25838 prevent crashes due to timeouts when calling into the limiter
      (since 3.1.0)
25842 treat 32 bit Unix platforms as limited to 2GB not 4GB address space
      (since 1.7.1)
25845 reduce default heartbeat from 600s to 580s for better compatibility
      with common load balancer configurations (since 3.0.0)
25826 fix incorrect placement of file sync which could theoretically
      corrupt files when written just before a crash (since 3.0.0)


building / packaging
--------------------
bug fixes
25835 ship a useful README in /usr/share/doc for Debian and RPM (since 1.0.0)


management plugin
-----------------
bug fixes
25811 fix web UI authentication via the initial URI for browsers which are
      not Chrome (since 3.2.0)
25861 fix web UI login when user name or password contains '%' (since 3.2.0)


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
