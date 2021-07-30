Release: RabbitMQ 3.1.5

Release Highlights
==================

server
------
bug fixes
25713 fix crash in the delegate mechanism leading to various crashes, and
      intra-cluster incompatibility between RabbitMQ 3.1.4 and other members
      of the 3.1.x series (since 3.1.4)
25700 25710 prevent (harmless) errors being logged when pausing in
      pause_minority mode (since 3.1.0)


LDAP plugin
-----------
bug fixes
25703 prevent channel crash when attempting to retrieve LDAP attribute that
      does not exist (since 2.7.0)


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
