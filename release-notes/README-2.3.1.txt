Release: RabbitMQ 2.3.1

Release Highlights
==================

server
------
bug fixes
- fix critical bug causing queue processes to sometimes crash when
  using transactions or confirms
- improve error message when failing to declare a queue or exchange due
  to argument equivalence

java client
-----------
bug fixes
- fix race condition closing a channel


Upgrading
=========
To upgrade a non-clustered RabbitMQ from release 2.1.1 or later, simply
install the new version. All configuration and persistent message data
is retained.

To upgrade a non-clustered RabbitMQ from release 2.1.0, first upgrade
to 2.1.1 (which retains all data), and then to the current version as
described above.

To upgrade a clustered RabbitMQ or from releases prior to 2.1.0, if
the RabbitMQ installation does not contain any important data then
simply install the new version. RabbitMQ will move the existing data
to a backup location before creating a fresh, empty database. A
warning is recorded in the logs. If your RabbitMQ installation
contains important data then we recommend you contact
rabbitmq-sales@pivotal.io for assistance with the upgrade.
