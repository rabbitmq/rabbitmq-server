Release: RabbitMQ 2.4.1

Release Highlights
==================

server
------
bug fixes
- fix breakage of upgrades when durable queues are present or
  following a non-clean shutdown
- prevent "rabbitmqctl wait" from waiting forever in certain
  circumstances
- the broker can be run on Erlang R12B-3 again
- some other small bug fixes

enhancements
- upgrades in clusters. See
    https://www.rabbitmq.com/clustering.html#upgrading
- improve memory usage when dealing with persistent messages waiting
  on acks from consumers
- better error reporting for some startup problems
- add timestamp to events published to the amq.rabbit.log exchange


java client
-----------
enhancements
- remove dependency on javax.security.sasl, thus improving
  compatibility with Android and WebSphere


.net client
-----------
bug fixes
- the client can be built on .NET 2.0 again


management plugin
-----------------
bug fixes
- fix issue that would cause non-admin users to be repeatedly prompted
  for their password when viewing the queues page


STOMP plugin
------------
bug fixes
- the plugin works on Erlang R12 again


SSL authentication mechanism plugin
-----------------------------------
bug fixes
- accept SSL certificates with fields containing characters
  outside ASN.1 PrintableString (e.g. underscores)


build and packaging
-------------------
bug fixes
- the OCF script works correctly when specifying an alternative
  configuration file


Upgrading
=========
To upgrade a non-clustered RabbitMQ from release 2.1.1 or later, simply
install the new version. All configuration and persistent message data
is retained.

To upgrade a clustered RabbitMQ from release 2.1.1 or later, install
the new version on all the nodes and follow these instructions:
    https://www.rabbitmq.com/clustering.html#upgrading
All configuration and persistent message data is retained.

To upgrade a non-clustered RabbitMQ from release 2.1.0, first upgrade
to 2.1.1 (which retains all data), and then to the current version as
described above.

To upgrade a clustered RabbitMQ prior to 2.1.1 or a stand-alone broker
from releases prior to 2.1.0, if the RabbitMQ installation does not
contain any important data then simply install the new
version. RabbitMQ will move the existing data to a backup location
before creating a fresh, empty database. A warning is recorded in the
logs. If your RabbitMQ installation contains important data then we
recommend you contact rabbitmq-sales@pivotal.io for assistance with the
upgrade.
