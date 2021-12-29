Release: RabbitMQ 2.6.1

Release Highlights
==================

server
------
bug fixes
- the broker failed to (re)start on reboot on systems that keep
  /var/run on a temporary file systems, e.g. Ubuntu.
- the Windows service failed to increase the Erlang process limit,
  limiting the broker to a few thousand queues, connections and
  channels.

.net client
-----------
enhancements
- add the "headers" exchange to RabbitMQ.Client.ExchangeType

management plugin
-----------------
bug fixes
- on a busy broker, /api/nodes could fail with a timeout, affecting
  several management UI pages.

topology visualiser
-------------------
First official release. See
https://www.rabbitmq.com/plugins.html#rabbitmq_management_visualiser

STOMP plugin
------------
enhancements
- trim whitespace from headers when speaking STOMP 1.0


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
