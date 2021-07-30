Release: RabbitMQ 2.8.5

Release Highlights
==================

server
------
bug fixes
- unnecessary CPU utilisation no longer occurs in the presence of large
  numbers of idle HA queues
- rapidly declaring and then deleting HA queues no longer crashes the master
- fixed a race condition in handling node down signals, that could result in
  HA queues failing to restart when bringing a follower replica (mirror) back online
- channels no longer crash when detecting nodes that have gone offline
- rabbitmqctl no longer garbles error messages when rendering non-ASCII
  characters
- the installer now places the .erlang.cookie file in %HOMEDRIVE%%HOMEPATH%
  on Windows, so that %USERPROFILE% can be safely relocated

STOMP plugin
------------
bug fixes
- fixed a bug in the test suite that was failing to check for the expected
  number of receipts before checking if a message had arrived successfully

jsonrpc-channel plugin
----------------------
bug fixes
- updated to support the latest versions of rfc4627 and Mochiweb

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
