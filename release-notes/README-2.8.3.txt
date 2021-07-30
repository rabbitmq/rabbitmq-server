Release: RabbitMQ 2.8.3

Release Highlights
==================

server
------
bug fixes
- several fixes to communication protocol underlying HA queues
- memory leak deleting HA queues
- rotating logs loaded the entire log file into memory
- queues with many busy consumers could refuse to accept publishes until empty
- stale transient queue information could be left behind when a node restarted
  quickly
- additional cluster nodes that started with insufficient disk space would
  never accept connections even after disk space increased
- rabbitmqctl displayed non-ASCII characters incorrectly in some error
  messages
- disk space monitoring on non-English versions of Windows did not work
- RABBITMQ_PLUGINS_DIR could not be set on Windows

enhancements
- set default disk space limit to 1GB since many users were running into the
  previous default limit of {mem_relative, 1.0} when running RabbitMQ for
  the first time


packaging
---------
bug fixes
- Debian: uninstalling failed if broker was stopped
- Debian: server process was not child of init, leading it to get closed in
  certain situations

enhancements
- Debian: emit upstart events


java client
-----------
enhancements
- improved performance with SSL


erlang client
-------------
bug fixes
- code_change/3 did not return {ok, State} in many places breaking
  applications that use code reloading and the Erlang client
- spurious function_clause error in logs when SSL connection closed abruptly
  under R15B0x


federation plugin
-----------------
enhancements
- set default prefetch count to 1000, rather than unlimited, to ensure there
  is flow control on federation links


management plugin
-----------------
bug fixes
- rare race condition that could cause management DB failover to fail


STOMP plugin
------------
bug fixes
- last message of a burst may not be received when flow control is active


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
