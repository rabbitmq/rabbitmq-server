Release: RabbitMQ 2.8.6

Release Highlights
==================

server
------
bug fixes
- ensure shutdown of mirrored queue nodes is recorded correctly
- removed unsupported plugins added in 2.8.5 (old-federation, sockjs-erlang,
  cowboy, web-stomp and web-stomp-examples)
- removing RAM nodes from a cluster no longer leads to inconsistent state
  on disk nodes (which previously failed to notice the RAM nodes' departure)
- reap TTL-expired messages promptly
- correct reporting of the vm_memory_high_watermark
- reduce likelihood of node name collision on Windows due to non-randomness
  of %RANDOM%


erlang client
-------------
bug fixes
- correctly account for file handles consumed by outgoing network connections
  when running as a plugin


management plugin
-----------------
bug fixes
- prevent publishing a message with non-binary content


shovel plugin
-------------
bug fixes
- guarantee that reconnect attempts continue if a failure occurs during
  connection establishment


federation plugin
-----------------
bug fixes
- guarantee that links continue to attempt reconnecting if a failure occurs
  during connection establishment
- report status correctly in the event of unexpected failure


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
