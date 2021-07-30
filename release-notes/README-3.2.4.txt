Release: RabbitMQ 3.2.4

Release Highlights
==================

server
------
bug fixes
26014 prevent 541 internal error removing a nonexistent binding from a topic
      exchange (since 3.2.0)
25762 fix failure to delete virtual host if a queue in the virtual host is
      deleted concurrently (since 1.0.0)
26013 ensure connection.blocked is sent in all circumstances it should be
      (since 3.2.0)
26006, 26038 ensure autoheal does not hang if a node is manually stopped
      during autoheal (since 3.1.0)
26000 prevent crash of mirrored supervisor in some circumstances after a
      network partition is healed (since 2.6.0)
25972 fix syntax error in example configuration file (since 3.2.0)


management plugin
-----------------
bug fixes
24476 prevent statistics database from vanishing after a network partition is
      healed (since 2.8.3)
25983 prevent "node statistics not available" error when mochiweb is
      configured with an explicit list of SSL ciphers (since 2.6.0)


federation plugin
-----------------
bug fixes
25998 ensure upstreams which are timing out establishing network connections
      can be deleted in a timely fashion (since 3.0.0)


shovel plugin
-------------
bug fixes
25996 ensure shovels which are timing out establishing network connections
      do not block broker shutdown (since 2.3.0)


STOMP plugin
------------
bug fixes
26028 prevent potential deadlocks during shutdown (since 2.3.0)


MQTT plugin
-----------
bug fixes
25982 ensure messages published with QOS=1 are persistent (since 3.1.0)


Erlang client
-------------
bug fixes
26041 prevent rare, fake "541 internal error" reported client-side when
      shutting down connections (since 2.1.1)


.NET client
-----------
bug fixes
26016 ensure SSL connection establishment times out if necessary (since 1.0.0)
26047 ensure IModel.ConfirmSelect() is idempotent (since 2.3.0)


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
