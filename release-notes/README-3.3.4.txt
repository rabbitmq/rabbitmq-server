Release: RabbitMQ 3.3.4

Release Highlights
==================

server
------
bug fixes
26258 fix startup failure on systems with non-GNU readlink when started from a
      symlink (e.g. Mac Homebrew) (since 3.3.3)
26247 fix startup failure when inet_dist_listen_min / inet_dist_listen_max set
      (since 3.3.3)
26253 prevent unclear error message when config file completely empty
      (since 3.3.0)


STOMP plugin
------------
bug fixes
26246 don't log a crash when client misses heartbeat (since 2.3.0)


web-STOMP plugin
----------------
bug fixes
26250 fix crash when Web-STOMP is the only SSL user in the broker on Erlang
      R16B03 or later (since 3.0.0)


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
