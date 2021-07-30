Release: RabbitMQ 3.2.2

Release Highlights
==================

server
------
bug fixes
25873 prevent possibility of deadlock when mirrors start up (since 2.6.0)
25867 ensure automatic synchronisation does not fail when policy change
      causes new mirrors to start and the leader replica to change simultaneously
      (since 3.1.0)
25870 prevent the worker pool from running out of processes due to processes
      crashing (since 1.8.0)
25899 prevent race leading to cluster upgrade failure when multiple nodes
      attempt secondary upgrade simultaneously (since 2.4.1)
25912 correct reporting of flow control when connections become idle
      (since 2.8.0)


LDAP plugin
-----------
bug fixes
25863 prevent channels crashing during broker shutdown (since 2.3.0)


management plugin
-----------------
bug fixes
25872 prevent empty queues from showing length as '?' after going idle
      (since 3.1.0)
25889 ensure GET /api/overview uses consistent data types when server is idle
      (since 2.1.0)
25920 prevent rabbitmqadmin failure when no home directory is set (since 3.2.0)


MQTT plugin
-----------
bug fixes
25887 prevent possible error in the presence of multiple client IDs
25905 fix handling of acks from the broker with the 'multiple' flag set


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
