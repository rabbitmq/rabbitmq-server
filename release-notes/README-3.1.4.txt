Release: RabbitMQ 3.1.4

Security Fixes
==============

server
------
25686 ensure DLX declaration checks for publish permission (since 2.8.0)


management plugin
-----------------
24803 update to a later version of Mochiweb that fixes a directory traversal
      vulnerability allowing arbitrary file access on Windows (since 2.1.0)


Release Highlights
==================

server
------
bug fixes
25638 fix resource leak with mirrored queues when whole clusters stop
      (since 3.0.0)
25624 fix queue crash in mirrored queue handling of messages during promotion
      (since 2.6.0)
25615 25670 fix race conditions in mirrored queues when different cluster
      nodes start and stop near-simultaneously (since 2.6.0)
25617 fix corrupt_cluster_status_files error after abrupt node shutdown
      (since 3.0.0)
25645 fix mirrored queue sync failure in the presence of un-acked messages
      not at the head of the queue (since 3.1.0)
25640 fix race condition leading to channel crash with low prefetch count
      repeated basic.consume and basic.cancel (since 3.1.0)
25625 fix memory leak of mirrored queue messages during promotion
      (since 2.6.0)
25649 allow hipe compilation on Erlang R16B01
25659 allow offline cluster node removal with a node which is not second
      from last (since 3.0.0)
25648 make `rabbitmqctl join_cluster' idempotent (since 3.0.0)
25651 improve `rabbitmqctl cluster_status' handling of partition info when
      cluster nodes are in the process of stopping (since 3.1.0)
25689 ensure launch of subprocesses to monitor disk space and file handles
      works correctly when clink shell is installed on Windows (since 2.1.0)
25594 fix rabbit_error_logger crash during failed startup (since 1.4.0)
25631 fix bug in shutdown sequence that could lead to spurious
      INTERNAL_ERRORs being sent to clients (since 3.1.0)


erlang client
-------------
bug fixes
25632 fix broken error handling in amqp_network_connection that could lead
      to a crash during broker shutdown (since 2.4.0)
25688 fix bug in challenge-response auth handling (since 2.3.0)

enhancements
25674 add amqp_rpc_{client,server}:start_link()


STOMP plugin
------------
bug fixes
25691 fix connection crash on consumer cancellation notification (since 3.0.0)


build and packaging
-------------------
bug fixes
25668 add ssl support to OS X standalone package
25584 ensure that VERSION is set correctly when building src packages
      (since 2.7.0)


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
