Release: RabbitMQ 3.4.3

Security Fixes
==============

management plugin
-----------------
26515 prevent XSS attack in table key names (since 2.4.0)
      (thanks to Robert Fitzpatrick)
      (CVE-2015-0862)
26516 prevent XSS attack in policy names (since 3.4.0)
      (thanks to Robert Fitzpatrick)
      (CVE-2015-0862)
26517 prevent XSS attack in client details in the connections list
      (CVE-2015-0862)
26518 prevent XSS attack in user names in the vhosts list or the vhost names
      in the user list (since 2.4.0)
      (CVE-2015-0862)
26520 prevent XSS attack in the cluster name (since 3.3.0)
      (CVE-2015-0862)


Release Highlights
==================

server
------
bug fixes
26490 in autoheal mode, ensure the leader doesn't stop before the winner told
      it so (since 3.3.0)
26491 in autoheal mode, prevent a race in Mnesia by waiting for Mnesia
      shutdown on all losing nodes (since 3.1.0)
26478 fix startup or rabbitmqctl failures when RABBITMQ_CTL_ERL_ARGS is set
      (since 3.4.0)
26498 fix queue crash with assertion failure in rare circumstances
      (since 3.4.0)
26081 improve error message when creating a cluster with mismatched Erlang
26446 improve error message when a plugin is incompatible with current Erlang
26265 ensure that plugins modules are picked before other third-party modules
26503 support ssl's verify_fun from Erlang R14B+ (since 3.2.0)
26502 fix 'backing_queue_status' duplication in /api/queues REST API (since
      3.4.0)

enhancements
26493 add top_memory_use and top_binary_refs diagnostic tools


federation management plugin
----------------------------
bug fixes
26519 fix double HTML escaping in upstream names (since 2.4.0)


shovel management plugin
------------------------
bug fixes
26519 fix double HTML escaping in dynamic shovel names (since 2.4.0)


tracing plugin
--------------
bug fixes
26519 fix double HTML escaping in tracing log file names (since 2.4.0)


AMQP 1.0 plugin
---------------
bug fixes
26486 use env(1) in codegen.py to find python(1) while building (since 3.1.0)


MQTT plugin
-----------
bug fixes
26482 ensure full exception details are logged (since 3.3.5)


java client
-----------
bug fixes
26492 fix off-by-one error in PerfTest --cmessages count


.net client
-----------
bug fixes
26501 make automatic recovery non-blocking to ensure user-defined handlers are
      not delayed (since 1.0.0)


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
