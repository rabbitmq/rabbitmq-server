Release: RabbitMQ 3.4.1

Security Fixes
==============

management plugin
-----------------
26437 prevent /api/* from returning text/html error messages which could
      act as an XSS vector (since 2.1.0)
26433 fix response-splitting vulnerability in /api/downloads (since 2.1.0)
(thanks to Atholl Stewart for finding the above)


Release Highlights
==================

server
------
bug fixes
26425 ensure RABBITMQ_USE_LONGNAME / USE_LONGNAME is picked up correctly from
      rabbitmq-env.conf (since 3.4.0)

enhancements
26429 add log messages when plugins are enabled or disabled at runtime


management plugin
-----------------
bug fixes
26431 fix web UI breakage when queue listing contains exclusive queues
      (since 3.4.0)
26438 fix internal server error when requesting permissions for a user or
      vhost which does not exist (since 2.1.0)


Java client
-----------
bug fixes
26434 prevent exchange binding recovery from swapping source and
      destination (since 3.3.0)
26428 ensure pom.xml lists ASL 2.0 (since 3.4.0)


.NET client
-----------
bug fixes
26439 ensure attempt to open a channel on a closed connection fails
      immediately (since 1.0.0)
26435 fix typos in documentation and remove references to immediate
      publishing (since 3.3.0)


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
