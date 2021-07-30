Release: RabbitMQ 3.4.2

Release Highlights
==================

server
------
bug fixes
25788 prevent HA queue synchronisation from taking quadratic time when
      there are many messages on disk (since 3.1.0)
26474 prevent false positive detection of partial partitions (since 3.4.0)
26460 prevent badarg in rabbit_diagnostics:looks_stuck/1 (since 3.3.5)
26417 ensure rabbitmqctl does not get falsely disconnected from the
      broker when net_ticktime has been reduced (since 1.0.0)
26449 fix garbled inequivalent argument error messages (since 3.4.0)
26468 fix removal of unmirrored queues as part of "rabbitmqctl
      forget_cluster_node --offline" (since 3.4.0)
26470 improve reliability of promotion of mirror mirrors as part of
      "rabbitmqctl forget_cluster_node --offline" (since 3.4.0)
26367 ensure dead letter exchange arguments are checked for equivalence
      on queue declaration (since 3.1.4)

building and packaging
----------------------
bug fixes
26441 fix rabbitmqctl on the OS X standalone release (since 3.4.0)


management plugin
-----------------
bug fixes
26472 prevent management agent crashing when log location set to 'tty'
      (since 3.4.0)
26451 make sure web UI disk chart says "disk free" not "disk used" (since 3.4.0)
26455 fix race condition rendering page (since 2.1.0)
26464 fix drop at the end of data rate charts (since 3.2.0)


shovel plugin
-------------
bug fixes
26452 make sure auto-delete shovels remove their record from shovel status
      when deleting (since 3.3.0)
26454 fix autodelete shovel behaviour when started on an empty
      queue (since 3.3.0)
26453 prevent shovel-management HTTP API returning 500 if queried just as
      dynamic shovel is being deleted (since 3.3.0)


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
