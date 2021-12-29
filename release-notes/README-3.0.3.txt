Release: RabbitMQ 3.0.3

server
------
bug fixes
25457 fix connection failure to start reading again in rare circumstances
      when coming out of flow control
25419 ensure invocation of "rabbitmqctl stop_app" during server startup
      on a fresh node does not leave a corrupted Mnesia schema
25448 ensure messages expire immediately when reaching the head of a queue
      after basic.get
25456 ensure parameters and policies for a vhost are removed with that vhost
25465 do not log spurious errors for connections that close very early
25443 ensure "rabbitmqctl forget_cluster_node" removes durable queue records
      for unmirrored queues on the forgotten node
25435 declare dependency on xmerl in rabbit application


Windows packaging
-----------------
bug fixes
25453 make rabbitmq-plugins.bat take note of %RABBITMQ_SERVICENAME%


management plugin
-----------------
bug fixes
25472 clean up connection and channel records from nodes that have crashed
25432 do not show 404 errors when rabbitmq_federation_management
      is installed and rabbitmq_federation is not


mqtt plugin
-----------
bug fixes
25469 ensure the reader process hibernates when idle


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
