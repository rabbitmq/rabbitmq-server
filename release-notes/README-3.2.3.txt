Release: RabbitMQ 3.2.3

Release Highlights
==================

server
------
bug fixes
25936 stem leak when queues with active consumers terminate (since 3.2.0)
25928 fix cosmetic error when sending connection.close-ok after client
      already closed the connection (since 1.0.0)
25965 limit messages to ~2GB to prevent "Absurdly large distribution output
      data buffer" VM crash (since 1.0.0)
24927 avoid broker being overwhelmed while logging benign messages starting with
      "Discarding messages" (since 1.0.0)
25952 prevent "Absurdly large distribution output data buffer" VM crash when
      sending many/large messages to a mirrored queue (since 2.6.0)
25925 remove extraneous service parameters when installing on windows
      (since 1.5.0)
25929 prevent error being logged when connection is closed while it is still
      being opened (since 1.0.0)


federation plugin
-----------------
bug fixes
25945 ensure federated queues correctly stop federating messages when channels
      close or crash without cancellation from consumers (since 3.2.0)
25971 prevent crash of federated mirrored queues on deletion (since 3.2.0)
25956 prevent federation of the queues used internally by federated exchanges
      (since 3.2.0)
25949 prevent unnecessary CPU use when ACKs are not in use (since 2.6.0)


shovel plugin
-----------------
bug fixes
25934 remove ordering constraint on configuration items (since 2.0.0)
25949 prevent unnecessary CPU use when ACKs are not in use (since 2.0.0)


LDAP plugin
-----------
bug fixes
25914 fix use of dn_lookup_attribute configuration on OpenLDAP (since 2.8.0)


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
