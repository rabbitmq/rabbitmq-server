Release: RabbitMQ 3.0.1

server
------
bug fixes
25305 fix leak when dead-lettering to an exchange which does not route to queues
25309 fix {badmatch,false} when queue is deleted as mirroring policy changes
25340 fix performance degradation when using small numbers of outstanding
      confirms with mirrored queues
25301 fix messages with per-message TTL not expiring under some circumstances
25351 increase robustness when calculating memory use by plugins

enhancements
25335, 25330 allow mixed patch versions of RabbitMQ in a cluster


management plugin
-----------------
bug fixes
25348 fix web UI breakage for users with "management" tag
25300 fix JSON encoding error listing non-AMQP connections
25325 fix web UI links when filtering by virtual host
25346 fix parameter and policy names missing from definitions export
25326 [MSIE] fix policies listing page when more than 1 vhost exists
25304 [MSIE] fix "Add policy" and "Add upstream" buttons
25352 [MSIE] fix "Add user" and "Update user" buttons when a tag link has
      been clicked
25320 fix misleading error message when administrator has no permissions
25321 fix invisible update of queue policy changes that had no effect


web plugin support
------------------
bug fixes
25318 issue well-formed Location header when redirecting to another port


erlang client / shovel plugin / federation plugin
-------------------------------------------------
bug fixes
25331 prevent deadlock when starting connection during application shutdown


tracing plugin
--------------
bug fixes
25341 fix failure of rabbitmqctl status when tracing plugin enabled


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
