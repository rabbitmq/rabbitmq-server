Release: RabbitMQ 3.0.2

server
------
bug fixes
25422 fix race causing queues to crash when stopping mirroring
25353 fix issue preventing idle queues from hibernating sometimes
25431 fix compilation on Erlang R16A
25420 fix issue causing crash at startup if another node reports Mnesia
      starting or stopping
25360 fix race allowing channel commands to be sent after connection.close-ok
25412 fix race allowing channel commands to be sent after server has closed
      a channel
25378 fix broken error reporting for rabbitmqctl


STOMP plugin
------------
bug fixes
25362 only add /reply-queue prefix in reply-to header when replying to a
      temporary queue


consistent hash exchange plugin
-------------------------------
bug fixes
25403 clean up Mnesia resources correctly on exchange deletion


management plugin
-----------------
bug fixes
25401 fix error reporting for some broken policy declarations
25413 prevent read access to /api/connections/<name>/channels for
      non-monitoring users
25335 fix rabbitmqadmin bash completion when extglob mode is switched off
      in the shell


management visualiser plugin
----------------------------
bug fixes
25387 fix broken RabbitMQ logo


shovel-management plugin
------------------------
bug fixes
25410 fix breakage when shovel application is stopped manually


web-STOMP plugin
----------------
bug fixes
25359 prevent rabbitmqctl status from killing web-STOMP connections
25357 update SockJS-Erlang to 0.3.4, fixing SockJS issue #41 (fix a traceback
      when websocket is slow or busy)


JSON-RPC plugin
---------------
bug fixes
25347 fix hang of rabbitmqctl status when JSON-RPC plugin enabled


.NET client
-----------
bug fixes
25389 send 0-9-1 header rather than 0-9 header when AMQP 0-9-1 is selected


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
