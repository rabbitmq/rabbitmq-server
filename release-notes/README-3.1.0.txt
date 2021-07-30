Release: RabbitMQ 3.1.0

server
------
bug fixes
25524 fix memory leak in mirror queue mirror with many short-lived publishing
      channels
25518 fix handling of certain ASN.1 fields in SSL certificates
25486 ensure we handle cross cluster resource alarms after a partition
25490 limit frequency with which the server invokes "df" or "dir" to
      measure disc use
25491 ensure we detect partitions in an idle cluster
25367 throttle in a more timely manner when reading large messages and
      resource alarm goes off
25535 correctly report failure when user with no password attempts
      password-based login
25463 fix logging of authentication mechanism on login failure
25385 check equivalence for dead-lettering related arguments

enhancements
24407 manual eager synchronisation of mirrors
25418 automatic eager synchronisation of mirrors by policy
25358 cluster "autoheal" mode to automatically choose nodes to restart
      when a partition has occurred
25358 cluster "pause minority" mode to prefer partition tolerance over
      availability
19375 add x-max-length argument for queue length limits
25247 allow "nodes" policy to change queue master
25107 permit dead-letter cycles
25415 return total number of consumers in queue.declare-ok, not "active" ones
24980 cut down size of stdout banner, ensure everything goes to logs
25474 ensure partitions warning clears without needing to stop the
      winning partition
25488 allow exchange types plugins to validate bindings
25512 improve robustness and correctness of memory use detail reporting
25501 allow plugins to implement new ha-modes
25517 more cross-cluster compatibility checks at startup
25513 support debugging for heavyweight gen_server2 state

performance improvements
25514 performance improvements in message store garbage collection
25302 performance improvements in mirrors when consuming
25311 performance improvements requeuing persistent messages
25373 memory-use improvement while fetching messages
25428 memory-use improvement in queue index
25504 25327 performance improvements in dead lettering
25363 25364 25365 25366 25368 25369 25371 25386 25388 25429
      various minor performance improvements


management plugin
-----------------
bug fixes
25290 fix per-queue memory leak recording stats for mirror queue mirrors
25526 ensure single-object API queries support ?columns= in query string

enhancements
23378 retain historical statistics, plot charts, show statistics per vhost,
      improve performance of overview page, ensure message counters are
      monotonic
24114 ignore incoming statistics if the statistics database is overwhelmed
23625 performance improvements when listing many queues
23382 filter lists in the web UI
25408 allow specification of arguments when declaring with rabbitmqadmin
23438 allow admin to specify a reason when closing a connection
25322 add "--version" support to rabbitmqadmin


federation plugin
-----------------
bug fixes
24223 handle basic.nack if sent by the server

enhancements
25406 allow specification of multiple URLs in an upstream for failover
25433 allow choice of acknowledgement mode, permitting faster but less
      reliable delivery


old-federation plugin
---------------------
feature removal
25484 remove the old-federation plugin which shipped with RabbitMQ 3.0.x


shovel plugin
-------------
enhancements
24850 support failover when running in a cluster


Web-STOMP plugin
----------------
enhancements
25333 update stomp.js library to support STOMP 1.1 (thanks to Jeff Mesnil)


AMQP 1.0 plugin
---------------
25381 new plugin: initial support for AMQP 1.0


STOMP plugin
-----------
bug fixes
25464 more graceful handling of connection abort while opening
25466 don't crash when SENDing to /temp-queue


MQTT plugin
-----------
bug fixes
25464 more graceful handling of connection abort while opening
25487 do not break "rabbitmqctl status" if MQTT plugin is enabled


consistent hash exchange
------------------------
enhancements
25392 allow hashing on something other than routing key


packaging
---------
enhancements
25271 new standalone release tarball for OS X
25497 add extra architectures to the apt repository
25519 allow debian packages to depend on esl-erlang
25002 merge contents of private umbrella into public one and remove private
25339 move rabbitmq-test to its own repository


java client
-----------
bug fixes
25509 ensure channel numbers do not get reused before the server has finished
      with them

enhancements
25356 make Channel.waitForConfirms(long) throw exception if confirm mode is
      not selected
24675 add support for existing exchanges / queues to MulticastMain


erlang client
-------------
bug fixes
25502 fail reasonably when attempting to connect to a server that does not
      speak AMQP 0-9-1

enhancements
25377 remove "there is no confirm handler" warnings
25503 don't allow client to negotiate frame sizes > 64Mb since it does not
      support them


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
