Release: RabbitMQ 3.2.0

server
------
bug fixes
25602 fix race condition that could cause mirrored queues to corrupt state
      during promotion (since 2.6.0)
25745 prevent HA queue from becoming masterless if multiple nodes shutdown in
      quick succession (since 2.6.0)
25685 prevent race that leads to a masterless queue when a mirror and
      previous leader replica start simultaneously (since 2.6.0)
25815 ensure that persistent messages with expiration property timeout
      correctly after broker restarts (since 3.0.0)
25780 stop ram nodes from becoming disc nodes when started in isolation
      (since 3.0.0)
25404 prevent potential deadlocks during shutdown
25822 prevent crash at startup when starting a clustered node hosting a
      durable non-HA queue which had been bound to a transient exchange which
      was deleted when the node was down (since 2.5.0)
25390 tolerate corrupt queue index files with trailing zeroes during boot
      (since 2.0.0)
25704 remove possibility of "incompatible Erlang bytecode" failure in cluster
      startup (since 3.1.0)
25721 fix logging of config file location (since 3.1.0)
25276 ensure queues declared as exclusive are not durable or mirrored
      (since 2.6.0)
25757 prevent error being logged when an exclusive queue owner disconnects
      during declaration (since 3.0.0)
25675 prevent crash when sending OTP status query to writer or heartbeater
      processes (since 1.0.0)

enhancements
25553 support for federated queues
25749 allow alternate and dead-letter exchanges, queue max length, expiry and
      message TTL to be controlled by policy as well as AMQP arguments
24094 report client authentication errors during connection establishment
      explicitly using connection.close
25191 inform clients when memory or disk alarms are set or cleared
25572 allow policies to target queues or exchanges or both
25726 make it harder to trigger the disk space alarm with default settings
25597 offer greater control over threshold at which messages are paged to disk
25716 allow missing exchanges & queues to be deleted and unbound without
      generating an AMQP error
25725 implement consumer priorities
23958 backport OTP process supervision infrastructure improvements
25733 relax type constraints of header exchanges
25809 add support for specifying a SSL verify_fun name in the config file


building & packaging
--------------------
enhancements
20384 add sample configuration file

dependency change
25581 require at least Erlang version R13B03 for broker and plugins

feature removal
25455 remove RabbitMQ-maintained MacPorts repository


management plugin
-----------------
bug fixes
25601 report on queue lengths and data rates in a more timely fashion
      (since 3.1.0)
25676 display chart times in the local time zone rather than UTC (since 3.1.0)
25770 prevent over-enthusiastic caching of web UI templates (since 2.1.0)

enhancements
25063 support arrays in web interface for arguments, policies and headers
25598 display queue paging information
25711 improve handling of defaults in config file by rabbitmqadmin (thanks to
      Simon Lundström)
25747 de-emphasise internal federation queues and exchanges
25778 introduce 'policymaker' tag, permitting policy & parameter operations
      without being full administrator
25616 more readable number formatting in graph labels
25641 permit turning tracing on/off using the HTTP API
25811 add support for web UI authentication via the initial URI
25792 optimise monitoring of file descriptors on OS X


LDAP plugin
-----------
enhancements
25479 support boolean operators in queries
25724 avoid setting a probably non-existent host in the default configuration


federation plugin
-----------------
bug fixes
25707 prevent upstream queues from being deleted, thus preventing deletion
      upon policy change (since 3.0.0)

enhancements
25554 allow federation policy to specify a single upstream instead of an
      upstream-set
25797 various performance enhancements


Web-STOMP plugin
----------------
enhancements
25699 support for implicit subscriptions


AMQP 1.0 plugin
---------------
bug fixes
25404 prevent potential deadlocks during shutdown (since 3.1.0)

enhancements
25539 make the default virtual host configurable


STOMP plugin
------------
bug fixes
25692 prevent potential deadlocks during shutdown (since 2.3.0)
25789 prevent incomplete TCP connection attempts from leaking processes
      (since 2.3.0)

enhancements
25539 make the default virtual host configurable


MQTT plugin
-----------
bug fixes
25577 ensure resumed subscriptions become active immediately after reconnecting
25744 correct client shutdown sequence in the event of failed startup


jsonrpc-channel plugin
----------------------
bug fixes
25776 fix dependencies that preventing plugin from running (since 3.1.4)


java client
-----------
bug fixes
25708 prevent deadlock when calling blocking operations in handleCancel
      (since 1.0.0)

enhancements
25736 added routing key to performance testing tool parameters
25767 rename the performance testing tool (formerly MulticastMain)


.net client
-----------
enhancements
25552 make better use of generic types
25595 consumer cancellation is now exposed as an event


erlang client
-------------
bug fixes
25682 prevent potential deadlocks during shutdown
25743 prevent failures due to connection string lookup errors in protocols
      other than AMQP 0-9-1 / 0-8 (since 2.8.1)
25794 prevent startup error when using SSL on versions of Erlang from R16B01
25677 prevent crash when reconsuming asynchronously with the same tag
      (since 2.6.0)

enhancements
25520, 25804 optimise network performance (thanks to Jesper Louis Andersen)
25782 support connection_timeout in AMQP URI


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
