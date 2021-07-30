Release: RabbitMQ 2.8.0

Release Highlights
==================

server
------
bug fixes
- reduce idle CPU usage when there are lots of mirrored queues
- fix a rare bug which could cause the server to stop accepting connections
- ensure starting a ram node when all disk nodes are down fails, instead
  of creating a blank ram node
- fix a race in mirrored queues where one node could run two mirrors
- improve internal accounting of file descriptors; make it harder to hit the
  limit unexpectedly
- rabbitmqctl <unknown-action> fixed on R15B
- fix race condition leading to monitoring other cluster nodes twice
- leave the Erlang distributed system, not just Mnesia, when resetting
- more consistent handling of RABBITMQ_* environment variables

enhancements
- dead lettering - queues can specify an exchange to which messages should be
  redirected when they are rejected or expire
- internal flow control to limit memory use and make performance more
  predictable if the server is overloaded
- fsync after Mnesia transactions to ensure recently-created queues, exchanges
  are not lost in the event of an unexpected shutdown
- much more eager fsync when persistent messages are published without
  confirms / transactions leading to far fewer messages lost in the event
  of an unexpected shutdown
- server no longer fails to start when a durable exchange is declared using
  an exchange type plugin which is subsequently disabled. Instead the exchange
  exists but routes no messages
- better OpenBSD support (thanks to Piotr Sikora)
- basic.reject and basic.nack now respect transactions
- rabbitmq-echopid.bat introduced: allows obtaining the server PID on Windows
- the start of logging configuration: initially just for per-connection logging
- set SO_LINGER to 0 to prevent file descriptors being used by closed
  connections
- improve error reporting when AMQP ports are already used by non-RabbitMQ
  brokers
- improve error reporting when Mnesia times out waiting for tables
- consistent naming of connections and channels across rabbitmqctl and the
  management plugin
- file descriptor statistics added to "rabbitmqctl status"
- more robustness if rabbitmq-plugins cannot parse the enabled plugins file
- don't start external cpu_sup process; we don't need it

performance improvements
- consuming has smarter flow control, leading to performance improvements in
  many cases
- deleting queues with many bindings to topic exchanges is no longer
  O(binding_count^2)
- message ID generation is somewhat faster


packaging
---------
bug fixes
- debian: add build dependency on erlang-nox
- debian / rpm: don't start the server with "su", fixing inability to
  shut down seen on Ubuntu
- macports: fix plugins showing version as "0.0.0"
- macports: create configuration directory if it does not already exist
- windows: INSTALL file now contain Windows-style line endings

enhancements
- generic unix tarball: by default locate all log / db / conf files within
  the unpacked tarball, simplifying installation


erlang client
-------------
bug fixes
- fix "make documentation"

enhancements
- wait_for_confirms() can now take a timeout


java client
-----------
bug fixes
- fix memory leak when channels were closed on a connection that stays open
- fix display of message headers and content in the tracer
- fix hang in ConnectionFactory when the first frame from the server is never
  received
- fix NullPointerException at ConsumerDispatcher.java:91

enhancements
- waitForConfirms() can now take a timeout
- allow use of Java arrays in AMQP arrays (e.g. for arguments and headers)
- don't depend on org.apache.commons classes except for tests
- fire channel shutdown listeners on connection shutdown
- show fractional message rates in MulticastMain
- show aggregated producer rates in MulticastMain


.net client
-----------
bug fixes
- don't try to close the socket more than once if a heartbeat is missed

enhancements
- WaitForConfirms() can now take a timeout


management plugin
-----------------
bug fixes
- fix overview page in MSIE
- escape HTML entities properly in the web UI
- fix incorrect display of mirrored queues as unsynchronised after database
  failover
- give sensible error if user tags field is missing
- fix [Admin] etc links which were broken in some browsers
- fix wrong date in "last updated" in the web UI

enhancements
- add separate form to update users
- add option to import file of entity definitions at startup
- publish messages from the queue details page of the web UI
- make "exchange type" into a select box in the web UI
- show the connection heartbeat more clearly in the web UI


json-rpc plugin
---------------
bug fixes
- fix memory leak in ETS


rabbitmqadmin
-------------
bug fixes
- fix "rabbitmqadmin get"
- display array info-items correctly
- allow specifying node for queue declaration

enhancements
- configuration file for connections (thanks to Massimo Paladin)


federation plugin
-----------------
enhancements
- allow the queue declared upstream to be mirrored


STOMP plugin
------------
bug fixes
- fix invalid MESSAGE Frames for reply-to temporary queues
- fix non-UTF-8 durable topic subscription queue names
- behave sensibly on death of the internal AMQP connection / channel
- prevent an infinite loop when implicit connect enabled with an invalid
  username / password
- allow more than one SSL handshake to happen at once

enhancements
- support client login via SSL certificate, similar to
  rabbitmq_auth_mechanism_ssl for AMQP
- performance improvement: don't declare a queue on every publish
- support the server's new flow control mechanism
- add "server" field to the "CONNECTED" frame


auth backend LDAP plugin
------------------------
enhancements
- optionally look up a user's DN after binding - useful for Microsoft
  Active Directory
- remove build time dependency on nmap
- allow queries to run as the user we bound as


auth mechanism SSL plugin
-------------------------
bug fixes
- don't blow up if a certificate contains more than one Common Name

enhancements
- support obtaining the user name from the certificate's Distinguished Name


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
