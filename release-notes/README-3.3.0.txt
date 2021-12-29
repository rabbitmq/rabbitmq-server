Release: RabbitMQ 3.3.0

Security Fixes / Changes
========================

server
------
25603 prevent access using the default guest/guest credentials except via
      localhost (since 1.0.0)


LDAP plugin
-----------
26052 do not allow use of password "" when connecting to servers which
      permit unauthenticated bind (since 2.3.0)


Release Highlights
==================

server
------
bug fixes
26069 ensure that memory use is bounded when logging crash of large
      processes (since 1.0.0)
25589 ensure that a queue which needs to reduce its RAM usage but is not
      receiving any publishes still pages out messages quickly (since 2.0.0)
25991 fix topic routing in the face of multiple bindings that differ only
      in arguments (e.g. those created by the federation plugin) (since 2.4.0)
26027 ensure autoheal does not hang winner node if 'rabbitmqctl stop_app'
      issued on other node during healing (since 3.1.0)
26043 ensure autoheal does not crash if multiple autoheal requests occur
      in quick succession and the leader make different decisions for each
      (since 3.1.0)
26088 fix failure to delete virtual host if a queue in the virtual host is
      deleted concurrently (since 1.0.0) (incorrectly reported fixed in 3.2.4)
25374 interpret AMQP field type 'b' as signed byte rather than unsigned, for
      compatibility with our errata, and the majority of clients (since 1.0.0)
26058 prevent inaccurate (sometimes negative) report of memory use by
      plugins (since 3.1.0)
26063 prevent spurious rabbit_node_monitor error messages when pausing in
      pause_minority mode (since 3.2.4)

enhancements
25888 give busy queues a bias towards delivering more messages than they
      accept, ensuring they tend to become empty rather than huge
26070 automatically reconsume when mirrored queues fail over (and
      introduce x-cancel-on-ha-failover argument for the old behaviour)
25882 provide 'consumer utilisation' metric to help determine if consumers
      are being held back by low prefetch counts
26039 ensure rabbitmqctl's formatting of process IDs is more shell-script
      friendly
25654 allow use of separate modules for authentication and authorisation
25722 explicitly set Erlang distribution port in all circumstances
25910 add process identification information to process dictionary to aid in
      debugging
25922 log reason why a node considered another node to be down
25860 enforce the rule that object names must be valid UTF-8 strings
25836 prevent deletion of amq.* built-in exchanges, and mark appropriate ones
      as internal
25817 allow plugins to rewrite AMQP methods
25979 announce cluster-id when clients connect
26042 allow log_levels config item to silence mirrored queue events
26065 ensure config file location is logged even if config file is absent
22525 allow server-wide configuration of channel_max
25627 show current alarms in 'rabbitmqctl status'

performance improvements
25957 change semantics of basic.qos global flag in order to allow for
      greatly enhanced prefetch performance
      (see https://www.rabbitmq.com/consumer-prefetch.html)
25827,25853 substantially improve performance stopping and starting brokers
      with many durable queues
26001 improve performance of persistent confirmed messages on spinning disks
24408 improve performance of mandatory publication (very substantially when
      publishing across clusters)
25939,25942,25943 improve performance reading and writing AMQP (especially
      reading large messages)

feature removal
25962 remove support for client-sent channel.flow method; basic.qos is
      superior in all cases
25985 remove support for 'impersonator' tag


building & packaging
--------------------
bug fixes
25989 put the example config file in the correct directory for generic
      Unix and standalone Mac, and put a copy in the AppData directory
      on Windows (since 3.2.0).
26093 Debian: remove obsolete DM-Upload-Allowed field (thanks to Thomas
      Goirand)
26094 Debian: ensure package can be built multiple times and remove
      files from debdiff that should not be there (thanks to Thomas Goirand)

enhancements
25844 switch standalone Mac release to 64 bit architecture


management plugin
-----------------
bug fixes
25868 ensure connections in flow control for a long time still show 'flow'
      and do not transition to 'blocked' (since 2.8.0)

enhancements
24829 provide information about flow control status of internal components,
      to help find bottlenecks
25876 provide regex option when filtering lists (with thanks to Joseph Weeks)

feature removal
25720 remove the redirect from 2.x-era port 55672 to 15672


federation plugin
-----------------
enhancements
23906 implement cycle detection for messages forwarded over federation
25985 remove requirement to configure local-username
26042 allow log_levels config item to silence federation events
25979 replace local-nodename with (non-federation-specific) cluster-id
25902 preserve original routing key when forwarding messages via
      queue federation
25904 tidy up nomenclature in federation status / management


shovel plugin
-------------
enhancements
24851 introduce "dynamic" shovels, controlled by parameters in a similar way
      to federation (see https://www.rabbitmq.com/shovel-dynamic.html)
25890 make shovel status rather more informative
25894 introduce "auto-delete" dynamic shovels
25935 allow dynamic shovels to record routing information similarly to
      federation
26045 default prefetch-count to 1000 rather than unlimited


LDAP plugin
-----------
enhancements
25785 add 'dn_lookup_bind' to allow lookup of a user's DN before binding
25839 allow specification of SSL options for (e.g.) presenting client
      certificates when connecting to an LDAP server
26022 support timeouts when connecting to an LDAP server
25570 replace 'as_user_no_password' error with something which makes more sense


STOMP plugin
------------
bug fixes
26010 remove examples for the headers exchange that haven't worked since 2010


MQTT plugin
-----------
bug fixes
25941 ensure keepalives are implemented completely (since xxx)

enhancements
26067 initial support for MQTT 3.1.1 draft
25877 support specifying vhost at the time of connection


Web-STOMP plugin
----------------
bug fixes
25896 ensure examples set content-type (since 3.0.0)

enhancements
25828 upgrade cowboy to get sec-websocket-protocol support
25913 support SSL (with thanks to James Shiell)


JSON-RPC channel plugin
-----------------------
feature removal
26029 remove JSON-RPC channel plugin from the official release


java client
-----------
enhancements
14587 support automatically reconnecting to server(s) if connection is
      interrupted
26008 make it easier to start a Connection with a custom ExceptionHandler
25833 allow specifying a per-ConnectionFactory ExecutorService
25999 handle running in a security-restricted context (e.g. Google App engine)
25663 improve type safety of ShutdownSignalException "reason" property
26068 improve clarity of AlreadyClosedException reason
26015 make Envelope.toString() do something useful


.net client
-----------
bug fixes
25911 ensure Subscriptions are informed if a channel closes (since 1.4.0)
25374 interpret AMQP field type 'b' as signed byte rather than unsigned, for
      compatibility with our errata, and the majority of other clients
      (since 1.0.0)
25046 ensure timeout establishing connection does not throw
      System.NotSupportedException (since 1.0.0)
25278 ensure timeout establishing connection does not throw
      ProtocolVersionMismatchException (since 2.0.0)
26071 ensure attempted use of a closed channel leads to
      AlreadyClosedException (since 1.0.0)
25082 ensure EventingConsumer sets supertype model field (since 1.0.0)

enhancements
25895 support for SSL cert selection and validation callbacks (thanks to
      "pooleja")
26068 improve clarity of AlreadyClosedException reason


erlang client
-------------
bug fixes
25374 interpret AMQP field type 'b' as signed byte rather than unsigned, for
      compatibility with our errata, and the majority of other clients
      (since 1.0.0)
26050 add missing handle_server_cancel/2 to amqp_gen_consumer_spec.hrl

enhancements
25985 do not require direct connections to specify a username


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
