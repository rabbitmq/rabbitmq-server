Release: RabbitMQ 2.3.0

Release Highlights
==================

server
------
bug fixes
- prevent message store deleting open files leading to eaccess on Windows
  and potential disk space leak
- various bugs in delegate leading to poor cluster performance and
  nodes blocking if other nodes are down
- ensure regular flushes of queue index data to disk resulting in better
  data retention in the event of a broker failure
- prevent queues from hibernating indefinitely on startup under memory
  pressure
- prevent message store in-memory cache from becoming too large
- prevent infinite loop after certain types of queue process crash,
  and prevent such a crash during queue deletion on Erlang R12B3
- make SASL PLAIN parser more robust
- fix startup scripts to work on Solaris 10
- prevent delivery of large messages to consumers from blocking deliveries
  on other channels
- basic.recover affects prefetch count
- prevent channel crash on basic.recover to a deleted queue
- correct serialisation of PIDs in clusters, without which the
  management plug-in failed to display some detailed stats
- prevent potential crash of queues in clusters in the event of
  improbable ordering of events upon the death of a channel
- add missing failure diagnostics on rabbitmqctl list_consumers
- fix truncated failure diagnostics for rabbitmqctl under Windows

enhancements
- add confirm mode - an extension to the AMQP 0-9-1 spec allowing
  clients to receive streaming receipt confirmations for the messages
  they publish. See
  https://www.rabbitmq.com/extensions.html#confirms for more information.
- add a basic.nack method. See
  https://www.rabbitmq.com/extensions.html#negative-acknowledgements
- add an unforgeable user-id header. See
  https://www.rabbitmq.com/extensions.html#validated-user-id
- pluggable SASL authentication mechanisms, and a new plugin
  to authenticate using SSL (see below)
- pluggable authentication / authorisation backends, and a new plugin
  to authenticate and authorise using LDAP (see below)
- internal exchanges (cannot be published to directly,
  typically used with exchange-to-exchange bindings)
- users can be made unable to log in with a password
- IPv6 support. RabbitMQ will listen on IPv4 and IPv6 by default.
- list SSL algorithm information in rabbitmqctl
- improved diagnostic error messages in common startup error cases
- allow node name to be specified without a host
- persister optimisation - eliminate unnecessary pending actions upon
  queue deletion (pseudo pipeline flush)
- improve pluggable exchange type API to allow better handling of race
  conditions


java client
-----------
bug fixes
- fix for compilation under Java 1.5
- remove support for Java 1.4

enhancements
- confirm mode
- pluggable SASL authentication mechanisms
- include generated source in Maven source bundle


.net client
-----------
bug fixes
- noAck set correctly in Subscription class

enhancements
- confirm mode
- pluggable SASL authentication mechanisms
- API tidied up to more closely resemble that of the Java client
- distribute XML documentation with binary release


management plugin
-----------------
bug fixes
- race condition that can lead to stats db failing on queue deletion
- closing connections on remote cluster nodes
- fix web UI memory leaks in Chrome
- mitigate web UI memory leaks in all browsers

enhancements
- command line tool rabbitmqadmin can display overview statistics and
  filter columns
- context-sensitive help
- web UI state is persistent
- display statistics for confirms
- API: empty fields can be omitted on PUT
- no longer depends on the crypto application, simplifying installation
  for some users


STOMP plugin
------------
bug fixes
- plug channel leak on UNSUBSCRIBE
- fix breakage of SEND after UNSUBSCRIBE
- gracefully handle SUBSCRIBE to non-existent exchange
- correct semantics of UNSUBSCRIBE receipts

enhancements
- updates to support the draft STOMP 1.1 spec
- major refactoring to use OTP behaviours
- enhanced and fixed examples
- IPv6 support


build and packaging
-------------------

enhancements
- Windows bundle now includes Erlang R14B01


shovel plugin
-------------
bug fixes
- close client connections properly if failure occurs during startup

enhancements
- allow specification of heartbeat, frame_max and channel_max in
  connection URI


ssl authentication mechanism plugin
-----------------------------------
Experimental plugin allowing clients to authenticate with the SASL
EXTERNAL mechanism and client SSL certificates. A password is not
required.


ldap authentication backend plugin
----------------------------------
Experimental plugin allowing the authentication / authorisation
database to be hosted in an LDAP server.


Upgrading
=========
To upgrade a non-clustered RabbitMQ from release 2.1.1 or later, simply
install the new version. All configuration and persistent message data
is retained.

To upgrade a non-clustered RabbitMQ from release 2.1.0, first upgrade
to 2.1.1 (which retains all data), and then to the current version as
described above.

To upgrade a clustered RabbitMQ or from releases prior to 2.1.0, if
the RabbitMQ installation does not contain any important data then
simply install the new version. RabbitMQ will move the existing data
to a backup location before creating a fresh, empty database. A
warning is recorded in the logs. If your RabbitMQ installation
contains important data then we recommend you contact
rabbitmq-sales@pivotal.io for assistance with the upgrade.
