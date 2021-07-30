Release: RabbitMQ 1.8.1

Release Highlights
==================

server
------
bug fixes
 - unbinding from an auto-delete exchange produced an error
 - the message count reported when declaring a queue was incorrect under rare 
   conditions
 - it was possible for a channel.close_ok message to get lost in rare 
   circumstances

enhancements
------------
 - clustering produces better error messages when clustering fails
 - the AMQP 0.8 specification permitted a rare case of deadlock while closing 
   channels and connections. AMQP 0.9.1 forbids this condition and RabbitMQ now
   implements the correction
 - the AMQP basic.recover method is now synchronous by default - the 
   asynchronous version is still available, but deprecated
 - the AMQP basic.recover method is now permitted in transacted channels, where
   this was previously forbidden
 - maximum AMQP frame size is specified more rigorously in AMQP 0.9.1 - RabbitMQ
   now enforces the negotiated maximum frame size
 - AMQP 0.9.1 guidance on error constants is now followed more closely and
   0.9.1 error codes are produced in more situations
 - SSL compatiblity under R14A has been improved

java client
-----------
enhancements
 - the API can now report on channel flow events
 - better handling of unsolicited messages and unknown consumer tags, by adding
   a default consumer
 - documentation enhancements around the use of AMQConnection

.net client
-----------
enhancements
 - better handling of unsolicited messages and unknown consumer tags, by adding
   a default consumer
 - documentation enhancements around the use of ConnectionFactory

building & packaging
--------------------
bug fixes
 - fix permission errors for commandline utilities in MacPorts

enhancements
 - compiles under Erlang R14A
 - builds using GNU Make 3.80 - previously version 3.81 was required
 - error output when using old versions of GNU Make has been added
 - builds under RHEL5 and distributions with xmlto version 0.0.18
 - better type-checking, making use of recent features in Dialyzer

Upgrading
=========
The database schema has not changed since version 1.8.0, so user accounts,
durable exchanges and queues, and persistent messages will all be retained
during the upgrade.

If, however, you are upgrading from a release prior to 1.8.0, when the
RabbitMQ server detects the presence of an old database, it moves it to a
backup location, creates a fresh, empty database, and logs a warning. If
your RabbitMQ installation contains important data then we recommend you
contact rabbitmq-sales@pivotal.io for assistance with the upgrade.

