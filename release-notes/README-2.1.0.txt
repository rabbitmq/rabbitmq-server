Release: RabbitMQ 2.1.0

Release Highlights
==================

server
------
enhancements
 - detects incorrect nodename in rabbitmq_multi
 - extend supported timeout types for queue lease, see
   https://www.rabbitmq.com/extensions.html#queue-leases
 - print plugin versions on startup
 - extend permissions system - add 'is_admin' field; useful for
   the management plugin
 - queue.declare and queue.delete should always work quickly, even
   if the broker is busy

bug fixes
 - the 'client' permission scope wasn't working correctly
 - in the presence of 'verify_peer' option broker will now not accept
   self-signed ssl certificates
 - fixed sasl logging to terminal
 - fixed 'rabbitmq_multi stop_all' on freebsd
 - fixed race condition which might result in a message being lost when
   the broker is quitting
 - fixed race condition in heartbeat handling, which could result
   in a connection being dropped without logging the reason for that

java client
-----------
enhancements
 - basic.consume 'filter' argument is now called 'arguments'
 - dropped Channel.queuePurge/2 method
 - added --help flag to MulticastMain

.net client
-----------
enhancements
 - basic.consume 'filter' argument is now called 'arguments'

bug fixes
 - fixed race condition in synchronous basic.recover
 - codegen was generating incorrect code for nowait parameter


Upgrading
=========
The database schema has changed since the last release (2.0.0). When
starting, the RabbitMQ server will detect the existence of an old
database and will move it to a backup location, before creating a
fresh, empty database, and will log a warning. If your RabbitMQ
installation contains important data then we recommend you contact
rabbitmq-sales@pivotal.io for assistance with the upgrade.
