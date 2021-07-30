Release: RabbitMQ 1.4.0
Status : beta

Release Highlights
==================

server
------
bug fixes
- maintain message order on persister replay
- do not throw away work on connection.close
- eliminate possibility of generating duplicate message ids when
  broker is restarted
- deal with race conditions during queue creation that could result in
  a queue being reported as 'not_found' when it did in fact exist, and
  the converse, or multiple queue processes being created per queue.
- suppress a few errors that would be logged in various connection
  shutdown scenarios but turn out to be harmless
- ensure preservation of content properties during persistence - this
  was working fine for the AMQP transport, but not the STOMP gateway
- fix various other small bugs

enhancements
- improve performance of queue creation
- add explanatory text to errors returned to the client and reported
  in the logs
- rationalise severities of logged errors, and log information
  allowing the correlation of log events with connections
- return 'connection_forced' error to clients on broker shutdown
- supervise queue processes
- improve/refactor internal APIs to assist in creation of extensions
- add type signature for all exported functions, and type check code
  with dialyzer
- generate AMQP codec from JSON representation of protocol spec

Java client
-----------
bug fixes
- completely revamp connection and channel closure handling, resolving
  a number race conditions and other bugs in the process and resulting
  in a cleaner, more comprehensive and consistent API
- correct a few minor errors in the javadocs

enhancements
- generate AMQP codec from JSON representation of protocol spec

building & packaging
--------------------
bug fixes
- only source /etc/default/rabbitmq in init.d scripts, thus stopping
  it from overriding env var settings
- pay attention to NODE* env vars in rabbitmq-multi script, thus
  allowing users to easily start multiple nodes with non-default
  settings for the node name, ip and port.
- make rpm update work

enhancements
- on Windows, place the server logs and db in a dir under the per-user
  %APPDATA% directory
- display names of nodes started by rabbitmq-multi
- migrate from cvs to hg, and split into separate repositories for
  server, java client, etc.
- clean up and refactor Makefiles
- avoid hanging for 30s when an invalid database schema is detected


Upgrading
=========

The database schema has changed in this version of RabbitMQ. If you
attempt to start RabbitMQ-1.4.0 over top of a previous installation,
it will fail, citing "schema_integrity_check_failed". To correct this,
delete your mnesia directory (on most platforms,
/var/lib/rabbitmq/mnesia) and restart the server. Note that this will
destroy all your durable exchanges and queues, and all your persisted
messages!

Care must be taken when upgrading a server that contains persisted
messages. The persister log format has changed between RabbitMQ-1.3.0
and this release. When RabbitMQ-1.4.0 first starts following an
upgrade it will move the existing persister log to a backup file -
check the log files for details. Thus the previously persisted
messages are not lost, but neither are they replayed. Therefore it is
recommended that the upgrade is performed only when there are no
important persistent messages remaining.

Due to a bug in the rpm packaging, which has now been fixed, users
with an existing rpm-based installation of the RabbitMQ server should
first remove the rabbitmq-server package ('rpm -e rabbitmq-server')
before proceeding with the normal upgrade.


Credits
=======

We would like to thank the following individuals for submitting bug
reports and feedback that we incorporated into this release:

Andrew Statsenko
David Corcoran
Dmitriy Samovskiy
Holger Hoffstaette
John Leuner
Kyle Salasko
Lars Bachmann
Michael Arnoldus
Petr Sturc
Sean Treadway
