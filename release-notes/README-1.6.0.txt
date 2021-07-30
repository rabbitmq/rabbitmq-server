Release: RabbitMQ 1.6.0
Status : final

Release Highlights
==================

server
------
bug fixes
- eliminate potential memory leak of transactionally published
  messages in certain system errors
- prevent possible starvation of some consumers on channels that get
  temporarily blocked due to backlogs
- do not send more messages to backlogged channels when accepting a
  new consumer
- prevent possible message reordering in the event of temporary node
  failure in a clustered setup
- return 'not_found' error in all cases of 'queue.unbind' attempting
  to remove a non-existing binding

enhancements
- implement AMQP's basic.qos' channel prefetch count limiting
- implement AMQP 0-9/0-9-1's headers exchange type
- introduce a permissions system which allows fine-grained access
  control on resources involved in AMQP operations. See
  https://www.rabbitmq.com/admin-guide.html#access-control for details
- introduce 'alternate exchanges' to handle messages which are
  otherwise unroutable. See
  https://www.rabbitmq.com/extensions.html#alternate-exchange for
  details
- improve performance and stability under high load
- reduce memory consumption
- prefix all mnesia tables with "rabbit_" in order to prevent name
  clashes with other Erlang applications, in particular ejabberd
- handle rabbitmqctl commands with higher priority, thus ensuring that
  answers are returned promptly even under high load
- reduce severity of "connection closed abruptly" log event from
  'error' to 'warning'

Java client
-----------
enhancements
- support extended list of table field types

.net client
-----------
bug fixes
- make unit tests work under .NET 2.0
 
enhancements
- rename public fields to avoid name clashes with properties that trip
  up tools like Powershell
- suppress inclusion of spec comments in generated code due to
  licensing issues
- generate strong named (i.e. signed) assemblies. See the bottom of
  https://www.rabbitmq.com/dotnet.html for details

building & packaging
--------------------
enhancements
- introduce wrapper scripts in macports, as in the Debian and RPM
  packaging, which ensure the real scripts are run as the right
  ('rabbitmq') user
- remove build-time dependency on mnesia
- trim Debian Erlang package dependencies
- auto-generate the module list in rabbit.app, thus ensuring it is
  always up to date

Upgrading
=========

When upgrading from releases earlier than 1.5.3, note that the place
from which the server startup and control scripts source configuration
information on Unix systems has changed from /etc/default/rabbitmq to
/etc/rabbitmq/rabbitmq.conf. If you have been using the former, just
move the file to the latter location.  The /etc/default/rabbitmq file
(/etc/sysconfig/rabbitmq on RPM-based systems) is still being sourced
by the init.d script, but it should only contain settings directly
affecting the behaviour of the init.d script, such as NODE_COUNT.

The database schema has changed. When the RabbitMQ server detects the
presence of an old database, it moves it to a backup location, creates
a fresh, empty database, and logs a warning. If your RabbitMQ
installation contains important data, such as user accounts, durable
exchanges and queues, or persistent messages, then we recommend you
contact rabbitmq-sales@pivotal.io for assistance with the upgrade.
