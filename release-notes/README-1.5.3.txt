Release: RabbitMQ 1.5.3
Status : final

Release Highlights
==================

server
------
bug fixes
- prevent the shell from attempting to perform path expansion on vars
  in the startup scripts, which was resulting in errors when starting
  rabbit on some systems.
- make guid generation independent of persister, thus preventing
  timeouts when the persister is busy
- get scripts to source configuration from /etc/rabbitmq/rabbitmq.conf
  rather than /etc/default/rabbitmq, since the latter is reserved for
  init.d scripts.

Java client
-----------
bug fixes
- eliminate race condition in server-initiated channel closure that
  could lead to deadlock

.net client
-----------
bug fixes
- eliminate race condition in server-initiated channel closure that
  could lead to deadlock

building & packaging
--------------------
enhancements
- minor tweaks in Debian and RPM packaging for better compliance with
  packaging guidelines
- place wrapper scripts for rabbitmq-server and rabbitmq-multi
  alongside the rabbitmqctl wrapper in /usr/sbin
- do not start the server by default on RPM-based systems, in order to
  comply with common practice and guidelines
- suppress stdout in logrotate scripts, to keep cron et al happy

Upgrading
=========

The place from which the server startup and control scripts source
configuration information on Unix systems has changed from
/etc/default/rabbitmq to /etc/rabbitmq/rabbitmq.conf. If you have been
using the former, just move the file to the latter location.  The
/etc/default/rabbitmq file (/etc/sysconfig/rabbitmq on RPM-based
systems) is still being sourced by the init.d script, but it should
only contain settings directly affecting the behaviour of the init.d
script, such as NODE_COUNT.

When upgrading from releases earlier than RabbitMQ-1.5.x, note that
the database schema has changed. When the RabbitMQ server detects the
presence of an old database, it moves it to a backup location, creates
a fresh, empty database, and logs a warning.

If your RabbitMQ installation contains important data, such as user
accounts, durable exchanges and queues, or persistent messages, then
we recommend you contact rabbitmq-sales@pivotal.io for assistance with the
upgrade.


Credits
=======

We would like to thank the following individuals for submitting bug
reports and feedback that we incorporated into this release:

Billy Chasen
Charl Matthee
Christopher Hoover
Darien Kindlund
Dmitriy Samovskiy
Jason Williams
Mathias Gug
Peter Lemenkov
Phil Stubbings
