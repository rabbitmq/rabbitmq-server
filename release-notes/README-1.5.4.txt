Release: RabbitMQ 1.5.4
Status : final

Release Highlights
==================

server
------
bug fixes
- starting a RabbitMQ instance that contains a large number
  (thousands) durable exchanges, queues or bindings now takes just a
  few seconds instead of several minutes.
- on Windows, rabbitmq-multi.bat can now start RabbitMQ even when the
  path to the startup script contains spaces, whereas previously that
  would fail.
- on Windows, the rabbitmqctl.bat and rabbitmq-multi.bat scripts now
  report errors correctly instead of swallowing them.

enhancements
- make the default settings of the various env vars which can be set
  in rabbitmq.conf visible to that script, thus permitting more
  advanced manipulation of the settings than was previously possible.
- permit configuration of rabbitmqctl's Erlang start parameters by
  sourcing rabbitmq.conf from the script and adding some env vars.
- on Windows, rabbitmq-server.bat and rabbitmq-multi.bat can now be
  configured with the RABBITMQ_{SERVER,MULTI}_{ERL,START}_ARGS env
  vars.

Java client
-----------
no changes

.net client
-----------
no changes

building & packaging
--------------------
bug fixes
- correct paths in 64-bit RPMs; the paths got broken in the 1.5.3
  release, preventing the server from starting
- in the Debian and RPM packages, set the current working dir of the
  various scripts to /var/lib/rabbitmq instead of /. The latter was
  preventing crash dumps from being written.
- fix BSD incompatibility in 'make srcdist'

enhancements
- minor tweaks in Debian and RPM packaging for better compliance with
  packaging guidelines


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

When upgrading from releases earlier than 1.5.x, note that the
database schema has changed. When the RabbitMQ server detects the
presence of an old database, it moves it to a backup location, creates
a fresh, empty database, and logs a warning. If your RabbitMQ
installation contains important data, such as user accounts, durable
exchanges and queues, or persistent messages, then we recommend you
contact rabbitmq-sales@pivotal.io for assistance with the upgrade.


Credits
=======

We would like to thank the following individuals for submitting bug
reports and feedback that we incorporated into this release:

Alex Clemesha
Aymerick Jehanne
John Leuner
