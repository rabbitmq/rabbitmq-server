Release: RabbitMQ 1.5.1
Status : final

Release Highlights
==================

server
------
bug fixes
- handle race condition between routing and queue deletion that could
  cause errors on message publication
- the default settings for RABBITMQ_SERVER_ERL_ARGS were not taken
  into account, resulting in decreased network performance
- add workaround for the Erlang/OTP bug OTP-7025, which caused errors
  in topic routing in Erlang/OTP versions older than R12B-1
- display the nodes of queue and connection processes in rabbitmqctl's
  list_{queues,connections} command. Previously we displayed pids,
  which was broken and not particularly useful.

enhancements
- enable 'channel.flow'-based producer throttling by default on Linux
- include stack traces in error reports in rabbit.log
- speed up rabbitmqctl and rabbitmq-multi

Java client
-----------
no significant changes

.net client
-----------
bug fixes
- handle race condition in client-initiated connection closure that
  could result in an OperationInterruptedException

enhancements
- re-enable heartbeating by default

building & packaging
--------------------
bug fixes
- fix bug that caused removal of RPMs to be incomplete
- produce separate RPMs for SuSE-like systems to resolve various
  incompatibilities
- rename BUILD to README in order to prevent build failures on systems
  with case-insensitive file naming (such as OS X)

enhancements
- minor tweaks in Debian and RPM packaging for better compliance with
  packaging guidelines
- better handling of errors encountered during code generation


Upgrading
=========

No special precautions are necessary when upgrading from
RabbitMQ-1.5.0.

When upgrading from earlier releases, note that the database schema
has changed in RabbitMQ-1.5.x. When the RabbitMQ server detects the
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

Aymerick Jéhanne
Chuck Remes
Ezra Zygmuntowicz
Glenn Robuck
Mathias Gug
Michael Barker
