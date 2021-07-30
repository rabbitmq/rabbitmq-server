Release: RabbitMQ 1.5.5
Status : final

Release Highlights
==================

server
------
bug fixes
- in a clustered setup, bindings to durable queues are now correctly
  recovered when a queue's node restarts.
- node failure in a clustered setup could trigger premature exchange
  auto-deletion
- the cluster config file name was inadvertently changed from
  rabbitmq_cluster.config to cluster.config in release 1.5.4. It has
  now been changed back.
- when attempting to delete a non-existing exchange, return 404 (not
  found), as defined by the spec, rather than 541 (internal error)
- correct some type specs to keep dialyzer happy

enhancements
- display the node name and database dir on startup

Java client
-----------
bug fixes
- correct semantics of connection.tune's channel-max parameter - it
  determines the range of usable channel numbers - from 1 to
  channel-max, inclusive. Previously the highest channel number we
  allowed was channel-max - 1.
- correct misleading javadoc for GetResponse.getMessageCount().

enhancements
- improve error reporting

.net client
-----------
bug fixes
- correct semantics of connection.tune's channel-max parameter - it
  determines the range of usable channel numbers - from 1 to
  channel-max, inclusive. Previously the highest channel number we
  allowed was channel-max - 1.

building & packaging
--------------------
bug fixes
- work around absence of escript in path on some Fedora/EPEL
  installations 
- make build work with python 2.4, which Fedore/EPEL 5 is on
- work around possible bug in Debian packaging of Erlang OTP R13,
  which misses a dependency on os-mon in erlang-nox

enhancements
- minor tweaks in RPM packaging for better compliance with packaging
  guidelines


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

Aaron Cline
Bradford Cross
John Leuner
Levi Greenspan
Peter Lemenkov
Rob Golkosky
Steve Marah
