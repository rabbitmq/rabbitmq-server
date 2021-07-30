Release: RabbitMQ 1.7.0
Status : beta

Release Highlights
==================

server
------
bug fixes
- prevent timeouts of rabbitmqctl when the server is busy
- prevent load avg calculation from failing under high load, which
  could cause connection establishment to break
- remove channel closing timeout since it can cause a protocol
  violation
- prevent client disconnects from sometimes resulting in enormous
  error log entries and causing considerable CPU and memory pressure

enhancements
- support SSL natively - see <https://www.rabbitmq.com/ssl.html>
- add a plugin mechanism to provide a framework for developing rabbit
  extensions and managing their installation - see 
  <https://www.rabbitmq.com/plugin-development.html>
- support configuration via erlang config file, which has fewer
  escaping and formatting requirements than the other configuration
  mechanisms - see <https://www.rabbitmq.com/install.html#configfile>
- display diagnostics when rabbitmqctl fails with a badrpc error,
  making it easier to track down the cause
- improve queue hibernation logic to reduce system load in
  pathological scenarios, like invocations of 'rabbitmqctl
  list_queues' at one second intervals
- increase consumer throughput under high load 
- improve performance of channel and connection termination
- escape output of all rabbitmqctl commands
- react to memory pressure more quickly
- more graceful handling of some rare error conditions during
  connection establishment, preventing spurious error log entries
- display location of application descriptor on startup, to make it
  easy to locate rabbit installations
- in 'rabbitmqctl list_connections', display the connection state by
  default, and no longer shows absent usernames as 'none', thus
  avoiding possible confusion
- add hook mechanism for altering/augmenting broker behaviour
- add cute banner :)

Java client
-----------
bug fixes
- work around Java Hotspot bug that could cause channel number
  allocation to return null

enhancements
- disable heartbeats by default
- add queuePurge to API
- make content properties (deep) cloneable 

.net client
-----------
bug fixes
- fix a number of race conditions in the Subscription close/shutdown
  logic, making it safe to close Subscriptions from any thread
- allow SharedQueues (and thus QueuingBasicConsumer and Subscription)
  to drain messages after close

enhancements
- disable heartbeats by default
- make content properties (deep) cloneable 
- add some more AmqpTcpEndpoint constructors to API

building & packaging
--------------------
bug fixes
- prevent purging of server debian and RPM packages from failing due
  to epmd still running
- fix escaping/quoting corner cases in server control wrapper scripts
- in RPM packages, eliminate spurious removal of server startup from
  all run levels during upgrade

enhancements
- install server Erlang app in RabbitMQ-specific directory tree
  (e.g. /usr/lib/rabbitmq) rather than the Erlang/OTP tree. This
  allows OTP to be upgraded w/o breaking rabbit.
- package .net client in Windows installer
- include .net client in complete windows bundle
- switch .net client build from nant to msbuild and include VS
  solution
- update complete windows bundle from Erlang/OTP R11B5 to R12B5
- make installation work under MacPorts 1.8.0
- make server buildable under freebsd 
- permit configuration of server startup log locations in
  /etc/default/rabbitmq
- improve formatting of man pages
- do not stomp on RABBITMQ_* environment variables in server Makefile

Upgrading
=========
The database schema has not changed since the 1.6.0 release, so user
accounts, durable exchanges and queues, and persistent messages will
all be retained during the upgrade.

If, however, you are upgrading from a release prior to 1.6.0, when the
RabbitMQ server detects the presence of an old database, it moves it
to a backup location, creates a fresh, empty database, and logs a
warning. If your RabbitMQ installation contains important data then we
recommend you contact rabbitmq-sales@pivotal.io for assistance with the
upgrade.
