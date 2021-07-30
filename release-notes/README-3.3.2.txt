Release: RabbitMQ 3.3.2

Release Highlights
==================

server
------
bug fixes
26180 prevent certain operations (including queue creation and deletion)
      blocking until a connection closes when the socket limit is reached
      (since 2.7.0)
26227 fix incorrect log message about config file location when running as
      a Windows service, changing RABBITMQ_CONFIG_FILE and not reinstalling
      the service (since 3.3.0)
26172 ensure mirror queue does not hang if the GM process crashes at queue
      startup (since 2.6.0)
26178 prevent error logger crash in rare circumstances (since 3.3.0)
26184 prevent small log messages being needlessly truncated (since 3.3.0)
26226 ensure rabbitmqctl status does not crash if invoked while Mnesia is
      starting or stopping (since 3.0.0)
26200 fix garbled SSL log messages (since 3.0.3)
26203 prevent spurious log message if mirror queue crashes early (since 3.2.2)


management plugin
-----------------
bug fixes
26197 fix garbled error message if importing JSON definitions file with invalid
      input (since 2.1.0)
26209 ensure reasons for authentication failure are always logged (since 2.1.0)

enhancements
25376 add documentation on the JSON schema returned by GET queries


shovel plugin
-------------
bug fixes
26219 fix creation of dynamic shovels using direct connection URLs through
      rabbitmqctl (since 3.3.1)
26176 prevent deadlock deleting virtual host with active dynamic shovel on
      single core machine (since 3.3.0)


federation plugin
-----------------
bug fixes
26176 prevent deadlock deleting virtual host with active federation link on
      single core machine (since 3.0.0)


shovel-management plugin
------------------------
bug fixes
26165 ensure the status of static shovels is correctly shown (since 3.3.1)


LDAP plugin
-----------
bug fixes
26190 fix crash when LDAP uses SSL and nothing else does (since 2.3.0)


auth-mechanism-ssl plugin
-------------------------
bug fixes
25550 allow use of both certificate and password based authentication at the
      same time (since 2.3.0)


MQTT plugin
-----------
bug fixes
26194 prevent hang on broker shutdown when there are active MQTT connections
      (since 3.0.0)
26189 fix connection crash on shutdown if the connection starts very early
      (since 3.0.0)


STOMP plugin
------------
bug fixes
25550 allow use of both certificate and password based authentication at the
      same time (since 2.3.0)


Java client
-----------
bug fixes
26187 ensure network recovery delay is used when recovering from all types of
      exception (since 3.3.0)
26188 ensure TopologyRecoveryException includes cause's message (since 3.3.0)
26196 fix Javadoc for ConnectionFactory.setSocketConfigurator()


Erlang client
-------------
bug fixes
26160 declare xmerl application dependency (since 3.3.0)


Upgrading
=========
To upgrade a non-clustered RabbitMQ from release 2.1.1 or later, simply install
the new version. All configuration and persistent message data is retained.

To upgrade a clustered RabbitMQ from release 2.1.1 or later, install the new
version on all the nodes and follow the instructions at
https://www.rabbitmq.com/clustering.html#upgrading .

To upgrade RabbitMQ from release 2.1.0, first upgrade to 2.1.1 (all data will be
retained), and then to the current version as described above.

When upgrading from RabbitMQ versions prior to 2.1.0, the existing data will be
moved to a backup location and a fresh, empty database will be created. A
warning is recorded in the logs. If your RabbitMQ installation contains
important data then we recommend you contact support at rabbitmq.com for
assistance with the upgrade.
