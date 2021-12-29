Release: RabbitMQ 2.6.0

Release Highlights
==================

server
------
bug fixes
- upgrading from RabbitMQ 2.1.1 to any later release could break if
  there were durable queues with persistent messages present
- on very slow machines, starting rabbit via the supplied init scripts
  could fail with a timeout
- rabbit could fail to stop (when asked to do so) in the presence of
  some plug-ins (e.g. shovel)
- 'ram' nodes in a cluster could consume ever increasing amounts of
  disk space
- the presence of fast consumers on a queue could significantly delay
  the addition of new consumers
- when a client was issuing a tx.commit in one channel, and
  simultaneously, in another channel, deleted a durable queue with
  persistent messages involved in that tx, rabbit could terminate with
  an error
- when a client was using both basic.qos and channel.flow, the latter
  would fail to re-enable message flow
- when using 'confirm' mode, the deletion of queues could cause nacks
  to be issued (incorrectly)
- in extremely rare circumstances (never observed in the wild), a
  queue with a per-queue message ttl could break during sudden changes
  in rabbit memory usage

enhancements
- introduce active-active HA, with queues getting mirrored on nodes in
  a cluster. See https://www.rabbitmq.com/ha.html
- revamp the handling of AMQP's tx (transaction) class and clarify its
  behaviour See https://www.rabbitmq.com/specification.html#tx
- replace the 'administrator' flag, as used by the management plugin,
  with a more general 'user tags' mechanism. See
  https://www.rabbitmq.com/man/rabbitmqctl.8.man.html#set_user_tags
- do not require 'configure' permissions for passive queue/exchange
  declaration
- optimise of message delivery on channels with a basic.qos
  prefetch limit that are consuming from many queues
- in 'rabbitmqctl list_channels', do not show the tx mode by default
- when a cluster 'degrades' to only containing ram nodes - through
  'rabbitmqctl' actions or node failure - display/log a warning.
- eliminate some spurious errors from the sasl log

java client
-----------
enhancements
- allow response timeouts to be specified in the {Json}RpcClient
- introduce Channel.waitForConfirms() helper method, to make usage of
  'confirm' mode more convenient in common cases.
- re-introduce default constructor for BasicProperties
- cater for multiple listeners in all APIs
- eradicate use of impl types in public APIs
- make Tracer embeddable

.net client
-----------
enhancements
- return the complete result of a QueueDeclare, rather than just the
  queue name.
- introduce IModel.WaitForConfirms() helper method, to make usage of
  'confirm' mode more convenient in common cases.
- document 'confirms' in user guide

management plugin
-----------------
bug fixes
- listing/inspecting queues with exclusive consumers would trigger a
  500 error
- lots of cookies would be created for recording implicit preferences
- /api/aliveness-test could return a 500 error instead of 401
- fix off-by-one error in used file descriptor count on some
  platforms, and gracefully deal with absence of 'lsof' command

enhancements
- introduce a more advanced permissions model, allowing access to
  information for monitoring purposes without the user needing to be a
  rabbit administrator. See
  https://www.rabbitmq.com/management.html#permissions
- simplify changing the URL; shorter default URL
- make the stats collection interval configurable, providing a way to
  reduce the impact of stats collection on servers with many active
  connections/channels/queues, and adjust the rate calculation
  period. See
  https://www.rabbitmq.com/management.html#statistics-interval
- in a cluster, make the management stats db highly available; it
  automatically fails over to a different node
- get the management stats db to 'catch up' when it is started in a
  cluster and there are existing nodes with queues etc
- report file descriptor counts on more platforms
- display message re-delivery rates
- show (mochi)web listeners
- handle encoding errors gracefully
- add an extension mechanisms - plug-ins for the management
  plug-in. The first of these is rabbitmq-shovel-management which
  displays status information of the rabbitmq-shovel plugin
- add fields for well-known arguments such as message TTL and alternate
  exchange to queue and exchange forms


mochiweb plugin
---------------
bug fixes
- on slow machines a timeout could occur during startup

enhancements
- the '*' listener context no longer needs to be specified, thus
  simplifying configuration

auth-backend-ldap plugin
------------------------
enhancements
- eliminate "undefined function" warning on startup

shovel plugin
-------------
enhancements
- support guaranteed delivery with 'confirm' mode
- support the use of AMQP 0-9-1 methods in configuration

STOMP plugin
------------
bug fixes
- heartbeats were issued as a 0x0 byte instead of LF (0x0A)

enhancements
- provide a way to send & subscribe to existing AMQP queues
- support temporary/reply queues
- support durable subscriptions
- set the default prefetch count for /queue destinations to
  'unlimited' instead of 1
- optionally allow clients to omit the login & passcode in CONNECT
  frames, using a configurable default user instead
- optionally allow clients to omit the CONNECT frame altogether

For more details on all the above see the STOMP plugin documentation
at https://www.rabbitmq.com/stomp.html

federation plugin
-----------------
First release of this plugin, which offers scalable publish /
subscribe messaging across WANs and administrative domains. See
http://hg.rabbitmq.com/rabbitmq-federation/file/default/README

build and packaging
-------------------
bug fixes

enhancements
- make Windows Start menu entries more easily identifiable/searchable
- stop producing the Windows bundle. The Windows installer has matured
  sufficiently to take its place.
- employ the same convention for plugin app source files as rebar
- clean up some xref warnings in the plugin build

Upgrading
=========
To upgrade a non-clustered RabbitMQ from release 2.1.1 or later, simply
install the new version. All configuration and persistent message data
is retained.

To upgrade a clustered RabbitMQ from release 2.1.1 or later, install
the new version on all the nodes and follow these instructions:
    https://www.rabbitmq.com/clustering.html#upgrading
All configuration and persistent message data is retained.

To upgrade a non-clustered RabbitMQ from release 2.1.0, first upgrade
to 2.1.1 (which retains all data), and then to the current version as
described above.

To upgrade a clustered RabbitMQ prior to 2.1.1 or a stand-alone broker
from releases prior to 2.1.0, if the RabbitMQ installation does not
contain any important data then simply install the new
version. RabbitMQ will move the existing data to a backup location
before creating a fresh, empty database. A warning is recorded in the
logs. If your RabbitMQ installation contains important data then we
recommend you contact rabbitmq-sales@pivotal.io for assistance with the
upgrade.
