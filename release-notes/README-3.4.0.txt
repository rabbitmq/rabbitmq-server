Release: RabbitMQ 3.4.0

Security Fixes
==============

management plugin
-----------------
26414 do not trust X-Forwarded-For header when enforcing 'loopback_users'

various
-------
26419 disable SSLv3 by default to prevent the POODLE attack


Release Highlights
==================

server
------
bug fixes
26354 prevent force_event_refresh message from killing connections that have
      not fully started (since 3.3.0)
26347 ensure bindings are deleted when deleting queue records as part
      of rabbitmqctl forget_cluster_node (since 3.0.3)
26341 add assertions to prevent silent failure from DETS errors in
      rabbit_recovery_terms (since 3.3.0)
26171 prevent crash in rare conditions in gm:find_member_or_blank/2 during
      mirror startup (since 2.6.0)
26368 prevent autoheal from hanging when loser shuts down before the winner
      learns it is the winner (since 3.1.0)
25850 prevent excessive binary memory use when accepting or delivering
      large messages at high speed (since 1.0.0)
26230 ensure exchanges and queues are federated appropriately when
      created when policy exists to make them so but the plugin is
      not enabled (since 3.0.0)
26389 prevent consumer utilisation getting stuck at 0% after busy queue
      goes idle (since 3.3.0)
26370 prevent "rabbitmqctl cluster_status" from breaking the database
      if invoked at the wrong point during first startup (since 3.0.0)
26295 ensure "rabbitmqctl wait" waits for plugins to start
26336 fix logging when cluster auto-config fails (since 3.0.0)
26338 log enotconn as 'connection_closed_abruptly', not an internal
      error (since 1.0.0)
26343 fix warning about missing behaviour_info/1 in supervisor2 with
      older Erlang (since 3.2.0)
26363 ensure cluster auto-config does not try to cluster with nodes
      which have had "rabbitmqctl stop_app" invoked (since 3.0.0)
26378 fix compilation warnings about conflicting behaviours (since 2.6.0)
26386 ensure broker starts even if vhost pointed to by default_vhost
      config item has been deleted (since 1.0.0)
26404 prevent queue synchronisation from hanging if there is a very
      short partition just as it starts (since 3.1.0)

enhancements
21446 allow crashing queue processes to recover, using persistent data
      if present
25813 provide fast direct route for RPC replies
        (see https://www.rabbitmq.com/direct-reply-to.html)
24926 allow plugins to be enabled / disabled without restarting the server
25884 add argument and policy to limit queue length in bytes
26150 prevent clean leader replica shutdown from promoting unsynchronised
      mirrors and thus losing messages; add ha-promote-on-shutdown
      to configure
26151 make queues located on down cluster nodes visible in "rabbitmqctl
      list_queues"
26213 prevent undefined behaviour during partial partitions by
      promoting them to full ones
26254 allow "rabbitmqctl_forget_cluster_node" to promote mirror queue mirrors
      that are down and thus recover from loss of a node containing masters
      after it was the last node to stop
26256 add "rabbitmqctl force_boot" command to allow administrator to
      override RabbitMQ's idea of the last node to shut down
26307 add messages_{ready,unacknowledged}_ram / messages_ram /
      messages_persistent queue info keys
25666 / 26339 add message_bytes / message_bytes_{ready,unacknowledged,
      ram,persistent} queue info keys
25214 improve robustness in the face of stray messages from Mnesia after
      partitions
25279 make SSL handshake timeout configurable
25678 make mnesia table loading timeout configurable
26148 add username and vhost to amqp_error log messages
26169 add username / connection information to firehose trace messages
26242 improve clarity of rabbitmqctl error messages when stop_app has been
      invoked
26225 provide connection age in rabbitmqctl list_connections
25446 add "rabbitmq-plugins set" subcommand
25824 / 26398 provide a summary of binary memory use
26397 split out memory used by mirrors vs masters / unmirrored in the
      memory summary
26401 split out memory used by connection readers / writers / channels / other
      in the memory summary
26192 improve usability of "rabbitmqctl remove_cluster_node --offline" by
      not requiring the user to start a node with RABBITMQ_NODE_ONLY
18626 add RABBITMQ_USE_LONGNAME (with thanks to Marcos Diez)
26204 allow message TTL and queue expiry times above (2^32)-1 milliseconds
26211 fix use of type specifications deprecated in Erlang 17
26366 improve error messages when queue / exchange equivalence checks fail
26387 use new strange way to determine OTP minor version number
26394 add cluster heartbeat messages at a faster rate than net_ticktime
26406 add environment for plugins and non-RabbitMQ apps to "rabbitmqctl
      environment"
25848 warn if RABBITMQ_SERVER_ERL_ARGS set in a way that will lead to
      poor performance
25454 warn if rabbitmq-plugins and rabbitmq-server disagree on the
      location of the enabled_plugins file
26221 improve performance of queue.declare{nowait=true}


building & packaging
--------------------
enhancements
26344 ensure missing config file is correctly logged in .deb / RPM
      (since 3.3.0)
26154 switch standalone OS X build to use Erlang 17.1
26040 add missing BuildRequires to RPM spec (since 1.8.0)
26411 fix warning on Debian build clean due to deleted files


management plugin
-----------------
enhancements
26107 provide (and default to) mode where we maintain message rates
      only per object (queue, exchange etc) not per object
      pair (queue->channel etc) to save memory
26174 improve responsiveness of management API under load
25329 maintain history and draw charts for some per-node stats
      (memory, disk space etc)
25470 provide UI to show / hide series in charts in the web UI
26382 provide UI to show / hide columns columns in the web UI
26225 provide connection age in connection list
25824 provide a summary of binary memory use
26151 make queues located on down cluster nodes visible in queue list
23724 provide API to list all consumers
26340 redesigned, more concise interface for queue / exchange /
      policy arguments
26193 display locations of configuration, database and logs in management
26193 detect and warn on mismatched net_ticktime setting
26235 show enabled plugins in management
25984 switch to HTML5 local storage where available, ensure
      multiple web UIs on same host do not share login
26358 support setting message properties with "rabbitmqadmin publish"
26390 ensure all charts have the same time range
26391 make "rabbitmqadmin list" restrict to a default set of columns

bug fixes
26399 ensure statistics do not depend on erlang:now/0 being in sync with
      os:timestamp/0 (since 3.2.0)


shovel plugin
-------------
enhancements
26239 allow dynamic shovels to set message properties like static
      ones do, and allow static shovels to use add_forward_headers like
      dynamic ones do


LDAP plugin
-----------
enhancements
26275 support LDAP connections using StartTLS (requires Erlang R16B03 or later)


tracing plugin
--------------
enhancements
26357 add milliseconds to timestamps


STOMP plugin
------------
enhancements
26306 add flow control for message deliveries through STOMP; greatly reduces
      memory use when slow consumers without prefetch-count connect to a
      large / fast moving queue
26243 ensure all stomp-named queues are named "stomp-*"
26266 support "requeue" header on NACK frames


MQTT plugin
-----------
enhancements
26330 add flow control for message deliveries through MQTT; greatly reduces
      memory use when slow consumers without prefetch-count connect to a
      large / fast moving queue

bug fixes
26356 fix incorrect reporting of MQTT protocol version when using MQTT 3.1.1


Web-STOMP plugin
----------------
enhancements
26392 don't depend on the SockJS CDN


java client
-----------
enhancements
26402 provide a means to configure the time given to slow consumers
      to continue consuming internally queued messages after the
      connection closes
26359 add listeners for queue name changes during recovery
26207 add APIs to make methods easier to use in nowait mode
26121 add --randomRoutingKey flag to PerfTest
26091 add --consumerRate flag to PerfTest
26348 make ConnectionFactory's networkRecoveryInterval property into a long

bug fixes
26364 clean up client-side references to auto-deleted queues in the
      common case (since 3.3.0)
26374 limit size of WorkPool queues, thus prevent slow consumer with no
      prefetch limit from consuming unbounded memory (since 2.7.0)
26413 prevent duplicate connection recovery listeners from being
      registered (since 3.3.0)

dependency change
26095 drop support for Java 1.5

licencing change
24543 make the Java client additionally avaliable under the ASL2


.net client
-----------
enhancements
26130 automatic connection recovery similar to that of the Java client
26208 add APIs to make methods easier to use in nowait mode
26324 introduce an interface for ConnectionFactory
26334 set up stream timeouts as early as possible (thanks to John Oliver)
26199 allow IO and heartbeat to be background threads
25525 allow Subscription class to set explicit consumer tag
26097 add support for nack / reject in Subscription
26122 remove unnecessary lock in Subscription

feature removal
26131 / 26132 remove support for versions of AMQP prior to 0-9-1
26133 remove redirect following


erlang client
-------------
enhancements
26166 allow default ssl options to be provided in the configuration file

bug fixes
26418 ensure writer death is detected in direct connections (since 3.2.0)
      (with thanks to Christopher Faulet)
26346 ensure amqp_rpc_client uses exclusive, autodelete response
      queues (since 1.3.0)


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
