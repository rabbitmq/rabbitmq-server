Release: RabbitMQ 1.7.1

Release Highlights
==================

server
------
bug fixes
- correct various quoting errors in the Windows scripts that caused
  them to fail
- ensure that stalled ssl negotiation do not block further ssl
  connection acceptance
- prohibit the (re)declaration of queues that reside on node that is
  currently stopped, thus preventing message loss or duplication when
  that node recovers
- eliminate race condition in queue auto-deletion, ensuring that it
  has completed before channel/connection closure completes
- ensure that ack processing cannot stall under heavy load when using
  basic.qos
- make plug-ins and config files work when running as a Windows
  service
- write crash dumps to a sensible location
  (%APPDATA%\RabbitMQ\erl_crash.dump by default) when running as a
  Windows service
- get the Windows service to use Erlang/OTP R12B-5 by default, since
  that, rather than R11B-5, is what we ship in the Windows bundle
- correct formatting of plug-in activation errors
- make column order of 'rabbitmqctl list_bindings' match the
  documentation
- do not escape spaces in rabbitmqctl output
- prevent vars declared in Windows scripts from polluting the
  environment
- clean up properly when the rabbit Erlang application is stopped,
  thus making it more well-behaved and easier to embed

enhancements
- make the various scripts work with complete short node names
- improve memory monitoring and producer throttling. See the updated
  documentation at https://www.rabbitmq.com/extensions.html#memsup.
- make tcp_listeners configurable via the rabbitmq.config file
- use the base64 module instead of ssl_base64 if we can, since the
  latter is sometimes missing from Erlang installations
- display pids instead of just nodes in 'rabbitmqctl list_connections'
  and 'rabbitmqctl list_queues', to aid troubleshooting
- add capability to display the transmitted client_properties in
  'rabbitmqctl list_connections'
- extend codec with array type ('A')
- add proper headers to auto-generated code

Java client
-----------
bug fixes
- eliminate race in connection establishment that could cause errors
  to be reported in different ways
- fix quoting in runjava.bat to stop it from tripping over exotic
  Windows paths

enhancements
- enforce codec size limits, specifically on AMQP's shortstr type,
  thus preventing the creation of invalid AMQP protocol frames
- add support for basic.recover in the API
- name threads, to aid troubleshooting
- allow applications to adjust socket configuration, e.g. buffer sizes
- extend codec with array type ('A')
- throw a more informative exception (UnknownChannelException) when
  receiving a frame for an unknown channel
- add proper headers to auto-generated code

.net client
-----------
bug fixes
- close connections on app domain unload, thus preventing spurious
  errors and possible connection leaks when the client is run in
  certain app containers, e.g. IIS
- close socket on ssl upgrade error, thus plugging a socket leak
- resolve various bugs in the ssl negotiation code that cause it to
  fail on .Net proper (though not mono)

enhancements
- improve performance by introducing I/O buffering
- permit ssl connections that do not validate the server certificate
- improve standard display of BrokerUnreachableException
- make SharedQueue implement IEnumerable and allow multiple concurrent
  enumerators per instance
- switch the code gen to the BSD-licensed version of the AMQP spec
- extend codec with array type ('A')
- add proper headers to auto-generated code

building & packaging
--------------------
bug fixes
- stop Debian package purge from failing after plug-in (de)activation
- when upgrading the rpm package, do not remove rabbit from any
  runlevels
- fix error handling in rabbit.app generation, ensuring that errors
  are reported rather than written to the generated file
- during Debian package removal, only kill epmd if it was started by
  the rabbitmq user, in order to reduce the likelihood of interference
  with other Erlang applications
- resolve minor incompatibility with some versions of 'echo' that
  could result in spurious '-e's appearing in script error messages

enhancements
- make MacPorts package work on Snow Leopard
- streamline dependencies in MacPorts package
- automate generation of MacPorts package and create a RabbitMQ
  MacPorts repository - see https://www.rabbitmq.com/macports.html
- mirror downloads onto Amazon Cloudfront, for better availability and
  download speed
- allow 'rabbitmq' user to execute the various wrapper scripts

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
