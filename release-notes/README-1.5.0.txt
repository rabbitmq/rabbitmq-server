Release: RabbitMQ 1.5.0
Status : beta

Release Highlights
==================

server
------
bug fixes
- support running on top of the latest Erlang/OTP release (R12B-5)
- maintain effect visibility guarantees in cross-node routing
- reduce likelihood of timeouts when channels interact with a large
  number of queues
- graceful handling of some corner cases in abrupt client disconnect

enhancements
- remove tickets and realms
- improve scalability of queue and binding creation and deletion
- add 'queue.unbind' command to protocol
- disable Nagle for more consistent latency
- throttle producers with 'channel.flow' when running low on memory
  Note that this feature is disabled by default; please see
  https://www.rabbitmq.com/admin-guide.html#memsup for details.
- remove a few spurious errors in the logs
- show the actual listening IP & port in logs
- improve rabbitmqctl:
  - add a few useful info commands
  - add a 'reopen_logs' command to assist in log management
  - add a '-q' flag to suppress informational messages and thus
    facilitate post-processing of the output
  - write errors to stderr instead of stdout

Java client
-----------
bug fixes
- eliminate several race condition in connection and channel closure
  that could result in deadlock
- always respond to a server's 'connection.close' and 'channel.close'
- prevent interference between interal locking on channels and
  application-level locking

enhancements
- remove tickets and realms
- support 'queue.unbind'
- disable Nagle for more consistent latency
- react to server-issued 'channel.flow' by (un)blocking producers
- add channel.abort method to unconditionally and idempotently close a
  channel
- complete the set of channel and connection close and abort methods

.net client
-----------
bug fixes
- eliminate "Cannot access a disposed object" exception in connection
  closure and disposal
- correct heartbeat logic to prevent spurious timeouts when not idling

enhancements
- remove tickets and realms
- support 'queue.unbind'
- disable Nagle for more consistent latency
- react to server-issued 'channel.flow' by (un)blocking producers
- add IModel.abort method to unconditionally and idempotently close a
  channel
- complete the set of channel and connection close and abort methods

building & packaging
--------------------
bug fixes
- correct locations of libraries et al on 64bit rpm systems

enhancements
- detect upgrade from pre-1.5.0 and warn/ask user (under debian & rpm)
- comply with debian and rpm packaging policies and guidelines
- prevent accidental executing of scripts as non-root user under
  debian & rpm
- read /etc/default/rabbitmq in scripts on Unix-y systems, with env
  vars taking precedence over vars set in that file and the defaults
  used by the scripts
- prefix env vars with 'RABBITMQ_'
- allow script execution from anywhere (not just the scripts' dir)
- add script & instructions to start RabbitMQ as a Windows service
- add 'status' command to init.d script under debian & rpm
- automatic log rotation under debian & rpm
- use simplejson.py instead of json.py in code generation, thus
  allowing use of Python 2.6


Upgrading
=========

The database schema has changed between RabbitMQ-1.4.0 and this
release. When the RabbitMQ server detects the presence of an old
database, it moves it to a backup location, creates a fresh, empty
database, and logs a warning.

If your RabbitMQ installation contains important data, such as user
accounts, durable exchanges and queues, or persistent messages, then
we recommend you contact rabbitmq-sales@pivotal.io for assistance with the
upgrade.


Credits
=======

We would like to thank the following individuals for submitting bug
reports and feedback that we incorporated into this release:

Alister Morton
Aman Gupta
Andrius Norkaitis
Barry Pedersen
Benjamin Black
Benjamin Polidore
Brian Sullivan
David Corcoran
Dmitriy Samovskiy
Edwin Fine
Eran Sandler
Esteve Fernandez
Ezra Zygmuntowicz
Ferret
Gavin Bong
Geoffrey Anderson
Holger Hoffst�tte
Jacek Korycki
John Leuner
Jonatan Kallus
Jonathan McGee
Kyle Sampson
Leo Martins
Maarten Engelen
Nathan Woodhull
Nigel Verdon
Paul Jones
Pete Kay
Peter Kieltyka
Sarah Jelinek
Sean Treadway
Steve Jenson
Terry Jones
Vadim Zaliva
Valentino Volonghi
