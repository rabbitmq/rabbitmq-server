Release: RabbitMQ 1.8.0

Release Highlights
==================

server
------
bug fixes
- prevent a change in host name from preventing RabbitMQ from being
  restarted.
- ensure that durable exclusive queues do not survive a restart of the
  broker.
- fix a race condition that could occur when concurrently declaring
  exclusive queues.
- ensure that queues being recovered by a node in a cluster cannot be
  accessed via other nodes until the queue is fully initialised.
- prevent bursts of declarations or deletions of queues or exchanges
  from exhausting mnesia's transactional capacity.
- prevent bursts of connections from exhausting TCP backlog buffers.
- various corrections to documentation to correct discrepancies
  between the website, the man pages, and the commands' usage outputs.

enhancements
------------
- introduce a pluggable exchange type API permitting plugins to the
  broker to define new exchange types which can then be used by
  clients.
- introduce a backing queue API permitting plugins to the broker to
  define new ways in which messages can be stored.
- several semantic changes to bring the behaviour inline with the AMQP
  0-9-1 spec:
  + honour many of the queue exclusivity requirements for AMQP 0-9-1,
    such as queue redeclaration, basic.get, queue.bind and
    queue.unbind.
  + honour exchange and queue equivalence requirements for AMQP 0-9-1,
    especially for queue and exchange redeclaration.
  + ensure that exclusive queues are synchronously deleted before the
    connection fully closes.
  + permit durable queues to be bound to transient exchanges.
  + enforce detection and raising exceptions due to invalid and reused
    delivery-tags in basic.ack rigorously
  + queue.purge now does not remove unacknowledged messages.
- require clients to respond to channel.flow messages within 10
  seconds to avoid an exception being raised and more rigorously deal
  with clients that disobey channel.flow messages. See
  https://www.rabbitmq.com/extensions.html#memsup
- the server now supports the client sending channel.flow messages to
  temporarily halt the flow of deliveries to the client.
- optimise cross-node routing of messages in a cluster scenario whilst
  maintaining visibility guarantees.
- ensure that clients who present invalid credentials cannot flood the
  broker with requests.
- drop support for versions of Erlang older than R12B-3.
- ensure that the minimum number of frames are used to deliver
  messages, regardless of incoming and outgoing frame sizes.
- display the current version of Erlang when booting Rabbit, and
  ensure the version is sufficiently youthful.
- work around some name resolver issues, especially under Windows.
- introduce a Pacemaker OCF script (and then fix it, thanks to patches
  by Florian Haas) to permit RabbitMQ to be used in basic
  active/passive HA scenarios (see
  https://www.rabbitmq.com/pacemaker.html).


java client
-----------
bug fixes
- fix a race condition when closing channels which could lead to the
  same channel being closed twice.
- MulticastMain could calculate negative rates, due to integer
  wrapping.
- be consistent about naming conventions.

enhancements
- Java client is now available via Maven Central.
- redesign the ConnectionFactory to be more idiomatic.
- expose server properties in connection.start.
- allow additional client properties to be set in connection.start_ok.
- attempt to infer authentication failures and construct appropriate
  exceptions.
- MulticastMain now logs returned publishes.


.net client
-----------
bug fixes
- prevent memory leak due to DomainUnload event handler.
- improvements to catching connections which are timing out.
- ensure explicitly numbered closed channels return their channel
  number to the pool correctly.
- removed artificial limitation on maximum incoming message size.

enhancements
- expose server properties in connection.start.
- allow additional client properties to be set in connection.start_ok.
- attempt to infer authentication failures and construct appropriate
  exceptions.


code generation
---------------
enhancements
- permit multiple specifications to easily be combined and merged.
- permit any number of different "actions" in code generation.


building & packaging
--------------------
bug fixes
- stop the INSTALL file from being installed in the wrong place by the
  Debian packages.

enhancements
- source rpm (.src.rpm) packages are now available
- rpm packages are now noarch, matching the debs


Upgrading
=========
The database schema and the format in which persistent messages are
stored have both changed since the last release (1.7.2). When
starting, the RabbitMQ server will detect the existence of an old
database and will move it to a backup location, before creating a
fresh, empty database, and will log a warning. If your RabbitMQ
installation contains important data then we recommend you contact
rabbitmq-sales@pivotal.io for assistance with the upgrade.


Important notes on the AMQP 0-9-1 semantic changes
==================================================

This release incorporates a number of semantic changes to the broker
behaviour which bring the broker more in-line with the AMQP 0-9-1
specification. We don't think any of these changes are going to be a
big problem for anyone, and will probably be irrelevant for most
people. In almost all cases they're tightening up or tidying up edge
cases where the 0-8 spec was incomplete or specified something
unhelpful. However, it's probably worth reading the list below to make
absolutely sure you're not depending on any of our existing weird
behaviour.


Reuse of delivery tags
----------------------

In previous versions of RabbitMQ, you could ack the same message with
the same delivery tag multiple times. In 1.8.0 this will cause a
not-found exception. Note that if a message is redelivered for any
reason it will get a new delivery tag so you can ack it again.


Exchange equivalence
--------------------

In previous versions of RabbitMQ you could actively declare an
exchange with one set of durable and auto-delete parameters, then
actively declare it again with different parameters and get the same
exchange back. This now causes a precondition_failed exception, as it
would if the type does not match. Note that with the old behaviour the
exchange did not actually change to match the new parameters; you just
got back something that was not what you asked for.

In previous versions, when passively declaring an exchange, the type
parameter was checked (but not the durable and auto-delete
parameters).  Now only the name is checked. Passive declaration cannot
create an exchange, and exchanges are only identified by their
name. Therefore it does not make sense to require the other parameters
of exchange.declare to match the exchange declaration in the passive
case.


Queue equivalence
-----------------

Similarly, when actively redeclaring a queue you could vary the
durable and auto-delete parameters and get back a queue which did not
match what you asked for. Again, this is now causes a
precondition_failed exception.  Likewise, passive declaration of
queues only needs to match on the queue name, not any other
parameters.


Purging unacknowledged messages
-------------------------------

When queue.purge is called, messages which had been send but not
acknowledged used to be purged. Now they are not. This makes much more
sense as consumers from a queue may have no idea whether or not a
queue has been purged by some other client.


Binding durable queues to transient exchanges
---------------------------------------------

This used not to be permitted. Now it is. The binding is considered
transient.


Queue exclusivity enforcement
-----------------------------

In previous versions of RabbitMQ, an exclusive queue could still be
accessed by other connections for (un)binding or basic.get. This is now
not permitted.

Also, an exclusive queue would continue to exist for a short time after
the connection was closed. It's now deleted while the connection is
being closed (assuming that's happening in an orderly manner).
