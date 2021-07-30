Release: RabbitMQ 1.3.0
Status : beta

Release Highlights
==================

server
------
bug fixes
- eliminate a number of race conditions that could result in message
  loss and other incorrect or unusual behaviour
- eliminate duplication of messages when bindings overlap
- prevent unbounded memory usage when topic exchanges encounter
  messages with highly variable routing keys
- redesigned persister so it works properly in a clustered broker
- fix a couple of bugs that could cause persisted messages to stick
  around forever, resulting in an unbounded memory usage
- prevent performance drop under high load
- do not requeue messages on tx.rollback
- fix bug in heartbeat logic that could result in a connection
  remaining alive even though the client had stopped sending any data
- correct handling of queue.bind with empty routing key and queue name
- complain about acks with an unknown delivery tag
- prevent sending of zero-length content body frames

enhancements
- improve error reporting for various framing-related errors
- improve rabbitmq-multi robustness and error reporting
- identify log locations in startup message
- keep log file contents on server restart
- support QPid's extended field types
- improve performance, particularly for persistent messaging
- re-architect internals to eliminate some code duplication, reduce
  component dependencies, and construct cleaner internal APIs

Java client
-----------
bug fixes
- eliminate edge case that could result in stuck AMQConnection.close
- use linear timers to prevent heartbeat timeouts on system clock
  adjustment, which happens in some virtualisation platforms 
- eliminate a race condition that could result in an exception when
  establishing a connection

enhancements
- add SSL support
- improve error reporting for various framing-related errors
- add new FileProducer/Consumer example
- make MulticastMain example more versatile, with improved command
  line options 
- improve performance

packaging
---------
bug fixes
- fix broken 'rabbitmqctl -n' on Debian
- fix broken removal of the rabbitmq-server Debian package
- fix broken Erlang library installation on 64bit RPM-based systems
- fix failure of server shutdown when started at boot time on Debian
- fix various problems with RPMs

improvements
- better compliance with debian packaging policies


Upgrading
=========

Care must be taken when upgrading a server that contains persisted
messages. The persister log format has changed between RabbitMQ-1.2.0
and this release. When RabbitMQ-1.3.0 first starts following an
upgrade it will move the existing persister log to a backup file -
check the log files for details. Thus the previously persisted
messages are not lost, but neither are they replayed. Therefore it is
recommended that the upgrade is performed only when there are no
important persistent messages remaining.


Credits
=======

We would like to thank the following individuals for submitting bug
reports and feedback that we incorporated into this release:

Andrew Munn
Barry Pederson
Ben Hood
David Pollak
Emmanuel Okyere
Joe Lee
John Leuner
Matt Darling
Michael Arnoldus
Nick Levine
Tom Samplonius
Willem van Heemstra
