Release: RabbitMQ 1.7.2

Release Highlights
==================

server
------
bug fixes
- fix a number of problems with memory monitoring under Windows,
  including compatibility issues with versions of Erlang/OTP older
  than R13, and 64-bit versions of Windows. See the updated
  documentation at https://www.rabbitmq.com/extensions.html#memsup
- correct various path escaping bugs under Windows that could result
  in RabbitMQ failing to start
- make 'rabbitmq-multi start_all <n>' work again for n>1
- issuing a basic.qos when there are outstanding acks can no longer
  result in a higher limit than requested
- enforce codec size limits, thus preventing the server from sending
  invalid AMQP frames

enhancements
- add rabbitmqctl list_channels and list_consumers commands, and add
  exclusivity information to list_queues. Also introduce a
  close_connection command to allow an administrator to selectively
  terminate client connections. See the updated admin guide at
  https://www.rabbitmq.com/admin-guide.html for details on these new
  features.
- remove the explicit setting of TCP buffer sizes in the server, thus
  allowing auto-(re)sizing to occur. This generally results in
  substantially improved throughput over high-latency links, and makes
  manual fine-tuning easier.
- introduce declarative boot sequencing, which allows plugins to be
  started at arbitrary chosen points during the sequence

Java client
-----------
bug fixes
- ensure that QueuingConsumer throws an ShutdownSignalException in
  *all* consuming threads, not just one
- fix race conditions in 'tracer' tool that could cause it to fail

enhancements
- make exception stack traces more meaningful
- allow overriding of several RpcClient methods, for easier extension
  and re-use
- improve performance of channel creation for high channel counts
- improve performance of 'tracer' tool
- add option to 'tracer' tool to suppress content bodies, which is
  useful for tracing connections carrying a high data volume
- better exception reporting in 'tracer' tool

.net client
-----------
enhancements
- improve performance of channel creation for high channel counts

building & packaging
--------------------
bug fixes
- under macports, ensure env var settings are passed to the various
  startup and control scripts


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
