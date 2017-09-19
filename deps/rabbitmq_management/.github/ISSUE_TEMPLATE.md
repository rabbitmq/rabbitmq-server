Thank you for using RabbitMQ and for taking the time to report an
issue.

## Does This Belong to GitHub or RabbitMQ Mailing List?

*Important:* please first read the `CONTRIBUTING.md` document in the
root of this repository. It will help you determine whether your
feedback should be directed to the RabbitMQ mailing list [1] instead.

## Please Help Maintainers and Contributors Help You

In order for the RabbitMQ team to investigate your issue, please provide
**as much as possible** of the following details:
  
* RabbitMQ version
* Erlang version
* RabbitMQ server and client application log files
* A runnable code sample, terminal transcript or detailed set of
  instructions that can be used to reproduce the issue
* RabbitMQ plugin information via `rabbitmq-plugins list`
* Client library version (for all libraries used)
* Operating system, version, and patch level

Running the `rabbitmq-collect-env` [2] script can provide most of the
information needed. Please make the archive available via a third-party
service and note that **the script does not attempt to scrub any
sensitive data**.

If your issue involves RabbitMQ management UI or HTTP API, please also provide
the following:

 * Browser and its version
 * What management UI page was used (if applicable)
 * How the HTTP API requests performed can be reproduced with `curl`
 * Operating system on which you are running your browser, and its version
 * Errors reported in the JavaScript console (if any)

This information **greatly speeds up issue investigation** (or makes it
possible to investigate it at all).  Please help project maintainers and
contributors to help you by providing it!

1. https://groups.google.com/forum/#!forum/rabbitmq-users
2. https://github.com/rabbitmq/support-tools/blob/master/scripts/rabbitmq-collect-env
