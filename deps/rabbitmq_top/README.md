# RabbitMQ Top Plugin

Adds UNIX top-like information on the Erlang VM to the [management UI](https://www.rabbitmq.com/management.html).
The closest interactive CLI alternative in recent releases is `rabbitmq-diagnostics observer`.

This is what it looks like:

![](https://i.imgur.com/m7cWTLV.pngP)

## Installation

This plugin ships with RabbitMQ as of `3.6.3`. Enable it with

``` bash
# use sudo as necessary
rabbitmq-plugins enable rabbitmq_top
```

### RabbitMQ 3.5.x

You can download a pre-built binary of this plugin for RabbitMQ `3.5.x` from [RabbitMQ Community plugins page](https://bintray.com/rabbitmq/community-plugins/rabbitmq_top).


## Usage

Sort by process ID, memory use or reductions/sec (an approximate
measure of CPU use).

Click on the process description (e.g. "my queue") to see that
object's management view.

Click on the process ID (e.g. "&lt;0.3423.0&gt;") to see some more
Erlang process details.

See [Memory Use Analysis guide](https://www.rabbitmq.com/memory-use.html) on RabbitMQ website
for more information.

## HTTP API

You can drive the HTTP API yourself. It installs into the management plugin's API; you should understand that first. Once you do, the additional paths look like:

    /api/top/<node-name>

List of processes. Takes similar query string parameters to other
lists, `sort`, `sort_reverse` and `columns`. Sorting is quite
important as it currently hard-codes returning the top 20 processes.

    /api/process/<pid>

Individual process details.

## More Screenshots

Individual process metrics are also available:

![](https://i.imgur.com/BYgIqQF.png)

## Building from Source

You can build and install it like any other plugin (see
[the plugin development guide](https://www.rabbitmq.com/plugin-development.html)).

## License and Copyright

(c) 2007-2020 VMware, Inc. or its affiliates.

Released under the same license as RabbitMQ.
