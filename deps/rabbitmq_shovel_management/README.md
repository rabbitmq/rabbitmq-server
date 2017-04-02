# RabbitMQ Shovel Management Plugin

Adds information on shovel status to the management plugin. Build it
like any other plugin.

If you have a heterogenous cluster (where the nodes have different
plugins installed), this should be installed on the same nodes as the
management plugin.


## Installing

This plugin ships with RabbitMQ. Enable it with

```
[sudo] rabbitmq-plugins rabbitmq_shovel_management
```


## Usage

When the plugin is enabled, you'll find a shovel management
link under the Admin tab.

The HTTP API is very small:

 * `GET /api/shovels`


## License and Copyright

Released under [the same license as RabbitMQ](https://www.rabbitmq.com/mpl.html).

2007-2017 (c) Pivotal Software Inc.
