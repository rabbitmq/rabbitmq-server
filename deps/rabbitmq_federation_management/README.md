# RabbitMQ Federation Management Plugin

This plugin adds information on federation link status to the management
plugin.


## Installation

In recent releases, this [plugin](https://www.rabbitmq.com/plugins.html) ships with RabbitMQ.
[Enable](https://www.rabbitmq.com/plugins.html#basics) it with

``` shell
rabbitmq-plugins enable rabbitmq_management rabbitmq_federation_management
```

If you have a heterogenous cluster (where the nodes have different
plugins installed), this should be installed on the same nodes as the
management plugin.


## Use over HTTP API

The HTTP API endpoints allow for retrieval of federation links:

    # lists all links
    GET /api/federation-links
    # lists links in a vhost
    GET /api/federation-links/{vhost}


## Building From Source

To [build the plugin](https://www.rabbitmq.com/plugin-development.html), use

    make dist

and see under the `./plugins` directory.


## Copyright and License

(c) 2007-2020 VMware, Inc. or its affiliates.

See `LICENSE` for license information.
