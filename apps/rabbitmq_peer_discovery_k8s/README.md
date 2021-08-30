# RabbitMQ Peer Discovery Kubernetes

## Overview

This is an implementation of RabbitMQ [peer discovery interface](https://www.rabbitmq.com/blog/2018/02/12/peer-discovery-subsystem-in-rabbitmq-3-7/)
for Kubernetes.

This plugin only performs peer discovery using Kubernetes API as the source of data on running cluster pods.
Please get familiar with [RabbitMQ clustering fundamentals](https://rabbitmq.com/clustering.html) before attempting
to use it.

Cluster provisioning and most of Day 2 operations such as [proper monitoring](https://rabbitmq.com/monitoring.html)
are not in scope for this plugin.

For a more comprehensive open source RabbitMQ on Kubernetes deployment solution,
see the [RabbitMQ Cluster Operator for Kubernetes](https://www.rabbitmq.com/kubernetes/operator/operator-overview.html).
The Operator is developed [on GitHub](https://github.com/rabbitmq/cluster-operator/) and contains its
own [set of examples](https://github.com/rabbitmq/cluster-operator/tree/master/docs/examples).


## Supported RabbitMQ Versions

This plugin ships with RabbitMQ 3.7.0 or later.


## Installation

This plugin ships with [supported RabbitMQ versions](https://www.rabbitmq.com/versions.html).
There is no need to install it separately.

As with any [plugin](https://rabbitmq.com/plugins.html), it must be enabled before it
can be used. For peer discovery plugins it means they must be [enabled](https://rabbitmq.com//plugins.html#basics) or [preconfigured](https://rabbitmq.com//plugins.html#enabled-plugins-file)
before first node boot:

```
rabbitmq-plugins --offline enable rabbitmq_peer_discovery_k8s
```

## Documentation

See [RabbitMQ Cluster Formation guide](https://www.rabbitmq.com/cluster-formation.html) for an overview
of the peer discovery subsystem, general and Kubernetes-specific configurable values and troubleshooting tips.

Example deployments that use this plugin can be found in an [RabbitMQ on Kubernetes examples repository](https://github.com/rabbitmq/diy-kubernetes-examples).
Note that they are just that, examples, and won't be optimal for every use case or cover a lot of important production
system concerns such as monitoring, persistent volume settings, access control, sizing, and so on.


## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) and our [development process overview](https://www.rabbitmq.com/github.html).


## License

[Licensed under the MPL](LICENSE-MPL-RabbitMQ), same as RabbitMQ server.


## Copyright

(c) 2007-2020 VMware, Inc. or its affiliates.
