# RabbitMQ Peer Discovery Kubernetes

## Overview

This is an implementation of RabbitMQ peer discovery interface for Kubernetes.

On Kubernetes, RabbitMQ should be deployed as a StatefulSet. Each Pod in a StatefulSet has
a name made up of the StatefulSet name and an ordinal index. The ordinal index values almost
always start with 0, although [this is configurable](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/#ordinal-index).

This plugin only allows the node with the lowest ordinal index (generally the pod with the `-0` suffix) to form a new cluster.
This node is referred to as the seed node.

All other nodes will join the seed node, or will forever keep try to join it, if they can't.

In the most common scenario, this means that:
* the pod with `-0` suffix will start immediately, effectively forming a new single-node cluster
* any other pod will join the pod with `-0` suffix and synchronize the cluster metadata with it

## Configuration

**In most cases, no configuration should be necessary beyond enabling this plugin.**

If you use the [RabbitMQ Cluster Operator](https://www.rabbitmq.com/kubernetes/operator/operator-overview)
or the [Bitnami Helm Chart](https://github.com/bitnami/charts/tree/main/bitnami/rabbitmq), this plugin is enabled by default,
so you don't have to do anything.

### Advanced Configuration

If you use a different ordinal start value in your StatefulSet, you have to configure this plugin to use it:
```
cluster_formation.k8s.ordinal_start = N
```
where `N` matches the `.spec.ordinals.start` value of the StatefulSet.

If the plugin doesn't work for any reason (a very unusual Kubernetes configuration or issues with hostname resolution)
and you have to force RabbitMQ to use a different seed node than it would automatically, you can do this:
```
cluster_formation.k8s.seed_node = rabbit@seed-node-hostname
```

If `cluster_formation.k8s.seed_node` is configured, this plugin will just use this value as the seed node.
If you do this, please open a GitHub issue and explain why the plugin didn't work for you, so we can improve it.

### Historical Notes

This implementation (version 2) of the plugin was introduced in RabbitMQ 4.1 and has little to do with the original design,
which was included in RabbitMQ from version 3.7.0 until 4.0. Nevertheless, backwards compatibility is maintained,
by simply ignoring the configuration options of the original implementation.

The original implementation of this plugin performed peer discovery by querying the Kubernetes API for the list of endpoints
serving as the backends of a Kubernetes Service. However, this approach had a few issues:
1. The query was not necessary, given that the Pod names are predictable
2. The query could fail or require configuration to work (eg. TLS settings had to be adjusted in some environments)
3. To perform the query, access to the Kubernetes API was required (unnecessary privileges)
4. In some environments, the plugin was prone to race conditions and could form more than 1 cluster

The new design solves all those problems:
1. It doesn't query the Kubernetes API at all
2. Only one node is allowed to form a cluster

## Supported RabbitMQ Versions

Version 2 was first included in RabbitMQ 4.1.

Version 1 of this plugin (which queried the Kubernetes API) was included from RabbitMQ 3.7.0 until 4.0.

## Documentation

See [RabbitMQ Cluster Formation guide](https://www.rabbitmq.com/cluster-formation.html) for an overview
of the peer discovery subsystem, general and Kubernetes-specific configurable values and troubleshooting tips.

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) and our [development process overview](https://www.rabbitmq.com/github.html).

## License

[Licensed under the MPL](LICENSE-MPL-RabbitMQ), same as RabbitMQ server.


## Copyright

(c) 2007-2025 Broadcom. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
