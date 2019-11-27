# Deploy RabbitMQ on Kubernetes with the Kubernetes Peer Discovery Plugin

This directory contains **examples** that demonstrates a RabbitMQ deployment on Kubernetes with peer discovery
via `rabbitmq-peer-discovery-k8s` plugin. Currently the primary environment targeted is Minikube.
Find it under the [Minikube](./minikube) directory.

## Production (Non-)Suitability

Some values in these example files **may or may not be optimal for your deployment**. There are many aspects to
a Kubernetes cluster that this example cannot know or make too many assumptions about.
Persistent volume configuration is one such aspect. The user is expected to expand
the example by adding more files under the `examples/{environment}` directory.

We encourage users to get familiar with the [RabbitMQ Peer Discovery guide](https://www.rabbitmq.com/cluster-formation.html),
[RabbitMQ Production Checklist](https://www.rabbitmq.com/production-checklist.html),
and the rest of [RabbitMQ documentation](https://www.rabbitmq.com/documentation.html) before going into production.

Having [metrics](https://www.rabbitmq.com/monitoring.html), both of RabbitMQ and applications that use it,
is critically important when making informed decisions about production systems.
