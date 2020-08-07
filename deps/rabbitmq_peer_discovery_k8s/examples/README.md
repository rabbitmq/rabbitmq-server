# Deploy RabbitMQ on Kubernetes with the Kubernetes Peer Discovery Plugin

This directory contains **examples** that demonstrates a minimalistic RabbitMQ deployment on Kubernetes
with peer discovery via `rabbitmq-peer-discovery-k8s` plugin. There are several examples:

 * [One that targets Minikube](./minikube)
 * [Another one that targets Kind](./kind)
 * [Another one that targets the Google Kubernetes Engine (GKE)](./gke)

For a more comprehensive open source RabbitMQ on Kubernetes deployment solution,
see [RabbitMQ Cluster Operator for Kubernetes](https://www.rabbitmq.com/kubernetes/operator/operator-overview.html).
The Operator is developed [on GitHub](https://github.com/rabbitmq/cluster-operator/) and contains its
own [set of examples](https://github.com/rabbitmq/cluster-operator/tree/master/docs/examples).

## Production (Non-)Suitability

Some values in these example files **may or may not be optimal for your deployment**. There are many aspects to
deploying and running a production-grade cluster on Kubernetes that this example cannot know or make too many assumptions about.
Persistent volume configuration is one obvious examples. You are welcome and encouraged to expand
the example by adding more files under the `examples/{environment}` directory.

We assume that the users of this plugin familiarize themselves with the [RabbitMQ Peer Discovery guide](https://www.rabbitmq.com/cluster-formation.html),
[RabbitMQ Production Checklist](https://www.rabbitmq.com/production-checklist.html),
and the rest of [RabbitMQ documentation](https://www.rabbitmq.com/documentation.html) before going into production.

Having [metrics](https://www.rabbitmq.com/monitoring.html), both of RabbitMQ and applications that use it,
is critically important when making informed decisions about production systems.
