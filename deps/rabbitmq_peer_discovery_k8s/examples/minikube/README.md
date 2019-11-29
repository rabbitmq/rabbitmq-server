# Deploy RabbitMQ on Kubernetes with the Kubernetes Peer Discovery Plugin to Minikube

This is an **example** that demonstrates a RabbitMQ deployment on Kubernetes with peer discovery
via `rabbitmq-peer-discovery-k8s` plugin.

## Production (Non-)Suitability

Some values in this example **may or may not be optimal for your deployment**. We encourage users
to get familiar with the [RabbitMQ Peer Discovery guide](https://www.rabbitmq.com/cluster-formation.html), [RabbitMQ Production Checklist](https://www.rabbitmq.com/production-checklist.html)
and the rest of [RabbitMQ documentation](https://www.rabbitmq.com/documentation.html) before going into production.

Having [metrics](https://www.rabbitmq.com/monitoring.html), both of RabbitMQ and applications that use it,
is critically important when making informed decisions about production systems.


## Pre-requisites

The example uses, targets or assumes:

 * [Minikube](https://kubernetes.io/docs/setup/learning-environment/minikube/) with the [VirtualBox](https://www.virtualbox.org/) driver (other drivers can be used, too)
 * Kubernetes 1.6
 * RabbitMQ [Docker image](https://hub.docker.com/_/rabbitmq/) (maintained [by Docker, Inc](https://hub.docker.com/_/rabbitmq/))
 * A [StatefulSets controller](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/)


## Quick Start with Make

This example comes with a Make target that sets up VirtualBox, Minikube and an example cluster
in a single command. It can be found under this directory. [Homebrew](https://brew.sh/) will be used to install
packages and on macOS, VirtualBox [will need OS permissions to install its kernel module](https://developer.apple.com/library/archive/technotes/tn2459/_index.html).

The Homebrew cask installer will ask for your password at some point with a prompt that looks like this:

```
Changing ownership of paths required by virtualbox; your password may be necessary
```

Please inspect the Make file to be extra sure that you understand and agree to what it does.
After enabling 3rd party kernel extensions in OS setings, run the default Make target in this directory:

```
make
```

which is equivalent to first running

```
make start-minikube
```

to install VirtualBox and Minikube using Homebrew, then

```
make run-in-minikube
```

to start Minikube and `kubectl apply` the example, and finally

```
make wait-for-rabbitmq
```

to wait for cluster formation.

Once the changes are applied, follow the steps in the Check Cluster Status section below.

In case you would prefer to install and run Minikube manually, see the following few sections.


## Running the Example Manually with Minikube

### Preresuites

 * Make sure that VirtualBox is installed
 * Install [`minikube`](https://kubernetes.io/docs/tasks/tools/install-minikube/) and start it with `--vm-driver=virtualbox`
 * Install [`kubectl`](https://kubernetes.io/docs/tasks/tools/install-kubectl/)

### Start Minikube

Start a `minikube` virtual machine:

``` sh
minikube start --cpus=2 --memory=2040 --disk-size "10 GB" --vm-driver=virtualbox
```

### Create a Namespace

Create a Kubernetes namespace for RabbitMQ tests:

``` sh
kubectl create namespace test-rabbitmq
```

### Set Up Kubernetes Permissions

In Kubernetes 1.6 or above, RBAC authorization is enabled by default.
This example configures RBAC related bits so that the peer discovery plugin is allowed to access
the nodes information it needs. The `ServiceAccount` and `Role` resources will be created
in the following step.

### kubectl Apply Things

Deploy the config map, services, a stateful set and so on:

``` sh
# will apply all files under this directory
kubectl create -f examples/minikube
```

### Check Cluster Status

Wait for a a few minutes for pods to start. Since this example uses a stateful set with ordered
startup, the pods will be started one by one. To monitor pod startup process, use

``` sh
kubectl --namespace="test-rabbitmq" get pods
```

To run `rabbitmq-diagnostics cluster_status`:

``` sh
FIRST_POD=$(kubectl get pods --namespace test-rabbitmq -l 'app=rabbitmq' -o jsonpath='{.items[0].metadata.name }')
kubectl exec --namespace=test-rabbitmq $FIRST_POD rabbitmq-diagnostics cluster_status
```

to check cluster status. Note that nodes can take some time to start and discover each other.

The output should look something like this:

```
Cluster status of node rabbit@rabbitmq-0.rabbitmq.test-rabbitmq.svc.cluster.local ...
Basics

Cluster name: rabbit@rabbitmq-0.rabbitmq.test-rabbitmq.svc.cluster.local

Disk Nodes

rabbit@rabbitmq-0.rabbitmq.test-rabbitmq.svc.cluster.local
rabbit@rabbitmq-1.rabbitmq.test-rabbitmq.svc.cluster.local
rabbit@rabbitmq-2.rabbitmq.test-rabbitmq.svc.cluster.local

Running Nodes

rabbit@rabbitmq-0.rabbitmq.test-rabbitmq.svc.cluster.local
rabbit@rabbitmq-1.rabbitmq.test-rabbitmq.svc.cluster.local
rabbit@rabbitmq-2.rabbitmq.test-rabbitmq.svc.cluster.local

Versions

rabbit@rabbitmq-0.rabbitmq.test-rabbitmq.svc.cluster.local: RabbitMQ 3.8.1 on Erlang 22.1.8
rabbit@rabbitmq-1.rabbitmq.test-rabbitmq.svc.cluster.local: RabbitMQ 3.8.1 on Erlang 22.1.8
rabbit@rabbitmq-2.rabbitmq.test-rabbitmq.svc.cluster.local: RabbitMQ 3.8.1 on Erlang 22.1.8

Alarms

(none)

Network Partitions

(none)

Listeners

Node: rabbit@rabbitmq-0.rabbitmq.test-rabbitmq.svc.cluster.local, interface: [::], port: 25672, protocol: clustering, purpose: inter-node and CLI tool communication
Node: rabbit@rabbitmq-0.rabbitmq.test-rabbitmq.svc.cluster.local, interface: [::], port: 5672, protocol: amqp, purpose: AMQP 0-9-1 and AMQP 1.0
Node: rabbit@rabbitmq-0.rabbitmq.test-rabbitmq.svc.cluster.local, interface: [::], port: 15672, protocol: http, purpose: HTTP API
Node: rabbit@rabbitmq-1.rabbitmq.test-rabbitmq.svc.cluster.local, interface: [::], port: 25672, protocol: clustering, purpose: inter-node and CLI tool communication
Node: rabbit@rabbitmq-1.rabbitmq.test-rabbitmq.svc.cluster.local, interface: [::], port: 5672, protocol: amqp, purpose: AMQP 0-9-1 and AMQP 1.0
Node: rabbit@rabbitmq-1.rabbitmq.test-rabbitmq.svc.cluster.local, interface: [::], port: 15672, protocol: http, purpose: HTTP API

Feature flags

Flag: drop_unroutable_metric, state: enabled
Flag: empty_basic_get_metric, state: enabled
Flag: implicit_default_bindings, state: enabled
Flag: quorum_queue, state: enabled
Flag: virtual_host_metadata, state: enabled
```

### Use Public Minikube IP Address to Connect

Get the public `minikube` VM IP address:

``` sh
minikube ip
# => 192.168.99.104
```

The [ports used](https://www.rabbitmq.com/networking.html#ports) by this example are:

 * `amqp://guest:guest@{minikube_ip}:30672`: [AMQP 0-9-1 and AMQP 1.0](https://www.rabbitmq.com/networking.html#ports) client connections
 * `http://{minikube_ip}:31672`: [HTTP API and management UI](https://www.rabbitmq.com/management.html)


### Scaling the Number of RabbitMQ Cluster Nodes (Kubernetes Pod Replicas)

``` sh
# Odd numbers of nodes are necessary for a clear quorum: 3, 5, 7 and so on
kubectl scale statefulset/rabbitmq --namespace=test-rabbitmq --replicas=5
```
