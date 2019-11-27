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


## Usage

 * Make sure that VirtualBox is installed
 * Install [`minikube`](https://kubernetes.io/docs/tasks/tools/install-minikube/) and start it with `--vm-driver=virtualbox`
 * Install [`kubectl`](https://kubernetes.io/docs/tasks/tools/install-kubectl/)

### Start Minikube

Start a `minikube` virtual machine:

```
minikube start --cpus=2 --memory=2040 --vm-driver=virtualbox
```

### Create a Namespace

Create a Kubernetes namespace for RabbitMQ tests:

```
kubectl create namespace test-rabbitmq
```

### Set Up Kubernetes Permissions

In Kubernetes 1.6 or above, RBAC authorization is enabled by default.
This example configures RBAC related bits so that the peer discovery plugin is allowed to access
the nodes information it needs. So deploy RBAC `YAML` file():

```
kubectl create -f examples/k8s_statefulsets/rabbitmq_rbac.yaml
```

### kubectl Apply Things

Deploy the config map, services, a stateful set and so on:

```
kubectl create -f examples/k8s_statefulsets/rabbitmq_statefulsets.yaml
```

### Check Cluster Status

Wait 30-60 seconds then run

```
FIRST_POD=$(kubectl get pods --namespace test-rabbitmq -l 'app=rabbitmq' -o jsonpath='{.items[0].metadata.name }')
kubectl exec --namespace=test-rabbitmq $FIRST_POD rabbitmq-diagnostics cluster_status
```

to check cluster status. Note that nodes can take some time to start and discover each other.

The output should look something like this:

```
Cluster status of node 'rabbit@172.17.0.2'
[{nodes,[{disc,['rabbit@172.17.0.2','rabbit@172.17.0.4',
                'rabbit@172.17.0.5']}]},
 {running_nodes,['rabbit@172.17.0.5','rabbit@172.17.0.4','rabbit@172.17.0.2']},
 {cluster_name,<<"rabbit@rabbitmq-0.rabbitmq.test-rabbitmq.svc.cluster.local">>},
 {partitions,[]},
 {alarms,[{'rabbit@172.17.0.5',[]},
          {'rabbit@172.17.0.4',[]},
          {'rabbit@172.17.0.2',[]}]}]
```

### Use Public Minikube IP to Connect

Get the public `minikube` VM IP address:

```
minikube ip
# => 192.168.99.104
```

The [ports used](https://www.rabbitmq.com/networking.html#ports) by this example are:

	* `http://<<minikube_ip>>:31672`: [HTTP API and management UI](https://www.rabbitmq.com/management.html)
	* `amqp://guest:guest@<<minikube_ip>>:30672`: [AMQP 0-9-1 and AMQP 1.0](https://www.rabbitmq.com/networking.html#selinux-ports)

## Scaling the Number of RabbitMQ Cluster Nodes (Kubernetes Pod Replicas)

```
# Odd numbers of nodes are necessary for a clear quorum: 3, 5, 7 and so on
kubectl scale statefulset/rabbitmq --namespace=test-rabbitmq --replicas=5
```
