# Deploy RabbitMQ on Kubernetes with the Kubernetes Peer Discovery Plugin

This is an **example** that demonstrates a RabbitMQ deployment on Kubernetes with peer discovery
via `rabbitmq-peer-discovery-k8s` plugin.

## Production (Non-)Suitability

Some values in this example **may or may not be optimal for your deployment**. We encourage users
to get familiar with the [RabbitMQ Peer Discovery guide](http://www.rabbitmq.com/cluster-formation.html), [RabbitMQ Production Checklist](http://www.rabbitmq.com/production-checklist.html)
and the rest of [RabbitMQ documentation](https://www.rabbitmq.com/documentation.html) before going into production.

Having [metrics](http://www.rabbitmq.com/monitoring.html), both of RabbitMQ and applications that use it,
is critically important when making informed decisions about production systems.


## Pre-requisites

The example uses:

* [RabbitMQ Docker image](https://hub.docker.com/_/rabbitmq/)
* [StatefulSets controller](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/) K8S feature


## Usage


* Install [`kubectl`](https://kubernetes.io/docs/tasks/tools/install-kubectl/)


* Install [`minikube`](https://kubernetes.io/docs/tasks/tools/install-minikube/)


* Start `minikube` virtual machine:
```
minikube start --cpus=2 --memory=2040 --vm-driver=virtualbox
```

* Create a namespace only for RabbitMQ test:
```
kubectl create namespace test-rabbitmq
```

* For kubernetes 1.6 or above, RBAC Authorization feature enabled by default. It need configure RBAC related stuff to support access nodes info successfully by plugin. So deploy RBAC `YAML` file():

```
kubectl create -f examples/k8s_statefulsets/rabbitmq_rbac.yaml
```

* Deploy RBAC `YAML` file:

```
kubectl create -f examples/k8s_statefulsets/rabbitmq_rbac.yaml
```

* Deploy Service/ConfigMap/Statefulset `YAML` file:

```
kubectl create -f examples/k8s_statefulsets/rabbitmq_statefulsets.yaml
```
6. Check the cluster status:

Wait few seconds....then run

```
FIRST_POD=$(kubectl get pods --namespace test-rabbitmq -l 'app=rabbitmq' -o jsonpath='{.items[0].metadata.name }')
kubectl exec --namespace=test-rabbitmq $FIRST_POD rabbitmqctl cluster_status
```

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

* Get your `minikube` ip:
```
minikube ip
# => 192.168.99.104
```
* Ports:
	* `http://<<minikube_ip>>:31672`: [HTTP API and management UI](https://www.rabbitmq.com/management.html)
	* `amqp://guest:guest@<<minikube_ip>>:30672`: [AMQP 0-9-1 and AMQP 1.0](https://www.rabbitmq.com/networking.html#selinux-ports)

* Scaling the number of nodes (Kubernetes pod replicas):

```
kubectl scale statefulset/rabbitmq --namespace=test-rabbitmq --replicas=5
```
