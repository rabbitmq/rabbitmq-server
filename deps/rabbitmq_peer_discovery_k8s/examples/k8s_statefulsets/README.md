Deploy RabbitMQ on K8s 
----
The example uses:
* [RabbitMQ Docker image](https://hub.docker.com/_/rabbitmq/)
* [StatefulSets controller](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/) K8S feature

**Note**: This is just an example to show how to use the `rabbitmq-peer-discovery-k8s` plugin.
   

* Install [`kubectl`](https://kubernetes.io/docs/tasks/tools/install-kubectl/)


* Install [`minikube`](https://kubernetes.io/docs/tasks/tools/install-minikube/)


* Start `minikube` virtual machine:
```
$ minikube start --cpus=2 --memory=2040 --vm-driver=virtualbox
```

* Create a namespace only for RabbitMQ test:
```
$ kubectl create namespace test-rabbitmq
```

* For kubernetes 1.6 or above, RBAC Authorization feature enabled by default. It need configure RBAC related stuff to support access nodes info successfully by plugin. So deploy RBAC `YAML` file():

```
$ kubectl create -f examples/k8s_statefulsets/rabbitmq_rbac.yaml
```

* Deploy Service/ConfigMap/Statefulset `YAML` file:

```
$ kubectl create -f examples/k8s_statefulsets/rabbitmq_statefulsets.yaml
```
6. Check the cluster status:

Wait few seconds....then 

```
$ FIRST_POD=$(kubectl get pods --namespace test-rabbitmq -l 'app=rabbitmq' -o jsonpath='{.items[0].metadata.name }')
kubectl exec --namespace=test-rabbitmq $FIRST_POD rabbitmqctl cluster_status
```
as result you should have:
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
$ minikube ip
192.168.99.104
```
* Ports:
	* `http://<<minikube_ip>>:31672` - Management UI
	* `amqp://guest:guest@<<minikube_ip>>:30672` - AMQP

* Scaling:
```
$ kubectl scale statefulset/rabbitmq --namespace=test-rabbitmq --replicas=5
```
 
