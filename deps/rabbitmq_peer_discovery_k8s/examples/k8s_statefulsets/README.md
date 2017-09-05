RabbitMQ-Autocluster on K8s  [StatefulSet  Controller](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/)   

1. Install [`kubectl`](https://kubernetes.io/docs/tasks/tools/install-kubectl/)


2. Install [`minikube`](https://kubernetes.io/docs/tasks/tools/install-minikube/)


3. Start `minikube` virtual machine:
```
$ minikube start --cpus=2 --memory=2040 --vm-driver=virtualbox
```

4. Create a namespace only for RabbitMQ test:
```
$ kubectl create namespace test-rabbitmq
```

5. Deploy the service `YAML` file:

```
$ kubectl create -f examples/k8s_statefulsets/rabbitmq-service.yaml
```
6. Deploy the RabbitMQ StatefulSet `YAML` file:

```
$ kubectl create -f examples/k8s_statefulsets/rabbitmq.yaml
```
7. Check the cluster status:

Wait  few seconds....then 

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

8. Get your `minikube` ip:
```
$ minikube ip
192.168.99.104
```
9. Ports:
	* `http://<<minikube_ip>>:31672` - Management UI
	* `amqp://guest:guest@<<minikube_ip>>:30672` - AMQP

10. Scaling:
```
$ kubectl scale statefulset/rabbitmq --namespace=test-rabbitmq --replicas=5
```




