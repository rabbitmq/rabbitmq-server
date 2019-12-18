# Deploy RabbitMQ on Kubernetes with the Kubernetes Peer Discovery Plugin to Kind

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

 * [Kind](https://github.com/kubernetes-sigs/kind) 
 * [Kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/) 
 
## Quick Start

 * Create Kind cluster 
```
kind create cluster --config kind/kind-cluster/kind-cluster.yaml
```

Deploy RabbitMQ with or without [persistent volumes](https://kubernetes.io/docs/concepts/storage/persistent-volumes/).

**Note:** You can use **one** deploy at time

* Deploy RabbitMQ without PV:
```
kubectl apply  -k overlays/dev
```

* Deploy RabbitMQ with PV using storage class:
```
kubectl apply  -k overlays/dev-persistence/
```

## Use Localhost Address to Connect

The ports used by this example are:

* `amqp://guest:guest@localhost`: AMQP 0-9-1 and AMQP 1.0 client connections
* http://localhost:15672: HTTP API and management UI


## Details

_kind is a tool for running local Kubernetes clusters using Docker container "nodes"._
Kind should be used only for developing or/and for CI integration.

### Port Mapping
The `kind-cluster.yaml` configuration binds localhost ports:

```yaml
 extraPortMappings:
  - containerPort: 31672
    hostPort: 15672
  - containerPort: 30672
    hostPort: 5672
```

The `NodePort` service exposes the ports: 
```yaml
spec:
  type: NodePort
  ports:
   - name: http
     protocol: TCP
     port: 15672
     targetPort: 15672
     nodePort: 31672  # <---- binds the extraPortMappings.containerPort 31672
   - name: amqp
     protocol: TCP
     port: 5672
     targetPort: 5672
     nodePort: 30672  # <---- binds the extraPortMappings.containerPort 30672
```

So in this way you can easly use the localhost ports.

### Persistent Volumes

`Kind` by default creates a [storage class](https://kubernetes.io/docs/concepts/storage/storage-classes/) called `standard`
```
kubectl get storageclass
NAME                 PROVISIONER               AGE
standard (default)   kubernetes.io/host-path   5h
```

used by:
```yaml
cat overlays/dev-persistence/deployment.yaml
apiVersion: apps/v1
# See the Prerequisites section of https://www.rabbitmq.com/cluster-formation.html#peer-discovery-k8s.
kind: StatefulSet
metadata:
  name: rabbitmq
spec:
  volumeClaimTemplates:
  - metadata:
      name: rabbitmq-data
    spec:
      accessModes: [ "ReadWriteOnce" ]
      storageClassName: "standard" # <----- standard storage class
      resources:
        requests:
          storage: 1Gi
```

Check the persistent volumes claim:
```
kubectl get pvc -n rabbitmq-dev-persistence
NAME                       STATUS   VOLUME                                     CAPACITY   ACCESS MODES   STORAGECLASS   AGE
rabbitmq-data-rabbitmq-0   Bound    pvc-0db837ec-a856-4a6b-9acf-2c9110bb9f12   1Gi        RWO            standard       2m38s
rabbitmq-data-rabbitmq-1   Bound    pvc-24514f31-5b40-4bd6-a721-84a16afaa697   1Gi        RWO            standard       78s
rabbitmq-data-rabbitmq-2   Bound    pvc-2b7162f5-c596-404d-be85-475600b9f82a   1Gi        RWO            standard       2s
```

Check the persistent volumes:
```
kubectl get pv -n rabbitmq-dev-persistence
NAME                                       CAPACITY   ACCESS MODES   RECLAIM POLICY   STATUS   CLAIM                                               STORAGECLASS   REASON   AGE
pvc-0db837ec-a856-4a6b-9acf-2c9110bb9f12   1Gi        RWO            Delete           Bound    rabbitmq-dev-persistence/rabbitmq-data-rabbitmq-0   standard                2m41s
pvc-24514f31-5b40-4bd6-a721-84a16afaa697   1Gi        RWO            Delete           Bound    rabbitmq-dev-persistence/rabbitmq-data-rabbitmq-1   standard                81s
pvc-2b7162f5-c596-404d-be85-475600b9f82a   1Gi        RWO            Delete           Bound    rabbitmq-dev-persistence/rabbitmq-data-rabbitmq-2   standard                5s
```

## Clean up

Clean up RabbitMQ without PV:
```
kubectl delete  -k overlays/dev
```

Clean up RabbitMQ with PV:
```
kubectl delete  -k overlays/dev-persistence/
```

