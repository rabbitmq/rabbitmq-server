# Deploy RabbitMQ on Kubernetes with the Kubernetes Peer Discovery Plugin to GKE

This is an **example** that demonstrates a RabbitMQ deployment on Kubernetes with peer discovery
via `rabbitmq-peer-discovery-k8s` plugin.

## Production (Non-)Suitability

Some values in this example **may or may not be optimal for your deployment**. We encourage users
to get familiar with the [RabbitMQ Peer Discovery guide](https://www.rabbitmq.com/cluster-formation.html), [RabbitMQ Production Checklist](https://www.rabbitmq.com/production-checklist.html)
and the rest of [RabbitMQ documentation](https://www.rabbitmq.com/documentation.html) before going into production.

Having [metrics](https://www.rabbitmq.com/monitoring.html), both of RabbitMQ and applications that use it,
is critically important when making informed decisions about production systems.

These examples have been created for a `DIY RabbitMQ on Kubernetes` blog post so will read like a blog post.

## Pre-requisites

The example uses, targets or assumes:

 * A [GKE cluster]()
 * Kubernetes 1.16
 * RabbitMQ [Docker image](https://hub.docker.com/_/rabbitmq/) (maintained [by Docker, Inc](https://hub.docker.com/_/rabbitmq/))

## Namespace and RBAC

We recommend using a dedicated [Kubernetes Namespace](https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/) to keep the RabbitMQ cluster separate from other services that may be deployed in the Kubernetes cluster. Having a dedicated namespace, also allows the specification of fine-grained [Kubernetes RBAC](https://kubernetes.io/docs/reference/access-authn-authz/rbac/) rules. 

RabbitMQ requires the `rabbitmq-peer-discovery-k8s` plugin for clustering. This plugin uses the Kubernetes API as a data source to discover and cluster RabbitMQ nodes. The plugin requires the following access to Kubernetes resources:
* `get` access to the `endpoints` resource
* `create` access to the `events` resource

Specify a [Role, Role Binding and a Service Account](https://kubernetes.io/docs/reference/access-authn-authz/rbac/) to configure this access.

An example namespace, along with RBAC rules can be seen in the rabbitmq-peer-discovery-k8s [plugin examples](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s/blob/master/examples/minikube/rbac.yaml).

If following from the example, use the following command to create a namespace and the required RBAC rules. Note that this creates a namespace called `test-rabbitmq`.

```shell
kubectl apply -f namespace.yaml
kubectl apply -f rbac.yaml
```

Since we will be working from the `test-rabbitmq` namespace, you can set this as the namespace. Run:

```shell
# set namespace
kubectl config set-context --current --namespace=test-rabbitmq
# verify
kubectl config view --minify | grep namespace:
```
## Use a Stateful Set

RabbitMQ *requires* using a [Stateful Set](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/) to deploy a RabbitMQ cluster. The Stateful Set ensures that the RabbitMQ nodes are deployed one at a time, which avoids running into a potential [peer discovery race condition](https://www.rabbitmq.com/cluster-formation.html#initial-formation-race-condition) when deploying a multi-node RabbitMQ cluster.

The Stateful Set definition file is packed with detail such as mounting  configuration, mounting credentials, openening ports, etc, which is explained topic-wise in the following sections.

The final Stateful Set file can be found in the [`rabbitmq_peer_discovery_k8s` repo examples](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s/tree/master/examples/gke/statefulset.yaml).

Do not deploy the Stateful Set yet. We will need to create a few other Kubernetes resources before we can tie everything together in the Stateful Set.

## Create a Service For Clustering

The Stateful Set definition can reference a Service which gives the Pods of the Stateful Set their network identity. Here, we are referring to the [`v1.StatefulSet.Spec.serviceName` property](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#statefulsetspec-v1-apps).

This is required by RabbitMQ for clustering, and as mentioned in the Kubernetes documentation, has to be created before the Stateful Set.

RabbitMQ uses port 4369 for inter-node communication, and since this Service is used internally and does not need to be exposed, we create a [Headless Service](https://kubernetes.io/docs/concepts/services-networking/service/#headless-services).

[Example Headless Service](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s/tree/master/examples/gke/headless-service.yaml).

If following from the example, you can now run the following to create a Headless Service:

```
kubectl apply -f rabbitmq-headless.yaml
```

You will now see the service deployed in the `test-rabbitmq` namespace.

```shell
$ kubectl get all
NAME                        TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)    AGE
service/rabbitmq-headless   ClusterIP   None         <none>        4369/TCP   7s
```

## Use a Persistent Volume for Node Data

RabbitMQ requires persistence is order to retain data over Pod restarts. Although some data can be synced from other nodes, transient volumes defy the benefits of clustering.

A [Persistent Volume](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) can be attached to each RabbitMQ Pod for this.

In our [example](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s/tree/master/examples/gke/statefulset.yaml#L12-L22), we create a Persistent Volume Claim to provision a Persistent Volume.

The Persistent Volume is mounted at `/var/lib/rabbitmq/mnesia`. This path refers to the `RABBITMQ_MNESIA_BASE` - the base location for RabbitMQ persistent data.

A description of [default file paths for RabbitMQ](https://www.rabbitmq.com/relocate.html) can be found in the RabbitMQ documentation.

You may also modify the `RABBITMQ_MNESIA_BASE` variable and mount the Persistent Volume at the changed path.

## Node Configuration

There are [several ways](https://www.rabbitmq.com/configure.html) to configure RabbitMQ. The recommended way is to use configuration files.

Configuration files can be expressed as [Config Maps](https://kubernetes.io/docs/concepts/configuration/configmap/), and mounted as a Volume onto the RabbitMQ pods.

See our [example minimal Config Map](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s/tree/master/examples/gke/configmap.yaml).

If following from the example, you can now create the RabbitMQ configuration Config Map -

```shell
kubectl apply -f configmap.yaml
```

### Use an Init Container

Since Kubernetes 1.9.4, Config Maps are mounted as read-only volumes onto Pods. This is a problem since the Community Docker image also writes to the config file on container start up.

Thus, the path at which the RabbitMQ config is mounted must be read-write. If a read-only file is detected by the Docker image, you'll see the following warning:

```
touch: cannot touch '/etc/rabbitmq/rabbitmq.conf': Permission denied

WARNING: '/etc/rabbitmq/rabbitmq.conf' is not writable, but environment variables have been provided which request that we write to it
  We have copied it to '/tmp/rabbitmq.conf' so it can be amended to work around the problem, but it is recommended that the read-only source file should be modified and the environment variables removed instead.
```

While the Docker image does work around the issue, it is not ideal to store the configuration file in `/tmp` and we recommend instead making the mount path read-write.

As a few other projects in the community, we use an [init container](https://kubernetes.io/docs/concepts/workloads/pods/init-containers/) to overcome this.

Examples:
* [The Config Map](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s/blob/master/examples/minikube/configmap.yaml)
* [Using an Init Container to mount the Config Map](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s/tree/master/examples/gke/statefulset.go#L30-L64)

### Run The Pod As the `rabbitmq` User

The Docker image [runs as the `rabbitmq` user with uid 999]([https://github.com/docker-library/rabbitmq/blob/38bc089c287d05d22b03a4d619f7ad9d9a4501bc/3.8/ubuntu/Dockerfile#L186-L187](https://github.com/docker-library/rabbitmq/blob/38bc089c287d05d22b03a4d619f7ad9d9a4501bc/3.8/ubuntu/Dockerfile#L186-L187)) and writes to the `rabbitmq.conf` file. Thus, the file permissions on `rabbitmq.conf` must allow this. You can add a [Pod Security Context](https://kubernetes.io/docs/tasks/configure-pod-container/security-context/) in the Stateful Set definition to specify this. Set the [`runAsUser`, `runAsGroup` and the `fsGroup`](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s/tree/master/examples/gke/statefulset.yaml#L66-L75) to 999 in the Security Context.

See [Security Context]() in the Stateful Set definition file.

### Importing Definitions

RabbitMQ also supports [importing definitions](https://www.rabbitmq.com/definitions.html) from another RabbitMQ cluster. This may also be done at [node boot time](https://www.rabbitmq.com/definitions.html#import-on-boot). Following from the RabbitMQ documentation, this can be done using the following steps:
1. Export definitions from the RabbitMQ cluster you wish to replicate and save the file
1. Create a Config Map with the key being the file name, and the value being the contents of the file (See the `rabbitmq.conf` Config Map [example]())
1. Mount the Config Map as a Volume on the RabbitMQ Pod in the Stateful Set definition
1. Update the `rabbitmq.conf` Config Map with `load_definitions = /path/to/definitions/file`

## Erlang Cookie

RabbitMQ nodes and cli tools use a shared secret - [the Erlang Cookie](https://www.rabbitmq.com/clustering.html#erlang-cookie), for communication. The cookie is a string of alphanumeric characters up to 255 characters in size. The cookie must be generated before creating a RabbitMQ cluster since it is needed by the nodes to form a cluster.

RabbitMQ expects the cookie to be at `var/lib/rabbitmq/.erlang.cookie`. We recommend creating a Secret and mounting it as a Volume on the Pods at this path. The cookie can also be specified directly by setting the `RABBITMQ_ERLANG_COOKIE` environment variable on the Community Docker image.

See [cookie example](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s/tree/master/examples/gke/statefulset.yaml#L101-L105) in the `rabbitmq_peer_discover_k8s` repository.

To create the Secret itself, you may run -

```shell
echo -n "secure1secret" > cookie
kubectl create secret generic erlang-cookie --from-file=./cookie
```

## Admin Credentials

RabbitMQ starts up with a [default user](https://www.rabbitmq.com/access-control.html#default-state) with well-known credentials - username and password `guest`.

These credentials only work on localhost although adding `loopback_users.guest = false` to `rabbitmq.conf` will make them work remotely too. This is useful for testing but insecure and the credentials should be replaced for more secure use cases.

Similar to the Erlang cookie, we recommend storing the admin credentials in a Secret, and mounting them onto the RabbitMQ Pods. This can be done by setting the `RABBITMQ_DEFAULT_USER` and `RABBITMQ_DEFAULT_PASS` environment variables on the Community Docker image. [Example for reference](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s/tree/master/examples/gke/statefulset.yaml#L91-L100).

If following from the example, you can now create an admin secret -

```shell
echo -n "admin" > user
echo -n "password" > pass
kubectl create secret generic rabbitmq-admin --from-file=./user --from-file=./pass
```

## Plugins

From the [RabbitMQ documentation](https://www.rabbitmq.com/plugins.html) -

> RabbitMQ supports plugins. Plugins extend core broker functionality in a variety of ways: with support for more protocols, system state monitoring, additional AMQP 0-9-1 exchange types, node federation, and more. A number of features are implemented as plugins that ship in the core distribution.

The [`rabbitmq_peer_discovery_k8s` plugin](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s) is required to deploy RabbitMQ on Kubernetes. You may also wish to enable the [`rabbitmq_management` plugin](https://www.rabbitmq.com/management.html) in order to get a browser-based UI and an HTTP API to manage and monitor the RabbitMQ nodes.

Plugins can be enabled in [different ways](https://www.rabbitmq.com/plugins.html#ways-to-enable-plugins). We recommend mounting the plugins file - `enabled_plugins`, to the node configuration directory - `/etc/rabbitmq`. A Config Map can be used to express the `enabled_plugins` file. It can then be mounted as a Volume onto each RabbitMQ container in the Stateful Set definition.

In our [example Config Map](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s/tree/master/examples/gke/configmap.yaml), we use two keys in a single Config Map object to store both the `enabled_plugins` and `rabbitmq.conf`. Both files are then mounted to `/etc/rabbitmq`.

## Ports

The final consideration for the Stateful Set is the ports to open on the RabbitMQ Pods. Protocols in RabbitMQ are served over TCP and require the protocol ports to be opened on the RabbitMQ nodes. Depending on the plugins that have enabled, you will have to open the required ports on the RabbitMQ pods.

Given our `enabled_plugins` file contains the plugins - `rabbitmq_peer_discovery_k8s`(required), `rabbitmq_management` and `rabbitmq_prometheus`, [we open the ports](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s/tree/master/examples/gke/statefulset.yaml#L106-L118) `5672` (AMQP), `15672`(management http api) and `15692`(prometheus).

You can consult the RabbitMQ plugins documentation to know the port a protocol is serving over. For example, MQTT is served over port `1883` by default.

We have now discussed all the components in the Stateful Set file. Please have a look [at the file](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s/tree/master/examples/gke/statefulset.yaml), and if following from the example, you can deploy the Stateful Set:

```shell
kubectl apply -f statefulset.yaml
```

This will start spinning up a RabbitMQ cluster. To watch the progress:

```shell
$ watch kubectl get all
NAME             READY   STATUS    RESTARTS   AGE
pod/rabbitmq-0   0/1     Pending   0          8s

NAME                        TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)    AGE
service/rabbitmq-headless   ClusterIP   None         <none>        4369/TCP   61m

NAME                        READY   AGE
statefulset.apps/rabbitmq   0/1     8s
```

## Client Service

If all has gone well, you now have a functioning RabbitMQ cluster deployed on Kubernetes! *celebrate emoji*

Time to create a Service to make the cluster accessible.

The type of this Service depends on your use case. The [Kubernetes API reference](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#servicespec-v1-core) gives a good overview of the types of Services.

In our [example](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s/tree/master/examples/gke/client-service.yaml), we have gone with a `LoadBalancer` Service. This gives us an external ip to RabbitMQ. For example, we can now try to view the RabbitMQ management dashboard by visiting `<external-ip>:15672`, and entering user credentials. Client applications can connect to `<external-ip>:5672` to establish an AMQP connection with RabbitMQ. You may refer to the [get started guide](https://www.rabbitmq.com/getstarted.html) to learn how to use RabbitMQ.

If following from the example, you can now run

```shell
kubectl apply -f client-service.yaml
```

This will create a Service of type LoadBalancer with an external ip.

```
$ k get svc
NAME                        TYPE           CLUSTER-IP     EXTERNAL-IP      PORT(S)                                          AGE
service/rabbitmq-client     LoadBalancer   10.59.243.60   34.105.135.116   15672:30902/TCP,15692:30605/TCP,5672:31210/TCP   2m19s
```

### Using `rabbitmq-perf-test` To Verify The Cluster

You can use the [`rabbitmq-perf-test`](https://rabbitmq.github.io/rabbitmq-perf-test/stable/htmlsingle/) application to test the RabbitMQ cluster. `perf-test` has a public [docker image](https://hub.docker.com/r/pivotalrabbitmq/perf-test/) and can be used simply by running -

```shell
kubectl run perf-test --image=pivotalrabbitmq/perf-test -- --uri amqp://<username>:<password>@<external-ip>
```

Here the `<username>` and `<password>` are the ones set up in the `rabbitmq-admin` Secret. The `<serivce>` is the name of the client service.

For a functioning RabbitMQ cluster, running `kubectl logs -f perf-test`  will give lines such as:

```
id: test-110102-976, time: 263.100s, sent: 21098 msg/s, received: 21425 msg/s, min/median/75th/95th/99th consumer latency: 1481452/1600817/1636996/1674410/1682972 ?s
id: test-110102-976, time: 264.100s, sent: 17314 msg/s, received: 17856 msg/s, min/median/75th/95th/99th consumer latency: 1509657/1600942/1636253/1695525/1718537 ?s
id: test-110102-976, time: 265.100s, sent: 18597 msg/s, received: 17707 msg/s, min/median/75th/95th/99th consumer latency: 1573151/1716519/1756060/1813985/1846490 ?s
```

They indicate the RabbitMQ is sending and receiving about 20k messages a second. Not bad :)

You can now tear down the `perf-test` pod.

```shell
kubectl delete pod perf-test
```

## Monitoring The Cluster


RabbitMQ comes with in-built support for Prometheus. This is done by enabling the `rabbitmq_prometheus` plugin. This in turn can be done by adding `rabbitmq_promethus` to the `enabled_plugins` Config Map. You will also need to open the Prometheus RabbitMQ port 15972 on both the Pod and the client Service.

You can now use Grafana to visualise the Promethus RabbitMQ metrics.
