# Deploy RabbitMQ on Kubernetes with the Kubernetes Peer Discovery Plugin to GKE

This is an **example** that demonstrates a RabbitMQ deployment on the Google Kubernetes Engine (GKE) with peer discovery
via `rabbitmq-peer-discovery-k8s` plugin. This example is meant to be more detailed compared to its Minikube and Kind
counterparts. We cover several key aspects of a manual RabbitMQ deployment on Kubernetes, such as

 * Namespacing
 * Access control (RBAC)
 * Using a stateful set for RabbitMQ nodes
 * A headless service for inter-node communication
 * Ensuring durable storage is used by node data directories
 * Permissions for RabbitMQ node data directory and configuration file(s)
 * How to configure a node
 * Secret management for initial RabbitMQ user credentials
 * A load balancer service for external client connections
 * How to deploy a PerfTest instance to do basic functional and load testing of the cluster
 * How to set up cluster monitoring with Prometheus and Grafana

## Production (Non-)Suitability

Some values in this example **may or may not be optimal for your deployment**. We encourage users
to get familiar with the [RabbitMQ Peer Discovery guide](https://www.rabbitmq.com/cluster-formation.html), [RabbitMQ Production Checklist](https://www.rabbitmq.com/production-checklist.html)
and the rest of [RabbitMQ documentation](https://www.rabbitmq.com/documentation.html) before going into production.

Having [metrics](https://www.rabbitmq.com/monitoring.html), both of RabbitMQ and applications that use it,
is critically important when making informed decisions about production systems.

These examples have been created as part of a blog post, and thus read like a blog post.

## Pre-requisites

The example uses, targets or assumes:

 * A [GKE cluster](https://cloud.google.com/kubernetes-engine), version `v1.16.13-gke.1` was used at the time of writing.
 * The `kubectl` CLI tool. Version `v1.18.0` was used at the time of writing.
 * RabbitMQ [community Docker image](https://hub.docker.com/_/rabbitmq/)

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

This example will use the `test-rabbitmq` namespace. Set this as the namespace using

```shell
# set namespace
kubectl config set-context --current --namespace=test-rabbitmq
# verify
kubectl config view --minify | grep namespace:
```

## Use a Stateful Set

RabbitMQ *requires* using a [Stateful Set](https://kubernetes.io/docs/concepts/workloads/controllers/statefulset/) to deploy a RabbitMQ cluster to Kubernetes.
The Stateful Set ensures that the RabbitMQ nodes are deployed one at a time, which avoids running into a potential [peer discovery race condition](https://www.rabbitmq.com/cluster-formation.html#initial-formation-race-condition) when deploying a multi-node RabbitMQ cluster.

The Stateful Set definition file is packed with detail such as mounting configuration, mounting credentials, openening ports, etc,
which is explained topic-wise in the following sections.

The final Stateful Set file can be found in the [under `./examples/gke`](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s/tree/master/examples/gke/statefulset.yaml).

Do not deploy the Stateful Set yet. We will need to create a few other Kubernetes resources
before we can tie everything together in the Stateful Set.

## Create a Service For Clustering

The Stateful Set definition can reference a Service which gives the Pods of the Stateful Set their network identity. Here, we are referring to the [`v1.StatefulSet.Spec.serviceName` property](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#statefulsetspec-v1-apps).

This is required by RabbitMQ for clustering, and as mentioned in the Kubernetes documentation, has to be created before the Stateful Set.

RabbitMQ uses port 4369 for inter-node communication, and since this Service is used internally and does not need to be exposed, we create a [Headless Service](https://kubernetes.io/docs/concepts/services-networking/service/#headless-services).

[Example Headless Service](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s/tree/master/examples/gke/headless-service.yaml).

If following from the example, run the following to create a Headless Service:

```
kubectl apply -f rabbitmq-headless.yaml
```

The service now can be observed in the `test-rabbitmq` namespace:

```shell
kubectl get all
# => NAME                        TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)    AGE
# => service/rabbitmq-headless   ClusterIP   None         <none>        4369/TCP   7s
```

## Use a Persistent Volume for Node Data

In order for RabbitMQ nodes to retain data between Pod restarts, node's data directory must use durable storage.
A [Persistent Volume](https://kubernetes.io/docs/concepts/storage/persistent-volumes/) can be attached to each RabbitMQ Pod.
Although some data can be synced from other nodes, transient volumes defy most benefits of clustering.

In our [statefulset.yaml example](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s/tree/master/examples/gke/statefulset.yaml#L12-L22),
we create a Persistent Volume Claim to provision a Persistent Volume.

The Persistent Volume is mounted at `/var/lib/rabbitmq/mnesia`. This path is used for a [`RABBITMQ_MNESIA_BASE` location](https://www.rabbitmq.com/relocate.html): the base directory
for all persistent data of a node.

A description of [default file paths for RabbitMQ](https://www.rabbitmq.com/relocate.html) can be found in the RabbitMQ documentation.

You may also modify the `RABBITMQ_MNESIA_BASE` variable and mount the Persistent Volume at the changed path.

## Node Configuration

There are [several ways](https://www.rabbitmq.com/configure.html) to configure a RabbitMQ node. The recommended way is to use configuration files.

Configuration files can be expressed as [Config Maps](https://kubernetes.io/docs/concepts/configuration/configmap/),
and mounted as a Volume onto the RabbitMQ pods.

To create a Config Map with RabbitMQ configuration, apply our [minimal configmap.yaml example](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s/tree/master/examples/gke/configmap.yaml):

```shell
kubectl apply -f configmap.yaml
```

### Use an Init Container

Since Kubernetes 1.9.4, Config Maps are mounted as read-only volumes onto Pods. This is problematic for the RabbitMQ community Docker image:
the image can try to update the config file on container start up.

Thus, the path at which the RabbitMQ config is mounted must be read-write. If a read-only file is detected by the Docker image,
you'll see the following warning:

```
touch: cannot touch '/etc/rabbitmq/rabbitmq.conf': Permission denied

WARNING: '/etc/rabbitmq/rabbitmq.conf' is not writable, but environment variables have been provided which request that we write to it
  We have copied it to '/tmp/rabbitmq.conf' so it can be amended to work around the problem, but it is recommended that the read-only
  source file should be modified and the environment variables removed instead.
```

While the Docker image does work around the issue, it is not ideal to store the configuration file in `/tmp` and we recommend instead
making the mount path read-write.

As a few other projects in the Kubernetes community, we use an [init container](https://kubernetes.io/docs/concepts/workloads/pods/init-containers/) to overcome this.

Examples:

* [The Config Map](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s/blob/master/examples/minikube/configmap.yaml)
* [Using an Init Container to mount the Config Map](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s/tree/master/examples/gke/statefulset.yml#L30-L64)

### Run The Pod As the `rabbitmq` User

The Docker image [runs as the `rabbitmq` user with uid 999]([https://github.com/docker-library/rabbitmq/blob/38bc089c287d05d22b03a4d619f7ad9d9a4501bc/3.8/ubuntu/Dockerfile#L186-L187](https://github.com/docker-library/rabbitmq/blob/38bc089c287d05d22b03a4d619f7ad9d9a4501bc/3.8/ubuntu/Dockerfile#L186-L187)) and writes to the `rabbitmq.conf` file.
Thus, the file permissions on `rabbitmq.conf` must allow this. A [Pod Security Context](https://kubernetes.io/docs/tasks/configure-pod-container/security-context/) can be
added to the Stateful Set definition to achieve this.
Set the [`runAsUser`, `runAsGroup` and the `fsGroup`](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s/tree/master/examples/gke/statefulset.yaml#L66-L75) to 999 in the Security Context.

See [Security Context](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s/blob/gke-examples/examples/gke/statefulset.yaml#L72-L75)
in the Stateful Set definition file.

### Importing Definitions

RabbitMQ nodes can [importi definitions](https://www.rabbitmq.com/definitions.html) exported from another RabbitMQ cluster.
This may also be done at [node boot time](https://www.rabbitmq.com/definitions.html#import-on-boot).

Following from the RabbitMQ documentation, this can be done using the following steps:

1. Export definitions from the RabbitMQ cluster you wish to replicate and save the file
1. Create a Config Map with the key being the file name, and the value being the contents of the file (See the `rabbitmq.conf` Config Map [example]())
1. Mount the Config Map as a Volume on the RabbitMQ Pod in the Stateful Set definition
1. Update the `rabbitmq.conf` Config Map with `load_definitions = /path/to/definitions/file`

## Node Authentication Secret: the Erlang Cookie

RabbitMQ nodes and CLI tools use a shared secret known as [the Erlang Cookie](https://www.rabbitmq.com/clustering.html#erlang-cookie), to authenticate to each other.
The cookie value is a string of alphanumeric characters up to 255 characters in size. The value must be generated before creating
a RabbitMQ cluster since it is needed by the nodes to [form a cluster](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s/blob/gke-examples/examples/gke/statefulset.yaml#L72-L75).

With the community Docker image, RabbitMQ nodes will expect the cookie to be at `/var/lib/rabbitmq/.erlang.cookie`.
We recommend creating a Secret and mounting it as a Volume on the Pods at this path.

See [cookie section in the statefulset.yaml example](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s/tree/master/examples/gke/statefulset.yaml#L101-L105) file.

To create the Secret itself, run

```shell
echo -n "this secret value is JUST AN EXAMPLE. Replace it!" > cookie
kubectl create secret generic erlang-cookie --from-file=./cookie
```

## Administrator Credentials

RabbitMQ will seed a [default user](https://www.rabbitmq.com/access-control.html#default-state) with well-known credentials on first boot. The username and password
of this user are both `guest`.

This default user can [only connect from localhost](https://www.rabbitmq.com/access-control.html#loopback-users) by default.
It is possible to lift this restriction by opting in. This may be useful for testing but **very insecure**.
Instead, an administrative user must be created using generated credentials.

W recommend storing the admin credentials in a Kubernetes Secret, and mounting them onto the RabbitMQ Pods.
The `RABBITMQ_DEFAULT_USER` and `RABBITMQ_DEFAULT_PASS` environment variables can be set to the Secret values.
The community Docker image will use them to [override default user credentials](https://www.rabbitmq.com/access-control.html#seeding).

[Example for reference](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s/tree/master/examples/gke/statefulset.yaml#L91-L100).

To create an administrative user Secret, use

```shell
echo -n "admin" > user
echo -n "password" > pass
kubectl create secret generic rabbitmq-admin --from-file=./user --from-file=./pass
```

Users can be create explicitly using CLI tools as well.
See [RabbitMQ doc section on user management](https://www.rabbitmq.com/access-control.html#seeding) to learn more.


## Plugins

RabbitMQ [supports plugins](https://www.rabbitmq.com/plugins.html).

The [`rabbitmq_peer_discovery_k8s` plugin](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s) is required to deploy RabbitMQ on Kubernetes.
You may also wish to enable the [`rabbitmq_management` plugin](https://www.rabbitmq.com/management.html) in order to get a browser-based UI and an HTTP API to manage and monitor the RabbitMQ nodes.

Plugins can be enabled in [different ways](https://www.rabbitmq.com/plugins.html#ways-to-enable-plugins).
We recommend mounting the plugins file, `enabled_plugins`, to the node configuration directory, `/etc/rabbitmq`.
A Config Map can be used to express the value of the `enabled_plugins` file. It can then be mounted
as a Volume onto each RabbitMQ container in the Stateful Set definition.

In our [example Config Map](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s/tree/master/examples/gke/configmap.yaml),
two keys are used in a single Config Map object to store both the `enabled_plugins` and `rabbitmq.conf`.
Both files are then mounted to `/etc/rabbitmq`.

## Ports

The final consideration for the Stateful Set is the ports to open on the RabbitMQ Pods.
Protocols supported by RabbitMQ are all TCP-based and require the [protocol ports](https://www.rabbitmq.com/networking.html#ports) to be opened on the RabbitMQ nodes.
Depending on the plugins that are enabled on a node, the list of required ports can vary.

Using the example `enabled_plugins` file (see above) contains the plugins `rabbitmq_peer_discovery_k8s` (mandatory), `rabbitmq_management` and `rabbitmq_prometheus`,
[several ports are opened](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s/tree/master/examples/gke/statefulset.yaml#L106-L118): `5672` (used by AMQP 0-9-1 and AMQP 1.0 clients),
`15672` (management HTTP API) and `15692`(Prometheus scraping endpoint).

## Deploy the Stateful Set

These are the key components in the Stateful Set file. Please have a look [at the file](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s/tree/master/examples/gke/statefulset.yaml),
and if following from the example, deploy the Stateful Set:

```shell
kubectl apply -f statefulset.yaml
```

This will start spinning up a RabbitMQ cluster. To watch the progress:

```shell
watch kubectl get all
# => NAME             READY   STATUS    RESTARTS   AGE
# => pod/rabbitmq-0   0/1     Pending   0          8s
# =>
# => NAME                        TYPE        CLUSTER-IP   EXTERNAL-IP   PORT(S)    AGE
# => service/rabbitmq-headless   ClusterIP   None         <none>        4369/TCP   61m
# =>
# => NAME                        READY   AGE
# => statefulset.apps/rabbitmq   0/1     8s
```

## Client Service

If all the steps above succeeded, you should have functioning RabbitMQ cluster deployed on Kubernetes! 🥳

Time to create a Service to make the cluster accessible to [client connections](https://www.rabbitmq.com/connections.html).

The type of the Service depends on your use case. The [Kubernetes API reference](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.18/#servicespec-v1-core)
gives a good overview of the types of Services available.

In our [client-service.yaml example](https://github.com/rabbitmq/rabbitmq-peer-discovery-k8s/tree/master/examples/gke/client-service.yaml), we have gone with a `LoadBalancer` Service.
This gives us an external IP that can be used to access the RabbitMQ cluter.

For example, this should make it possible to visit the RabbitMQ management UI by visiting `{external-ip}:15672`, and signing in.
Client applications can connect to endpoints such as `{external-ip}:5672` (AMQP 0-9-1, AMQP 1.0) or `{external-ip}:1883` (MQTT).
Please refer to the [get started guide](https://www.rabbitmq.com/getstarted.html) to learn how to use RabbitMQ.

If following from the example, run

``` shell
kubectl apply -f client-service.yaml
```

to create a Service of type LoadBalancer with an external IP address. To find out what the external IP address is,
use `kubectl get svc`:

``` shell
kubectl get svc
# => NAME                        TYPE           CLUSTER-IP     EXTERNAL-IP      PORT(S)                                          AGE
# => service/rabbitmq-client     LoadBalancer   10.59.243.60   34.105.135.116   15672:30902/TCP,15692:30605/TCP,5672:31210/TCP   2m19s
```

### Using `rabbitmq-perf-test` to Run a Functional and Load Test of the Cluster

RabbitMQ comes with a load simulation tool, [PerfTest](https://rabbitmq.github.io/rabbitmq-perf-test/stable/htmlsingle/), which can be executed from outside of a cluster or
deployed to Kubernetes using the `perf-test` public [docker image](https://hub.docker.com/r/pivotalrabbitmq/perf-test/). Here's an example of how
the image can be deployed to a Kubernetes cluster

```shell
kubectl run perf-test --image=pivotalrabbitmq/perf-test -- --uri amqp://{username}:{password}@{service}
```

Here the `{username}` and `{password}` are the user credentials, e.g. those set up in the `rabbitmq-admin` Secret.
The `{serivce}` is the hostname to connect to. We use the name of the client service that will resolve as a hostname when deployed.

For a functioning RabbitMQ cluster, running `kubectl logs -f perf-test`  will produce output similar to this:

```
id: test-110102-976, time: 263.100s, sent: 21098 msg/s, received: 21425 msg/s, min/median/75th/95th/99th consumer latency: 1481452/1600817/1636996/1674410/1682972 μs
id: test-110102-976, time: 264.100s, sent: 17314 msg/s, received: 17856 msg/s, min/median/75th/95th/99th consumer latency: 1509657/1600942/1636253/1695525/1718537 μs
id: test-110102-976, time: 265.100s, sent: 18597 msg/s, received: 17707 msg/s, min/median/75th/95th/99th consumer latency: 1573151/1716519/1756060/1813985/1846490 μs
```

To learn more about PerfTest, its settings, capabilities and output, see the [PerfTest doc guide](https://rabbitmq.github.io/rabbitmq-perf-test/stable/htmlsingle/).

PerfTest is not meant to be running permanently. To tear down the `perf-test` pod, use

```shell
kubectl delete pod perf-test
```

## Monitoring the Cluster

[Monitoring](https://www.rabbitmq.com/monitoring.html) is a critically important part of any production deployment.

RabbitMQ comes with [in-built support for Prometheus](https://www.rabbitmq.com/prometheus.html). To enable it, enable the `rabbitmq_prometheus` plugin.
This in turn can be done by adding `rabbitmq_promethus` to the `enabled_plugins` Config Map as explained above.

The Prometheus scraping port, 15972, must be open on both the Pod and the client Service.

Node and cluster metrics can be [visualised with Grafana](https://www.rabbitmq.com/prometheus.html).
