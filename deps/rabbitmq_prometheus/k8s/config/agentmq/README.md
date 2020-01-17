## Agentmq

* all commands are assumed to be running from the example directory

All the main variables are in `values.yaml`.  Changes should be made there.

### Install

Make sure we set the correct variables.  The default values in `values.yaml` are for local.

```
helm install --name agentmq --namespace default -f values.yaml ../agentmq --set environment=dev --set key1=value1 --set key2=value2
```

Check all the things.

```
helm ls
NAME               	REVISION	UPDATED                 	STATUS  	CHART                     	APP VERSION	NAMESPACE
...omitted
agentmq          1       	Thu Aug 23 15:17:37 2018	DEPLOYED	agentmq-0.1.0           	1.0        	default
```

### Upgrading 
```
helm upgrade -f values.yaml agentmq ../agentmq
```

### Delete chart

```
helm delete agentmq --purge
release "agentmq" deleted
```


### Testing locally

to test the template and install procedure without actually doing it.

```
helm install --dry-run --debug ../agentmq --set key1=var1
```

To just see the template

```
helm template -f values.yaml agentmq ../agentmq
```


## Configuration

The following table lists the configurable vales that can be used in a XXX-values.yaml file. If no value is specified, it will default to whatever is in the **values.yaml**



Parameter | Description | Default
--------- | ----------- | -------
`rabbitmqUsers` | list of rabbitmq users | `[]`
`serviceAccountName` | service account name | `agentmq`
`service.type` | Type of service to create [service](https://kubernetes.io/docs/concepts/services-networking/service/) | `NodePort`
`service.name` | service name [service](https://kubernetes.io/docs/concepts/services-networking/service/) | `agentmq`
`service.selector.name` | selector for service [service](https://kubernetes.io/docs/concepts/services-networking/service/) | `agentmq`
`image.tag` | Image tag [image](https://kubernetes.io/docs/concepts/containers/images/) | `test`
`image.repository` | Image repository [image](https://kubernetes.io/docs/concepts/containers/images/) | `automox/local/agentmq`
`replicaCount` | replicas to make | `1`
`containerPorts` | ports to expose on the container | `[]`
`environment` | local,dev,stg,prod | `local`
`serviceName` | name of agentmq service | `agentmq`
`affinity` | Node affinity | `{}`
`tolerations` | List of node taints to tolerate | `[]`
`nodeSelector` | Node to select | `{}`
`servicePorts` | service ports to expose | `[]`
`resources` | CPU/Memory resource requests/limits | `{}`
`name` | name of the pod/deployment | `agentmq`

## Exporter

A sidecar container is deployed that exports rabbitmq data for prometheus.

For detailed instructions, [check out the exporter](https://github.com/PatchSimple/rabbitmq-exporter)

It is mounted to /tmp/configs/config.yaml and _should_ be updated in real time. 

**NOTE:** if the port is changed, the pod **needs** to be restarted to take effect. Idk how to solve this but if you do feel free!

