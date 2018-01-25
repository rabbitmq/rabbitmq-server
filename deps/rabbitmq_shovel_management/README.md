# RabbitMQ Shovel Management Plugin

Adds information on shovel status to the management plugin. Build it
like any other plugin.

If you have a heterogenous cluster (where the nodes have different
plugins installed), this should be installed on the same nodes as the
management plugin.


## Installing

This plugin ships with RabbitMQ. Enable it with

```
[sudo] rabbitmq-plugins rabbitmq_shovel_management
```


## Usage

When the plugin is enabled, you'll find a shovel management
link under the Admin tab.

### HTTP API

The HTTP API adds endpoints for listing, creating, and deleting shovels.

#### `GET /api/shovels[/VHOST]`
Lists all shovels, optionally filtering by Virtual Host.

**Example**

```bash
curl -u guest:guest -v http://localhost:15672/api/shovels/%2f
```

#### `POST /api/parameters/shovel/<VHOST>/<NAME>`
Create a shovel, passing in the configuration as JSON in the request body.

**Example**

Create a file called ``shovel.json`` similar to the following, replacing the parameter values as desired:
```json
{
  "component": "shovel",
  "name": "my-shovel",
  "value": {
    "ack-mode": "on-publish",
    "add-forward-headers": false,
    "delete-after": "never",
    "dest-exchange": null,
    "dest-queue": "dest",
    "dest-uri": "amqp://",
    "prefetch-count": 250,
    "reconnect-delay": 30,
    "src-queue": "source",
    "src-uri": "amqp://"
  },
  "vhost": "/"
}
```

Once created, post the file to the HTTP API:

```bash
curl -u guest:guest -v -X POST -H 'Content-Type: application/json' -d @./shovel.json \
  http://localhost:15672/api/parameters/shovel/%2F/my-shovel
```
*Note* You can specify either `dest_queue` OR `dest_exchange` in the `value` stanza of the JSON, but not both.

#### `GET /api/parameters/shovel/<VHOST>/<NAME>`
Shows the configurtion parameters for a shovel.

**Example** 

```bash
curl -u guest:guest -v http://localhost:15672/api/parameters/shovel/%2F/my-shovel
```

#### `DELETE /api/parameters/shovel/<VHOST>/<NAME>`
Delete a shovel.

**Example** 

```bash
curl -u guest:guest -v -X DELETE http://localhost:15672/api/parameters/shovel/%2F/my-shovel
```

## License and Copyright

Released under [the same license as RabbitMQ](https://www.rabbitmq.com/mpl.html).

2007-2018 (c) Pivotal Software Inc.
