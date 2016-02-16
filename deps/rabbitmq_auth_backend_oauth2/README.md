## RabbitMQ authorisation Backend for [Cloud Foundry UAA](https://github.com/cloudfoundry/uaa)

Allows to use access tokens provided by CF UAA to authorize in RabbitMQ.
Make requests to `/check_token` endpoint on UAA server. See https://github.com/cloudfoundry/uaa/blob/master/docs/UAA-APIs.rst#id32

### Usage

First, enable the plugin. Then, configure access to UAA:

``` erlang
{rabbitmq_auth_backend_uaa,
  [{uri,      <<"https://your-uaa-server">>},
   {username, <<"uaa-client-id">>},
   {password, <<"uaa-client-secret">>},
   {resource_server_id, <<"your-resource-server-id"}]}
   
```

where

 * `your-uaa-server` is a UAA server host
 * `uaa-client-id` is a UAA client ID
 * `uaa-client-secret` is the shared secret
 * `your-resource-server-id` is a resource server ID (e.g. 'rabbitmq')

To learn more about UAA/OAuth 2 clients, see [UAA docs](https://github.com/cloudfoundry/uaa/blob/master/docs/UAA-APIs.rst#id73).

Then you can use `access_tokens` acquired from UAA as username to authenticate in RabbitMQ.

### Scopes

Note: *scopes is a subject to change, the current implementation provides limited flexibility.*

Current scope format is `<vhost>_<kind>_<permission>_<name>`, where

 * `<vhost>` is resource vhost
 * `<kind>`: `q` or `queue` for queue, `ex` or `exchange` for exchange, `t` or `topic` for topic, or other string without `_` for custom resource kinds.
 * `<permission>` is an access permission (`configure`, `read`, or `write`)
 * `<name>` is an exact resource name (no regular expressions are supported)

The scopes implementation is shared with the [RabbitMQ OAuth 2.0 backend](https://github.com/rabbitmq/rabbitmq_auth_backend_oauth).

### Authorization workflow

#### Prerequisites

1. There should be application client registered on UAA server.
2. Client id and secret should be set in plugin env as `username` and `password`
3. Client authorities should include `uaa.resource`
4. RabbitMQ auth_backends should include `rabbit_auth_backend_uaa`

#### Authorization

1. Client authorize with UAA, requesting `access_token` (using any grant type)
2. Token scope should contain rabbitmq resource scopes (e.g. /_q_configure_foo - configure queue 'foo')
3. Client use token as username to connect to RabbitMQ server

