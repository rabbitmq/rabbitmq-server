## Rabbitmq auth backend to use with [CF UAA](https://github.com/cloudfoundry/uaa)

Allows to use access tokens provided by CF UAA to authorize in RabbitMQ.
Make requests to `/check_token` endpoint on UAA server. See https://github.com/cloudfoundry/uaa/blob/master/docs/UAA-APIs.rst#id32

### Usage

Enable plugin, set up config:
```
{rabbitmq_auth_backend_uaa,
  [{uri,      <<"https://your-uaa-server">>},
   {username, <<"uaa-client-id">>},
   {password, <<"uaa-client-secret">>},
   {resource_server_id, <<"your-resource-server-id"}]}
   
```

Where 
- `your-uaa-server` - server host of UAA server, 
- `uaa-client-id` - Client ID
- `uaa-client-secret` - Client Secret
- `your-resource-server-id` - Resource id of server used by UAA (e.g. 'rabbitmq')

For information about clients see https://github.com/cloudfoundry/uaa/blob/master/docs/UAA-APIs.rst#id73

Then you can use `access_tokens` aqured from UAA as username to authenticate in RabbitMQ.

### Scopes

*Scopes is discussion topic, because current implementation provide not enough flexibility.*

Format of scope element: `<vhost>_<kind>_<permission>_<name>`, where

- `<vhost>` - vhost of recource
- `<kind>` can be `q` - queue, `ex` - exchange, or `t` - topic
- `<permission>` - access permission (configure, read, write)
- `<name>` - resource name (exact, no regexps allowed)

**Scopes logic had been taken from [oauth backend plugin](https://github.com/rabbitmq/rabbitmq_auth_backend_oauth)**

Currently there are duplicate module `rabbit_oauth2_scope.erl`, because I'm not sure how to organize dependencies.

### Authorization workflow:

#### Prerequisites

1. There should be application client registered on UAA server.
2. Client id and secret should be set in plugin env as `username` and `password`
3. Client authorities should include `uaa.resource`
4. RabbitMQ auth_backends should include `rabbit_auth_backend_uaa`

#### Authorization

1. Client authorize with UAA, requesting `access_token` (using any grant type)
2. Token scope should contain rabbitmq resource scopes (e.g. /_q_configure_foo - configure queue 'foo')
3. Client use token as username to connect to RabbitMQ server

