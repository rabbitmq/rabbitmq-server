## Rabbitmq auth backend to use with [CF UAA](https://github.com/cloudfoundry/uaa)

Allows to use access tokens provided by CF UAA to authorize in RabbitMQ.
Make requests to `/check_token` endpoint on UAA server. See https://github.com/cloudfoundry/uaa/blob/master/docs/UAA-APIs.rst#id32

### Usage

Enable plugin, set up config:
```
{rabbitmq_auth_backend_uaa,
  [{uri, <<"http://your-uaa-server">>},
   {username, <<"uaa-client-id">>},
   {password, <<"uaa-client-secret">>}]}
   
```

Where 
- `your-uaa-server` - server host of UAA server, 
- `uaa-client-id` - Client ID
- `uaa-client-secret` - Client Secret

For information about clients see https://github.com/cloudfoundry/uaa/blob/master/docs/UAA-APIs.rst#id73

Then you can use `access_tokens` aqured from UAA as username to authenticate in RabbitMQ.
