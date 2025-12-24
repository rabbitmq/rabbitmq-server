# RabbitMQ LDAP Management Plugin

Provides an API to validate LDAP connection credentials and parameters.

## Installing

This plugin ships with RabbitMQ. Like all [plugins](https://www.rabbitmq.com/plugins.html), it must be enabled
before it can be used:

```
rabbitmq-plugins enable rabbitmq_auth_backend_ldap_management
```

## Usage

When the plugin is enabled, an HTTP API will be available.

### HTTP API

The HTTP API adds an endpoint for validating LDAP credentials.

#### `PUT /api/ldap/validate/simple-bind`

Attempts an LDAP connection given the provided JSON configuration.

**Example**

Create a file called ``config.json`` similar to the following, replacing the parameter values as desired:

```json
{
  "user_dn": "cn=foo,ou=baz,ou=bat,dc=rabbitmq,dc=com",
  "password": "test1234",
  "servers": ["localhost","my.ldap.com"],
  "port": 389 
}
```

Once created, `PUT` the file to the HTTP API:

```bash
curl -u guest:guest -v -X PUT -H 'Content-Type: application/json' -d @./config.json \
  http://localhost:15672/api/ldap/validate/simple-bind
```

Be sure to check the returned HTTP code and JSON to determine success!

See [this test file](https://github.com/rabbitmq/rabbitmq-server/blob/main/deps/rabbitmq_auth_backend_ldap_management/test/system_SUITE.erl) for a comprehensive example of allowed configuration values to test LDAP connections.

## License and Copyright

Released under [the same license as RabbitMQ](https://www.rabbitmq.com/mpl.html).

2007-2018 (c) 2007-2025 Broadcom. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
