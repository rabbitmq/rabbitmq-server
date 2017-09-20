## RabbitMQ authorisation Backend for [Cloud Foundry UAA](https://github.com/cloudfoundry/uaa)

Allows to use access tokens provided by CF UAA to authorize in RabbitMQ.

This plugin is **experimental** and should not be used until this notice is removed.

### RabbitMQ version

This plugins is developed for RabbitMQ 3.7 which is pending release.
This plugin **will not work** with any released versions of RabbitMQ

To test the plugin, you should build it yourself, together with RabbitMQ.

You can find build instructions [here](https://github.com/rabbitmq/rabbitmq-public-umbrella)

### Authorization Workflow

This plugin does not communicate with an UAA server. It decodes an access token and
authorises a user based on the token data.

The token can be any JWT token, which contains `scope` and `aud` fields.
The way the token was retrieved (such as grant type) is outside of this plufin scope.

#### Prerequisites

1. A symmetrically encrypted JWT token containing RabbitMQ scopes.
2. RabbitMQ auth_backends should include `rabbit_auth_backend_uaa`
3. The RabbitMQ scope prefix (e.g. `rabbitmq` in `rabbitmq.read:*/*`) needs to
match the `resource_server_id` configuration (empty by default).

#### Authorization

1. Client authorize with UAA, requesting an `access_token` (using any grant type)
2. Token scope should contain RabbitMQ resource scopes (e.g. `configure:%2F/foo` means "configure queue 'foo' in vhost '/'")
3. Client passes token as the username when connecting to a RabbitMQ node. The password
field is not used.

### Usage

The plugin should be configured with UAA signing key to check token signatures.
You can get the signing key from the UAA server using
[token_key api](https://docs.cloudfoundry.org/api/uaa/version/4.6.0/index.html#token-key-s)
or [uaac](https://github.com/cloudfoundry/cf-uaac) using `uaac signing key` command.

Important fields are `kty`, `value`, `alg` and `kid`.

For example if UAA returns

```
uaac signing key
  kty: RSA
  e: AQAB
  use: sig
  kid: legacy-token-key
  alg: RS256
  value: -----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA2dP+vRn+Kj+S/oGd49kq
6+CKNAduCC1raLfTH7B3qjmZYm45yDl+XmgK9CNmHXkho9qvmhdksdzDVsdeDlhK
IdcIWadhqDzdtn1hj/22iUwrhH0bd475hlKcsiZ+oy/sdgGgAzvmmTQmdMqEXqV2
B9q9KFBmo4Ahh/6+d4wM1rH9kxl0RvMAKLe+daoIHIjok8hCO4cKQQEw/ErBe4SF
2cr3wQwCfF1qVu4eAVNVfxfy/uEvG3Q7x005P3TcK+QcYgJxav3lictSi5dyWLgG
QAvkknWitpRK8KVLypEj5WKej6CF8nq30utn15FQg0JkHoqzwiCqqeen8GIPteI7
VwIDAQAB
-----END PUBLIC KEY-----
  n: ANnT_r0Z_io_kv6BnePZKuvgijQHbggta2i30x-wd6o5mWJuOcg5fl5oCvQjZh15IaPar5oXZLHcw1bHXg5YSiHXCFmnYag83bZ9YY_9tolMK4R9G3eO-YZSnLImfqMv7HYBoAM75pk0JnTKhF6ldgfavShQZqOAIYf-vneMDNax_ZMZdEbzACi3vnWqCByI6JPIQju
      HCkEBMPxKwXuEhdnK98EMAnxdalbuHgFTVX8X8v7hLxt0O8dNOT903CvkHGICcWr95YnLUouXcli4BkAL5JJ1oraUSvClS8qRI-Vino-ghfJ6t9LrZ9eRUINCZB6Ks8Igqqnnp_BiD7XiO1c
```

The configuration for a signing key should be:

```erlang
[ {rabbitmq_auth_backend_uaa, [{resource_server_id, <<"my_rabbit_server">>}]},
  {uaa_jwt, [
    {signing_keys, #{
        <<"legacy-token-key">> => {map, #{<<"kty">> => <<"RSA">>,
                                          <<"alg">> => <<"RS256">>,
                                          <<"kid">> => <<"legacy-token-key">>,
                                          <<"value">> => <<"-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA2dP+vRn+Kj+S/oGd49kq
6+CKNAduCC1raLfTH7B3qjmZYm45yDl+XmgK9CNmHXkho9qvmhdksdzDVsdeDlhK
IdcIWadhqDzdtn1hj/22iUwrhH0bd475hlKcsiZ+oy/sdgGgAzvmmTQmdMqEXqV2
B9q9KFBmo4Ahh/6+d4wM1rH9kxl0RvMAKLe+daoIHIjok8hCO4cKQQEw/ErBe4SF
2cr3wQwCfF1qVu4eAVNVfxfy/uEvG3Q7x005P3TcK+QcYgJxav3lictSi5dyWLgG
QAvkknWitpRK8KVLypEj5WKej6CF8nq30utn15FQg0JkHoqzwiCqqeen8GIPteI7
VwIDAQAB
-----END PUBLIC KEY-----">>}}
        }}
    ]}
].
```

If you are using a symmetric key, the configuration will be:

```erlang
[ {rabbitmq_auth_backend_uaa, [{resource_server_id, <<"my_rabbit_server">>}]},
  {uaa_jwt, [
    {signing_keys, #{
        <<"legacy-token-key">> => {map, #{<<"kty">> => <<"MAC">>,
                                          <<"alg">> => <<"HMACSHA256">>,
                                          <<"kid">> => <<"legacy-token-key">>,
                                          <<"value">> => <<"my_signing_key">>}}
        }}
    ]}
].
```

see [UAA_JWT](https://github.com/rabbitmq/uaa_jwt) library for more info

`resource_server_id` is a prefix for scopes in UAA to avoid overlap of scopes.
It is empty by default

To learn more about UAA/OAuth 2 clients, see [UAA docs](https://github.com/cloudfoundry/uaa/blob/master/docs/UAA-APIs.rst#id73).

Then you can use `access_tokens` acquired from UAA as username to authenticate in RabbitMQ.

### Scopes

Scopes are translated into permission grants to RabbitMQ resources for the provided token.

The current scope format is `<permission>:<vhost_pattern>/<name_pattern>[/<routing_key_pattern>]` where

 * `<permission>` is an access permission (`configure`, `read`, or `write`)
 * `<vhost_pattern>` is a wildcard pattern for vhosts, token has access to.
 * `<name_pattern>` is a wildcard pattern for resource name
 * `<routing_key_pattern>` is an optional wildcard pattern for routing key in topic authorization

Wildcard patterns are strings with optional wildcard symbols `*` that match
any sequence of characters.

Wildcard patterns match as wollowing:

 * `*` matches any string
 * `foo*` matches any string starting with a `foo`
 * `*foo` matches any string ending with a `foo`
 * `foo*bar` matches any string starting with a `foo` and ending with a `bar`

There can be multiple wildcards in a pattern:

 * `start*middle*end`
 * `*before*after*`

**If you want to use special characters like `*`, `%`, or `/` in a wildacrd pattern,
the pattern must be [URL-encoded](https://en.wikipedia.org/wiki/Percent-encoding).**

These are the typical permissions examples:

- `read:*/*`(`read:*/*/*`) - read permissions to any resource on any vhost
- `write:*/*`(`write:*/*/*`) - write permissions to any resource on any vhost
- `read:vhost1/*`(`read:vhost1/*/*`) - read permissions to any resource on the `vhost1` vhost
- `read:vhost1/some*` - read permissions to all the resources, starting with `some` on the `vhost1` vhost
- `write:vhsot1/some*/routing*` - topic write permissions to publish to an exchange starting with `some` with a routing key starting with `routing`

See the [./test/wildcard_match_SUITE.erl](wildcard matching test suite) and [./test/scope_SUITE.erl](scopes test suite) for more examples.

Scopes should be prefixed with `resource_server_id`. For example,
if `resource_server_id` is "my_rabbit", a scope to enable read from any vhost will
be `my_rabbit.read:*/*`

### Examples

The [demo](/demo) directory contains configuration files which can be used to set up
a development UAA server and issue tokens, which can be used to access RabbitMQ
resources.

To run the demo you should download [UAA](https://github.com/cloudfoundry/uaa)

You can set `CLOUD_FOUNDRY_CONFIG_PATH` to  `demo/symmetric_keys` to set
signing key to be symmetric `MAC` key. The value is `rabbit_signing_key`.
`demo/symmetric_keys/rabbit.config` contains a RabbitMQ configuration file to
set up this signing key for RabbitMQ.

To run UAA, from the UAA directory:
```
CLOUD_FOUNDRY_CONFIG_PATH=<path_to_plugin>/demo/symmetric_keys ./gradlew run
```

To run RabbitMQ:
```
## To run rabbitmq-server
RABBITMQ_CONFIG_FILE=<path_to_plugin>/demo/symmetric_keys/rabbitmq rabbitmq-server
## Or to run from source from the plugin directory
make run-broker RABBITMQ_CONFIG_FILE=demo/symmetric_keys/rabbitmq
```

You should enable `rabbitmq_auth_backend_uaa` plugin in RabbitMQ.

Or to use an RSA key, you can set `CLOUD_FOUNDRY_CONFIG_PATH` to  `demo/rsa_keys`.
This directory also contains `rabbit.config` file, as well as `public_key.pem`,
which will be used for signature verification.

UAA sets scopes from client scopes and user groups. The demo uses groups to set up
a set of RabbitMQ permissions scopes.

The `demo/setup.sh` script can be used to configure a demo user and groups.
The script will also create RabbitMQ resources associated with permissions.
The script uses `uaac` and `bunny` (RabbitMQ client) and requires rubygems to be installed.
When running the script, UAA server and RabbitMQ server should be running.
You should configure `UAA_HOST` (localhost:8080/uaa for local machine) and
`RABBITMQCTL` (a path to `rabbitmqctl` script) environment variables to run this script.

Please refer to `demo/setup.sh` to get more info about configuring UAA permissions.

The script will return an access tokens, which can be used to authorise
in RabbitMQ. When authorising, you should use the token as a **username**.

