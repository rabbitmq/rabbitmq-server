# OAuth 2.0 (JWT) Token Authorisation Backend for RabbitMQ

This **experimental** [RabbitMQ authentication/authorisation backend](http://www.rabbitmq.com/access-control.html) plugin lets applications (clients)
and users authenticate and authorize using JWT-encoded [OAuth 2.0 access tokens](https://tools.ietf.org/html/rfc6749#section-1.4).

It is not specific to but developed against [Cloud Foundry UAA](https://github.com/cloudfoundry/uaa).

OAuth 2.0 primers are available [elsewhere on the Web](https://auth0.com/blog/oauth2-the-complete-guide/).


## Supported RabbitMQ Versions

This plugins is developed for RabbitMQ 3.8 (currently in development) and has to be built
against compatible RabbitMQ server, e.g. using the [umbrella repository](https://github.com/rabbitmq/rabbitmq-public-umbrella).


## How it Works

### Authorization Workflow

This plugin does not communicate with an UAA server. It decodes an access token provided by
the client and authorises a user based on the data stored in the token.

The token can be any [JWT token](https://jwt.io/introduction/) which
contains the `scope` and `aud` fields.  The way the token was
retrieved (such as what grant type was used) is outside of the scope
of this plugin.

### Prerequisites

To use this plugin

1. A symmetrically encrypted JWT token containing a set of RabbitMQ permission scopes.
2. All RabbitMQ nodes must be [configured to use the `rabbit_auth_backend_oauth2` backend](http://www.rabbitmq.com/access-control.html)
3. All RabbitMQ nodes must be configure with a resource service ID (`resource_server_id`) that matches the scope prefix (e.g. `rabbitmq` in `rabbitmq.read:*/*`).

### Authorization Flow

1. Client authorize with OAuth 2.0 provider, requesting an `access_token` (using any grant type desired)
2. Token scope returned by OAuth 2.0 provider must include RabbitMQ resource scopes that follow a convention used by this plugin: `configure:%2F/foo` means "configure permissions for 'foo' in vhost '/'")
3. Client passes the token as password when connecting to a RabbitMQ node. **The username field is ignored**.


## Usage

The plugin needs a UAA signing key to be configured in order to decrypt and verify client-provided tokens.
To get the signing key from a running UAA node, use the
[token_key endpoint](https://docs.cloudfoundry.org/api/uaa/version/4.6.0/index.html#token-key-s)
or [uaac](https://github.com/cloudfoundry/cf-uaac) (the `uaac signing key` command).

The following fields are required: `kty`, `value`, `alg`, and `kid`.

Assuming UAA reports the following signing key information:

```
uaac signing key
  kty: RSA
  e: AQAB
  use: sig
  kid: a-key-ID
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

it will translate into the following configuration (in the [advanced RabbitMQ config format](http://next.rabbitmq.com/configure.html)):

```erlang
[
  %% ...
  %% backend configuration
  {rabbitmq_auth_backend_oauth2, [
    {resource_server_id, <<"my_rabbit_server">>},
    %% UAA signing key configuration
    {uaa_jwt, [
      {signing_keys, #{
        <<"a-key-ID">> => {map, #{<<"kty">> => <<"RSA">>,
                                  <<"alg">> => <<"RS256">>,
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
    ]},
].
```

If you are using a symmetric key, the configuration will look like this:

```erlang
[
  {rabbitmq_auth_backend_oauth2, [
    {resource_server_id, <<"my_rabbit_server">>}
    {uaa_jwt, [
      {signing_keys, #{
        <<"a-key-ID">> => {map, #{<<"kty">> => <<"MAC">>,
                                  <<"alg">> => <<"HS256">>,
                                  <<"value">> => <<"my_signing_key">>}}
      }}
    ]}
  ]},
].
```

Key signature verification is implemented by the [UAA JWT library](https://github.com/rabbitmq/uaa_jwt).

### Resource Server ID and Scope Prefixes

OAuth 2.0 and (thus UAA-provided) tokens use scopes to communicate what set of permissions are particular
client has been granted. The scopes are free form strings.

`resource_server_id` is a prefix used for scopes in UAA to avoid scope collisions (or unintended overlap).
It is an empty string by default.

### Scope-to-Permission Translation

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

**If you want to use special characters like `*`, `%`, or `/` in a wildcard pattern,
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
be `my_rabbit.read:*/*`.

### Using Tokens with Clients

A client must present a valid `access_token` acquired from an OAuth 2.0 provider (UAA) as the **password**
in order to authenticate with RabbitMQ.

To learn more about UAA/OAuth 2.0 clients see [UAA docs](https://github.com/cloudfoundry/uaa/blob/master/docs/UAA-APIs.rst#id73).


### Examples

The [demo](/demo) directory contains example configuration files which can be used to set up
a development UAA server and issue tokens, which can be used to access RabbitMQ
resources.

#### UAA and RabbitMQ Config Files

To run the demo you need to have a [UAA](https://github.com/cloudfoundry/uaa) node
intalled or built from source.

To make UAA use a particular config file, such as those provided in the demo directory,
export the `CLOUD_FOUNDRY_CONFIG_PATH` environment variable. For example, to use symmetric keys,
see the UAA config files under the `demo/symmetric_keys` directory.

`demo/symmetric_keys/rabbit.config` contains a RabbitMQ configuration file that
sets up a matching signing key on the RabbitMQ end.

### Running UAA

To run UAA with a custom config file path, use the following from the UAA git repository:

```
CLOUD_FOUNDRY_CONFIG_PATH=<path_to_plugin>/demo/symmetric_keys ./gradlew run
```

#### Running RabbitMQ

```
RABBITMQ_CONFIG_FILE=<path_to_plugin>/demo/symmetric_keys/rabbitmq rabbitmq-server
## Or to run from source from the plugin directory
make run-broker RABBITMQ_CONFIG_FILE=demo/symmetric_keys/rabbitmq
```

The `rabbitmq_auth_backend_oauth2` plugin must be enabled on the RabbitMQ node.

#### Asymmetric Key Example

To use an RSA (asymmetric) key, you can set `CLOUD_FOUNDRY_CONFIG_PATH` to  `demo/rsa_keys`.
This directory also contains `rabbit.config` file, as well as a public key (`public_key.pem`)
which will be used for signature verification.

#### UAA User and Permission Management

UAA sets scopes from client scopes and user groups. The demo uses groups to set up
a set of RabbitMQ permissions scopes.

The `demo/setup.sh` script can be used to configure a demo user and groups.
The script will also create RabbitMQ resources associated with permissions.
The script uses `uaac` and `bunny` (RabbitMQ client) and requires them to be installed.

When running the script, UAA server and RabbitMQ server should be running.
You should configure `UAA_HOST` (localhost:8080/uaa for local machine) and
`RABBITMQCTL` (a path to `rabbitmqctl` script) environment variables to run this script.

```
gem install uaac
gem install bunny
RABBITMQCTL=<path_to_rabbitmqctl> demo/setup.sh
```

Please refer to `demo/setup.sh` to get more info about configuring UAA permissions.

The script will return an access tokens, which can be used to authenticate and authorise
in RabbitMQ. When connecting, pass the token in the **password** field. The username
field will be ignored as long as the token provides a client ID.


## License and Copyright

(c) 2016-2018 Pivotal Software Inc.

Released under the Mozilla Public License 1.1, same as RabbitMQ.
