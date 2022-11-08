# OAuth 2.0 (JWT) Token Authorisation Backend for RabbitMQ

This [RabbitMQ authentication/authorisation backend](https://www.rabbitmq.com/access-control.html) plugin lets
applications (clients) and users authenticate and present their permissions
using JWT-encoded [OAuth 2.0 access tokens](https://tools.ietf.org/html/rfc6749#section-1.4).

The plugin supports several identity providers, sometimes with vendor-specific configuration bits:

 * [Cloud Foundry UAA](./demo), the original provider
 * [Keycloak](https://github.com/rabbitmq/rabbitmq-oauth2-tutorial/blob/rich_auth_request/use-cases/keycloak.md)
 * [Microsoft AD on Azure](https://github.com/rabbitmq/rabbitmq-oauth2-tutorial/blob/rich_auth_request/use-cases/azure.md)
 * [Auth0](https://github.com/rabbitmq/rabbitmq-oauth2-tutorial/blob/rich_auth_request/use-cases/oauth0.md)

An OAuth 2.0 primer is available [elsewhere on the Web](https://auth0.com/blog/oauth2-the-complete-guide/).


## Supported RabbitMQ Versions

The plugin targets and ships with RabbitMQ. Like all RabbitMQ [plugins](https://www.rabbitmq.com/plugins.html),
it must be enabled before it can be used:

``` shell
rabbitmq-plugins enable rabbitmq_auth_backend_oauth2
```


## How it Works

### Authorization Workflow

This plugin does not communicate with a UAA server. It decodes an access token provided by
the client in the password field, verifies the token and authorises the user based on
the data stored in the verified token.

The token can be any [JWT token](https://jwt.io/introduction/) which
contains the `scope` and `aud` fields that follow certain conventions.

The way the token was  retrieved (such as what grant type was used) is outside of the scope
of this plugin.

### Prerequisites

To use this plugin

1. Identity server such as UAA and Keycloak should be configured to produce encrypted JWT tokens containing a set of RabbitMQ permission scopes
2. All RabbitMQ nodes must be [configured to use the `rabbit_auth_backend_oauth2` backend](https://www.rabbitmq.com/access-control.html)
3. All RabbitMQ nodes must be configured with a resource service ID (`resource_server_id`) that matches the scope prefix (e.g. `rabbitmq` in `rabbitmq.read:*/*`).
4. The token's `aud` field **must** have a value that is equal to or includes the `resource_server_id` value.

### Authorization Flow

1. Client authorizes to the OAuth 2.0 provider, requesting an `access_token` (using any grant type desired)
2. Token scopes returned by the OAuth 2.0 provider must include scopes that follow the convention used by this plugin: `configure:%2F/q1` means "configure permissions for 'q1' in vhost '/'". The `scope` field can be extended using the `extra_scopes_source` in **advanced.config** file.
3. Client passes the token in the password field when connecting to a RabbitMQ node. **The username field will be ignored**.
4. The translated permissions are stored as part of the authenticated connection state and used the same
   way permissions retrieved from the node's internal database would be


## Usage

The following section describes plugin configuration using UAA
as example identity provider. In case you use another supported provider, please
go over the contentes below and also an example for your service provider:

 * [Keycloak](https://github.com/rabbitmq/rabbitmq-oauth2-tutorial/blob/rich_auth_request/use-cases/keycloak.md)
 * [Microsoft AD on Azure](https://github.com/rabbitmq/rabbitmq-oauth2-tutorial/blob/rich_auth_request/use-cases/azure.md)
 * [Auth0](https://github.com/rabbitmq/rabbitmq-oauth2-tutorial/blob/rich_auth_request/use-cases/oauth0.md)


### UAA

The plugin needs a signing key to be configured in order to decrypt and verify client-provided tokens.
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

it will translate into the following configuration (in the [`advanced.config` format](https://www.rabbitmq.com/configure.html)):

```erlang
[
  %% ...
  %% backend configuration
  {rabbitmq_auth_backend_oauth2, [
    {resource_server_id, <<"my_rabbit_server">>},
    %% UAA signing key configuration
    {key_config, [
      {signing_keys, #{
        <<"a-key-ID">> => {pem, <<"-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA2dP+vRn+Kj+S/oGd49kq
6+CKNAduCC1raLfTH7B3qjmZYm45yDl+XmgK9CNmHXkho9qvmhdksdzDVsdeDlhK
IdcIWadhqDzdtn1hj/22iUwrhH0bd475hlKcsiZ+oy/sdgGgAzvmmTQmdMqEXqV2
B9q9KFBmo4Ahh/6+d4wM1rH9kxl0RvMAKLe+daoIHIjok8hCO4cKQQEw/ErBe4SF
2cr3wQwCfF1qVu4eAVNVfxfy/uEvG3Q7x005P3TcK+QcYgJxav3lictSi5dyWLgG
QAvkknWitpRK8KVLypEj5WKej6CF8nq30utn15FQg0JkHoqzwiCqqeen8GIPteI7
VwIDAQAB
-----END PUBLIC KEY-----">>}
          }}
      ]}
    ]}
].
```

If a symmetric key is used, the configuration will look like this:

```erlang
[
  {rabbitmq_auth_backend_oauth2, [
    {resource_server_id, <<"my_rabbit_server">>},
    {key_config, [
      {signing_keys, #{
        <<"a-key-ID">> => {map, #{<<"kty">> => <<"MAC">>,
                                  <<"alg">> => <<"HS256">>,
                                  <<"value">> => <<"my_signing_key">>}}
      }}
    ]}
  ]},
].
```

The key set can also be retrieved dynamically from a URL serving a [JWK Set](https://tools.ietf.org/html/rfc7517#section-5).
In that case, the configuration would look like this:

```erlang
[
  {rabbitmq_auth_backend_oauth2, [
    {resource_server_id, <<"my_rabbit_server">>},
    {key_config, [
      {jwks_url, <<"https://jwt-issuer.my-domain.local/jwks.json">>}
    ]}
  ]},
].
```

Note: if both are configured, `jwks_url` takes precedence over `signing_keys`.

### Variables Configurable in rabbitmq.conf

| Key                                      | Documentation
|------------------------------------------|---------------------------------------------------------------------------
| `auth_oauth2.resource_server_id`         | [The Resource Server ID](#resource-server-id-and-scope-prefixes)
| `auth_oauth2.resource_server_type`       | [The Resource Server Type](#rich-authorization-request)
| `auth_oauth2.additional_scopes_key`      | Key to fetch additional scopes from (maps to `additional_rabbitmq_scopes` in the `advanced.config` format)
| `auth_oauth2.default_key`                | ID (name) of the default signing key
| `auth_oauth2.signing_keys`               | Paths to signing key files
| `auth_oauth2.jwks_url`                   | The URL of key server. According to the [JWT Specification](https://datatracker.ietf.org/doc/html/rfc7515#section-4.1.2) key server URL must be https
| `auth_oauth2.https.cacertfile`           | Path to a file containing PEM-encoded CA certificates. The CA certificates are used during key server [peer verification](https://rabbitmq.com/ssl.html#peer-verification)
| `auth_oauth2.https.depth`                | The maximum number of non-self-issued intermediate certificates that may follow the peer certificate in a valid [certification path](https://rabbitmq.com/ssl.html#peer-verification-depth). Default is 10.
| `auth_oauth2.https.peer_verification`    | Should [peer verification](https://rabbitmq.com/ssl.html#peer-verification) be enabled Available values: `verify_none`, `verify_peer`. Default is `verify_none`. It is recommended to configure `verify_peer`. Peer verification requires a certain amount of setup and is more secure.
| `auth_oauth2.https.fail_if_no_peer_cert` | Used together with `auth_oauth2.https.peer_verification = verify_peer`. When set to `true`, TLS connection will be rejected if client fails to provide a certificate. Default is `false`.
| `auth_oauth2.https.hostname_verification`| Enable wildcard-aware hostname verification for key server. Available values: `wildcard`, `none`. Default is `none`.
| `auth_oauth2.algorithms`                 | Restrict [the usable algorithms](https://github.com/potatosalad/erlang-jose#algorithm-support).
| `auth_oauth2.verify_aud`                 | [Verify token's `aud`](#token-validation).
| `auth_oauth2.preferred_username_claims`  | [Determine user identity](#determine-user-identity).

Two examples below demonstrate a set of key files and a JWKS key server.

### Using Key Files

```
auth_oauth2.resource_server_id = new_resource_server_id
auth_oauth2.additional_scopes_key = my_custom_scope_key
auth_oauth2.default_key = id1
auth_oauth2.signing_keys.id1 = test/config_schema_SUITE_data/certs/key.pem
auth_oauth2.signing_keys.id2 = test/config_schema_SUITE_data/certs/cert.pem
auth_oauth2.algorithms.1 = HS256
auth_oauth2.algorithms.2 = RS256
```

### Using a JWKS Key Server

```
auth_oauth2.resource_server_id = new_resource_server_id
auth_oauth2.jwks_url = https://my-jwt-issuer/jwks.json
auth_oauth2.https.cacertfile = test/config_schema_SUITE_data/certs/cacert.pem
auth_oauth2.https.peer_verification = verify_peer
auth_oauth2.https.depth = 5
auth_oauth2.https.fail_if_no_peer_cert = true
auth_oauth2.https.hostname_verification = wildcard
auth_oauth2.algorithms.1 = HS256
auth_oauth2.algorithms.2 = RS256
```

## Resource Server ID and Scope Prefixes

OAuth 2.0 (and thus UAA-provided) tokens use scopes to communicate what set of permissions particular
client has been granted. The scopes are free form strings.

`resource_server_id` is a prefix used for scopes in UAA to avoid scope collisions (or unintended overlap).
It is an empty string by default.

## Determine user identity

Although OAuth 2.0 is all about authorization there are two situations where we need to determine the
user's identity. One is when we display the user's name in the management ui. And the second one is
when we have to capture the user identity in some logging statement.

By default, RabbitMQ first looks up the JWT claim `sub`. And if it is not present, it uses `client_id`.
Else it uses `unknown`. In other words, RabbitMQ could not figure out the user's identity from the token.

It is quite often that Identity Providers reserve the `sub` claim for the user's internal GUID and it uses instead
a different claim for the actual username such as `username`, `user_name` or `emailaddress` and similar.

For the latter case, RabbitMQ exposes a new configuration setting which can be either a single string or
an array of strings. Given the configuration below, RabbitMQ uses the following claims in the same order to
resolve the user's identity: `username`, `user_name`, `email`, `sub`, `client_id`.

```erlang
[
  {rabbitmq_auth_backend_oauth2, [
    {resource_server_id, <<"my_rabbit_server">>},
    {preferred_username_claims, [ <<"username">>, <<"user_name">>, <<"email">> ]}
    {key_config, [
      {jwks_url, <<"https://jwt-issuer.my-domain.local/jwks.json">>}
    ]}
  ]},
].
```

## Token Verification

When RabbitMQ receives a JWT token, it validates it before accepting it.

### Must be digitally signed

The token must carry a digital signature and optionally a `kid` header attribute which identifies the key RabbitMQ should
use to validate the signature.

### Must not be expired

Tokens are also checked for expiration using the `exp` ([exp](https://tools.ietf.org/html/rfc7519#page-9)) field, if present.
Expired tokens (past their expiration timestamp) will not be accepted.

### Audience must match (or include) the configured resource_server_id

The `aud` ([Audience](https://tools.ietf.org/html/rfc7519#page-9)) identifies the recipients and/or resource_server of the JWT. By default, **RabbitMQ uses this field to validate the token** although it can be disabled by setting `verify_aud` to `false`.  When set to `true`, this attribute must either be equal to the value of the `resource_server_id` setting or, in case of a list, it must contain the value of `resource_server_id`.


## Scope-to-Permission Translation

Scopes fetched from the provided JWT token are translated into [permission grants to RabbitMQ resources](https://www.rabbitmq.com/access-control.html#authorisation).

The scope format convention is `{permission}:{vhost_pattern}/{name_pattern}[/{routing_key_pattern}]` where

 * `{permission}` is an access permission (`configure`, `read`, or `write`)
 * `{vhost_pattern}` is a wildcard pattern for vhosts token has access to.
 * `{name_pattern}` is a wildcard pattern for resource name
 * `{routing_key_pattern}` is an optional wildcard pattern for routing key in topic authorization

Wildcard patterns are strings with optional wildcard symbols `*` that match
any sequence of characters.

Wildcard patterns match as following:

 * `*` matches any string
 * `prefix*` matches any string starting with a `prefix`
 * `*suffix` matches any string ending with a `suffix`
 * `prefix*suffix` matches any string starting with a `prefix` and ending with a `suffix`

There can be multiple wildcards in a pattern:

 * `start*middle*end`
 * `*before*after*`

**If special characters (`*`, `%`, or `/`) are used in a wildcard pattern,
the pattern must be [percent-encoded](https://en.wikipedia.org/wiki/Percent-encoding).**

These are the typical permissions examples:

 * `read:*/*`(`read:*/*/*`): read permissions to any resource on any vhost
 * `write:*/*`(`write:*/*/*`): write permissions to any resource on any vhost
 * `read:vhost1/*`(`read:vhost1/*/*`): read permissions to any resource on the `vhost1` vhost
 * `read:vhost1/some*`: read permissions to all the resources, starting with `some` on the `vhost1` vhost
 * `write:vhsot1/some*/routing*`: topic write permissions to publish to an exchange starting with `some` with a routing key starting with `routing`

See the [wildcard matching test suite](./test/wildcard_match_SUITE.erl) and [scopes test suite](./test/scope_SUITE.erl) for more examples.

Scopes should be prefixed with `resource_server_id`. For example,
if `resource_server_id` is "my_rabbit", a scope to enable read from any vhost will
be `my_rabbit.read:*/*`.

### Using a different token field for the Scope

By default the plugin will look for the `scope` key in the token, you can configure the plugin to also look in other fields using the `extra_scopes_source` setting. Values format accepted are scope as **string** or **list**


```erlang
[
  {rabbitmq_auth_backend_oauth2, [
    {resource_server_id, <<"my_rabbit_server">>},
    {extra_scopes_source, <<"my_custom_scope_key">>},
    ...
    ]}
  ]},
].
```
Token sample:
```
{
 "exp": 1618592626,
 "iat": 1618578226,
 "aud" : ["my_id"],
 ...
 "scope_as_string": "my_id.configure:*/* my_id.read:*/* my_id.write:*/*",
 "scope_as_list": ["my_id.configure:*/*", "my_id.read:*/*", my_id.write:*/*"],
 ...
 }
```

### Tags in Scopes

Users in RabbitMQ can have [tags associated with them](https://www.rabbitmq.com/access-control.html#user-tags).
Tags are used to [control access to the management plugin](https://www.rabbitmq.com/management.html#permissions).


In the OAuth context, tags can be added as part of the scope, using a format like `<resource_server_id>.tag:<tag>`. For
example, if `resource_server_id` is "my_rabbit", a scope to grant access to the management plugin with
the `monitoring` tag will be `my_rabbit.tag:monitoring`.

## Token Expiration and Refresh

On an existing connection the token can be refreshed by the [update-secret](https://rabbitmq.com/amqp-0-9-1-reference.html#connection.update-secret) AMQP 0.9.1 method. Please check your client whether it supports this method. (Eg. see documentation of the [Java client](https://rabbitmq.com/api-guide.html#oauth2-refreshing-token).) Otherwise the client has to disconnect and reconnect to use a new token.

If the latest token expires on an existing connection, after a limited time the broker will
refuse all operations (but it won't disconnect).


## Rich Authorization Request

The [Rich Authorization Request](https://oauth.net/2/rich-authorization-requests/) extension provides a way for
OAuth clients to request fine-grained permissions during an authorization request.
It moves away from the concept of scopes that are text labels and instead
defines a more sophisticated permission model.

RabbitMQ supports JWT tokens compliant with the extension. Below is a sample example section of JWT token:

```
{
  "authorization_details": [
    {
      "type" : "rabbitmq",
      "locations": ["cluster:finance/vhost:production-*"],
      "actions": [ "read", "write", "configure"  ]
    },
    {
      "type" : "rabbitmq",
      "locations": ["cluster:finance", "cluster:inventory" ],
      "actions": ["administrator" ]
    }
  ]
}
```

The token above contains two permissions under the attribute `authorization_details`.
Both permissions are meant for RabbitMQ servers with `resource_server_type` set to `rabbitmq`.
This field identifies RabbitMQ-specific permissions.

The first permission grants `read`, `write` and `configure` permissions to any queue and/or exchange on any virtual host whose name matches the pattern `production-*`, and that reside in clusters whose `resource_server_id` contains the string `finance`.
The `cluster` attribute's value is also a regular expression. To match exactly the string `finance`,
use `^finance$`.

The second permission grants the `administrator` user tag in two clusters, `finance` and `inventory`. Other
supported user tags as `management`, `policymaker` and `monitoring`.

### Type Field

In order for a RabbitMQ node to accept a permission, its value must match that node's `resource_server_type` setting value. A JWT token may have permissions for multiple resource types.

### Locations Field

The `locations` field can be either a string containing a single location or a JSON array containing
one or more locations.

A location consists of a list of key-value pairs separated by a forward slash `/` character. The format
used is:

```
cluster:{resource_server_id_pattern}[/vhost:{virtual_host_pattern}][/queue:{queue_name_pattern}|/exchange:{exchange_name_pattern}][/routing-key:{routing_key_pattern}]
```

Any string separated by `/` which does not conform to the `{key}:{value}` format will be ignored.
For instance, if locations start with a prefix, e.g. `vrn/cluster:rabbitmq`, the `vrn` pattern part will be
ignored.

The supported location attributed are:

 * `cluster`: this is the only mandatory attribute. It is a wildcard pattern which must match RabbitMQ's `resource_server_id`, otherwise the location is ignored.
 * `vhost`: This is the virtual host we are granting access to. It also a wildcard pattern. RabbitMQ defaults to `*`.
 * `queue`|`exchange`: queue or exchange name pattern. The location grants the permission to a set of queues (or exchanges) that match it. One location can only specify either `queue` or `exchange` but not both
 * `routing_key`: this is the routing key pattern the location grants the permission to. If not specified, `*` will be used

For more information about wildcard patterns, check the section [Scope-to-Permission Translation](#scope-to-permission-translation).

### Actions Field

The `actions` field can be either a string containing a single action or a JSON array containing one or more actions.

The supported actions map to either [RabbitMQ permissions](https://www.rabbitmq.com/access-control.html#authorisation):

*`configure`
*`read`
*`write`

Or RabbitMQ user tags:

*`administrator`
*`monitoring`
*`management`
*`policymaker`

### Rich-Permission to Scope translation

Rich Authorization Request permissions are translated into JWT token scopes that use the
aforementioned convention using the following algorithm:

* For each location found in the `locations` field where the `cluster` attribute matches the current RabbitMQ node's `resource_server_id`, the plugin extracts the `vhost`, `queue` or `exchange` and `routing_key` attributes from the location. If the location does not  have any of those attributes, the default value of `*` is assumed. Out of those values, the following scope suffix will be produced:
    ```
    scope_suffix = {vhost}/{queue}|{exchange}/{routing_key}
    ```
  The plugin will not accept a location which specifies both `queue` and `exchange`
* For each action found in the `actions` field:

    if the action is not a known user tag, the following scope is produced out of it:
    ```
    scope = {resource_server_id}.{action}:{scope_suffix}
    ```
    For known user tag actions, the following scope is produced:
    ```
    scope = {resource_server_id}.{action}
    ```

The plugin produces permutations of all `actions` by  all `locations` that match the node's configured `resource_server_id`.

In the following RAR example

``` json
{
  "authorization_details": [
    {
      "type" : "rabbitmq",
      "locations": ["cluster:finance/vhost:primary-*"],
      "actions": [ "read", "write", "configure"  ]
    },
    {
      "type" : "rabbitmq",
      "locations": ["cluster:finance", "cluster:inventory"],
      "actions": ["administrator" ]
    }
  ]
}
```
if RabbitMQ node's `resource_server_id` is equal to `finance`, the plugin will compute the following sets of scopes:

 * `finance.read:primary-*/*/*`
 * `finance.write:primary-*/*/*`
 * `finance.configure:primary-*/*/*`
 * `finance.tag:administrator`


## UAA Example

The [demo](/deps/rabbitmq_auth_backend_oauth2/demo) directory contains example configuration files which can be used to set up
a development UAA server and issue tokens, which can be used to access RabbitMQ
resources.

### UAA and RabbitMQ Config Files

To run the demo you need to have a [UAA](https://github.com/cloudfoundry/uaa) node
installed or built from source.

To make UAA use a particular config file, such as those provided in the demo directory,
export the `CLOUDFOUNDRY_CONFIG_PATH` environment variable. For example, to use symmetric keys,
see the UAA config files under the `demo/symmetric_keys` directory.

`demo/symmetric_keys/rabbit.config` contains a RabbitMQ configuration file that
sets up a matching signing key on the RabbitMQ end.

### Running UAA

To run UAA with a custom config file path, use the following from the UAA git repository:

```
CLOUDFOUNDRY_CONFIG_PATH=<path_to_plugin>/demo/symmetric_keys ./gradlew run
```

### Running RabbitMQ

```
RABBITMQ_CONFIG_FILE=<path_to_plugin>/demo/symmetric_keys/rabbitmq rabbitmq-server
## Or to run from source from the plugin directory
make run-broker RABBITMQ_CONFIG_FILE=demo/symmetric_keys/rabbitmq
```

The `rabbitmq_auth_backend_oauth2` plugin must be enabled on the RabbitMQ node.

### Asymmetric Key Example

To use an RSA (asymmetric) key, you can set `CLOUDFOUNDRY_CONFIG_PATH` to  `demo/rsa_keys`.
This directory also contains `rabbit.config` file, as well as a public key (`public_key.pem`)
which will be used for signature verification.

### UAA User and Permission Management

UAA sets scopes from client scopes and user groups. The demo uses groups to set up
a set of RabbitMQ permissions scopes.

The `demo/setup.sh` script can be used to configure a demo user and groups.
The script will also create RabbitMQ resources associated with permissions.
The script uses `uaac` and `bunny` (RabbitMQ client) and requires them to be installed.

When running the script, UAA server and RabbitMQ server should be running.
You should configure `UAA_HOST` (localhost:8080/uaa for local machine) and
`RABBITMQCTL` (a path to `rabbitmqctl` script) environment variables to run this script.

```
gem install cf-uaac
gem install bunny
RABBITMQCTL=<path_to_rabbitmqctl> demo/setup.sh
```

Please refer to `demo/setup.sh` to get more info about configuring UAA permissions.

The script will return access tokens which can be used to authenticate and authorise
in RabbitMQ. When connecting, pass the token in the **password** field. The username
field will be ignored as long as the token provides a client ID.


## License and Copyright

(c) 2016-2023 VMware, Inc. or its affiliates.

Released under the Mozilla Public License 2.0, same as RabbitMQ.
