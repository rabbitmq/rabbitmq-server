## RabbitMQ authorisation Backend for [Cloud Foundry UAA](https://github.com/cloudfoundry/uaa)

Allows to use access tokens provided by CF UAA to authorize in RabbitMQ.

This plugin is **experimental** and should not be used until this notice is removed.

### RabbitMQ version

This plugins is developed for RabbitMQ 3.7 which is pending release.
This plugin **will not work** with any released versions of RabbitMQ

To test the plugin, you should build it yourself, together with RabbitMQ.

You can find build instructions [here](https://github.com/rabbitmq/rabbitmq-public-umbrella)

### Usage

First, enable the plugin. Then, configure access to UAA:

``` erlang
[{rabbitmq_auth_backend_uaa,
  [{resource_server_id, <<"your-resource-server-id"}]},
 {uaa_jwt, [
  {default_key, <<"key1">>},
  {signing_keys, #{
    <<"key1">> => {map, #{<<"kty">> => <<"oct">>, <<"k">> => <<"dG9rZW5rZXk">>}},
    <<"key2">> => {pem_file, <<"/path/to/public_key.pem">>},
    <<"key3">> => {json, "{\"kid\":\"key3\",\"alg\":\"HMACSHA256\",\"value\":\"tokenkey\",\"kty\":\"MAC\",\"use\":\"sig\"}"}}}]}].
```

where

 * `your-resource-server-id` is a resource server ID (e.g. 'rabbitmq')
 * `signing_keys` is a map of keys to sign JWT tokens (see [UAA_JWT](https://github.com/rabbitmq/uaa_jwt) library for mode info)
 * `default_key` is the default value used for the `kid` (key id) header parameter.

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

### Authorization Workflow

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

