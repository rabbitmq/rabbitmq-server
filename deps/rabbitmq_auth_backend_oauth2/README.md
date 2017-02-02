## RabbitMQ authorisation Backend for [Cloud Foundry UAA](https://github.com/cloudfoundry/uaa)

Allows to use access tokens provided by CF UAA to authorize in RabbitMQ.

### Usage

First, enable the plugin. Then, configure access to UAA:

``` erlang
[{rabbitmq_auth_backend_uaa,
  [{resource_server_id, <<"your-resource-server-id"}]},
 {uaa_jwt, [
  {default_key, <<"key1">>},
  {signing_keys, #{
    <<"key1">> => {map, #{<<"kty">> => <<"oct">>, <<"k">> => <<"dG9rZW5rZXk">>}},
    <<"key2">> => {pem, <<"/path/to/public_key.pem">>},
    <<"key3">> => {json, "{\"kid\":\"key3\",\"alg\":\"HMACSHA256\",\"value\":\"tokenkey\",\"kty\":\"MAC\",\"use\":\"sig\"}"}}}]}].
```

where

 * `your-resource-server-id` is a resource server ID (e.g. 'rabbitmq')
 * `signing_keys` is a map of keys to sign JWT tokens (see [UAA_JWT](uaa_jwt) library for mode info)

To learn more about UAA/OAuth 2 clients, see [UAA docs](https://github.com/cloudfoundry/uaa/blob/master/docs/UAA-APIs.rst#id73).

Then you can use `access_tokens` acquired from UAA as username to authenticate in RabbitMQ.

### Scopes

Scopes are translated into permission grants to RabbitMQ resources for the provided token.

The current scope format is `<permission>:<vhost_pattern>/<name_pattern>[/<routing_key_pattern>]` where

 * `<permission>` is an access permission (`configure`, `read`, or `write`)
 * `<vhost_pattern>` is a wildcard pattern for vhosts, token has acces to.
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

See the [./test/wildcard_match_SUITE.erl](wildcard matching test suite) for more examples.

### Authorization Workflow

#### Prerequisites

1. There should be application client registered on UAA server.
2. Client id and secret should be set in plugin env as `username` and `password`
3. Client authorities should include `uaa.resource`
4. RabbitMQ auth_backends should include `rabbit_auth_backend_uaa`

#### Authorization

1. Client authorize with UAA, requesting `access_token` (using any grant type)
2. Token scope should contain RabbitMQ resource scopes (e.g. `configure:%2F/foo` means "configure queue 'foo' in vhost '/'")
3. Client passes token for a username when connecting to a RabbitMQ node

[uaa_jwt](https://github.com/rabbitmq/uaa_jwt)
