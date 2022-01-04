# RabbitMQ Access Control Cache Plugin

This plugin provides a caching layer for [access control operations](https://rabbitmq.com/access-control.html)
performed by RabbitMQ nodes.

## Project Maturity

As of 3.7.0, this plugin is distributed with RabbitMQ.

## Overview

This plugin provides a way to cache [authentication and authorization backend](https://rabbitmq.com/access-control.html)
results for a configurable amount of time.
It's not an independent auth backend but a caching layer for existing backends
such as the built-in, [LDAP](https://github.com/rabbitmq/rabbitmq-auth-backend-ldap), or [HTTP](https://github.com/rabbitmq/rabbitmq-auth-backend-http)
ones.

Cache expiration is currently time-based. It is not very useful with the built-in
(internal) [authn/authz backends](https://rabbitmq.com/access-control.html) but can be very useful for LDAP, HTTP or other backends that
use network requests.

## RabbitMQ Version Requirements

As with all authentication plugins, this plugin requires 2.3.1 or later.

As of 3.7.0, this plugin is distributed with RabbitMQ. Enable it like any other plugin.

To use this plugin with RabbitMQ 3.6.x, download a [3.6.x version from Bintray](https://bintray.com/rabbitmq/community-plugins/rabbitmq_auth_backend_cache) or see the `3.6.x` branch.

## Erlang Version Requirements

This plugin requires Erlang `19.3` or a later version.

## Binary Builds

Binary builds can be obtained [from project releases](https://github.com/rabbitmq/rabbitmq-auth-backend-cache/releases/) on GitHub.

## Building

You can build and install it like any other plugin (see
[the plugin development guide](https://www.rabbitmq.com/plugin-development.html)).

## Authentication and Authorization Backend Configuration

To enable the plugin, set the value of the `auth_backends` configuration item
for the `rabbit` application to include `rabbit_auth_backend_cache`.
`auth_backends` is a list of authentication providers to try in order.


So a configuration fragment that enables this plugin *only* (this example is **intentionally incomplete**) would look like:

    auth_backends.1 = cache

In the [classic config format](https://www.rabbitmq.com/configure.html#config-file-formats):

``` erlang
[
  {rabbit, [
            {auth_backends, [rabbit_auth_backend_cache]}
            ]
  }
].
```

This plugin wraps another auth backend (an "upstream" one) to reduce load on it.

To configure upstream auth backend, use the `auth_cache.cached_backend` configuration key
(`rabbitmq_auth_backend_cache.cached_backend` in the classic config format).

The following configuration uses the [LDAP backend]((https://rabbitmq.com/ldap.html)) for both authentication and authorization
and wraps it with caching:

    auth_backends.1 = cache

    auth_cache.cached_backend = ldap

In the classic config format:

``` erlang
[
  {rabbit, [
    %% ...
  ]},
  {rabbitmq_auth_backend_cache, [
                                  {cached_backend, rabbit_auth_backend_ldap}
                                ]},
  {rabbit_auth_backend_ldap, [
    %% ...
  ]},
].
```

The following example combines this backend with the [HTTP backend](https://github.com/rabbitmq/rabbitmq-auth-backend-http/tree/master) and its [example Spring Boot application](https://github.com/rabbitmq/rabbitmq-auth-backend-http/tree/master/examples):


    auth_backends.1 = cache
    auth_cache.cached_backend = http

    auth_http.http_method   = post
    auth_http.user_path     = http://localhost:8080/auth/user
    auth_http.vhost_path    = http://localhost:8080/auth/vhost
    auth_http.resource_path = http://localhost:8080/auth/resource
    auth_http.topic_path    = http://localhost:8080/auth/topic

In the classic config format:

``` erlang
[
 {rabbit, [
           {auth_backends, [rabbit_auth_backend_cache]}
          ]
 },
 {rabbitmq_auth_backend_cache, [
                                {cached_backend, rabbit_auth_backend_http}
                               ]
  },
  {rabbitmq_auth_backend_http, [{http_method,   post},
                                {user_path,            "http://127.0.0.1:8080/auth/user"},
                                {vhost_path,           "http://127.0.0.1:8080/auth/vhost"},
                                {resource_path,        "http://127.0.0.1:8080/auth/resource"},
                                {topic_path,           "http://127.0.0.1:8080/auth/topic"}
                               ]
  }
].
```

It is still possible to [use different backends for authorization and authentication](https://www.rabbitmq.com/access-control.html).

The following example configures plugin to use LDAP backend for authentication
but internal backend for authorisation:

    auth_backends.1 = cache

    auth_cache.cached_backend.authn = ldap
    auth_cache.cached_backend.authz = internal

In the classic config format:

``` erlang
[
  {rabbit, [
    %% ...
  ]},
  {rabbitmq_auth_backend_cache, [{cached_backend, {rabbit_auth_backend_ldap,
                                                   rabbit_auth_backend_internal}}]}].
```



## Cache Configuration

You can configure TTL for cache items, by using `cache_ttl` configuration item, specified in **milliseconds**

    auth_cache.cached_backend = ldap
    auth_cache.cache_ttl = 5000

Or using the classic config for both parameters:

``` erlang
[
 {rabbit, [
   %% ...
 ]},
 {rabbitmq_auth_backend_cache, [{cached_backend, rabbit_auth_backend_ldap},
                                {cache_ttl, 5000}]}].
```

You can also use a custom cache module to store cached requests. This module
should be an erlang module implementing `rabbit_auth_cache` behaviour and (optionally)
define `start_link` function to start cache process.

This repository provides several implementations:

 * `rabbit_auth_cache_dict` stores cache entries in the internal process dictionary. **This module is for demonstration only and should not be used in production**.
 * `rabbit_auth_cache_ets` stores cache entries in an [ETS](https://learnyousomeerlang.com/ets) table and uses timers for cache invalidation. **This is the default implementation**.
 * `rabbit_auth_cache_ets_segmented` stores cache entries in multiple ETS tables and does not delete individual cache items but rather
   uses a separate process for garbage collection.
 * `rabbit_auth_cache_ets_segmented_stateless` same as previous, but with minimal use of `gen_server` state, using ets tables to store information about segments.

To specify module for caching you should use `cache_module` configuration item and
specify start args with `cache_module_args`.
Start args should be list of arguments passed to module `start_link` function

Cache module can be set via sysctl config format:

    auth_cache.cache_module = rabbit_auth_backend_ets_segmented

Additional cache module arguments can only be defined via the [advanced config](https://www.rabbitmq.com/configure.html#advanced-config-file) or classic config format:

``` erlang
[
 {rabbit, [
   %% ...
 ]},

 {rabbitmq_auth_backend_cache, [{cache_module_args, [10000]}]}
].
```

The above two snippets combined in the classic config format:

``` erlang
[
 {rabbit, [
   %% ...
 ]},

 {rabbitmq_auth_backend_cache, [{cache_module, rabbit_auth_backend_ets_segmented},
                                {cache_module_args, [10000]}]}
].
```

The default values are `rabbit_auth_cache_ets` and `[]`, respectively.


## License and Copyright

(c) 2016-2021 VMware, Inc. or its affiliates.

Released under the same license as RabbitMQ, see `LICENSE`.
