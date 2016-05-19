## Plugin status

This plugin is considered experimental. Work is still in progress.
You can try it on your own risk.

# Overview

This plugin provides ability to cache authentication and authorization backend
responses to configurable amount of time.
It's not an independent auth backend, but proxy for existing backends.

This plugin will cache all requests to upstream auth backend for specific 
(configurable) amount of time. This makes few sense if used with broker 
internal auth backend but can be useful in LDAP, HTTP or other backends that use
network for access checks.

**Be aware that this implementation does not provide any automatical invalidation other than TTL**

As with all authentication plugins, this one requires rabbitmq-server
2.3.1 or later.

## Building

You can build and install it like any other plugin (see
[the plugin development guide](http://www.rabbitmq.com/plugin-development.html)).

## Enabling the Plugin

To enable the plugin, set the value of the `auth_backends` configuration item
for the `rabbit` application to include `rabbit_auth_backend_cache`.
`auth_backends` is a list of authentication providers to try in order.


So a configuration fragment that enables this plugin *only* would look like:

    [{rabbit, [{auth_backends, [rabbit_auth_backend_cache]}]}].

To configure upstream auth backend, you should use `cached_backend` configuration item
for the `rabbitmq_auth_backend_cache` application.

Configuration to use LDAP auth backend:

    [{rabbitmq_auth_backend_cache, [{cached_backend, rabbit_auth_backend_ldap}]}].

You can use different backends for authorization and authentication same way,
[as it used in broker](https://www.rabbitmq.com/access-control.html):

The following example configures plugin to use LDAP backend for authentication
but internal backend for authorisation:

    [{rabbitmq_auth_backend_cache, [{cached_backend, {rabbit_auth_backend_ldap,
                                                      rabbit_auth_backend_internal}}]}].

## Configuring the plugin

You can configure TTL for cache items, by using `cache_ttl` configuration item, specified in **milliseconds**

    [{rabbitmq_auth_backend_cache, [{cached_backend, rabbit_auth_backend_ldap}
                                    {cache_ttl, 5000}]}].

You can also use a custom cache module to store cached requests. This module
should be an erlang module implementing `rabbit_auth_cache` behaviour and (optionally)
define `start_link` function to start cache process.

This repository contains three such modules:

- `rabbit_auth_cache_dict` stores cache in internal process dictionary **this module is for demonstration only and should not be used in production**
- `rabbit_auth_cache_ets` stores cache in `ets` table and uses timers to invalidate **this is default module**
- `rabbit_auth_cache_ets_segmented` stores cache in multiple `ets` tables and do not deletes individual cache items, deletes tables during garbage collection periodically.
- `rabbit_auth_cache_ets_segmented_stateless` same as previous, but with minimal use of `gen_server` state, using ets tables to store information about segments.

To specify module for caching you should use `cache_module` configuration item and 
specify start args with `cache_module_args`.
Start args should be list of arguments passed to module `start_link` function

    [{rabbitmq_auth_backend_cache, [{cache_module, rabbit_auth_backend_ets_segmented},
                                    {cache_module_args, [10000]}]}].

Default values is `rabbit_auth_cache_ets` and `[]` respectively.



