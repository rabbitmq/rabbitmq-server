# RabbitMQ LDAP Authentication Backend

This plugin provides [authentication and authorisation backends](https://rabbitmq.com/access-control.html)
for RabbitMQ that use LDAP.

Under a heavy load this plugin can put a higher than expected amount of load on it's backing LDAP service.
We recommend using it together with [rabbitmq_auth_backend_cache](https://github.com/rabbitmq/rabbitmq-auth-backend-cache)
with a reasonable caching interval (e.g. 2-3 minutes).

## Installation

This plugin ships with reasonably recent RabbitMQ versions
(e.g. `3.3.0` or later). Enable it with

    rabbitmq-plugins enable rabbitmq_auth_backend_ldap

## Documentation

[See LDAP guide](https://www.rabbitmq.com/ldap.html) on rabbitmq.com.


## Building from Source

See [Plugin Development guide](https://www.rabbitmq.com/plugin-development.html).

TL;DR: running

    make dist

will build the plugin and put build artifacts under the `./plugins` directory.


## Copyright and License

(c) 2007-2020 VMware, Inc. or its affiliates.

Released under the MPL, the same license as RabbitMQ.
