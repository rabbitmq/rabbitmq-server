# RabbitMQ LDAP Authentication Backend

This plugin provides [authentication and authorisation backends](http://rabbitmq.com/access-control.html)
for RabbitMQ that use LDAP.

## Installation

This plugin ships with reasonably recent RabbitMQ versions
(e.g. `3.3.0` or later). Enable it with

    rabbitmq-plugins enable rabbitmq_auth_backend_ldap

## Documentation

[See LDAP guide](http://www.rabbitmq.com/ldap.html) on rabbitmq.com.


## Building from Source

See [Plugin Development guide](http://www.rabbitmq.com/plugin-development.html).

TL;DR: running

    make dist

will build the plugin and put build artifacts under the `./plugins` directory.


## Copyright and License

(c) Pivotal Software Inc, 2007-20016

Released under the MPL, the same license as RabbitMQ.
