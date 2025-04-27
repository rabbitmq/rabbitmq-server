# RabbitMQ Internal Loopback Authentication Backend

This plugin provides [authentication and authorisation backends](https://rabbitmq.com/access-control.html)
for RabbitMQ for basic authentication for only (loopback) localhost connections.

## Installation

As of 4.1.1, this plugin is distributed with RabbitMQ. Enable it with

    rabbitmq-plugins enable rabbitmq_auth_backend_internal_loopback

## Documentation

[See the Access Control guide](https://www.rabbitmq.com/access-control.html) on rabbitmq.com.


## Building from Source

See [Plugin Development guide](https://www.rabbitmq.com/plugin-development.html).

TL;DR: running

    make dist

will build the plugin and put build artifacts under the `./plugins` directory.


## Copyright and License

(c) 2007-2024 Broadcom. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

Released under the MPL, the same license as RabbitMQ.
