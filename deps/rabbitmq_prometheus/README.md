# Prometheus Exporter of Core (Raw, Unaggregated) RabbitMQ Metrics

## Getting Started

This is a Prometheus exporter of core (raw, unaggregated) RabbitMQ metrics.

## Project Maturity

This plugin is reasonably mature and will ship in the RabbitMQ distribution as of `3.8.0`.


## Installation

This plugin is included into RabbitMQ 3.8.x releases. Like all [plugins](https://www.rabbitmq.com/plugins.html), it has to be
[enabled](https://www.rabbitmq.com/plugins.html#ways-to-enable-plugins) before it can be used:

To enable it with [rabbitmq-plugins](http://www.rabbitmq.com/man/rabbitmq-plugins.1.man.html):

    rabbitmq-plugins enable rabbitmq_prometheus


## Usage

Default port used by the plugin is `15692`.


## Configuration

This exporter supports the following options via `rabbitmq_prometheus` app env:

 * `path` defines a scrape endpoint. Default is `"metrics"`.
 * `ssl_config`
 * `tcp_config`

Sample `/etc/rabbitmq/rabbitmq.config`:

```erlang
[
 {rabbitmq_prometheus, [
   {path, "/metrics"},
   {tcp_config, [{port, 15692}]}
 ]}
].
```

## Contributing

See [CONTRIBUTING.md](https://github.com/rabbitmq/rabbitmq-prometheus/blob/master/CONTRIBUTING.md).


## Copyright

(c) Pivotal Software Inc., 2007-2019.
