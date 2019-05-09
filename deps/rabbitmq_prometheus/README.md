# Prometheus Exporter of Core (Raw, Unaggregated) RabbitMQ Metrics

## Getting Started

This is a Prometheus exporter of core (raw, unaggregated) RabbitMQ metrics.

## Project Maturity

This plugin is currently very immature and not ready for public consumption. The plan is to include
it into a future RabbitMQ release.


## Enable the Plugin

To enable it, use [rabbitmq-plugins](http://www.rabbitmq.com/man/rabbitmq-plugins.1.man.html):

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
