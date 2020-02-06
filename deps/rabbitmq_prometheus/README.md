# Prometheus Exporter of Core RabbitMQ Metrics

## Getting Started

This is a Prometheus exporter of core RabbitMQ metrics, developed by the RabbitMQ core team.
It is largely a "clean room" design that reuses some prior work from Prometheus exporters done by the community.

## Project Maturity

This plugin is new as of RabbitMQ `3.8.0`.

## Documentation

See [Monitoring RabbitMQ with Prometheus and Grafana](https://www.rabbitmq.com/prometheus.html).


## Installation

This plugin is included into RabbitMQ 3.8.x releases. Like all [plugins](https://www.rabbitmq.com/plugins.html), it has to be
[enabled](https://www.rabbitmq.com/plugins.html#ways-to-enable-plugins) before it can be used:

To enable it with [rabbitmq-plugins](http://www.rabbitmq.com/man/rabbitmq-plugins.1.man.html):

    rabbitmq-plugins enable rabbitmq_prometheus

## Usage

See the [documentation guide](https://www.rabbitmq.com/prometheus.html).

Default port used by the plugin is `15692`. In most environments there would be no configuration
necessary.

See the entire list of [metrics](metrics.md) exposed via the default port.


## Configuration

This exporter supports the following options via a set of `prometheus.*` configuration keys:

 * `prometheus.enable_metrics_aggregation` returns all metrics aggregated (default is `false`).
    Nodes with over 50k objects (queues, connections, channels) can take 30 seconds or more to return metrics without this option.
    See #26 for more details.
 * `prometheus.path` defines a scrape endpoint (default is `"/metrics"`).
 * `prometheus.tcp.*` controls HTTP listener settings that match [those used by the RabbitMQ HTTP API](https://www.rabbitmq.com/management.html#configuration)
 * `prometheus.ssl.*` controls TLS (HTTPS) listener settings that match [those used by the RabbitMQ HTTP API](https://www.rabbitmq.com/management.html#single-listener-https)

Sample configuration snippet:

``` ini
# these values are defaults
prometheus.enable_metrics_aggregation = false
prometheus.path = /metrics
prometheus.tcp.port =  15692
```


## Contributing

See [CONTRIBUTING.md](https://github.com/rabbitmq/rabbitmq-prometheus/blob/master/CONTRIBUTING.md).


## Makefile

This project uses [erlang.mk](https://erlang.mk/), running `make help` will return erlang.mk help.

To see all custom targets that have been documented, run `make h`.

For BASH shell autocompletion, run `eval "$(make autocomplete)"`, then type `make a<TAB>` to see all Make targets starting with the letter `a`, e.g.:

```sh
$ make a<TAB
ac               all.coverdata    app-build        apps             apps-eunit       asciidoc-guide   autocomplete
all              app              app-c_src        apps-ct          asciidoc         asciidoc-manual
```


## Copyright

(c) Pivotal Software Inc., 2007-2020.
