# Prometheus Exporter of Core (Raw, Unaggregated) RabbitMQ Metrics

## Getting Started

This is a Prometheus exporter of core (raw, unaggregated) RabbitMQ metrics, developed by the RabbitMQ core team.
It is largely a "clean room" design that reuses some prior work from Prometheus exporters done by the community.

## Project Maturity

This plugin is new and relatively immature. It shipped in the RabbitMQ distribution starting with `3.8.0`.

## Documentation

See [Monitoring RabbitMQ with Prometheus and Grafana](https://www.rabbitmq.com/prometheus.html).

## Enable the Plugin

To enable it, use [rabbitmq-plugins](http://www.rabbitmq.com/man/rabbitmq-plugins.1.man.html):

    rabbitmq-plugins enable rabbitmq_prometheus


## Usage

See the [documentation guide](https://www.rabbitmq.com/prometheus.html).

Default port used by the plugin is `15692`. In most environments there would be no configuration
necessary.


## Configuration

This exporter supports the following options via a set of `prometheus.*` configuration keys:

 * `prometheus.path` defines a scrape endpoint. Default is `"/metrics"`.
 * `prometheus.tcp.*` controls HTTP listener settings that match [those used by the RabbitMQ HTTP API](https://www.rabbitmq.com/management.html#configuration)
 * `prometheus.ssl.*` controls TLS (HTTPS) listener settings that match [those used by the RabbitMQ HTTP API](https://www.rabbitmq.com/management.html#single-listener-https)

Sample configuration snippet:

``` ini
# these values are defaults
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

(c) Pivotal Software Inc., 2007-2019.
