# RabbitMQ Prometheus.io core metrics exporter

## Getting Started

This is an Prometheus.io core metrics exporter plugin for RabbitMQ.

The plugin is included in the RabbitMQ distribution.  To enable
it, use [rabbitmq-plugins](http://www.rabbitmq.com/man/rabbitmq-plugins.1.man.html):

    rabbitmq-plugins enable rabbitmq_prometheus

Default port used by the plugin is `15673`.

## Configuration

This exporter supports the following options via `rabbitmq_prometheus` app env:
 - `path` - scrape endpoint. Default is `"metrics"`.
 - `ssl_config`
 - `tcp_config`
 
Sample `/etc/rabbitmq/rabbitmq.config`:
       
```erlang
[
 {rabbitmq_exporter, [
   {path, "/mymetrics"},
   {tcp_config, [{port, 15673}]}
 ]}
].
```

## Contributing

See [CONTRIBUTING.md](https://github.com/rabbitmq/rabbitmq-prometheus/blob/master/CONTRIBUTING.md).


## Copyright

(c) Pivotal Software Inc., 2007-2019.
