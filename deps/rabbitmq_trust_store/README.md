# RabbitMQ Certificate Trust Store

This plugin provides support for TLS (x509) certificate whitelisting.

## Rationale

This plugin whitelists the .PEM formatted TLS certificates in a given
directory, refreshing at configurable intervals, or when `rabbitmqctl
eval 'rabbit_trust_store:refresh().'` is invoked.

While RabbitMQ can be configured to accepted self-signed certificates
through various TLS socket options, namely the `ca_certs` and
`partial_chain` properties, this configuration is somewhat static.
There is no convenient means with which to change it in realtime, that
is, without making configuration changes to TLS listening sockets.

## Building

See [Plugin Development guide](http://www.rabbitmq.com/plugin-development.html).

    make dist

will build the plugin and put build artifacts under the `./plugins` directory.

## Usage

Configure the trust store with a directory of whitelisted certificates
and a refresh interval:

```
    {rabbitmq_trust_store,
     [{directory,        "$HOME/rabbit/whitelist"}, %% trusted certificate directory path
      {refresh_interval, {seconds, 30}}             %% refresh interval in seconds (only)
    ]}
```

Setting `refresh_interval` to `0` seconds will disable automatic refresh.

Certificates are distinguished by their **filename**:

> Changing the contents of a certificate file will **NOT** change the
  whitelist to correspond with that change.

### Installing a Certificate

Write a `PEM` formatted certificate file to the configured directory
to whitelist it. This contains all the necessary information to
authorize a client which presents the very same ceritificate to the
server.

### Removing a Certificate

Delete the certificate file from the configured directory to remove it
from the whitelist.

## How it Works

When the trust store starts it'll whitelist the certificates in the
given directory, then install and remove certificate details which are
written-to and deleted-from the directory, respectively after the
given refresh time or manual refresh (by invoking a `rabbitmqctl eval
'rabbit_trust_store:refresh().'`).

## Copyright and License

(c) Pivotal Software Inc, 2007-20016

Released under the MPL, the same license as RabbitMQ.
