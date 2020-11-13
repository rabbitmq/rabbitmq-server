# RabbitMQ Certificate Trust Store

This plugin provides support for TLS (x509) certificate whitelisting.
All plugins which use the global TLS options will be configured with
the same whitelist.

## Rationale

RabbitMQ can be configured to accepted self-signed certificates
through various TLS socket options, namely the `ca_certs` and
`partial_chain` properties. However, this configuration is largely static.
There is no convenient means with which to change it in realtime, that
is, without making configuration changes to TLS listening sockets.

This plugin maintains a list of trusted .PEM formatted TLS (x509) certificates,
refreshing at configurable intervals, or when `rabbitmqctl
eval 'rabbit_trust_store:refresh().'` is invoked. Said certificates are then used
to verify inbound TLS connections for the entire RabbitMQ node (all plugins and protocols).
The list is node-local.

Certificates can be loaded from different sources (e.g. filesystem, HTTP server)
Sources are loaded using "providers" - erlang modules, implementing `rabbit_trust_store_certificate_provider`
behaviour.

The default provider is `rabbit_trust_store_file_provider`, which will load certificates
from a configured local filesystem directory.

## RabbitMQ Version Requirements

This plugin requires RabbitMQ `3.6.1` or later.

## Erlang Version Requirements

This plugin requires Erlang version 17.3 or later.

## Installation and Binary Builds

This plugin is now available from the [RabbitMQ community plugins page](http://www.rabbitmq.com/community-plugins.html).
Please consult the docs on [how to install RabbitMQ plugins](http://www.rabbitmq.com/plugins.html#installing-plugins).

## Usage

### Filesystem provider

Configure the trust store with a directory of whitelisted certificates
and a refresh interval:

```
trust_store.directory        = $HOME/rabbit/whitelist ## trusted certificate directory path
trust_store.refresh_interval = 30                     ## refresh interval in seconds (only)
```

In the erlang terms format:

```
    {rabbitmq_trust_store,
     [{directory,        "$HOME/rabbit/whitelist"}, %% trusted certificate directory path
      {refresh_interval, {seconds, 30}}             %% refresh interval in seconds (only)
    ]}
```

Setting `refresh_interval` to `0` seconds will disable automatic refresh.

Certificates are distinguished by their **filenames**, file modification time and
the hash of file contents.

#### Installing a Certificate

Write a `PEM` formatted certificate file to the configured directory
to whitelist it. This contains all the necessary information to
authorize a client which presents the very same certificate to the
server.

#### Removing a Certificate

Delete the certificate file from the configured directory to remove it
from the whitelist.

> Note: TLS session caching bypasses the trust store certificate validation and can
make it seem as if a removed certificate is still active. Disabling session caching
in the broker by setting the `reuse_sessions` ssl option to `false` can be done if
timely certificate removal is important.

### HTTP provider

HTTP provider loads certificates via HTTP(S) from remote server.

The server should have following API:

- `GET <root>` - list certificates in JSON format: `{"certificates": [{"id": <id>, "path": <url>}, ...]}`
- `GET <root>/<path>` - download PEM encoded certificate.

Where `<root>` is a configured certificate path, `<id>` - unique certificate identifier,
`<path>` - relative certificate path to load it from server.

Configuration of the HTTP provider:


```
trust_store.providers.1      = http
trust_store.url              = http://example.cert.url/path
trust_store.refresh_interval = 30
```

The example above uses an alias, `http` for `rabbit_trust_store_http_provider`.
Available aliases are:

- `file` - `rabbit_trust_store_file_provider`
- `http` - `rabbit_trust_store_http_provider`

In the erlang terms format:

```
{rabbitmq_trust_store,
 [{providers, [rabbit_trust_store_http_provider]},
  {url, "http://example.cert.url/path"},
  {refresh_interval, {seconds, 30}}
 ]}.
```

You can specify TLS options if you use HTTPS:

```
trust_store.providers.1      = http
trust_store.url              = https://example.secure.cert.url/path
trust_store.refresh_interval = 30
trust_store.ssl_options.certfile   = /client/cert.pem
trust_store.ssl_options.keyfile    = /client/key.pem
trust_store.ssl_options.cacertfile = /ca/cert.pem
```

In the erlang terms format:

```
{rabbitmq_trust_store,
 [{providers, [rabbit_trust_store_http_provider]},
  {url, "https://example.secure.cert.url/path"},
  {refresh_interval, {seconds, 30}},
  {ssl_options, [{certfile, "/client/cert.pem"},
                 {keyfile, "/client/key.pem"},
                 {cacertfile, "/ca/cert.pem"}
                ]}
 ]}.
```

HTTP provider uses `If-Modified-Since` during list request header to avoid updating
unchanged list of certificates.

You can additionally specify headers (e.g. authorization) using Erlang term format:

```
{rabbitmq_trust_store,
 [{providers, [rabbit_trust_store_http_provider]},
  {url, "http://example.cert.url/path"},
  {headers, [{"Authorization", "Bearer token"}]},
  {refresh_interval, {seconds, 30}}
 ]}.
```

#### Example

`examples/rabbitmq_trust_store_django` is an example Django application, which serves
certificates from a directory.


### Listing certificates

To list the currently loaded certificates use the `rabbitmqctl` utility as follows:

```
    rabbitmqctl eval 'io:format(rabbit_trust_store:list()).'
```

This will output a formatted list of certificates similar to:

```
    Name: cert.pem
    Serial: 1 | 0x1
    Subject: O=client,CN=snowman.local
    Issuer: L=87613,CN=MyTestRootCA
    Validity: "2016-05-24T15:28:25Z - 2026-05-22T15:28:25Z"
```

Note that this command reads each certificate from disk in order to extract
all the relevant information. If there are a large number of certificates in the
trust store use this command sparingly.


## How it Works

When the trust-store starts it configures TLS listening sockets,
whitelists the certificates in the given directory, then accepting
sockets can query the trust-store with their client's certificate. It
refreshes the whitelist to correspond with changes in the directory's
contents, installing and removing certificate details, after a refresh
interval or a manual refresh (by invoking a `rabbitmqctl eval
'rabbit_trust_store:refresh().'` from the commandline).


## Building from Source

See [Plugin Development guide](http://www.rabbitmq.com/plugin-development.html).

TL;DR: running

    make dist

will build the plugin and put build artifacts under the `./plugins` directory.


## Copyright and License

(c) Pivotal Software Inc, 2007-20016

Released under the MPL, the same license as RabbitMQ.
