# RabbitMQ Certificate Trust Store

This plugin provides an alternative [TLS (x.509) certificate verification](https://www.rabbitmq.com/ssl.html#peer-verification)
strategy. Instead of the traditional PKI chain traversal mechanism,
this strategy simply checks the leaf certificate presented by a client
against an approved ("whitelisted") set of certificates.


## Rationale

When RabbitMQ is configured to use TLS for client connections, by default it will
use the standard [PKI certificate chain traversal](https://www.rabbitmq.com/ssl.html#peer-verification) process.
What certificates are trusted is ultimately controlled by adjusting the set of trusted CA certificates.

This configuration is standard for data services and it works well for many use cases. However,
this configuration is largely static: a change in trusted CA certificates requires a cluster
reconfiguration (redeployment).
There is no convenient means with which to change the set in realtime, and in particular
without affecting existing client connections.

This plugin offers an alternative. It maintains a set (list) of trusted .PEM formatted TLS (x509) certificates,
refreshed at configurable intervals. Said certificates are then used
to verify inbound TLS-enabled client connections across the entire RabbitMQ node (affects all plugins and protocols).
The set is node-local.

Certificates can be loaded into the trusted list from different sources. Sources are loaded using "providers".
Two providers ship with the plugin: the local filesystem and an HTTPS endpoint.

New providers can be added by implementing the `rabbit_trust_store_certificate_provider` behaviour.

The default provider is `rabbit_trust_store_file_provider`, which will load certificates
from a configured local filesystem directory.


## Installation

This plugin ships with modern RabbitMQ versions. Like all [plugins](https://www.rabbitmq.com/plugins.html),
it has to be enabled before it can be used:

``` sh
rabbitmq-plugins enable rabbitmq_trust_store
```

## Usage

### Filesystem provider

Configure the trust store with a directory of whitelisted certificates
and a refresh interval:

``` ini
## trusted certificate directory path
trust_store.directory        = $HOME/rabbit/whitelist
trust_store.refresh_interval = 30
```

Setting `refresh_interval` to `0` seconds will disable automatic refresh.

Certificates are identified and distinguished by their **filenames**, file modification time and
a hash value of file contents.

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
trust_store.url              = https://example.cert.url/path
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
  {url, "https://example.cert.url/path"},
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
  {url, "https://example.cert.url/path"},
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

See [Plugin Development guide](https://www.rabbitmq.com/plugin-development.html).

TL;DR: running

    make dist

will build the plugin and put build artifacts under the `./plugins` directory.


## Copyright and License

(c) 2007-2020 VMware, Inc. or its affiliates.

Released under the MPL, the same license as RabbitMQ.
