# x509 (TLS/SSL) certificate Authentication Mechanism for RabbitMQ

This plugin allows RabbitMQ clients authenticate using x509 certificates
and TLS (PKI) [peer verification mechanism](https://tools.ietf.org/html/rfc5280#section-6)
instead of credentials (username/password pairs).


## How it Works

When a client connects and performs TLS upgrade,
the username is obtained from the client's
TLS (x509) certificate. The user's password is not checked.

In order to use this mechanism the client must connect with TLS enabled, and
present a client certificate.


## Usage

This mechanism must also be enabled in RabbitMQ's configuration file,
see [Authentication Mechanisms](https://www.rabbitmq.com/docs/access-control/) and
[Configuration](https://www.rabbitmq.com/docs/configure) guides for
more details.

A couple of examples:

``` ini
auth_mechanisms.1 = PLAIN
auth_mechanisms.2 = AMQPLAIN
auth_mechanisms.3 = EXTERNAL
```

to allow this mechanism in addition to the defaults, or:

``` ini
auth_mechanisms.1 = EXTERNAL
```

to allow only this mechanism and prohibit connections that use
username and passwords.

For safety the server must be configured with the SSL option 'verify'
set to 'verify_peer', to ensure that if an SSL client presents a
certificate, it gets verified.

### On Certificate Formats and Generation

RabbitMQ uses certificates and private keys in the PEM format. How they are generated
is entirely up to the cluster operator. They can be obtained from a well-known and trusted
commercial certificate authority or generated as "self-signed" (the CA will be project-specific
and will not be widely trusted).

[`tls-gen`](https://github.com/rabbitmq/tls-gen) is a tool that can generate self-signed certificate chains:
a CA, a CA certificate, zero or more intermediate certificates and a client or server (leaf) certificate.

Some of the examples below will use `openssl` CLI tools directly because of their widespread use.
However, this plugin will work just fine with any x.509 standards compliant certificate in the PEM format,
regardless of what tool has generated them.


### Username Extraction from Certificate

#### Distinguished Name

By default this will set the username to an [RFC 4514](https://tools.ietf.org/html/rfc4514)-ish string form of
the certificate's subject's Distinguished Name, similar to that
produced by OpenSSL's "-nameopt [RFC 2253"](https://tools.ietf.org/html/rfc2253) option.

You can obtain this string form from a certificate with a command like:

```
openssl x509 -nameopt RFC2253 -subject -noout -in path/to/cert.pem
```

or from an existing amqps connection with commands like:

``` bash
rabbitmqctl list_connections peer_cert_subject
```

#### Subject Alternative Name

To extract username from a Subject Alternative Name (SAN) field, a few
settings need to be configured. Since a certificate can have more than
one SAN field and they can represent identities of different types,
the type and the index of the field to use must be provided.

For example, to use the first SAN value of type DNS:

``` ini
auth_mechanisms.1 = EXTERNAL

ssl_cert_login_from      = subject_alternative_name
ssl_cert_login_san_type  = dns
ssl_cert_login_san_index = 0
```

Or of type email:

``` ini
auth_mechanisms.1 = EXTERNAL

ssl_cert_login_from      = subject_alternative_name
ssl_cert_login_san_type  = email
ssl_cert_login_san_index = 0
```

#### Common Name

To use the Common Name instead, set `rabbit.ssl_cert_login_from` to `common_name`:

``` ini
auth_mechanisms.1 = EXTERNAL

ssl_cert_login_from = common_name
```

Note that the authenticated user will then be looked up in the
[configured authentication / authorisation backend(s)](https://www.rabbitmq.com/docs/access-control). This will be
the internal node database by default but could include other
backends if so configured.

## Copyright & License

(c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.

Released under the same license as RabbitMQ.
