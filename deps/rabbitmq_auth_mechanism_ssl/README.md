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
see [Authentication Mechanisms](https://www.rabbitmq.com/authentication.html) and
[Configuration](https://www.rabbitmq.com/configure.html) guides for
more details.

A couple of examples:

``` ini
auth_mechanisms.1 = PLAIN
auth_mechanisms.1 = AMQPLAIN
auth_mechanisms.1 = EXTERNAL
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

### Username Extraction from Certificate

#### Distinguished Name

By default this will set the username to an [RFC 4514](https://tools.ietf.org/html/rfc4514)-ish string form of
the certificate's subject's Distinguished Name, similar to that
produced by OpenSSL's "-nameopt [RFC 2253"](https://tools.ietf.org/html/rfc2253) option.

You can obtain this string form from a certificate with a command like:

```
openssl x509 -in path/to/cert.pem -nameopt RFC2253 -subject -noout
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
[configured authentication / authorisation backend(s)](https://www.rabbitmq.com/access-control.html). This will be
the internal node database by default but could include other
backends if so configured.


## Usage for MQTT Clients

To use this plugin with MQTT clients, set `mqtt.ssl_cert_login` to `true`:

``` ini
# It makes no sense to allow or expect anonymous client connections
# with certificate-based authentication 
mqtt.allow_anonymous = false

# require the peer to provide a certificate, enforce certificate exchange
ssl_options.verify = verify_peer
ssl_options.fail_if_no_peer_cert = true

# allow MQTT connections to compute their name from client certificate's CN
# (for simplicity: CN has been deprecated in favor of SAN for a long time)
mqtt.ssl_cert_login = true
ssl_cert_login_from = common_name
```


## Copyright & License

(c) 2007-2022 VMware, Inc. or its affiliates.

Released under the same license as RabbitMQ.
