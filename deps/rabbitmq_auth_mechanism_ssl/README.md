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
see [Authentication Mechanisms](http://www.rabbitmq.com/authentication.html) and
[Configuration](http://www.rabbitmq.com/configure.html) guides for
more details.

A couple of examples:

``` erlang
[
  {rabbit, [
    {auth_mechanisms, ['PLAIN', 'AMQPLAIN', 'EXTERNAL']}
  ]}
].
```

to allow this mechanism in addition to the defaults, or:

``` erlang
[
  {rabbit, [
    {auth_mechanisms, ['EXTERNAL']}
  ]}
].
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

```
rabbitmqctl list_connections peer_cert_subject
```

#### Common Name

To use the Common Name instead, set `rabbit.ssl_cert_login_from` to `common_name`:

```
[
  {rabbit, [
    {auth_mechanisms, ['PLAIN', 'AMQPLAIN', 'EXTERNAL']},
    {ssl_cert_login_from, common_name}
  ]}
].
```

Note that the authenticated user will then be looked up in the
[configured authentication / authorisation backend(s)](http://www.rabbitmq.com/access-control.html). This will be
the internal node database by default but could include other
backends if so configured.


## Copyright & License

(c) Pivotal Software Inc., 2007-2019.

Released under the same license as RabbitMQ.
