# Overview

This plugin provides the ability for your RabbitMQ server to perform
authentication (determining who can log in) and authorisation
(determining what permissions they have) by making requests to an HTTP
server.

As with all [authentication plugins](http://rabbitmq.com/access-control.html), this one requires RabbitMQ server
2.3.1 or later.

Note: it's at an early stage of development, although it's
conceptually very simple.

## Downloading

You can download a pre-built binary of this plugin from
the [Community Plugins page](http://www.rabbitmq.com/community-plugins.html).

## Building

You can build and install it like any other plugin (see
[the plugin development guide](http://www.rabbitmq.com/plugin-development.html)).

This plugin depends on the Erlang client (just to grab a URI parser).

## Enabling the Plugin

To enable the plugin, set the value of the `auth_backends` configuration item
for the `rabbit` application to include `rabbit_auth_backend_http`.
`auth_backends` is a list of authentication providers to try in order.

See the [Access Control guide](http://rabbitmq.com/access-control.html) for more information.

To use this backend exclusively, use the following snippet in `rabbitmq.conf` (currently
in master)

    auth_backends.1 = http

Or, in the classic config format (`rabbitmq.config`, prior to 3.7.0) or `advanced.config`:

    [{rabbit, [{auth_backends, [rabbit_auth_backend_http]}]}].

See [RabbitMQ Configuration guide](http://www.rabbitmq.com/configure.html) for more detail
on `auth_backends`.

## Configuring the Plugin

You need to configure the plugin to know which URIs to point at
and which HTTP method to use.

Below is a minimal configuration file example.

In `rabbitmq.conf` (currently RabbitMQ master):
    
    auth_backends.1 = http
    rabbitmq_auth_backend_http.user_path     = http://some-server/auth/user
    rabbitmq_auth_backend_http.vhost_path    = http://some-server/auth/vhost
    rabbitmq_auth_backend_http.resource_path = http://some-server/auth/resource
    rabbitmq_auth_backend_http.topic_path    = http://some-server/auth/topic

In the classic config format (`rabbitmq.config` prior to 3.7.0 or `advanced.config`):

    [
      {rabbit, [{auth_backends, [rabbit_auth_backend_http]}]},
      {rabbitmq_auth_backend_http,
       [{http_method,   post},
        {user_path,     "http(s)://some-server/auth/user"},
        {vhost_path,    "http(s)://some-server/auth/vhost"},
        {resource_path, "http(s)://some-server/auth/resource"},
        {topic_path,    "http(s)://some-server/auth/topic"}]}
    ].

By default `http_method` configuration is `GET` for backwards compatibility. It's recommended
to use `POST` requests to avoid credentials logging.

## What Must My Web Server Do?

This plugin requires that your web server respond to requests in a
certain predefined format. It will make GET (by default) or POST requests
against the URIs listed in the configuration file. It will add query string
(for `GET` requests) or a URL-encoded request body (for `POST` requests) parameters as follows:

### user_path

* `username` - the name of the user
* `password` - the password provided (may be missing if e.g. rabbitmq-auth-mechanism-ssl is used)

### vhost_path

* `username`   - the name of the user
* `vhost`      - the name of the virtual host being accessed
* `ip`         - the client ip address

Note that you cannot create arbitrary virtual hosts using this plugin; you can only determine whether your users can see / access the ones that exist.

### resource_path

* `username`    - the name of the user
* `vhost`       - the name of the virtual host containing the resource
* `resource`    - the type of resource (`exchange`, `queue`, `topic`)
* `name`        - the name of the resource
* `permission`  - the access level to the resource (`configure`, `write`, `read`) - see [the Access Control guide](http://www.rabbitmq.com/access-control.html) for their meaning

### topic_path

* `username`    - the name of the user
* `vhost`       - the name of the virtual host containing the resource
* `resource`    - the type of resource (`topic` in this case)
* `name`        - the name of the exchange
* `permission`  - the access level to the resource (`write` or `read`)
* `routing_key` - the routing key of the published message

See [topic authorisation](http://www.rabbitmq.com/access-control.html#topic-authorisation) for more information
about topic authorisation.

Your web server should always return HTTP 200 OK, with a body
containing:

* `deny`  - deny access to the user / vhost / resource
* `allow` - allow access to the user / vhost / resource
* `allow [list of tags]` - (for `user_path` only) - allow access, and mark the user as an having the tags listed

## Using TLS/HTTPS

If your Web server uses HTTPS and certificate verification, you need to
configure the plugin to use a CA and client certificate/key pair using the `rabbitmq_auth_backend_http.ssl_options` config variable:

    [
      {rabbit, [{auth_backends, [rabbit_auth_backend_http]}]},
      {rabbitmq_auth_backend_http,
       [{http_method,   post},
        {user_path,     "https://some-server/auth/user"},
        {vhost_path,    "https://some-server/auth/vhost"},
        {resource_path, "https://some-server/auth/resource"},
        {topic_path,    "https://some-server/auth/topic"},
        {ssl_options,
         [{cacertfile, "/path/to/cacert.pem"},
          {certfile,   "/path/to/client/cert.pem"},
          {keyfile,    "/path/to/client/key.pem"},
          {verify,     verify_peer},
          {fail_if_no_peer_cert, true}]}]}
    ].

It is recommended to use TLS for authentication and enable peer verification.


## Debugging

Check the RabbitMQ logs if things don't seem to be working
properly. Look for log messages containing "rabbit_auth_backend_http
failed".

## Example App (in Python)

In `examples/rabbitmq_auth_backend_django` there's a very simple
Django app that can be used for authentication. On Debian / Ubuntu you
should be able to run start.sh to launch it after installing the
python-django package. It's really not designed to be anything other
than an example.

See `examples/README` for slightly more information.
