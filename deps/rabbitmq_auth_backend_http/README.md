# Overview

This plugin provides the ability for your RabbitMQ server to perform
authentication (determining who can log in) and authorisation
(determining what permissions they have) by making requests to an HTTP
server.

As with all authentication plugins, this one requires rabbitmq-server
2.3.1 or later.

Note: it's at an early stage of development, although it's
conceptually very simple.

# Requirements

You can build and install it like any other plugin (see
[the plugin development guide](http://www.rabbitmq.com/plugin-development.html)).

**Binary packages are available at github in the downloads section.**

This plugin does not depend on any others.

# Enabling the plugin

To enable the plugin, set the value of the `auth_backends` configuration item
for the `rabbit` application to include `rabbit_auth_backend_http`.
`auth_backends` is a list of authentication providers to try in order.

So a configuration fragment that enables this plugin *only* would look like:

    [{rabbit, [{auth_backends, [rabbit_auth_backend_http]}]}].

to use only HTTP, or:

    [{rabbit,
      [{auth_backends, [rabbit_auth_backend_http, rabbit_auth_backend_internal]}]
     }].

to use both HTTP and the internal database.

# Configuring the plugin

You need to configure the plugin to know which URIs to point at.

A minimal configuration file might look like:

    [
      {rabbit, [{auth_backends, [rabbit_auth_backend_http]}]},
      {rabbitmq_auth_backend_http,
       [{user_path,     "http://some-server/auth/user"},
        {vhost_path,    "http://some-server/auth/vhost"},
        {resource_path, "http://some-server/auth/resource"}]}
    ].

# What must my web server do?

This plugin requires that your web server respond to requests in a
certain predefined format. It will make GET requests against the URIs
listed in the configuration file. It will add query string parameters
as follows:

### user_path

* `username` - the name of the user
* `password` - the password provided (may be missing if e.g. rabbitmq-auth-mechanism-ssl is used)

### vhost_path

* `username`   - the name of the user
* `vhost`      - the name of the virtual host being accessed

Note that you cannot create arbitrary virtual hosts using this plugin; you can only determine whether your users can see / access the ones that exist.

### resource_path

* `username`   - the name of the user
* `vhost`      - the name of the virtual host containing the resource
* `resource`   - the type of resource (`exchange`, `queue`)
* `name`       - the name of the resource
* `permission` - the access level to the resource (`configure`, `write`, `read`) - see [the admin guide](http://www.rabbitmq.com/access-control.html) for their meaning

Your web server should always return HTTP 200 OK, with a body
containing:

* `deny`  - deny access to the user / vhost / resource
* `allow` - allow access to the user / vhost / resource
* `allow [list of tags]` - (for `user_path` only) - allow access, and mark the user as an having the tags listed

# Debugging

Check the RabbitMQ logs if things don't seem to be working
properly. Look for log messages containing "rabbit_auth_backend_http
failed".

# Example

In `examples/rabbitmq_auth_backend_django` there's a very simple
Django app that can be used for authentication. On Debian / Ubuntu you
should be able to run start.sh to launch it after installing the
python-django package. It's really not designed to be anything other
than an example.

See `examples/README` for slightly more information.
