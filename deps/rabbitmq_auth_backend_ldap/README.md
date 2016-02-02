# RabbitMQ LDAP Authentication Backend

This plugin provides [authentication and authorisation backends](http://rabbitmq.com/access-control.html)
for RabbitMQ that use LDAP.

## Requirements

You can build and install it like any other plugin (see
http://www.rabbitmq.com/plugin-development.html).

## Documentation

[See LDAP guide](http://www.rabbitmq.com/ldap.html) on rabbitmq.com.

## Limitations

Prior to RabbitMQ 3.6.0, this plugin opened a new LDAP server
connection for every operation. 3.6.0 and later versions use
a pool of connections.
