# Running Tests

## The OpenLDAP Dependency

The testsuite depends on an OpenLDAP server. slapd(8) and the CLI must
be installed for it to work. However there is no need to configure the
server.

The testsuite takes care of starting its own server and configuring it.
It won't conflict with the system OpenLDAP server instance.

The testsuite needs the following modules to be available:
* `bdb`
* `memberof`
* `refint`

## Running All Suites

As with all other RabbitMQ subprojects,

``` sh
make tests
```

will run all test suites.
