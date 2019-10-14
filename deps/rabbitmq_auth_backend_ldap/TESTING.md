# Running Tests

## The OpenLDAP Dependency

There are two ways of setting up an OpenLDAP node with a seed expectd
by the test suite:

 * Vagrant
 * Docker

The test setup will seed the LDAP database with the required objects.

### With Vagrant

If you have [Vagrant](https://www.vagrantup.com) installed you
can simply `vagrant up` from the root of the project directory.
This will start a vagrant box with OpenLDAP running, accessible
on local port 3890.
Alternatively run OpenLDAP locally on a local port specified via the `LDAP_PORT` environment variable
(3890 by default to avoid conflicts) and use
`example/setup.sh` to create the appropriate ldap databases.

**IMPORTANT**: this will **wipe out your local OpenLDAP installation**!
The setup script currently needs to be executed between test suite runs,
too.

### With Docker

First build the image:

``` sh
docker build -t rabbitmq_auth_backend_ldap .
```

Run it with

``` sh
docker run -p 3890:389 -i -t rabbitmq_auth_backend_ldap
```

## Running All Suites

As with all other RabbitMQ subprojects,

``` sh
make tests
```

will run all test suites.
