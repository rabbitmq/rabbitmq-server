# Running Tests

## The OpenLDAP Dependency

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

The test setup will seed the LDAP database with the required objects.

## Running All Suites

As with all other RabbitMQ subprojects,

    make tests

will run all test suites.
