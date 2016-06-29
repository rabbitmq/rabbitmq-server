# Running LDAP Backend Tests

If you have [Vagrant](https://www.vagrantup.com) installed you
can simply `vagrant up` from the root of the project directory.
This will start a vagrant box with OpenLDAP running, accessible
on local port 3890.
Alternatively run OpenLDAP locally on port 3890 and use
`example/setup.sh` to create the appropriate ldap databases.

IMPORTANT: this will wipe out your local OpenLDAP installation!
The setup script currently needs to be executed between test suite runs,
too.

The test setup will seed the LDAP database with the required objects.

Run `make test` to run the complete test suite.
