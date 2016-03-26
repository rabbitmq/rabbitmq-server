# Running LDAP Backend Tests

The tests *require* a locally installed LDAP server with some
predefined objects inside. If there's no LDAP server running on port
389, they will be skipped.

On a Debian-based distro you can set up a LDAP server
and run the tests with:

  ./example/setup.sh && make tests

IMPORTANT: this will wipe out your local OpenLDAP installation!

See the `example` for more details about the setup and seed data.
