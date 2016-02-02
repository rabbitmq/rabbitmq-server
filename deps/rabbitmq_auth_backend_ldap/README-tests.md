# Running LDAP Backend Tests

The tests *require* a locally installed LDAP server with some
predefined objects inside. If there's no LDAP server running on port
389, they will be skipped.

On a Debian-based distro you can set up a LDAP server
and run the tests with:

  ./example/setup.sh
   make tests

but be aware that this will wipe out your local OpenLDAP installation.

Poke around in example/ if using any other distro, you can probably
make it work.
