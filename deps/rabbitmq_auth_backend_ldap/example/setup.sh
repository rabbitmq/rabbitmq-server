#!/bin/sh -e

# Based on instructions found at
# http://ubuntuforums.org/showthread.php?p=8161118#post8161118
# - yes that does seem to be the most authoritative place.

sudo apt-get --yes purge slapd
sudo rm -rf /var/lib/ldap
sudo apt-get --yes install slapd
sleep 1

sudo ldapadd -Y EXTERNAL -H ldapi:/// -f global.ldif
ldapadd -x -D cn=admin,dc=example,dc=com -w admin -f people.ldif
ldapadd -x -D cn=admin,dc=example,dc=com -w admin -f groups.ldif
ldapadd -x -D cn=admin,dc=example,dc=com -w admin -f rabbit.ldif
