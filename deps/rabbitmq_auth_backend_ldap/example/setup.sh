#!/bin/sh

# Based on instructions found at
# http://ubuntuforums.org/showthread.php?p=8161118#post8161118
# - yes that does sseem to be the most authoritative place.

ldapadd -Y EXTERNAL -H ldapi:/// -f example.ldif
ldapadd -x -D cn=admin,dc=example,dc=com -w admin -f simon.ldif
