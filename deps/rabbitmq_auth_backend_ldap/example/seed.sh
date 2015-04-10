#!/bin/sh -e

DIR=$(dirname $0)

sudo ldapadd -Y EXTERNAL -H ldapi:/// -f ${DIR}/global.ldif
ldapadd -x -D cn=admin,dc=example,dc=com -w admin -f ${DIR}/people.ldif
ldapadd -x -D cn=admin,dc=example,dc=com -w admin -f ${DIR}/groups.ldif
ldapadd -x -D cn=admin,dc=example,dc=com -w admin -f ${DIR}/rabbit.ldif
