#!/bin/sh -e

sudo apt-get --yes purge slapd
sudo rm -rf /var/lib/ldap
sudo apt-get --yes install slapd ldap-utils
sleep 1

DIR=$(dirname $0)

./$DIR/seed.sh
