#!/bin/sh -e
export DEBIAN_FRONTEND=noninteractive
sudo apt-get --yes purge slapd
sudo rm -rf /var/lib/ldap
echo -e " \
slapd    slapd/internal/generated_adminpw    password   openstack
slapd    slapd/password2    password    openstack
slapd    slapd/internal/adminpw    password openstack
slapd    slapd/password1    password    openstack
slapd    slapd/backend    select    BDB
" | sudo debconf-set-selections
sudo apt-get --yes install slapd ldap-utils
sleep 1

DIR=$(dirname $0)

$DIR/seed.sh
