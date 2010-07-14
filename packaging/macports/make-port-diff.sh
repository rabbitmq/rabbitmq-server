#!/bin/bash

# This script grabs the latest rabbitmq-server bits from the main
# macports subversion repo, and from the rabbitmq.com macports repo,
# and produces a diff from the former to the latter for submission
# through the macports trac.

set -e

dir=/tmp/$(basename $0).$$
mkdir -p $dir/macports $dir/rabbitmq

# Get the files from the macports subversion repo
cd $dir/macports
svn checkout http://svn.macports.org/repository/macports/trunk/dports/net/rabbitmq-server/ 2>&1 >/dev/null

# Clear out the svn $id tag
sed -i -e 's|^# \$.*$|# $Id$|' rabbitmq-server/Portfile

# Get the files from the rabbitmq.com macports repo
cd ../rabbitmq
curl -s http://www.rabbitmq.com/releases/macports/net/rabbitmq-server.tgz | tar xzf -

cd ..
diff -Naur --exclude=.svn macports rabbitmq
cd /
rm -rf $dir
