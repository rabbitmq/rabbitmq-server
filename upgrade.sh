#!/bin/sh

set -e

archive=$1
version=$(basename "$archive" .tar.gz)
version=${version##*-}

bindir=$(dirname "$0")
rootdir="$bindir/.."
relsdir="$rootdir/releases"
reldir="$relsdir/$version"

node=$("$bindir/rabbitmq" eval 'node().')
echo "===> Upgrading node $node to version $verion"

mkdir "$reldir"
cp -a "$archive" "$reldir"
"$bindir/rabbitmq" unpack $version
"$bindir/migrate-sysconfig" $node
"$bindir/rabbitmq" upgrade $version
