#!/usr/bin/env sh

set -ex

package_path="/tmp/daemonize.tar.gz"
curl -L https://github.com/bmc/daemonize/archive/release-1.7.8.tar.gz -o "$package_path"

cd /tmp
tar xf "$package_path"
cd daemonize-release-1.7.8
./configure
make
sudo make install

