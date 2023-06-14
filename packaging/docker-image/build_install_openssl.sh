#!/usr/bin/env bash

set -euxo pipefail

OPENSSL_PATH="/usr/local/src/openssl-$OPENSSL_VERSION"
OPENSSL_CONFIG_DIR=/usr/local/etc/ssl

cd "$OPENSSL_PATH"
debMultiarch="$(dpkg-architecture --query DEB_HOST_MULTIARCH)"
# OpenSSL's "config" script uses a lot of "uname"-based target detection...
MACHINE="$(dpkg-architecture --query DEB_BUILD_GNU_CPU)" \
RELEASE="4.x.y-z" \
SYSTEM='Linux' \
BUILD='???' \
./config \
    --openssldir="$OPENSSL_CONFIG_DIR" \
    --libdir="lib/$debMultiarch" \
    -Wl,-rpath="/usr/local/lib/$debMultiarch" # add -rpath to avoid conflicts between our OpenSSL's "libssl.so" and the libssl package by making sure /usr/local/lib is searched first (but only for Erlang/OpenSSL to avoid issues with other tools using libssl; https://github.com/docker-library/rabbitmq/issues/364)

# Compile, install OpenSSL, verify that the command-line works & development headers are present
make -j "$(getconf _NPROCESSORS_ONLN)"
make install_sw install_ssldirs
ldconfig
# use Debian's CA certificates
rmdir "$OPENSSL_CONFIG_DIR/certs" "$OPENSSL_CONFIG_DIR/private"
ln -sf /etc/ssl/certs /etc/ssl/private "$OPENSSL_CONFIG_DIR"
# cleanup sources
rm -rf "$OPENSSL_PATH"*
# smoke test
openssl version -a
