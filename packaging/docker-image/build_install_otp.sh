#!/usr/bin/env bash

set -euxo pipefail

OTP_PATH="$(cd /usr/local/src/otp-OTP-* && pwd)"

# Configure Erlang/OTP for compilation, disable unused features & applications
# https://erlang.org/doc/applications.html
# ERL_TOP is required for Erlang/OTP makefiles to find the absolute path for the installation
cd "$OTP_PATH"
export ERL_TOP="$OTP_PATH"
./otp_build autoconf
CFLAGS="$(dpkg-buildflags --get CFLAGS)"; export CFLAGS
# add -rpath to avoid conflicts between our OpenSSL's "libssl.so" and the libssl package by making sure /usr/local/lib is searched first (but only for Erlang/OpenSSL to avoid issues with other tools using libssl; https://github.com/docker-library/rabbitmq/issues/364)
export CFLAGS="$CFLAGS -Wl,-rpath=/usr/local/lib/$(dpkg-architecture --query DEB_HOST_MULTIARCH)"
hostArch="$(dpkg-architecture --query DEB_HOST_GNU_TYPE)"
buildArch="$(dpkg-architecture --query DEB_BUILD_GNU_TYPE)"
dpkgArch="$(dpkg --print-architecture)"; dpkgArch="${dpkgArch##*-}"
./configure \
    --host="$hostArch" \
    --build="$buildArch" \
    --disable-dynamic-ssl-lib \
    --disable-hipe \
    --disable-sctp \
    --disable-silent-rules \
    --enable-jit \
    --enable-clock-gettime \
    --enable-hybrid-heap \
    --enable-kernel-poll \
    --enable-shared-zlib \
    --enable-smp-support \
    --enable-threads \
    --with-microstate-accounting=extra \
    --without-common_test \
    --without-debugger \
    --without-dialyzer \
    --without-diameter \
    --without-edoc \
    --without-erl_docgen \
    --without-et \
    --without-eunit \
    --without-ftp \
    --without-hipe \
    --without-jinterface \
    --without-megaco \
    --without-observer \
    --without-odbc \
    --without-reltool \
    --without-ssh \
    --without-tftp \
    --without-wx

# Compile & install Erlang/OTP
make -j "$(getconf _NPROCESSORS_ONLN)" GEN_OPT_FLGS="-O2 -fno-strict-aliasing"
make install
cd ..
rm -rf \
    "$OTP_PATH"* \
    /usr/local/lib/erlang/lib/*/examples \
    /usr/local/lib/erlang/lib/*/src

# Check that Erlang/OTP crypto & ssl were compiled against OpenSSL correctly
erl -noshell -eval 'io:format("~p~n~n~p~n~n", [crypto:supports(), ssl:versions()]), init:stop().'
