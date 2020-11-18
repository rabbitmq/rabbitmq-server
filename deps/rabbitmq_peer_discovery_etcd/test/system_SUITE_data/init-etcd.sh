#!/bin/sh
# vim:sw=4:et:

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
#

set -ex

case "$(uname -s)" in
    Linux)
        package_os="linux"
        tmpdir=/tmp
        archive="tar.gz"
        ;;
    Darwin)
        package_os="darwin"
        tmpdir=$TMPDIR
        archive="zip"
        ;;
    *)
        exit 1
        ;;
esac

etcd_data_dir=${1:-"$tmpdir/etcd/data"}
pidfile="$etcd_data_dir/etcd.pid"
tcp_port=${2:-2379}

ETCD_VER=v3.4.6

GITHUB_URL=https://github.com/etcd-io/etcd/releases/download
DOWNLOAD_URL=${GITHUB_URL}

rm -rf "${tmpdir}/etcd-${ETCD_VER}"

if ! [ -f "${tmpdir}/etcd-${ETCD_VER}-$package_os-amd64.$archive" ]; then
  curl -L "${DOWNLOAD_URL}/${ETCD_VER}/etcd-${ETCD_VER}-$package_os-amd64.$archive" -o "${tmpdir}/etcd-${ETCD_VER}-$package_os-amd64.$archive"
fi

case "$(uname -s)" in
    Linux)
      mkdir -p "${tmpdir}/etcd-${ETCD_VER}-$package_os-amd64/"
      tar xzvf "/tmp/etcd-${ETCD_VER}-$package_os-amd64.$archive" -C "${tmpdir}/etcd-${ETCD_VER}-$package_os-amd64/" --strip-components=1
      ;;
    Darwin)
      unzip -q -o -d "$tmpdir" "${tmpdir}/etcd-${ETCD_VER}-$package_os-amd64.$archive"
      ;;
    *)
      exit 1
      ;;
esac

mv "${tmpdir}/etcd-${ETCD_VER}-$package_os-amd64/" "${tmpdir}/etcd-${ETCD_VER}/"

rm -rf "$etcd_data_dir"
mkdir -p "$etcd_data_dir"

# daemonize(1) is installed under this path on Debian
PATH=$PATH:/usr/sbin

daemonize -p "$pidfile" -l "${etcd_data_dir}/daemonize_lock" -- "$tmpdir/etcd-${ETCD_VER}/etcd" \
            --data-dir "$etcd_data_dir" --name peer-discovery-0 --initial-advertise-peer-urls http://127.0.0.1:2380 \
            --listen-peer-urls http://127.0.0.1:2380 \
            --listen-client-urls "http://127.0.0.1:${tcp_port}" \
            --advertise-client-urls "http://127.0.0.1:${tcp_port}" \
            --initial-cluster-token rabbitmq-peer-discovery-etcd \
            --initial-cluster peer-discovery-0=http://127.0.0.1:2380 \
            --initial-cluster-state new


for seconds in {1..30}; do
  "$tmpdir/etcd-${ETCD_VER}/etcdctl" put rabbitmq-ct rabbitmq-ct --dial-timeout=1s && break
  sleep 1
done

echo ETCD_PID=$(cat "$pidfile")

