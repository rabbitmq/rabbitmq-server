#!/bin/bash

set -euo pipefail

refs="$@"

for version in ${refs}; do
  umbrella="umbrellas/$version"
  if ! test -d "$umbrella" ||
     ! make -C "$umbrella/deps/rabbit" test-dist; then
    rm -rf "$umbrella"
    git config --global advice.detachedHead false
    git clone \
      https://github.com/rabbitmq/rabbitmq-public-umbrella.git \
      "$umbrella"
    # `make co` in the public umbrella will use files from rabbitmq-server
    # to know what to fetch, and these are now different post monorepo. So,
    # we must clone rabbitmq-server manually and check out $version before
    # we run `make co`
    mkdir -p "$umbrella"/deps
    git clone \
      https://github.com/rabbitmq/rabbitmq-server.git \
      "$umbrella"/deps/rabbit
    git -C "$umbrella"/deps/rabbit checkout "$version"
    make -C "$umbrella" co
    make -C "$umbrella" up BRANCH="$version"
    # To remove third-party deps which were checked out when the
    # projects were on the `master` branch. Thus, possibly not the
    # version pinning we expect. We update the Umbrella one last time
    # to fetch the correct third-party deps.
    make -C "$umbrella" clean-3rd-party-repos
    make -C "$umbrella" up
    make -C "$umbrella/deps/rabbit" test-dist
    rm -rf "$umbrella"/deps/rabbitmq_website
    rm -rf "$umbrella"/deps/rabbitmq_prometheus/docker
    rm -rf "$umbrella"/deps/*/{.git,test} "$umbrella"/.git
  fi
done

for version in ${refs}; do
  umbrella="umbrellas/$version"
  mv ${umbrella} rabbitmq-${version}
done

rm -fr umbrellas
