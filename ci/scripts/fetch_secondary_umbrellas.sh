#!/bin/bash

set -euo pipefail

refs="$@"

for version in ${refs}; do
  umbrella="umbrellas/$version"
  if ! test -d "$umbrella"  ||
     ! make -C "$umbrella/deps/rabbit" test-dist; then
    rm -rf "$umbrella"

    # Fetch the master Umbrella; the final umbrellas are created from
    # the master copy.
    if ! test -d umbrellas/master; then
      git config --global advice.detachedHead false
      git clone \
        https://github.com/rabbitmq/rabbitmq-public-umbrella.git \
        umbrellas/master
      make -C umbrellas/master co # To get RabbitMQ components.
    fi

    # We copy the master Umbrella and checkout the appropriate tag.
    cp -a umbrellas/master "$umbrella"
    git -C "$umbrella" checkout "master"
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
