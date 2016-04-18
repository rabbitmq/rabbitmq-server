#!/bin/sh

set -ex

(
  cd amqp_client
  make DEPS_DIR="$PWD/.." tests
)
