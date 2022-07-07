#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export RABBITMQ_CONFIG=${SCRIPT}/rabbitmq.config

. $SCRIPT/../../../bin/rabbitmq.sh

stop_uaa() {
  docker kill local-uaa
  docker rm local-uaa
}


stop_rabbitmq
stop_uaa
