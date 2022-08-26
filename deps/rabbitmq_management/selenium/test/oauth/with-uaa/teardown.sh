#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export RABBITMQ_CONFIG=${SCRIPT}/rabbitmq.config

. $SCRIPT/../../../bin/rabbitmq.sh

stop_uaa() {
  docker kill uaa
  docker rm uaa
}

stop_rabbitmq
stop_uaa
