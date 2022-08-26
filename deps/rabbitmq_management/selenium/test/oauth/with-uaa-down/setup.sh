#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export RABBITMQ_CONFIG=${SCRIPT}/../with-uaa/${RABBITMQ_CONFIG_FILE:-rabbitmq.config}

. $SCRIPT/../../../bin/rabbitmq.sh

start_rabbitmq
