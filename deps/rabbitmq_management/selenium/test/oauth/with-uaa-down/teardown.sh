#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export RABBITMQ_CONFIG=${SCRIPT}/rabbitmq.config

. $SCRIPT/../../../bin/rabbitmq.sh

stop_rabbitmq
