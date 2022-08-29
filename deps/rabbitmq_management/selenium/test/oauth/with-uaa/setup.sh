#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export RABBITMQ_CONFIG=${SCRIPT}/${RABBITMQ_CONFIG_FILE:-rabbitmq.config}
export UAA_CONFIG=${SCRIPT}/${UAA_CONFIG_FOLDER:-uaa}

. $SCRIPT/../../../bin/rabbitmq.sh
start_rabbitmq

. $SCRIPT/uaa.sh
