#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export RABBITMQ_CONFIG=${SCRIPT}/../with-uaa/rabbitmq-headless.config
export ENABLED_PLUGINS=${SCRIPT}/../with-uaa/enabled_plugins

${SCRIPT}/../../../bin/start-rabbitmq.sh
