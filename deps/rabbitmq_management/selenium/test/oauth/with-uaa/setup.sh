#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export RABBITMQ_CONFIG=${SCRIPT}/rabbitmq-headless.config
export ENABLED_PLUGINS=${SCRIPT}/enabled_plugins
export UAA_CONFIG=${SCRIPT}/uaa-headless

${SCRIPT}/../../../bin/start-rabbitmq.sh
${SCRIPT}/start-uaa.sh
