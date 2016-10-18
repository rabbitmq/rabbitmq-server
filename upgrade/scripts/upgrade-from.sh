#!/usr/bin/env bash

set -ex

echo "UPGRADE_FROM ${UPGRADE_FROM}"

. "rabbitmq-ci/tasks/helpers.sh"
. "rabbitmq-ci/tasks/upgrade-from-${UPGRADE_FROM_SCRIPT}-helpers.sh"

setup_steps