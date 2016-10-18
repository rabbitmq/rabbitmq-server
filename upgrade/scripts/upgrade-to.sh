#!/usr/bin/env bash

set -ex

. "rabbitmq-ci/tasks/helpers.sh"
. "rabbitmq-ci/tasks/upgrade-to-${UPGRADE_TO_SCRIPT}-helpers.sh"

verify_steps

echo "Verified steps"