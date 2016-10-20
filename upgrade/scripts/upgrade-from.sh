#!/usr/bin/env bash

set -ex

echo "UPGRADE_FROM ${UPGRADE_FROM}"

. "$(dirname "$0")/upgrade-helpers.sh"
. "$(dirname "$0")/upgrade-from-${UPGRADE_FROM_SCRIPT}-helpers.sh"

setup_steps