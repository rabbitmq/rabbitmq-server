#!/usr/bin/env bash

set -ex

. "$(dirname "$0")/upgrade-helpers.sh"
. "$(dirname "$0")/upgrade-to-${UPGRADE_TO_SCRIPT}-helpers.sh"

verify_steps
