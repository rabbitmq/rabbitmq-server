#!/usr/bin/env bash

do_not_requeue="requeue=false"
export do_not_requeue
# shellcheck source=/dev/null
. "$(dirname "$0")/upgrade-to-helpers.sh"
