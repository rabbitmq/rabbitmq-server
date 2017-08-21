#!/usr/bin/env bash

do_not_requeue="ackmode=ack_requeue_false"
export do_not_requeue
# shellcheck source=/dev/null
. "$(dirname "$0")/upgrade-to-helpers.sh"
