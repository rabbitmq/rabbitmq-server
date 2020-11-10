#!/bin/bash

set -euo pipefail

cd /workspace/rabbitmq/deps/$project

! test -d ebin || touch ebin/*

trap 'catch $?' EXIT

catch() {
    if [ "$1" != "0" ]; then
        make ct-logs-archive && mv *-ct-logs-*.tar.xz /workspace/ct-logs/
    fi
}

CMD=ct-${CT_SUITE}
SECONDARY_UMBRELLA_ARGS=""
if [[ "${SECONDARY_UMBRELLA_VERSION:-}" != "" ]]; then
    CMD=ct-${CT_SUITE}-mixed-${SECONDARY_UMBRELLA_VERSION}
    SECONDARY_UMBRELLA_ARGS="SECONDARY_UMBRELLA=/workspace/rabbitmq-${SECONDARY_UMBRELLA_VERSION} RABBITMQ_FEATURE_FLAGS="
fi

buildevents cmd ${GITHUB_RUN_ID} ${GITHUB_RUN_ID}-${project} ${CMD} -- \
            make ct-${CT_SUITE} \
                 FULL= \
                 FAIL_FAST=1 \
                 SKIP_AS_ERROR=1 ${SECONDARY_UMBRELLA_ARGS}
