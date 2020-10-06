#!/bin/bash

set -euo pipefail

cd /workspace/rabbitmq/deps/$project

trap 'catch $?' EXIT

catch() {
    if [ "$1" != "0" ]; then
        make ct-logs-archive && mv *-ct-logs-*.tar.xz /workspace/ct-logs/
    fi

    buildevents step ${GITHUB_RUN_ID} ${project} ${STEP_START} ${project}
}

buildevents cmd ${GITHUB_RUN_ID} ${project} test-build -- \
            make test-build

buildevents cmd ${GITHUB_RUN_ID} ${project} tests -- \
            make tests \
                 FULL= \
                 FAIL_FAST=1 \
                 SKIP_AS_ERROR=1
