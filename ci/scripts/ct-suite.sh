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

buildevents cmd ${GITHUB_RUN_ID} ${GITHUB_RUN_ID}-${project} ct-${CT_SUITE} -- \
            make ct-${CT_SUITE} \
                 FULL= \
                 FAIL_FAST=1 \
                 SKIP_AS_ERROR=1
