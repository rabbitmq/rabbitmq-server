#!/bin/bash

set -euo pipefail

cd /workspace/rabbitmq

trap 'catch $?' EXIT

catch() {
    buildevents step ${GITHUB_RUN_ID} packaging ${STEP_START} packaging
}

buildevents cmd ${GITHUB_RUN_ID} packaging package-generic-unix -- \
            make package-generic-unix
