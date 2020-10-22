#!/bin/bash

set -euo pipefail

cd /workspace/rabbitmq/deps/$project

buildevents cmd ${GITHUB_RUN_ID} ${GITHUB_RUN_ID}-dialyze ${project} -- \
        make dialyze
