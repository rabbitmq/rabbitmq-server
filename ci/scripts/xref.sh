#!/bin/bash

set -euo pipefail

cd /workspace/rabbitmq/deps/$project

buildevents cmd ${GITHUB_RUN_ID} ${GITHUB_RUN_ID}-xref ${project} -- \
        make xref
