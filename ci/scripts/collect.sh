#!/bin/bash

set -euo pipefail

echo "Recording buildevents step finish for ${project} started at ${STEP_START}..."
buildevents step ${GITHUB_RUN_ID} ${GITHUB_RUN_ID}-${project} ${STEP_START} ${project}
echo "done."
