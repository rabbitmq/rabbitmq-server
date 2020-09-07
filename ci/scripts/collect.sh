#!/bin/bash

set -euo pipefail

buildevents step ${GITHUB_RUN_ID} ${project} ${STEP_START} ${project}
