#!/bin/bash

set -euo pipefail

buildevents build ${GITHUB_RUN_ID} ${BUILD_START} ${BUILD_RESULT}
