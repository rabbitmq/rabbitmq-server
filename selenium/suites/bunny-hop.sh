#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/bunny-hop
TEST_CONFIG_PATH=/basic-auth
PROFILES="${BUNNY_HOP_PROFILES:-}"

source "$SCRIPT/../bin/suite_template" "$@"
run
