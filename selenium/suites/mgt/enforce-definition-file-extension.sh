#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/definitions
TEST_CONFIG_PATH=/basic-auth
PROFILES="enforce-definition-file-extension"
TEST_CASES_FILTER="enforce-definition-file-extension"

source $SCRIPT/../../bin/suite_template $@
run
