#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/feature-flags
TEST_CONFIG_PATH=/basic-auth
PROFILES="ff-disabled"

source $SCRIPT/../../bin/suite_template $@
run
