#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/channels/with-basic-info
TEST_CONFIG_PATH=/basic-auth
PROFILES="disable-stats"

source $SCRIPT/../../bin/suite_template $@
run
