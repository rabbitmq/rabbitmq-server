#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/queuesAndStreams/with-disabled-stats
TEST_CONFIG_PATH=/basic-auth
PROFILES="disable-stats disable-stream-management"

source $SCRIPT/../../bin/suite_template $@
run
