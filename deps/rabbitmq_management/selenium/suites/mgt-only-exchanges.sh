#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/exchanges
TEST_CONFIG_PATH=/mgt-only

source $SCRIPT/../bin/suite_template $@
run
