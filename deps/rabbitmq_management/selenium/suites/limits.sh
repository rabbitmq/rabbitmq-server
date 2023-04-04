#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/limits
TEST_CONFIG_PATH=/basic-auth

source $SCRIPT/../bin/suite_template $@
run
