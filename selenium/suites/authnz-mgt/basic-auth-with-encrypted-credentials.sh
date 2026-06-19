#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/basic-auth
PROFILES="one-minute-session-timeout encrypted-credentials"

source $SCRIPT/../../bin/suite_template $@
run
