#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/basic-auth
HTTPD_CONFIG_PATH=/httpd-proxy
PROFILES="proxy"

source $SCRIPT/../bin/suite_template
runWith proxy
