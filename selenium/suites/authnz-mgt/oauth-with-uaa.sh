#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/oauth/with-sp-initiated
TEST_CONFIG_PATH=/oauth
PROFILES="uaa uaa-oauth-provider uaa-mgt-oauth-provider tls"

source $SCRIPT/../../bin/suite_template $@
runWith uaa
