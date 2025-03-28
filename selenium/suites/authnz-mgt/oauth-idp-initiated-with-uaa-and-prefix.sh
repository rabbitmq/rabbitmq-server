#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/oauth/with-idp-initiated
TEST_CONFIG_PATH=/oauth
PROFILES="uaa fakeportal-mgt-oauth-provider idp-initiated mgt-prefix uaa-oauth-provider tls"

source $SCRIPT/../../bin/suite_template $@
runWith uaa fakeportal
