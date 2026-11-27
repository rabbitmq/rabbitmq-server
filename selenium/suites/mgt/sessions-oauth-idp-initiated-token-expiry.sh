#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/sessions-idp-initiated-token-expiry
TEST_CONFIG_PATH=/oauth
PROFILES="uaa fakeportal idp-initiated uaa-oauth-provider fakeportal-mgt-oauth-provider sessions internal-backend load-user-definitions oauth2"

source $SCRIPT/../../bin/suite_template $@
runWith uaa fakeportal
