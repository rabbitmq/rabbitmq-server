#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/oauth/with-idp-initiated
TEST_CONFIG_PATH=/oauth
PROFILES="spring idp-initiated spring-oauth-provider fakeportal-mgt-oauth-provider tls opaque-token"

source $SCRIPT/../../bin/suite_template $@
runWith spring fakeportal
