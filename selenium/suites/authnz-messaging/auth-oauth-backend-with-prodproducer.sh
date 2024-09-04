#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/authnz-msg-protocols
PROFILES="devkeycloak prodkeycloak oauth-prodproducer auth-oauth-prod auth_backends-oauth"

source $SCRIPT/../../bin/suite_template
runWith prodkeycloak
