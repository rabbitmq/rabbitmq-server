#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/authnz-msg-protocols
PROFILES="devkeycloak prodkeycloak oauth-devproducer auth-oauth-dev auth_backends-oauth"

source $SCRIPT/../../bin/suite_template
runWith devkeycloak
