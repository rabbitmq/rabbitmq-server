#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/sessions
TEST_CONFIG_PATH=/oauth
PROFILES="cluster keycloak keycloak-oauth-provider keycloak-mgt-oauth-provider tls sessions oauth2"

source $SCRIPT/../../bin/suite_template $@
runWith keycloak
