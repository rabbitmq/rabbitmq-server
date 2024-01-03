#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/oauth/with-basic-auth
TEST_CONFIG_PATH=/oauth
PROFILES="keycloak jwks keycloak-oauth-provider enable-basic-auth"

source $SCRIPT/../../bin/suite_template $@
runWith keycloak
