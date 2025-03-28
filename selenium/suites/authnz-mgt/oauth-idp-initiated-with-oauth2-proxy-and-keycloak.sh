#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/oauth/with-idp-initiated
TEST_CONFIG_PATH=/oauth
PROFILES="oauth2-proxy keycloak keycloak-oauth-provider oauth2-proxy-mgt-oauth-provider tls"

source $SCRIPT/../../bin/suite_template $@
runWith keycloak oauth2-proxy
