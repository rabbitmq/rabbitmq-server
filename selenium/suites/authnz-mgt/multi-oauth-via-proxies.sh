#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/multi-oauth/without-basic-auth
TEST_CONFIG_PATH=/multi-oauth
PROFILES="devkeycloak prodkeycloak with-resource-label with-resource-scopes tls devkeycloak-proxy prodkeycloak-proxy"

source $SCRIPT/../../bin/suite_template $@
runWith devkeycloak prodkeycloak devkeycloak-proxy prodkeycloak-proxy
