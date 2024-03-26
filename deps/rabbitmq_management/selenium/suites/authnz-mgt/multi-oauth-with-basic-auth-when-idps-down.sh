#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/multi-oauth/with-basic-auth-idps-down
TEST_CONFIG_PATH=/multi-oauth
PROFILES="devkeycloak prodkeycloak enable-basic-auth with-resource-label with-resource-scopes tls"

source $SCRIPT/../../bin/suite_template $@
run
