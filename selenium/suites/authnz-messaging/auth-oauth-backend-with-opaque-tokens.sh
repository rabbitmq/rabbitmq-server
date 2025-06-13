#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/authnz-msg-protocols
PROFILES="spring oauth-prodproducer auth-oauth-spring auth_backends-oauth-opaque "

source $SCRIPT/../../bin/suite_template
runWith spring
