#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/authnz-msg-protocols
PROFILES="spring oauth-producer auth-oauth-spring auth_backends-opaque-oauth auth-mtls"

source $SCRIPT/../../bin/suite_template
runWith spring
