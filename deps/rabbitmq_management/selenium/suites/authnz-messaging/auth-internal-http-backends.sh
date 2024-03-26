#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/authnz-msg-protocols
PROFILES="internal-user auth_http auth_backends-internal-http "

source $SCRIPT/../../bin/suite_template
runWith mock-auth-backend-http
