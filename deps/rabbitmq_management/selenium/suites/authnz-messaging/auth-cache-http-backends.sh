#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/authnz-msg-protocols
PROFILES="http-user auth-http auth_backends-cache-http "

source $SCRIPT/../../bin/suite_template
runWith mock-auth-backend-http
