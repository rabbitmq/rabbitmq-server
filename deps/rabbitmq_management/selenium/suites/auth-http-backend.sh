#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/auth-http-backend
PROFILES="auth-http auth_backends-http"

source $SCRIPT/../bin/suite_template
runWith mock-auth-backend-http
