#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

<<<<<<< HEAD
TEST_CASES_PATH=/auth-http-backend
PROFILES="auth-http auth_backends-cache-http "
=======
TEST_CASES_PATH=/authnz-msg-protocols
PROFILES="http-user auth-http auth_backends-cache-http "
>>>>>>> c3ee4e5a6d (Fix #9043)

source $SCRIPT/../bin/suite_template
runWith mock-auth-backend-http
