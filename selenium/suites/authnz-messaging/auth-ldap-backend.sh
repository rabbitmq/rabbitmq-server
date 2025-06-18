#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/authnz-msg-protocols
PROFILES="ldap-user auth-ldap auth_backends-ldap "

source $SCRIPT/../../bin/suite_template
runWith mock-auth-backend-ldap
