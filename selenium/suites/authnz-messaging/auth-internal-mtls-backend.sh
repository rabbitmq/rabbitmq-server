#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/authnz-msg-protocols
PROFILES="internal-user auth_backends-internal tls auth-mtls"

source $SCRIPT/../../bin/suite_template
run
