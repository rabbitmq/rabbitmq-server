#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/authnz-msg-protocols
PROFILES="internal-user auth-http auth_backends-http auth-mtls"
# internal-user profile is used because the client certificates to 
# access rabbitmq are issued with the alt_name = internal-user 

source $SCRIPT/../../bin/suite_template
runWith mock-auth-backend-http
