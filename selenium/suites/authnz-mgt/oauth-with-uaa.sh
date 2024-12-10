#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/oauth/with-sp-initiated
TEST_CONFIG_PATH=/oauth
<<<<<<< HEAD
PROFILES="uaa uaa-oauth-provider uaa-mgt-oauth-provider"
=======
PROFILES="uaa uaa-oauth-provider uaa-mgt-oauth-provider tls"
>>>>>>> 8d7535e0b (amqqueue_process: adopt new `is_duplicate` backing queue callback)

source $SCRIPT/../../bin/suite_template $@
runWith uaa
