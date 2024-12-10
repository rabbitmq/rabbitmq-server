#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/oauth/with-sp-initiated
TEST_CONFIG_PATH=/oauth
<<<<<<< HEAD
PROFILES="uaa uaa-oauth-provider uaa-mgt-oauth-provider"
=======
PROFILES="uaa uaa-oauth-provider uaa-mgt-oauth-provider tls"
>>>>>>> f3540ee7d2 (web_mqtt_shared_SUITE: propagate flow_classic_queue to mqtt_shared_SUITE #12907 12906)

source $SCRIPT/../../bin/suite_template $@
runWith uaa
