#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/authnz-msg-protocols
<<<<<<< HEAD
PROFILES="internal-user auth_backends-internal "
=======
PROFILES="internal-user auth_backends-internal"
>>>>>>> f3540ee7d2 (web_mqtt_shared_SUITE: propagate flow_classic_queue to mqtt_shared_SUITE #12907 12906)

source $SCRIPT/../../bin/suite_template
run
