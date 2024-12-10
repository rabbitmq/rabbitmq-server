#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/authnz-msg-protocols
<<<<<<< HEAD
PROFILES="internal-user auth_backends-internal "
=======
PROFILES="internal-user auth_backends-internal"
>>>>>>> 8d7535e0b (amqqueue_process: adopt new `is_duplicate` backing queue callback)

source $SCRIPT/../../bin/suite_template
run
