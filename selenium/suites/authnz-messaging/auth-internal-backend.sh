#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/authnz-msg-protocols
<<<<<<< HEAD
PROFILES="internal-user auth_backends-internal "
=======
PROFILES="internal-user auth_backends-internal"
>>>>>>> 5086e283b (Allow building CLI with elixir 1.18.x)

source $SCRIPT/../../bin/suite_template
run
