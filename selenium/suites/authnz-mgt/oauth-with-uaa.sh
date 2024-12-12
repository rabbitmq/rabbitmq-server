#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

TEST_CASES_PATH=/oauth/with-sp-initiated
TEST_CONFIG_PATH=/oauth
<<<<<<< HEAD
PROFILES="uaa uaa-oauth-provider uaa-mgt-oauth-provider"
=======
PROFILES="uaa uaa-oauth-provider uaa-mgt-oauth-provider tls"
>>>>>>> 5086e283b (Allow building CLI with elixir 1.18.x)

source $SCRIPT/../../bin/suite_template $@
runWith uaa
