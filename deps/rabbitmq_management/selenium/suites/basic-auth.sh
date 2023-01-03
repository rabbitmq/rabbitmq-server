#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Name of the suite used to generate log and screen folders
SUITE=$( basename "${BASH_SOURCE[0]}" .sh)

# Path to the test cases this suite should run. It is relative to the selenium/test folder
TEST_CASES_PATH=/basic-auth
# Path to the folder where all configuration file reside. It is relative to the selenim/test folder
TEST_CONFIG_PATH=/basic-auth

source $SCRIPT/suite_template

_setup () {
  start_rabbitmq
}
_save_logs() {
  save_container_logs rabbitmq
}
_teardown() {
  kill_container_if_exist rabbitmq
}
run
