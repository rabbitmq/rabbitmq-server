#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Name of the suite used to generate log and screen folders
SUITE=$( basename "${BASH_SOURCE[0]}" .sh)

# Path to the test cases this suite should run. It is relative to the selenium/test folder
TEST_CASES_PATH=/oauth/with-uaa
# Path to the folder where all configuration file reside. It is relative to the selenim/test folder
TEST_CONFIG_PATH=/oauth
# Path to the uaa configuration. It is relative to the TEST_CONFIG_PATH
KEYCLOAK_CONFIG_PATH=/keycloak
# Name of the rabbitmq config file. It is relative to the TEST_CONFIG_PATH
RABBITMQ_CONFIG_FILENAME=rabbitmq-with-keycloak.config

source $SCRIPT/suite_template

_setup () {
  start_keycloak
  start_rabbitmq
}
_save_logs() {
  save_container_logs rabbitmq
  save_container_logs keycloak
}
_teardown() {
  kill_container_if_exist rabbitmq
  kill_container_if_exist keycloak
}
run
