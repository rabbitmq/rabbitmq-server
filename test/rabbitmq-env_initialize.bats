#!/usr/bin/env bats

RABBITMQ_SCRIPTS_DIR="$BATS_TEST_DIRNAME/../scripts"
_rabbitmq_env_load='false'

@test "default RABBITMQ_HOME, ESCRIPT_DIR, RABBITMQ_CONF_ENV_FILE" {
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    local -r _want_rabbitmq_home="$(readlink -f "$BATS_TEST_DIRNAME/../")"
    local -r _want_escript_dir="$(readlink -f "$BATS_TEST_DIRNAME/../escript")"
    local -r _want_rabbitmq_conf_env_file='/etc/rabbitmq/rabbitmq-env.conf'

    echo "expected RABBITMQ_HOME to be '$_want_rabbitmq_home', but got: \"$RABBITMQ_HOME\""
    [[ $RABBITMQ_HOME == "$_want_rabbitmq_home" ]]

    echo "expected ESCRIPT_DIR to be '$_want_escript_dir', but got: \"$ESCRIPT_DIR\""
    [[ $ESCRIPT_DIR == "$_want_escript_dir" ]]

    echo "expected RABBITMQ_CONF_ENV_FILE to be '$_want_rabbitmq_conf_env_file', but got: \"$RABBITMQ_CONF_ENV_FILE\""
    [[ $RABBITMQ_CONF_ENV_FILE == "$_want_rabbitmq_conf_env_file" ]]
}

@test "RABBITMQ_PID_FILE is saved if set" {
    local -r _want='/var/run/rabbitmq.pid'
    RABBITMQ_PID_FILE="$_want"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"

    echo "expected _rmq_env_saved_RABBITMQ_PID_FILE to be '$_want', but got: \"$_rmq_env_saved_RABBITMQ_PID_FILE \""
    [[ $_rmq_env_saved_RABBITMQ_PID_FILE == "$_want" ]]
}
