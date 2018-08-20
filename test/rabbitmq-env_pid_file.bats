#!/usr/bin/env bats

RABBITMQ_MNESIA_DIR='/var/lib/rabbitmq/mnesia/rabbit@frazzle'
RABBITMQ_SCRIPTS_DIR="$BATS_TEST_DIRNAME/../scripts"
_rabbitmq_env_load='false'

setup() {
    RABBITMQ_CONF_ENV_FILE="$BATS_TMPDIR/rabbitmq-config.$BATS_TEST_NAME.conf"
    rm -f "$RABBITMQ_CONF_ENV_FILE"
}

@test "default RABBITMQ_PID_FILE" {
    local -r _want="$RABBITMQ_MNESIA_DIR.pid"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_mnesia_dirs
    _rmq_env_config_pid_file

    echo "expected RABBITMQ_PID_FILE to be '$_want', but got: \"$RABBITMQ_PID_FILE\""
    [[ $RABBITMQ_PID_FILE == "$_want" ]]
}

@test "can configure RABBITMQ_PID_FILE via env" {
    local -r _want="/var/run/rabbitmq.pid"
    RABBITMQ_PID_FILE="$_want"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_mnesia_dirs
    _rmq_env_config_pid_file

    echo "expected RABBITMQ_PID_FILE to be '$_want', but got: \"$RABBITMQ_PID_FILE\""
    [[ $RABBITMQ_PID_FILE == "$_want" ]]
}

@test "can configure RABBITMQ_PID_FILE via rabbitmq-env.conf" {
    local -r _want="/var/run/rabbitmq.pid"
    echo "PID_FILE='$_want'" > "$RABBITMQ_CONF_ENV_FILE"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_mnesia_dirs
    _rmq_env_config_pid_file

    echo "expected RABBITMQ_PID_FILE to be '$_want', but got: \"$RABBITMQ_PID_FILE\""
    [[ $RABBITMQ_PID_FILE == "$_want" ]]
}

@test "RABBITMQ_PID_FILE via env overrides rabbitmq-env.conf" {
    local -r _want="/var/run/rabbitmq.pid"
    RABBITMQ_PID_FILE="$_want"
    echo "PID_FILE='/var/run/do-not-want-this.pid'" > "$RABBITMQ_CONF_ENV_FILE"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_mnesia_dirs
    _rmq_env_config_pid_file

    echo "expected RABBITMQ_PID_FILE to be '$_want', but got: \"$RABBITMQ_PID_FILE\""
    [[ $RABBITMQ_PID_FILE == "$_want" ]]
}
