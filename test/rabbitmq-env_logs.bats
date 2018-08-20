#!/usr/bin/env bats

RABBITMQ_NODENAME='rabbit@frazzle'
RABBITMQ_SCRIPTS_DIR="$BATS_TEST_DIRNAME/../scripts"
_rabbitmq_env_load='false'

setup() {
    RABBITMQ_CONF_ENV_FILE="$BATS_TMPDIR/rabbitmq-config.$BATS_TEST_NAME.conf"
    rm -f "$RABBITMQ_CONF_ENV_FILE"
}

@test "default RABBITMQ_LOG_BASE" {
    local -r _want='/var/log/rabbitmq'

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_logs

    echo "expected RABBITMQ_LOG_BASE to be '$_want', but got: \"$RABBITMQ_LOG_BASE\""
    [[ $RABBITMQ_LOG_BASE == "$_want" ]]
}

@test "normalized RABBITMQ_LOG_BASE via rabbitmq-env.conf" {
    local -r _want='/var/log/rabbitmq/logs'

    echo 'LOG_BASE=//var/log////rabbitmq/logs//' > "$RABBITMQ_CONF_ENV_FILE"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_logs

    echo "expected RABBITMQ_LOG_BASE to be '$_want', but got: \"$RABBITMQ_LOG_BASE\""
    [[ $RABBITMQ_LOG_BASE == "$_want" ]]
}

@test "default RABBITMQ_LOGS" {
    local -r _want="/var/log/rabbitmq/$RABBITMQ_NODENAME.log"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_logs

    echo "expected RABBITMQ_LOGS to be '$_want', but got: \"$RABBITMQ_LOGS\""
    [[ $RABBITMQ_LOGS == "$_want" ]]
}

@test "default RABBITMQ_UPGRADE_LOG" {
    local -r _want="/var/log/rabbitmq/${RABBITMQ_NODENAME}_upgrade.log"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_logs

    echo "expected RABBITMQ_UPGRADE_LOG to be '$_want', but got: \"$RABBITMQ_UPGRADE_LOG\""
    [[ $RABBITMQ_UPGRADE_LOG == "$_want" ]]
}

@test "can configure RABBITMQ_LOGS via env" {
    local -r _want="/var/log/rmq/logs.log"
    RABBITMQ_LOGS="$_want"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_logs

    echo "expected RABBITMQ_LOGS to be '$_want', but got: \"$RABBITMQ_LOGS\""
    [[ $RABBITMQ_LOGS == "$_want" ]] && \
        [[ $RABBITMQ_LOGS_source == 'environment' ]]
}

@test "can configure RABBITMQ_LOGS via rabbitmq-env.conf" {
    local -r _want="/var/log/rmq/logs.log"
    echo "LOGS='$_want'" > "$RABBITMQ_CONF_ENV_FILE"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_logs

    echo "expected RABBITMQ_LOGS to be '$_want', but got: \"$RABBITMQ_LOGS\""
    [[ $RABBITMQ_LOGS == "$_want" ]] && \
        [[ $RABBITMQ_LOGS_source == 'environment' ]]
}
