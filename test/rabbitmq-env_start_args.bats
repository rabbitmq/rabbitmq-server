#!/usr/bin/env bats

RABBITMQ_SCRIPTS_DIR="$BATS_TEST_DIRNAME/../scripts"
_rabbitmq_env_load='false'

setup() {
    RABBITMQ_CONF_ENV_FILE="$BATS_TMPDIR/rabbitmq-config.$BATS_TEST_NAME.conf"
    rm -f "$RABBITMQ_CONF_ENV_FILE"
}

@test "default _rmq_env_start_rabbit" {
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_start_args

    echo "expected _rmq_env_start_rabbit to be '', but got: \"$_rmq_env_start_rabbit\""
    [[ $_rmq_env_start_rabbit == '-noinput -s rabbit boot' ]]
}

@test "can configure RabbitMQ to allow input via env" {
    RABBITMQ_ALLOW_INPUT=true
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_start_args

    echo "expected _rmq_env_start_rabbit to be '-s rabbit boot', but got: \"$_rmq_env_start_rabbit\""
    [[ $_rmq_env_start_rabbit == '-s rabbit boot' ]]
}

@test "can configure RabbitMQ to be node only via env" {
    RABBITMQ_NODE_ONLY=true
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_start_args

    echo "expected _rmq_env_start_rabbit to be '-noinput -s TODO boot', but got: \"$_rmq_env_start_rabbit\""
    [[ $_rmq_env_start_rabbit == '-noinput' ]]
}

@test "can configure RabbitMQ to allow input and be node only via env" {
    RABBITMQ_ALLOW_INPUT=true
    RABBITMQ_NODE_ONLY=true
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_start_args

    echo "expected _rmq_env_start_rabbit to be '', but got: \"$_rmq_env_start_rabbit\""
    [[ $_rmq_env_start_rabbit == '' ]]
}
