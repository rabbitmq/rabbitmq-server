#!/usr/bin/env bats

RABBITMQ_SCRIPTS_DIR="$BATS_TEST_DIRNAME/../scripts"
_rabbitmq_env_load='false'

setup() {
    RABBITMQ_CONF_ENV_FILE="$BATS_TMPDIR/rabbitmq-config.$BATS_TEST_NAME.conf"
    rm -f "$RABBITMQ_CONF_ENV_FILE"
}

@test "default RABBITMQ_CONFIG_ARG" {
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    rabbitmq_config

    echo "expected RABBITMQ_CONFIG_ARG to be '', but got: $RABBITMQ_CONFIG_ARG"
    [[ $RABBITMQ_CONFIG_ARG == '' ]]
}

@test "default RABBITMQ_GENERATED_CONFIG_ARG" {
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    rabbitmq_config

    echo "expected RABBITMQ_GENERATED_CONFIG_ARG to be '', but got: $RABBITMQ_GENERATED_CONFIG_ARG"
    [[ $RABBITMQ_GENERATED_CONFIG_ARG == '' ]]
}
