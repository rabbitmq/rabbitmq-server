#!/usr/bin/env bats

export RABBITMQ_SCRIPTS_DIR="$BATS_TEST_DIRNAME/../scripts"
export _rabbitmq_env_load='false'

setup() {
    export RABBITMQ_CONF_ENV_FILE="$BATS_TMPDIR/rabbitmq-config.$BATS_TEST_NAME.conf"
    if [[ -f $RABBITMQ_CONF_ENV_FILE ]]
    then
    	rm -f "$RABBITMQ_CONF_ENV_FILE"
    fi
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
