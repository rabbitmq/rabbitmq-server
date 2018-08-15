#!/usr/bin/env bats

export RABBITMQ_SCRIPTS_DIR="$BATS_TEST_DIRNAME/../scripts"

@test "default RABBITMQ_CONFIG_ARG" {
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    echo $RABBITMQ_CONFIG_ARG

    echo "expected RABBITMQ_CONFIG_ARG to be '', but got: $RABBITMQ_CONFIG_ARG"
    [[ $RABBITMQ_CONFIG_ARG == '' ]]
}

@test "default RABBITMQ_GENERATED_CONFIG_ARG" {
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    echo $RABBITMQ_CONFIG_ARG

    echo "expected RABBITMQ_GENERATED_CONFIG_ARG to be '', but got: $RABBITMQ_GENERATED_CONFIG_ARG"
    [[ $RABBITMQ_GENERATED_CONFIG_ARG == '' ]]
}
