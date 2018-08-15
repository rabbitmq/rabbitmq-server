#!/usr/bin/env bats

export RABBITMQ_SCRIPTS_DIR="$BATS_TEST_DIRNAME/../scripts"

setup() {
    export RABBITMQ_CONF_ENV_FILE="$BATS_TMPDIR/rabbitmq-config.$BATS_TEST_NAME.conf"
}

@test "default _rabbitmq_start_rabbit" {
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    config_start_args

    echo "expected _rabbitmq_start_rabbit to be '', but got: \"$_rabbitmq_start_rabbit\""
    [[ $_rabbitmq_start_rabbit == '-noinput -s rabbit boot' ]]
}

@test "can configure RabbitMQ to allow input via env" {
    RABBITMQ_ALLOW_INPUT=true
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    config_start_args

    echo "expected _rabbitmq_start_rabbit to be '-s rabbit boot', but got: \"$_rabbitmq_start_rabbit\""
    [[ $_rabbitmq_start_rabbit == '-s rabbit boot' ]]
}

@test "can configure RabbitMQ to be node only via env" {
    RABBITMQ_NODE_ONLY=true
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    config_start_args

    echo "expected _rabbitmq_start_rabbit to be '-noinput -s TODO boot', but got: \"$_rabbitmq_start_rabbit\""
    [[ $_rabbitmq_start_rabbit == '-noinput' ]]
}

@test "can configure RabbitMQ to allow input and be node only via env" {
    RABBITMQ_ALLOW_INPUT=true
    RABBITMQ_NODE_ONLY=true
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    config_start_args

    echo "expected _rabbitmq_start_rabbit to be '', but got: \"$_rabbitmq_start_rabbit\""
    [[ $_rabbitmq_start_rabbit == '' ]]
}

@test "default Erlang allocator arguments" {
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    config_alloc_args

    echo "expected RABBITMQ_ALLOC_ARGS to be '+MBas ageffcbf +MHas ageffcbf +MBlmbcs 512 +MHlmbcs 512 +MMmcs 30', but got: \"$RABBITMQ_ALLOC_ARGS\""
    [[ $RABBITMQ_ALLOC_ARGS == '+MBas ageffcbf +MHas ageffcbf +MBlmbcs 512 +MHlmbcs 512 +MMmcs 30' ]]
}

@test "can configure Erlang allocator arguments via env" {
    RABBITMQ_ALLOC_ARGS='foo bar baz'
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    config_alloc_args

    echo "expected RABBITMQ_ALLOC_ARGS to be 'foo bar baz', but got: \"$RABBITMQ_ALLOC_ARGS\""
    [[ $RABBITMQ_ALLOC_ARGS == 'foo bar baz' ]]
}

@test "can configure Erlang allocator arguments via rabbitmq-env.conf file" {
    echo "ALLOC_ARGS='foo bar baz'" > "$RABBITMQ_CONF_ENV_FILE"
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    config_alloc_args

    echo "expected RABBITMQ_ALLOC_ARGS to be 'foo bar baz', but got: \"$RABBITMQ_ALLOC_ARGS\""
    [[ $RABBITMQ_ALLOC_ARGS == 'foo bar baz' ]]
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
