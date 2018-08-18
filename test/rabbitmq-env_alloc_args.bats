#!/usr/bin/env bats

export RABBITMQ_SCRIPTS_DIR="$BATS_TEST_DIRNAME/../scripts"
export _rabbitmq_env_load='false'

setup() {
    export RABBITMQ_CONF_ENV_FILE="$BATS_TMPDIR/rabbitmq-config.$BATS_TEST_NAME.conf"
    rm -f "$RABBITMQ_CONF_ENV_FILE"
}

@test "default Erlang allocator arguments" {
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_alloc_args

    echo "expected RABBITMQ_ALLOC_ARGS to be '+MBas ageffcbf +MHas ageffcbf +MBlmbcs 512 +MHlmbcs 512 +MMmcs 30', but got: \"$RABBITMQ_ALLOC_ARGS\""
    [[ $RABBITMQ_ALLOC_ARGS == '+MBas ageffcbf +MHas ageffcbf +MBlmbcs 512 +MHlmbcs 512 +MMmcs 30' ]]
}

@test "can configure Erlang allocator arguments via env" {
    RABBITMQ_ALLOC_ARGS='foo bar baz'
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_alloc_args

    echo "expected RABBITMQ_ALLOC_ARGS to be 'foo bar baz', but got: \"$RABBITMQ_ALLOC_ARGS\""
    [[ $RABBITMQ_ALLOC_ARGS == 'foo bar baz' ]]
}

@test "can configure Erlang allocator arguments via rabbitmq-env.conf file" {
    echo "ALLOC_ARGS='foo bar baz'" > "$RABBITMQ_CONF_ENV_FILE"
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_alloc_args

    echo "expected RABBITMQ_ALLOC_ARGS to be 'foo bar baz', but got: \"$RABBITMQ_ALLOC_ARGS\""
    [[ $RABBITMQ_ALLOC_ARGS == 'foo bar baz' ]]
}
