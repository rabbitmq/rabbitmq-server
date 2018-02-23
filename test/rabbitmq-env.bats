@test "can configure RABBITMQ_SCHEDULER_BIND_TYPE" {
    declare -r scripts_dir="$BATS_TEST_DIRNAME/../scripts"
    export RABBITMQ_SCRIPTS_DIR="$scripts_dir"
    export RABBITMQ_CONF_ENV_FILE="$BATS_TMPDIR/rabbitmq-env.conf"
    echo 'SCHEDULER_BIND_TYPE=u' > "$RABBITMQ_CONF_ENV_FILE"
    source "$scripts_dir/rabbitmq-env"
    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain '+stbt u', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *+stbt\ u* ]]
}

@test "can configure RABBITMQ_DISTRIBUTION_BUFFER_SIZE" {
    declare -r scripts_dir="$BATS_TEST_DIRNAME/../scripts"
    export RABBITMQ_SCRIPTS_DIR="$scripts_dir"
    export RABBITMQ_CONF_ENV_FILE="$BATS_TMPDIR/rabbitmq-env.conf"
    echo 'DISTRIBUTION_BUFFER_SIZE=123456' > "$RABBITMQ_CONF_ENV_FILE"
    source "$scripts_dir/rabbitmq-env"
    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain '+zdbbl 123456', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *+zdbbl\ 123456* ]]
}

@test "can configure RABBITMQ_MAX_NUMBER_OF_PROCESSES" {
    declare -r scripts_dir="$BATS_TEST_DIRNAME/../scripts"
    export RABBITMQ_SCRIPTS_DIR="$scripts_dir"
    export RABBITMQ_CONF_ENV_FILE="$BATS_TMPDIR/rabbitmq-env.conf"
    echo 'MAX_NUMBER_OF_PROCESSES=2000000' > "$RABBITMQ_CONF_ENV_FILE"
    source "$scripts_dir/rabbitmq-env"
    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain '+P 2000000', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *+P\ 2000000* ]]
}

@test "can configure RABBITMQ_MAX_NUMBER_OF_ATOMS" {
    declare -r scripts_dir="$BATS_TEST_DIRNAME/../scripts"
    export RABBITMQ_SCRIPTS_DIR="$scripts_dir"
    export RABBITMQ_CONF_ENV_FILE="$BATS_TMPDIR/rabbitmq-env.conf"
    echo 'MAX_NUMBER_OF_ATOMS=10000000' > "$RABBITMQ_CONF_ENV_FILE"
    source "$scripts_dir/rabbitmq-env"
    echo "expected RABBITMQ_SERVER_ERL_ARGS to contain '+t 10000000', but got: $RABBITMQ_SERVER_ERL_ARGS"
    [[ $RABBITMQ_SERVER_ERL_ARGS == *+t\ 10000000* ]]
}
