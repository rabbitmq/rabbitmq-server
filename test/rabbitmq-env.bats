@test "can set RABBITMQ_SCHEDULER_BIND_TYPE from rabbitmq-env.conf" {
    declare -r scripts_dir="$BATS_TEST_DIRNAME/../scripts"
    export RABBITMQ_SCRIPTS_DIR="$scripts_dir"
    export RABBITMQ_CONF_ENV_FILE="$BATS_TMPDIR/rabbitmq-env.conf"
    echo 'RABBITMQ_SCHEDULER_BIND_TYPE=u' > "$RABBITMQ_CONF_ENV_FILE"
    source "$scripts_dir/rabbitmq-env"
    echo "expect RABBITMQ_SERVER_ERL_ARGS to contain '+stbt u' but got '$SERVER_ERL_ARGS'" 
    [[ $RABBITMQ_SERVER_ERL_ARGS == *+stbt\ u* ]]
}

@test "can set RABBITMQ_DISTRIBUTION_BUFFER_SIZE from rabbitmq-env.conf" {
    declare -r scripts_dir="$BATS_TEST_DIRNAME/../scripts"
    export RABBITMQ_SCRIPTS_DIR="$scripts_dir"
    export RABBITMQ_CONF_ENV_FILE="$BATS_TMPDIR/rabbitmq-env.conf"
    echo 'RABBITMQ_DISTRIBUTION_BUFFER_SIZE=123456' > "$RABBITMQ_CONF_ENV_FILE"
    source "$scripts_dir/rabbitmq-env"
    echo "expect RABBITMQ_SERVER_ERL_ARGS to contain '+zdbbl 123456' but got '$SERVER_ERL_ARGS'" 
    [[ $RABBITMQ_SERVER_ERL_ARGS == *+zdbbl\ 123456* ]]
}
