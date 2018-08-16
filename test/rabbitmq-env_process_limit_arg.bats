#!/usr/bin/env bats

export RABBITMQ_SCRIPTS_DIR="$BATS_TEST_DIRNAME/../scripts"
export _rabbitmq_env_load='false'

setup() {
    export RABBITMQ_CONF_ENV_FILE="$BATS_TMPDIR/rabbitmq-env.$BATS_TEST_NAME.conf"
    if [[ -f $RABBITMQ_CONF_ENV_FILE ]]
    then
    	rm -f "$RABBITMQ_CONF_ENV_FILE"
    fi
}

@test "default Erlang maximum number of processes" {
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_process_limit_arg

    echo "expected RABBITMQ_MAX_NUMBER_OF_PROCESSES to be 1048576, but got: \"$RABBITMQ_MAX_NUMBER_OF_PROCESSES\""
    [[ $RABBITMQ_MAX_NUMBER_OF_PROCESSES == 1048576 ]]
}

@test "can configure Erlang maximum number of processes via rabbitmq-env.conf file" {
    echo 'MAX_NUMBER_OF_PROCESSES=2000000' > "$RABBITMQ_CONF_ENV_FILE"
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_process_limit_arg

    echo "expected RABBITMQ_MAX_NUMBER_OF_PROCESSES to be 2000000, but got: \"$RABBITMQ_MAX_NUMBER_OF_PROCESSES\""
    [[ $RABBITMQ_MAX_NUMBER_OF_PROCESSES == 2000000 ]]
}

@test "can configure Erlang maximum number of processes via env" {
    RABBITMQ_MAX_NUMBER_OF_PROCESSES=3000000
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_process_limit_arg

    echo "expected RABBITMQ_MAX_NUMBER_OF_PROCESSES to be 3000000, but got: \"$RABBITMQ_MAX_NUMBER_OF_PROCESSES\""
    [[ $RABBITMQ_MAX_NUMBER_OF_PROCESSES == 3000000 ]]
}

@test "Erlang maximum number of processes env takes precedence over rabbitmq-env.conf file" {
    echo 'MAX_NUMBER_OF_PROCESSES=4000000' > "$RABBITMQ_CONF_ENV_FILE"
    RABBITMQ_MAX_NUMBER_OF_PROCESSES=5000000
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_process_limit_arg

    echo "expected RABBITMQ_MAX_NUMBER_OF_PROCESSES to be 5000000, but got: \"$RABBITMQ_MAX_NUMBER_OF_PROCESSES\""
    [[ $RABBITMQ_MAX_NUMBER_OF_PROCESSES == 5000000 ]]
}

