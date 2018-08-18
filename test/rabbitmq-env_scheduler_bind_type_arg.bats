#!/usr/bin/env bats

export RABBITMQ_SCRIPTS_DIR="$BATS_TEST_DIRNAME/../scripts"
export _rabbitmq_env_load='false'

setup() {
    export RABBITMQ_CONF_ENV_FILE="$BATS_TMPDIR/rabbitmq-env.$BATS_TEST_NAME.conf"
    rm -f "$RABBITMQ_CONF_ENV_FILE"
}

@test "default Erlang scheduler bind type" {
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_scheduler_bind_type_arg

    echo "expected RABBITMQ_SCHEDULER_BIND_TYPE to be db', but got: \"$RABBITMQ_SCHEDULER_BIND_TYPE\""
    [[ $RABBITMQ_SCHEDULER_BIND_TYPE == 'db' ]]
}

@test "can configure Erlang scheduler bind type via rabbitmq-env.conf file" {
    echo 'SCHEDULER_BIND_TYPE=u' > "$RABBITMQ_CONF_ENV_FILE"
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_scheduler_bind_type_arg

    echo "expected RABBITMQ_SCHEDULER_BIND_TYPE to be u', but got: \"$RABBITMQ_SCHEDULER_BIND_TYPE\""
    [[ $RABBITMQ_SCHEDULER_BIND_TYPE == 'u' ]]
}

@test "can configure Erlang scheduler bind type via env" {
    RABBITMQ_SCHEDULER_BIND_TYPE=tnnps
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_scheduler_bind_type_arg

    echo "expected RABBITMQ_SCHEDULER_BIND_TYPE to be tnnps', but got: \"$RABBITMQ_SCHEDULER_BIND_TYPE\""
    [[ $RABBITMQ_SCHEDULER_BIND_TYPE == 'tnnps' ]]
}

@test "Erlang scheduler bind type env takes precedence over rabbitmq-env.conf file" {
    echo 'SCHEDULER_BIND_TYPE=s' > "$RABBITMQ_CONF_ENV_FILE"
    RABBITMQ_SCHEDULER_BIND_TYPE=nnps
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_scheduler_bind_type_arg

    echo "expected RABBITMQ_SCHEDULER_BIND_TYPE to be nnps', but got: \"$RABBITMQ_SCHEDULER_BIND_TYPE\""
    [[ $RABBITMQ_SCHEDULER_BIND_TYPE == 'nnps' ]]
}
