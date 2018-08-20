#!/usr/bin/env bats

RABBITMQ_SCRIPTS_DIR="$BATS_TEST_DIRNAME/../scripts"
_rabbitmq_env_load='false'

setup() {
    RABBITMQ_CONF_ENV_FILE="$BATS_TMPDIR/rabbitmq-env.$BATS_TEST_NAME.conf"
    rm -f "$RABBITMQ_CONF_ENV_FILE"
}

@test "default RABBITMQ_DISTRIBUTION_BUFFER_SIZE" {
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_distribution_buffer_arg

    echo "expected RABBITMQ_DISTRIBUTION_BUFFER_SIZE to be 128000, but got: \"$RABBITMQ_DISTRIBUTION_BUFFER_SIZE\""
    [[ $RABBITMQ_DISTRIBUTION_BUFFER_SIZE == 128000 ]]
}

@test "can configure RABBITMQ_DISTRIBUTION_BUFFER_SIZE via rabbitmq-env.conf file" {
    echo 'DISTRIBUTION_BUFFER_SIZE=123123' > "$RABBITMQ_CONF_ENV_FILE"
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_distribution_buffer_arg

    echo "expected RABBITMQ_DISTRIBUTION_BUFFER_SIZE to be 123123, but got: \"$RABBITMQ_DISTRIBUTION_BUFFER_SIZE\""
    [[ $RABBITMQ_DISTRIBUTION_BUFFER_SIZE == 123123 ]]
}

@test "can configure RABBITMQ_DISTRIBUTION_BUFFER_SIZE via env" {
    RABBITMQ_DISTRIBUTION_BUFFER_SIZE=2000000
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_distribution_buffer_arg

    echo "expected RABBITMQ_DISTRIBUTION_BUFFER_SIZE to be 2000000, but got: \"$RABBITMQ_DISTRIBUTION_BUFFER_SIZE\""
    [[ $RABBITMQ_DISTRIBUTION_BUFFER_SIZE == 2000000 ]]
}

@test "RABBITMQ_DISTRIBUTION_BUFFER_SIZE env takes precedence over rabbitmq-env.conf file" {
    echo 'DISTRIBUTION_BUFFER_SIZE=3000000' > "$RABBITMQ_CONF_ENV_FILE"
    RABBITMQ_DISTRIBUTION_BUFFER_SIZE=4000000
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_distribution_buffer_arg

    echo "expected RABBITMQ_DISTRIBUTION_BUFFER_SIZE to be 4000000, but got: \"$RABBITMQ_DISTRIBUTION_BUFFER_SIZE\""
    [[ $RABBITMQ_DISTRIBUTION_BUFFER_SIZE == 4000000 ]]
}
