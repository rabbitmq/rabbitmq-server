#!/usr/bin/env bats

RABBITMQ_SCRIPTS_DIR="$BATS_TEST_DIRNAME/../scripts"
_rabbitmq_env_load='false'

setup() {
    RABBITMQ_CONF_ENV_FILE="$BATS_TMPDIR/rabbitmq-config.$BATS_TEST_NAME.conf"
    rm -f "$RABBITMQ_CONF_ENV_FILE"
}

@test "default RABBITMQ_SCHEMA_DIR" {
    local -r _want='/var/lib/rabbitmq/schema'

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_schema_dir

    echo "expected RABBITMQ_SCHEMA_DIR to be '$_want', but got: \"$RABBITMQ_SCHEMA_DIR\""
    [[ $RABBITMQ_SCHEMA_DIR == "$_want" ]] && \
        [[ $_rmq_env_schema_dir_configured == 'true' ]]
}

@test "normalized RABBITMQ_SCHEMA_DIR via rabbitmq-env.conf" {
    local -r _want='/var/lib/rabbitmq-schemas'

    echo 'SCHEMA_DIR=//var/lib////rabbitmq-schemas///' > "$RABBITMQ_CONF_ENV_FILE"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_schema_dir

    echo "expected RABBITMQ_SCHEMA_DIR to be '$_want', but got: \"$RABBITMQ_SCHEMA_DIR\""
    [[ $RABBITMQ_SCHEMA_DIR == "$_want" ]] && \
        [[ $_rmq_env_schema_dir_configured == 'true' ]]
}
