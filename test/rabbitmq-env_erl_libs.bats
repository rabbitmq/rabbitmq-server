#!/usr/bin/env bats

RABBITMQ_SCRIPTS_DIR="$BATS_TEST_DIRNAME/../scripts"
_rabbitmq_env_load='false'

setup() {
    RABBITMQ_CONF_ENV_FILE="$BATS_TMPDIR/rabbitmq-config.$BATS_TEST_NAME.conf"
    rm -f "$RABBITMQ_CONF_ENV_FILE"
}

@test "default ERL_LIBS" {
    local -r _want='TODO'

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_erl_libs

    echo "expected ERL_LIBS to be '$_want', but got: \"$ERL_LIBS\""
    [[ $ERL_LIBS == "$_want" ]]
}
