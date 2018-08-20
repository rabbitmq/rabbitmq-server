#!/usr/bin/env bats

RABBITMQ_SCRIPTS_DIR="$BATS_TEST_DIRNAME/../scripts"
_rabbitmq_env_load='false'

setup() {
    RABBITMQ_CONF_ENV_FILE="$BATS_TMPDIR/rabbitmq-config.$BATS_TEST_NAME.conf"
    rm -f "$RABBITMQ_CONF_ENV_FILE"
}

@test "default RABBITMQ_SERVER_ERL_ARGS" {
    local -r _want=' +P 1048576 +t 5000000 +stbt db +zdbbl 128000 '

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_erl_args

    echo "expected RABBITMQ_SERVER_ERL_ARGS to be '$_want', but got: \"$RABBITMQ_SERVER_ERL_ARGS\""
    [[ $RABBITMQ_SERVER_ERL_ARGS == "$_want" ]]
}
