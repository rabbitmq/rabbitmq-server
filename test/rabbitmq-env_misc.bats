#!/usr/bin/env bats

RABBITMQ_SCRIPTS_DIR="$BATS_TEST_DIRNAME/../scripts"
_rabbitmq_env_load='false'

setup() {
    RABBITMQ_CONF_ENV_FILE="$BATS_TMPDIR/rabbitmq-config.$BATS_TEST_NAME.conf"
    rm -f "$RABBITMQ_CONF_ENV_FILE"
}

@test "default RABBITMQ_IO_THREAD_POOL_SIZE" {
    local -r _want=''

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_logs
    _rmq_env_misc

    echo "expected RABBITMQ_IO_THREAD_POOL_SIZE to be '$_want', but got: \"$RABBITMQ_IO_THREAD_POOL_SIZE\""
    [[ $RABBITMQ_IO_THREAD_POOL_SIZE == "$_want" ]]
}

@test "can configure RABBITMQ_IO_THREAD_POOL_SIZE via env" {
    local -r _want='256'
    RABBITMQ_IO_THREAD_POOL_SIZE="$_want"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_logs
    _rmq_env_misc

    echo "expected RABBITMQ_IO_THREAD_POOL_SIZE to be '$_want', but got: \"$RABBITMQ_IO_THREAD_POOL_SIZE\""
    [[ $RABBITMQ_IO_THREAD_POOL_SIZE == "$_want" ]]
}

@test "can configure RABBITMQ_IO_THREAD_POOL_SIZE via rabbitmq-env.config" {
    local -r _want='256'
    echo "IO_THREAD_POOL_SIZE='$_want'" > "$RABBITMQ_CONF_ENV_FILE"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_logs
    _rmq_env_misc

    echo "expected RABBITMQ_IO_THREAD_POOL_SIZE to be '$_want', but got: \"$RABBITMQ_IO_THREAD_POOL_SIZE\""
    [[ $RABBITMQ_IO_THREAD_POOL_SIZE == "$_want" ]]
}

@test "default RABBITMQ_SERVER_CODE_PATH" {
    local -r _want=''

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_logs
    _rmq_env_misc

    echo "expected RABBITMQ_SERVER_CODE_PATH to be '$_want', but got: \"$RABBITMQ_SERVER_CODE_PATH\""
    [[ $RABBITMQ_SERVER_CODE_PATH == "$_want" ]]
}

@test "can configure normalized RABBITMQ_SERVER_CODE_PATH via env" {
    local -r _want='/usr/lib/foo/bar/baz'
    RABBITMQ_SERVER_CODE_PATH='///usr/lib/foo//bar/baz'

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_logs
    _rmq_env_misc

    echo "expected RABBITMQ_SERVER_CODE_PATH to be '$_want', but got: \"$RABBITMQ_SERVER_CODE_PATH\""
    [[ $RABBITMQ_SERVER_CODE_PATH == "$_want" ]]
}

@test "can configure RABBITMQ_SERVER_CODE_PATH via rabbitmq-env.config" {
    local -r _want='256'
    echo "SERVER_CODE_PATH='$_want'" > "$RABBITMQ_CONF_ENV_FILE"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_logs
    _rmq_env_misc

    echo "expected RABBITMQ_SERVER_CODE_PATH to be '$_want', but got: \"$RABBITMQ_SERVER_CODE_PATH\""
    [[ $RABBITMQ_SERVER_CODE_PATH == "$_want" ]]
}

@test "default RABBITMQ_IGNORE_SIGINT, RABBITMQ_IGNORE_SIGINT_FLAG" {
    local -r _want='true'
    local -r _want_flag='+B i'

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_logs
    _rmq_env_misc

    echo "expected RABBITMQ_IGNORE_SIGINT to be '$_want', but got: \"$RABBITMQ_IGNORE_SIGINT\""
    [[ $RABBITMQ_IGNORE_SIGINT == "$_want" ]]

    echo "expected RABBITMQ_IGNORE_SIGINT_FLAG to be '$_want', but got: \"$RABBITMQ_IGNORE_SIGINT_FLAG\""
    [[ $RABBITMQ_IGNORE_SIGINT_FLAG == "$_want_flag" ]]
}

@test "default ERL_CRASH_DUMP" {
    local -r _want='/var/log/rabbitmq/erl_crash.dump'

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_logs
    _rmq_env_misc

    echo "expected ERL_CRASH_DUMP to be '$_want', but got: \"$ERL_CRASH_DUMP\""
    [[ $ERL_CRASH_DUMP == "$_want" ]]
}
