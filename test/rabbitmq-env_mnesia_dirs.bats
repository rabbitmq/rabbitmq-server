#!/usr/bin/env bats

RABBITMQ_NODENAME='frazzle'
RABBITMQ_SCRIPTS_DIR="$BATS_TEST_DIRNAME/../scripts"
_rabbitmq_env_load='false'

setup() {
    RABBITMQ_CONF_ENV_FILE="$BATS_TMPDIR/rabbitmq-config.$BATS_TEST_NAME.conf"
    rm -f "$RABBITMQ_CONF_ENV_FILE"
}

@test "default RABBITMQ_MNESIA_DIR" {
    MNESIA_BASE='/var/lib/rabbitmq/mnesia'
    local -r _want="$MNESIA_BASE/$RABBITMQ_NODENAME"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_mnesia_dirs

    echo "expected RABBITMQ_MNESIA_DIR to be '$_want', but got: \"$RABBITMQ_MNESIA_DIR\""
    [[ $RABBITMQ_MNESIA_DIR == "$_want" ]] && \
        [[ $_rmq_env_mnesia_dirs_configured == 'true' ]]
}

@test "RABBITMQ_MNESIA_DIR overrides RABBITMQ_MNESIA_BASE" {
    RABBITMQ_MNESIA_BASE='/var/lib/do-not-want/mnesia'
    RABBITMQ_MNESIA_DIR='/var/lib/this-is-correct/mnesia'
    local -r _want="$RABBITMQ_MNESIA_DIR"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_mnesia_dirs

    echo "expected RABBITMQ_MNESIA_DIR to be '$_want', but got: \"$RABBITMQ_MNESIA_DIR\""
    [[ $RABBITMQ_MNESIA_DIR == "$_want" ]] && \
        [[ $_rmq_env_mnesia_dirs_configured == 'true' ]]
}

@test "MNESIA_DIR overrides MNESIA_BASE via rabbitmq-env.conf" {
    local -r _want="/var/lib/this-is-correct/mnesia"

    echo "MNESIA_BASE='/var/lib/do-not-want/mnesia'" > "$RABBITMQ_CONF_ENV_FILE"
    echo "MNESIA_DIR='$_want'" >> "$RABBITMQ_CONF_ENV_FILE"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_mnesia_dirs

    echo "expected RABBITMQ_MNESIA_DIR to be '$_want', but got: \"$RABBITMQ_MNESIA_DIR\""
    [[ $RABBITMQ_MNESIA_DIR == "$_want" ]] && \
        [[ $_rmq_env_mnesia_dirs_configured == 'true' ]]
}

@test "RABBITMQ_MNESIA_DIR is normalized via env" {
    RABBITMQ_MNESIA_BASE='//var//lib//rabbitmq//mnesia/'
    local -r _want="/var/lib/rabbitmq/mnesia/$RABBITMQ_NODENAME"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_mnesia_dirs

    echo "expected RABBITMQ_MNESIA_DIR to be '$_want', but got: \"$RABBITMQ_MNESIA_DIR\""
    [[ $RABBITMQ_MNESIA_DIR == "$_want" ]] && \
        [[ $_rmq_env_mnesia_dirs_configured == 'true' ]]
}

@test "RABBITMQ_MNESIA_DIR is normalized via rabbitmq-env.conf" {
    local -r _want="/var/lib/this-is-correct/mnesia"

    # Note leading and trailing slash
    echo "MNESIA_DIR='/$_want/'" >> "$RABBITMQ_CONF_ENV_FILE"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_mnesia_dirs

    echo "expected RABBITMQ_MNESIA_DIR to be '$_want', but got: \"$RABBITMQ_MNESIA_DIR\""
    [[ $RABBITMQ_MNESIA_DIR == "$_want" ]] && \
        [[ $_rmq_env_mnesia_dirs_configured == 'true' ]]
}
