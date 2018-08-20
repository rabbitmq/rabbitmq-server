#!/usr/bin/env bats

RABBITMQ_NODENAME='rabbit@frazzle'
RABBITMQ_SCRIPTS_DIR="$BATS_TEST_DIRNAME/../scripts"
_rabbitmq_env_load='false'

setup() {
    RABBITMQ_CONF_ENV_FILE="$BATS_TMPDIR/rabbitmq-config.$BATS_TEST_NAME.conf"
    rm -f "$RABBITMQ_CONF_ENV_FILE"
}

@test "default RABBITMQ_PLUGINS_EXPAND_DIR" {
    MNESIA_BASE='/var/lib/rabbitmq/mnesia'
    local -r _want="$MNESIA_BASE/${RABBITMQ_NODENAME}-plugins-expand"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_mnesia_dirs
    _rmq_env_config_plugins

    echo "expected RABBITMQ_PLUGINS_EXPAND_DIR to be '$_want', but got: \"$RABBITMQ_PLUGINS_EXPAND_DIR\""
    [[ $RABBITMQ_PLUGINS_EXPAND_DIR == "$_want" ]]
}

@test "can configure RABBITMQ_PLUGINS_EXPAND_DIR via env" {
    MNESIA_BASE='/var/lib/rabbitmq/this-is-ignored'
    RABBITMQ_PLUGINS_EXPAND_DIR='/var/lib/rabbitmq-plugins'
    local -r _want="$RABBITMQ_PLUGINS_EXPAND_DIR"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_mnesia_dirs
    _rmq_env_config_plugins

    echo "expected RABBITMQ_PLUGINS_EXPAND_DIR to be '$_want', but got: \"$RABBITMQ_PLUGINS_EXPAND_DIR\""
    [[ $RABBITMQ_PLUGINS_EXPAND_DIR == "$_want" ]]
}

@test "can configure RABBITMQ_PLUGINS_EXPAND_DIR via rabbitmq-env.conf" {
    local -r _want='/var/lib/rabbitmq-plugins'

    echo "MNESIA_BASE='/var/lib/do-not-want/mnesia'" > "$RABBITMQ_CONF_ENV_FILE"
    echo "MNESIA_DIR='/var/lib/do-not-want/mnesia'" > "$RABBITMQ_CONF_ENV_FILE"
    echo "RABBITMQ_PLUGINS_EXPAND_DIR='$_want'" >> "$RABBITMQ_CONF_ENV_FILE"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_mnesia_dirs
    _rmq_env_config_plugins

    echo "expected RABBITMQ_PLUGINS_EXPAND_DIR to be '$_want', but got: \"$RABBITMQ_PLUGINS_EXPAND_DIR\""
    [[ $RABBITMQ_PLUGINS_EXPAND_DIR == "$_want" ]]
}

@test "default RABBITMQ_ENABLED_PLUGINS_FILE" {
    local -r _want='/etc/rabbitmq/enabled_plugins'

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_mnesia_dirs
    _rmq_env_config_plugins

    echo "expected RABBITMQ_ENABLED_PLUGINS_FILE to be '$_want', but got: \"$RABBITMQ_ENABLED_PLUGINS_FILE\""
    [[ $RABBITMQ_ENABLED_PLUGINS_FILE == "$_want" ]] && \
        [[ -z $_rmq_env_plugins_dir_source ]]
}

@test "can configure RABBITMQ_ENABLED_PLUGINS_FILE via env" {
    RABBITMQ_ENABLED_PLUGINS_FILE='/var/lib/rabbitmq/enabled_plugins'
    local -r _want="$RABBITMQ_ENABLED_PLUGINS_FILE"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_mnesia_dirs
    _rmq_env_config_plugins

    echo "expected RABBITMQ_ENABLED_PLUGINS_FILE to be '$_want', but got: \"$RABBITMQ_ENABLED_PLUGINS_FILE\""
    [[ $RABBITMQ_ENABLED_PLUGINS_FILE == "$_want" ]] && \
        [[ $_rmq_env_enabled_plugins_file_source == 'environment' ]]
}

@test "can configure RABBITMQ_ENABLED_PLUGINS_FILE via rabbitmq-env.conf" {
    local -r _want='/var/lib/rabbitmq/plugins'

    echo "MNESIA_BASE='/var/lib/do-not-want/mnesia'" > "$RABBITMQ_CONF_ENV_FILE"
    echo "MNESIA_DIR='/var/lib/do-not-want/mnesia'" > "$RABBITMQ_CONF_ENV_FILE"
    echo "RABBITMQ_ENABLED_PLUGINS_FILE='$_want'" >> "$RABBITMQ_CONF_ENV_FILE"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_mnesia_dirs
    _rmq_env_config_plugins

    echo "expected RABBITMQ_ENABLED_PLUGINS_FILE to be '$_want', but got: \"$RABBITMQ_ENABLED_PLUGINS_FILE\""
    [[ $RABBITMQ_ENABLED_PLUGINS_FILE == "$_want" ]]
}

@test "default RABBITMQ_PLUGINS_DIR" {
    local -r _want="$(readlink -f "$BATS_TEST_DIRNAME/../plugins")"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_mnesia_dirs
    _rmq_env_config_plugins

    echo "expected RABBITMQ_PLUGINS_DIR to be '$_want', but got: \"$RABBITMQ_PLUGINS_DIR\"" \
        "(RABBITMQ_HOME: \"$RABBITMQ_HOME\" PLUGINS_DIR: \"$PLUGINS_DIR\""
    [[ $RABBITMQ_PLUGINS_DIR == "$_want" ]] && \
        [[ -z $_rmq_env_plugins_dir_source ]]
}

@test "can configure RABBITMQ_PLUGINS_DIR via env" {
    RABBITMQ_PLUGINS_DIR='/var/lib/rabbitmq/plugins'
    local -r _want="$RABBITMQ_PLUGINS_DIR"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_mnesia_dirs
    _rmq_env_config_plugins

    echo "expected RABBITMQ_PLUGINS_DIR to be '$_want', but got: \"$RABBITMQ_PLUGINS_DIR\""
    [[ $RABBITMQ_PLUGINS_DIR == "$_want" ]] && \
        [[ $_rmq_env_plugins_dir_source == 'environment' ]]
}

@test "can configure RABBITMQ_PLUGINS_DIR via rabbitmq-env.conf" {
    local -r _want='/var/lib/rabbitmq/plugins'

    echo "MNESIA_BASE='/var/lib/do-not-want/mnesia'" > "$RABBITMQ_CONF_ENV_FILE"
    echo "MNESIA_DIR='/var/lib/do-not-want/mnesia'" > "$RABBITMQ_CONF_ENV_FILE"
    echo "RABBITMQ_PLUGINS_DIR='$_want'" >> "$RABBITMQ_CONF_ENV_FILE"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_mnesia_dirs
    _rmq_env_config_plugins

    echo "expected RABBITMQ_PLUGINS_DIR to be '$_want', but got: \"$RABBITMQ_PLUGINS_DIR\""
    [[ $RABBITMQ_PLUGINS_DIR == "$_want" ]]
}
