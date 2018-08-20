#!/usr/bin/env bats

RABBITMQ_SCRIPTS_DIR="$BATS_TEST_DIRNAME/../scripts"
_rabbitmq_env_load='false'

setup() {
    RABBITMQ_CONF_ENV_FILE="$BATS_TMPDIR/rabbitmq-config.$BATS_TEST_NAME.conf"
    rm -f "$RABBITMQ_CONF_ENV_FILE"
}

@test "default short RABBITMQ_NODENAME" {
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_names

    if [[ -n $HOSTNAME ]]
    then
        local -r _want="rabbit@$HOSTNAME"
    else
        local -r _want="rabbit@$(hostname)"
    fi

    echo "expected RABBITMQ_NODENAME to be \"$_want\", but got: \"$RABBITMQ_NODENAME\""
    [[ $RABBITMQ_NODENAME == $_want ]]
    [[ $RABBITMQ_NAME_TYPE == '-sname' ]]
}

@test "default long RABBITMQ_NODENAME" {
    RABBITMQ_USE_LONGNAME=true
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_names

    if [[ -n $HOSTNAME ]]
    then
        local -r _want="rabbit@$HOSTNAME"
    else
        local -r _want="rabbit@$(hostname -f)"
    fi

    echo "expected RABBITMQ_NODENAME to be \"$_want\", but got: \"$RABBITMQ_NODENAME\""
    [[ $RABBITMQ_NODENAME == $_want ]]
    [[ $RABBITMQ_NAME_TYPE == '-name' ]]
}

@test "can configure short RABBITMQ_NODENAME via rabbitmq-env.conf" {
    local -r _want="rabbit@foobarbaz"

    echo 'USE_LONGNAME=false' > "$RABBITMQ_CONF_ENV_FILE"
    echo "NODENAME=$_want" >> "$RABBITMQ_CONF_ENV_FILE"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_names

    echo "expected RABBITMQ_NODENAME to be \"$_want\", but got: \"$RABBITMQ_NODENAME\""
    [[ $RABBITMQ_NODENAME == $_want ]]
    [[ $RABBITMQ_NAME_TYPE == '-sname' ]]
}

@test "can configure long RABBITMQ_NODENAME via rabbitmq-env.conf" {
    local -r _want="rabbit@foo.bar.bat.com"

    echo 'USE_LONGNAME=true' > "$RABBITMQ_CONF_ENV_FILE"
    echo "NODENAME=$_want" >> "$RABBITMQ_CONF_ENV_FILE"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_names

    echo "expected RABBITMQ_NODENAME to be \"$_want\", but got: \"$RABBITMQ_NODENAME\""
    [[ $RABBITMQ_NODENAME == $_want ]]
    [[ $RABBITMQ_NAME_TYPE == '-name' ]]
}
