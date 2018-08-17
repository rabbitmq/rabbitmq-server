#!/usr/bin/env bats

export RABBITMQ_SCRIPTS_DIR="$BATS_TEST_DIRNAME/../scripts"
export _rabbitmq_env_load='false'

setup() {
    export RABBITMQ_CONF_ENV_FILE="$BATS_TMPDIR/rabbitmq-config.$BATS_TEST_NAME.conf"
    if [[ -f $RABBITMQ_CONF_ENV_FILE ]]
    then
    	rm -f "$RABBITMQ_CONF_ENV_FILE"
    fi
}

@test "default ip address arguments" {
    _want=''

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_addr_ports

    echo "expected RABBITMQ_NODE_IP_ADDRESS to be '$_want', but got: \"$RABBITMQ_NODE_IP_ADDRESS\""
    [[ $RABBITMQ_NODE_IP_ADDRESS == $_want ]]
}

@test "can configure ip address via env" {
    _want='127.0.0.3'

    RABBITMQ_NODE_IP_ADDRESS="$_want"
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_addr_ports

    echo "expected RABBITMQ_NODE_IP_ADDRESS to be '$_want', but got: \"$RABBITMQ_NODE_IP_ADDRESS\""
    [[ $RABBITMQ_NODE_IP_ADDRESS == $_want ]]
}

@test "can configure ip address via rabbitmq-env.conf file" {
    _want='127.0.0.4'

    echo "NODE_IP_ADDRESS='$_want'" > "$RABBITMQ_CONF_ENV_FILE"
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_addr_ports

    echo "expected RABBITMQ_NODE_IP_ADDRESS to be '$_want', but got: \"$RABBITMQ_NODE_IP_ADDRESS\""
    [[ $RABBITMQ_NODE_IP_ADDRESS == $_want ]]
}
