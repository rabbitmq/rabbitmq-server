#!/usr/bin/env bats

RABBITMQ_SCRIPTS_DIR="$BATS_TEST_DIRNAME/../scripts"
_rabbitmq_env_load='false'

readonly _default_rmq_node_port=''
readonly _default_rmq_dist_port=25672
readonly _default_ctl_dist_port_min=35672
readonly _default_ctl_dist_port_max=35682
readonly _default_rmq_node_ip=''

setup() {
    RABBITMQ_CONF_ENV_FILE="$BATS_TMPDIR/rabbitmq-config.$BATS_TEST_NAME.conf"
    rm -f "$RABBITMQ_CONF_ENV_FILE"
}

_validate()
{
    local -r _want_rmq_node_port="${1:-$_default_rmq_node_port}"
    local -r _want_rmq_dist_port="${2:-$_default_rmq_dist_port}"
    local -r _want_ctl_dist_port_min="${3:-$_default_ctl_dist_port_min}"
    local -r _want_ctl_dist_port_max="${4:-$_default_ctl_dist_port_max}"
    local -r _want_rmq_node_ip="${5:-$_default_rmq_node_ip}"

    echo "expected RABBITMQ_NODE_PORT to be '$_want_rmq_node_port', but got: \"$RABBITMQ_NODE_PORT\""
    [[ $RABBITMQ_NODE_PORT == "$_want_rmq_node_port" ]]

    echo "expected RABBITMQ_DIST_PORT to be '$_want_rmq_dist_port', but got: \"$RABBITMQ_DIST_PORT\""
    [[ $RABBITMQ_DIST_PORT == "$_want_rmq_dist_port" ]]

    echo "expected RABBITMQ_CTL_DIST_PORT_MIN to be '$_want_ctl_dist_port_min', but got: \"$RABBITMQ_CTL_DIST_PORT_MIN\""
    [[ $RABBITMQ_CTL_DIST_PORT_MIN == "$_want_ctl_dist_port_min" ]]

    echo "expected RABBITMQ_CTL_DIST_PORT_MAX to be '$_want_ctl_dist_port_max', but got: \"$RABBITMQ_CTL_DIST_PORT_MAX\""
    [[ $RABBITMQ_CTL_DIST_PORT_MAX == "$_want_ctl_dist_port_max" ]]

    echo "expected RABBITMQ_NODE_IP_ADDRESS to be '$_want_rmq_node_ip', but got: \"$RABBITMQ_NODE_IP_ADDRESS\""
    [[ $RABBITMQ_NODE_IP_ADDRESS == "$_want_rmq_node_ip" ]]
}

@test "default RABBITMQ_NODE_IP_ADDRESS and port arguments" {
    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_addr_ports
    _validate
}

@test "can configure RABBITMQ_NODE_IP_ADDRESS via rabbitmq-env.conf file" {
    local -r _want_rmq_node_port=5672
    local -r _want_ip_address='127.1.1.1'

    echo "NODE_IP_ADDRESS='$_want_ip_address'" > "$RABBITMQ_CONF_ENV_FILE"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_addr_ports

    _validate "$_want_rmq_node_port" \
              "$_default_rmq_dist_port" \
              "$_default_ctl_dist_port_min" \
              "$_default_ctl_dist_port_max" \
              "$_want_ip_address"
}

@test "can configure RABBITMQ_NODE_IP_ADDRESS and RABBITMQ_NODE_PORT via rabbitmq-env.conf file" {
    local -r _want_rmq_node_port=5671
    local -r _want_rmq_dist_port=25671
    local -r _want_ctl_dist_port_min=35671
    local -r _want_ctl_dist_port_max=35681
    local -r _want_ip_address='127.1.1.1'

    echo "NODE_IP_ADDRESS='$_want_ip_address'" > "$RABBITMQ_CONF_ENV_FILE"
    echo "NODE_PORT='$_want_rmq_node_port'" >> "$RABBITMQ_CONF_ENV_FILE"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_addr_ports

    _validate "$_want_rmq_node_port" \
              "$_want_rmq_dist_port" \
              "$_want_ctl_dist_port_min" \
              "$_want_ctl_dist_port_max" \
          "$_want_ip_address"
}

@test "can configure RABBITMQ_NODE_PORT via rabbitmq-env.conf file" {
    local -r _want_rmq_node_port=10000
    local -r _want_rmq_dist_port=30000
    local -r _want_ctl_dist_port_min=40000
    local -r _want_ctl_dist_port_max=40010
    local -r _want_ip_address='auto'

    echo "NODE_PORT='$_want_rmq_node_port'" > "$RABBITMQ_CONF_ENV_FILE"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_addr_ports

    _validate "$_want_rmq_node_port" \
              "$_want_rmq_dist_port" \
              "$_want_ctl_dist_port_min" \
              "$_want_ctl_dist_port_max" \
          "$_want_ip_address"
}

@test "can configure RABBITMQ_DIST_PORT, RABBITMQ_CTL_DIST_PORT_(MIN|MAX) via rabbitmq-env.conf file" {
    local -r _want_rmq_dist_port=1234
    local -r _want_ctl_dist_port_min=11234
    local -r _want_ctl_dist_port_max=11244

    echo "DIST_PORT='$_want_rmq_dist_port'" > "$RABBITMQ_CONF_ENV_FILE"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_addr_ports

    _validate "$_default_rmq_node_port" \
              "$_want_rmq_dist_port" \
              "$_want_ctl_dist_port_min" \
              "$_want_ctl_dist_port_max" \
          "$_default_ip_address"
}

@test "can configure RABBITMQ_CTL_DIST_PORT_(MIN|MAX) via env" {
    local -r _want_ctl_dist_port_min=10000
    local -r _want_ctl_dist_port_max=11000

    RABBITMQ_CTL_DIST_PORT_MIN="$_want_ctl_dist_port_min"
    RABBITMQ_CTL_DIST_PORT_MAX="$_want_ctl_dist_port_max"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_addr_ports

    _validate "$_default_rmq_node_port" \
              "$_default_rmq_dist_port" \
              "$_want_ctl_dist_port_min" \
              "$_want_ctl_dist_port_max" \
          "$_default_ip_address"
}

@test "can configure  RABBITMQ_CTL_DIST_PORT_(MIN|MAX) via rabbitmq-env.conf file" {
    local -r _want_ctl_dist_port_min=10000
    local -r _want_ctl_dist_port_max=11000

    echo "CTL_DIST_PORT_MIN='$_want_ctl_dist_port_min'" > "$RABBITMQ_CONF_ENV_FILE"
    echo "CTL_DIST_PORT_MAX='$_want_ctl_dist_port_max'" >> "$RABBITMQ_CONF_ENV_FILE"

    source "$RABBITMQ_SCRIPTS_DIR/rabbitmq-env"
    _rmq_env_config_addr_ports

    _validate "$_default_rmq_node_port" \
              "$_default_rmq_dist_port" \
              "$_want_ctl_dist_port_min" \
              "$_want_ctl_dist_port_max" \
              "$_default_ip_address"
}
