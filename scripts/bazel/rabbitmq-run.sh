#!/usr/bin/env bash
set -euo pipefail

GREEN='\033[0;32m'
NO_COLOR='\033[0m'

rmq_realpath() {
    local path=$1

    if [ -d "$path" ]; then
        cd "$path" && pwd
    elif [ -f "$path" ]; then
        cd "$(dirname "$path")" && echo "$(pwd)/$(basename "$path")"
    else
        echo "$path"
    fi
}

write_config_file() {
    local rabbit_fragment=
    local rabbitmq_management_fragment=
    local rabbitmq_mqtt_fragment=
    local rabbitmq_web_mqtt_fragment=
    local rabbitmq_web_mqtt_examples_fragment=
    local rabbitmq_stomp_fragment=
    local rabbitmq_web_stomp_fragment=
    local rabbitmq_web_stomp_examples_fragment=
    local rabbitmq_stream_fragment=
    local rabbitmq_prometheus_fragment=

    if [[ -n ${RABBITMQ_NODE_PORT+x} ]]; then
        rabbit_fragment="{tcp_listeners, [$RABBITMQ_NODE_PORT]}"
        rabbitmq_management_fragment="{listener, [{port, $(($RABBITMQ_NODE_PORT + 10000))}]}"
        rabbitmq_mqtt_fragment="{tcp_listeners, [$((1883 + $RABBITMQ_NODE_PORT - 5672))]}"
        rabbitmq_web_mqtt_fragment="{tcp_config, [{port, $((15675 + $RABBITMQ_NODE_PORT - 5672))}]}"
        rabbitmq_web_mqtt_examples_fragment="{listener, [{port, $((15670 + $RABBITMQ_NODE_PORT - 5672))}]}"
        rabbitmq_stomp_fragment="{tcp_listeners, [$((61613 + $RABBITMQ_NODE_PORT - 5672))]}"
        rabbitmq_web_stomp_fragment="{tcp_config, [{port, $((15674 + $RABBITMQ_NODE_PORT - 5672))}]}"
        rabbitmq_web_stomp_examples_fragment="{listener, [{port, $((15670 + $RABBITMQ_NODE_PORT - 5672))}]}"
        rabbitmq_stream_fragment="{tcp_listeners, [$((5552 + $RABBITMQ_NODE_PORT - 5672))]}"
        rabbitmq_prometheus_fragment="{tcp_config, [{port, $((15692 + $RABBITMQ_NODE_PORT - 5672))}]}"
    fi
    cat << EOF > "$RABBITMQ_CONFIG_FILE"
%% vim:ft=erlang:

[
  {rabbit, [
      ${rabbit_fragment}${rabbit_fragment:+,}
      {loopback_users, []}
    ]},
  {rabbitmq_management, [
      ${rabbitmq_management_fragment}
    ]},
  {rabbitmq_mqtt, [
      ${rabbitmq_mqtt_fragment}
    ]},
  {rabbitmq_web_mqtt, [
      ${rabbitmq_web_mqtt_fragment}
    ]},
  {rabbitmq_web_mqtt_examples, [
      ${rabbitmq_web_mqtt_examples_fragment}
    ]},
  {rabbitmq_stomp, [
      ${rabbitmq_stomp_fragment}
    ]},
  {rabbitmq_web_stomp, [
      ${rabbitmq_web_stomp_fragment}
    ]},
  {rabbitmq_web_stomp_examples, [
      ${rabbitmq_web_stomp_examples_fragment}
    ]},
  {rabbitmq_stream, [
      ${rabbitmq_stream_fragment}
    ]},
  {rabbitmq_prometheus, [
      ${rabbitmq_prometheus_fragment}
    ]},
  {ra, [
      {data_dir, "${RABBITMQ_QUORUM_DIR}"},
      {wal_sync_method, sync}
    ]},
  {osiris, [
      {data_dir, "${RABBITMQ_STREAM_DIR}"}
    ]}
].
EOF
}

setup_node_env() {
    local node_index=""
    if [ -n "${1-}" ]; then
        node_index="-$1"
        unset RABBITMQ_NODENAME RABBITMQ_NODENAME_FOR_PATHS
    fi

    RABBITMQ_NODENAME=${RABBITMQ_NODENAME:=rabbit${node_index}@${HOSTNAME}}
    RABBITMQ_NODENAME_FOR_PATHS=${RABBITMQ_NODENAME_FOR_PATHS:=${RABBITMQ_NODENAME}}
    NODE_TMPDIR=${TEST_TMPDIR}/${RABBITMQ_NODENAME_FOR_PATHS}

    RABBITMQ_BASE=${NODE_TMPDIR}
    RABBITMQ_PID_FILE=${NODE_TMPDIR}/${RABBITMQ_NODENAME_FOR_PATHS}.pid
    RABBITMQ_LOG_BASE=${NODE_TMPDIR}/log
    RABBITMQ_MNESIA_BASE=${NODE_TMPDIR}/mnesia
    RABBITMQ_MNESIA_DIR=${RABBITMQ_MNESIA_BASE}/${RABBITMQ_NODENAME_FOR_PATHS}
    RABBITMQ_QUORUM_DIR=${RABBITMQ_MNESIA_DIR}/quorum
    RABBITMQ_STREAM_DIR=${RABBITMQ_MNESIA_DIR}/stream
    RABBITMQ_PLUGINS_EXPAND_DIR=${NODE_TMPDIR}/plugins
    RABBITMQ_FEATURE_FLAGS_FILE=${NODE_TMPDIR}/feature_flags
    RABBITMQ_ENABLED_PLUGINS_FILE=${NODE_TMPDIR}/enabled_plugins

    export \
        RABBITMQ_NODENAME \
        RABBITMQ_BASE \
        RABBITMQ_PID_FILE \
        RABBITMQ_LOG_BASE \
        RABBITMQ_MNESIA_BASE \
        RABBITMQ_MNESIA_DIR \
        RABBITMQ_QUORUM_DIR \
        RABBITMQ_STREAM_DIR \
        RABBITMQ_PLUGINS_EXPAND_DIR \
        RABBITMQ_FEATURE_FLAGS_FILE \
        RABBITMQ_ENABLED_PLUGINS_FILE

    mkdir -p "$TEST_TMPDIR"
    mkdir -p "$RABBITMQ_LOG_BASE"
    mkdir -p "$RABBITMQ_MNESIA_BASE"
    mkdir -p "$RABBITMQ_PLUGINS_DIR"
    mkdir -p "$RABBITMQ_PLUGINS_EXPAND_DIR"
}

await_startup() {
    RMQCTL_WAIT_TIMEOUT=${RMQCTL_WAIT_TIMEOUT:=60}

    # rabbitmqctl wait shells out to 'ps', which is broken in the bazel macOS
    # sandbox (https://github.com/bazelbuild/bazel/issues/7448)
    # adding "--spawn_strategy=local" to the invocation is a workaround
    "$RABBITMQCTL" \
        -n "$RABBITMQ_NODENAME" \
        wait \
        --timeout "$RMQCTL_WAIT_TIMEOUT" \
        "$RABBITMQ_PID_FILE"

    "$RABBITMQCTL" \
        -n "$RABBITMQ_NODENAME" \
        await_startup
    }

if [ -z ${TEST_SRCDIR+x} ]; then
    BASE_DIR=$PWD
else
    BASE_DIR=$TEST_SRCDIR/$TEST_WORKSPACE
fi

if [ "$1" = "-C" ]; then
    cd "$2"
    shift 2
fi

for arg in "$@"; do
    case $arg in
        run-broker)
            CMD="$arg"
            ;;
        start-background-broker)
            CMD="$arg"
            ;;
        stop-node)
            CMD="$arg"
            ;;
        start-cluster)
            CMD="$arg"
            ;;
        stop-cluster)
            CMD="$arg"
            ;;
        set-resource-alarm)
            CMD="$arg"
            ;;
        clear-resource-alarm)
            CMD="$arg"
            ;;
        *)
            export "$arg"
            ;;
    esac
done

# shellcheck disable=SC1083
DEFAULT_PLUGINS_DIR=${BASE_DIR}/{RABBITMQ_HOME}/plugins
if [[ -n ${EXTRA_PLUGINS_DIR+x} ]]; then
    DEFAULT_PLUGINS_DIR=${DEFAULT_PLUGINS_DIR}:${EXTRA_PLUGINS_DIR}
fi

RABBITMQ_PLUGINS_DIR=${RABBITMQ_PLUGINS_DIR:=${DEFAULT_PLUGINS_DIR}}
export RABBITMQ_PLUGINS_DIR
RABBITMQ_SERVER_START_ARGS="${RABBITMQ_SERVER_START_ARGS:=-ra wal_sync_method sync}"
export RABBITMQ_SERVER_START_ARGS

# Enable colourful debug logging by default
# To change this, set RABBITMQ_LOG to info, notice, warning etc.
RABBITMQ_LOG=${RABBITMQ_LOG:='debug,+color'}
export RABBITMQ_LOG

if [ -z ${LEAVE_PLUGINS_DISABLED+x} ]; then
    RABBITMQ_ENABLED_PLUGINS=${RABBITMQ_ENABLED_PLUGINS:=ALL}
else
    RABBITMQ_ENABLED_PLUGINS=${RABBITMQ_ENABLED_PLUGINS:=}
fi
export RABBITMQ_ENABLED_PLUGINS


TEST_TMPDIR=${TEST_TMPDIR:=$(dirname "$(mktemp -u)")/rabbitmq-test-instances}
printf "RabbitMQ node(s) in directory $GREEN$(realpath "$TEST_TMPDIR")$NO_COLOR\n"

# shellcheck disable=SC1083
RABBITMQ_SCRIPTS_DIR="$(rmq_realpath "$BASE_DIR"/{RABBITMQ_HOME}/sbin)"
RABBITMQ_SERVER=${RABBITMQ_SCRIPTS_DIR}/rabbitmq-server
RABBITMQCTL=${RABBITMQ_SCRIPTS_DIR}/rabbitmqctl
export RABBITMQ_SCRIPTS_DIR \
    RABBITMQ_SERVER \
    RABBITMQCTL

HOSTNAME="$(hostname -s)"

case $CMD in
    run-broker)
        setup_node_env
        export RABBITMQ_ALLOW_INPUT=true
        if [ -z ${RABBITMQ_CONFIG_FILE+x} ]; then
            export RABBITMQ_CONFIG_FILE=${TEST_TMPDIR}/test.config
            write_config_file
        fi
        "$RABBITMQ_SERVER"
        ;;
    start-background-broker)
        setup_node_env
        "$RABBITMQ_SERVER" \
            > "$RABBITMQ_LOG_BASE"/startup_log \
            2> "$RABBITMQ_LOG_BASE"/startup_err &
        await_startup
        ;;
    stop-node)
        setup_node_env
        pid=$(test -f "$RABBITMQ_PID_FILE" && cat "$RABBITMQ_PID_FILE"); \
            test "$pid" && \
            kill -TERM "$pid" && \
            echo "waiting for process to exit" && \
            while ps -p "$pid" >/dev/null 2>&1; do sleep 1; done
        ;;
    start-cluster)
        nodes=${NODES:=3}
        for ((n=0; n < nodes; n++))
        do
            setup_node_env "$n"

            RABBITMQ_NODE_PORT=$((5672 + n)) \
            RABBITMQ_SERVER_START_ARGS=" \
            -rabbit loopback_users [] \
            -rabbitmq_management listener [{port,$((15672 + n))}] \
            -rabbitmq_mqtt tcp_listeners [$((1883 + n))] \
            -rabbitmq_web_mqtt tcp_config [{port,$((1893 + n))}] \
            -rabbitmq_web_mqtt_examples listener [{port,$((1903 + n))}] \
            -rabbitmq_stomp tcp_listeners [$((61613 + n))] \
            -rabbitmq_web_stomp tcp_config [{port,$((61623 + n))}] \
            -rabbitmq_web_stomp_examples listener [{port,$((61633 + n))}] \
            -rabbitmq_prometheus tcp_config [{port,$((15692 + n))}] \
            -rabbitmq_stream tcp_listeners [$((5552 + n))]" \
            "$RABBITMQ_SERVER" \
                > "$RABBITMQ_LOG_BASE"/startup_log \
                2> "$RABBITMQ_LOG_BASE"/startup_err &

            await_startup
            if [ -n "${nodename0-}" ]; then
                "$RABBITMQCTL" -n "$RABBITMQ_NODENAME" stop_app
                "$RABBITMQCTL" -n "$RABBITMQ_NODENAME" join_cluster "$nodename0"
                "$RABBITMQCTL" -n "$RABBITMQ_NODENAME" start_app
            else
                nodename0=$RABBITMQ_NODENAME
            fi
        done
        ;;
    stop-cluster)
        nodes=${NODES:=3}
        for ((n=nodes-1; n >= 0; n--))
        do
            "$RABBITMQCTL" -n "rabbit-$n@$HOSTNAME" stop
        done
        ;;
    set-resource-alarm)
        setup_node_env
        ERL_LIBS="${BASE_DIR}/{ERL_LIBS}" \
           "$RABBITMQCTL" -n "$RABBITMQ_NODENAME" \
            eval "rabbit_alarm:set_alarm({{resource_limit, ${SOURCE}, node()}, []})."
        ;;
    clear-resource-alarm)
        setup_node_env
        ERL_LIBS="${BASE_DIR}/{ERL_LIBS}" \
            "$RABBITMQCTL" -n "$RABBITMQ_NODENAME" \
            eval "rabbit_alarm:clear_alarm({resource_limit, ${SOURCE}, node()})."
        ;;
    *)
        echo "rabbitmq-run does not support $CMD"
        exit 1
        ;;
esac
