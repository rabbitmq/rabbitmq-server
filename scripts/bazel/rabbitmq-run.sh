#!/usr/bin/env bash
set -euo pipefail

rmq_realpath() {
    local path=$1

    if [ -d "$path" ]; then
        cd "$path" && pwd
    elif [ -f "$path" ]; then
        cd "$(dirname "$path")" && echo $(pwd)/$(basename "$path")
    else
        echo "$path"
    fi
}

if [ -z ${TEST_SRCDIR+x} ]; then
BASE_DIR=$PWD
else
BASE_DIR=$TEST_SRCDIR/$TEST_WORKSPACE
fi

if [ $1 = "-C" ]; then
    cd $2
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

DEFAULT_PLUGINS_DIR=${BASE_DIR}/{RABBITMQ_HOME}/plugins
if [ ! -z ${EXTRA_PLUGINS_DIR+x} ]; then
    DEFAULT_PLUGINS_DIR=${DEFAULT_PLUGINS_DIR}:${EXTRA_PLUGINS_DIR}
fi

TEST_TMPDIR=${TEST_TMPDIR:=${TMPDIR}/rabbitmq-test-instances}
RABBITMQ_SCRIPTS_DIR="$(rmq_realpath ${BASE_DIR}/{RABBITMQ_HOME}/sbin)"
RABBITMQ_PLUGINS=${RABBITMQ_SCRIPTS_DIR}/rabbitmq-plugins
RABBITMQ_SERVER=${RABBITMQ_SCRIPTS_DIR}/rabbitmq-server
RABBITMQCTL=${RABBITMQ_SCRIPTS_DIR}/rabbitmqctl

export RABBITMQ_SCRIPTS_DIR RABBITMQCTL RABBITMQ_PLUGINS RABBITMQ_SERVER

HOSTNAME="$(hostname -s)"

RABBITMQ_NODENAME=${RABBITMQ_NODENAME:=rabbit@${HOSTNAME}}
RABBITMQ_NODENAME_FOR_PATHS=${RABBITMQ_NODENAME_FOR_PATHS:=${RABBITMQ_NODENAME}}
NODE_TMPDIR=${TEST_TMPDIR}/${RABBITMQ_NODENAME_FOR_PATHS}

RABBITMQ_BASE=${NODE_TMPDIR}
RABBITMQ_PID_FILE=${NODE_TMPDIR}/${RABBITMQ_NODENAME_FOR_PATHS}.pid
RABBITMQ_LOG_BASE=${NODE_TMPDIR}/log
RABBITMQ_MNESIA_BASE=${NODE_TMPDIR}/mnesia
RABBITMQ_MNESIA_DIR=${RABBITMQ_MNESIA_BASE}/${RABBITMQ_NODENAME_FOR_PATHS}
RABBITMQ_QUORUM_DIR=${RABBITMQ_MNESIA_DIR}/quorum
RABBITMQ_STREAM_DIR=${RABBITMQ_MNESIA_DIR}/stream
RABBITMQ_PLUGINS_DIR=${RABBITMQ_PLUGINS_DIR:=${DEFAULT_PLUGINS_DIR}}
RABBITMQ_PLUGINS_EXPAND_DIR=${NODE_TMPDIR}/plugins
RABBITMQ_FEATURE_FLAGS_FILE=${NODE_TMPDIR}/feature_flags
RABBITMQ_ENABLED_PLUGINS_FILE=${NODE_TMPDIR}/enabled_plugins

RABBITMQ_SERVER_START_ARGS="${RABBITMQ_SERVER_START_ARGS:=-ra wal_sync_method sync}"

# Enable colourful debug logging by default
# To change this, set RABBITMQ_LOG to info, notice, warning etc.
RABBITMQ_LOG=${RABBITMQ_LOG:='debug,+color'}
export RABBITMQ_LOG

if [ -z ${LEAVE_PLUGINS_DISABLED+x} ]; then
    RABBITMQ_ENABLED_PLUGINS=${RABBITMQ_ENABLED_PLUGINS:=ALL}
else
    RABBITMQ_ENABLED_PLUGINS=${RABBITMQ_ENABLED_PLUGINS:=}
fi

mkdir -p ${TEST_TMPDIR}

mkdir -p ${RABBITMQ_LOG_BASE}
mkdir -p ${RABBITMQ_MNESIA_BASE}
mkdir -p ${RABBITMQ_PLUGINS_EXPAND_DIR}

export \
    RABBITMQ_NODENAME \
    RABBITMQ_NODE_IP_ADDRESS \
    RABBITMQ_BASE \
    RABBITMQ_PID_FILE \
    RABBITMQ_LOG_BASE \
    RABBITMQ_MNESIA_BASE \
    RABBITMQ_MNESIA_DIR \
    RABBITMQ_QUORUM_DIR \
    RABBITMQ_STREAM_DIR \
    RABBITMQ_FEATURE_FLAGS_FILE \
    RABBITMQ_PLUGINS_DIR \
    RABBITMQ_PLUGINS_EXPAND_DIR \
    RABBITMQ_SERVER_START_ARGS \
    RABBITMQ_ENABLED_PLUGINS \
    RABBITMQ_ENABLED_PLUGINS_FILE

write_config_file() {
cat << EOF > ${RABBITMQ_CONFIG_FILE}
%% vim:ft=erlang:

[
  {rabbit, [
      {loopback_users, []}
    ]},
  {rabbitmq_management, [
    ]},
  {rabbitmq_mqtt, [
    ]},
  {rabbitmq_stomp, [
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

case $CMD in
    run-broker)
        export RABBITMQ_ALLOW_INPUT=true
        export RABBITMQ_CONFIG_FILE=${TEST_TMPDIR}/test.config
        write_config_file
        ${RABBITMQ_SCRIPTS_DIR}/rabbitmq-server
        ;;
    start-background-broker)
        RMQCTL_WAIT_TIMEOUT=${RMQCTL_WAIT_TIMEOUT:=60}

        ${RABBITMQ_SCRIPTS_DIR}/rabbitmq-server \
            > ${RABBITMQ_LOG_BASE}/startup_log \
            2> ${RABBITMQ_LOG_BASE}/startup_err &

        # rabbitmqctl wait shells out to 'ps', which is broken in the bazel macOS
        # sandbox (https://github.com/bazelbuild/bazel/issues/7448)
        # adding "--spawn_strategy=local" to the invocation is a workaround
        ${RABBITMQ_SCRIPTS_DIR}/rabbitmqctl \
            -n ${RABBITMQ_NODENAME} \
            wait \
            --timeout ${RMQCTL_WAIT_TIMEOUT} \
            ${RABBITMQ_PID_FILE}

        {ERLANG_HOME}/bin/erl \
            -noinput \
            -eval "true = rpc:call('${RABBITMQ_NODENAME}', rabbit, is_running, []), halt()." \
            -sname {SNAME} \
            -hidden
        ;;
    stop-node)
        pid=$(test -f $RABBITMQ_PID_FILE && cat $RABBITMQ_PID_FILE); \
            test "$pid" && \
            kill -TERM "$pid" && \
            echo "waiting for process to exit" && \
            while ps -p "$pid" >/dev/null 2>&1; do sleep 1; done
        ;;
    set-resource-alarm)
        ERL_LIBS="${BASE_DIR}/{ERL_LIBS}" \
           ${RABBITMQ_SCRIPTS_DIR}/rabbitmqctl -n ${RABBITMQ_NODENAME} \
            eval "rabbit_alarm:set_alarm({{resource_limit, ${SOURCE}, node()}, []})."
        ;;
    clear-resource-alarm)
        ERL_LIBS="${BASE_DIR}/{ERL_LIBS}" \
            ${RABBITMQ_SCRIPTS_DIR}/rabbitmqctl -n ${RABBITMQ_NODENAME} \
            eval "rabbit_alarm:clear_alarm({resource_limit, ${SOURCE}, node()})."
        ;;
    *)
        echo "rabbitmq-run does not support $CMD"
        exit 1
        ;;
esac
