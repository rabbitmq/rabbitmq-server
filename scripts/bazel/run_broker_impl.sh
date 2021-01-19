#!/usr/bin/env bash
set -exuo pipefail

# pwd

TEST_TMPDIR=${TMPDIR}/rabbitmq-test-instances
RABBITMQ_SCRIPTS_DIR="$(dirname {RABBITMQ_SERVER_PATH})"
RABBITMQ_PLUGINS=${RABBITMQ_SCRIPTS_DIR}/rabbitmq-plugins
RABBITMQ_SERVER=${RABBITMQ_SCRIPTS_DIR}/rabbitmq-server
RABBITMQCTL=${RABBITMQ_SCRIPTS_DIR}/rabbitmqctl

export RABBITMQ_SCRIPTS_DIR RABBITMQCTL RABBITMQ_PLUGINS RABBITMQ_SERVER

HOSTNAME="$(hostname -s)"

RABBITMQ_NODENAME=rabbit@${HOSTNAME}
RABBITMQ_NODENAME_FOR_PATHS=${RABBITMQ_NODENAME}
NODE_TMPDIR=${TEST_TMPDIR}/${RABBITMQ_NODENAME_FOR_PATHS}

RABBITMQ_BASE=${NODE_TMPDIR}
RABBITMQ_PID_FILE=${NODE_TMPDIR}/${RABBITMQ_NODENAME_FOR_PATHS}.pid
RABBITMQ_LOG_BASE=${NODE_TMPDIR}/log
RABBITMQ_MNESIA_BASE=${NODE_TMPDIR}/mnesia
RABBITMQ_MNESIA_DIR=${RABBITMQ_MNESIA_BASE}/${RABBITMQ_NODENAME_FOR_PATHS}
RABBITMQ_QUORUM_DIR=${RABBITMQ_MNESIA_DIR}/quorum
RABBITMQ_STREAM_DIR=${RABBITMQ_MNESIA_DIR}/stream
RABBITMQ_PLUGINS_DIR={PLUGINS_DIR}
RABBITMQ_PLUGINS_EXPAND_DIR=${NODE_TMPDIR}/plugins
RABBITMQ_FEATURE_FLAGS_FILE=${NODE_TMPDIR}/feature_flags
RABBITMQ_ENABLED_PLUGINS_FILE=${NODE_TMPDIR}/enabled_plugins

# Enable colourful debug logging by default
# To change this, set RABBITMQ_LOG to info, notice, warning etc.
RABBITMQ_LOG='debug,+color'
export RABBITMQ_LOG

RABBITMQ_ENABLED_PLUGINS=ALL

mkdir -p ${TEST_TMPDIR}

mkdir -p ${RABBITMQ_LOG_BASE} \
    ${RABBITMQ_MNESIA_BASE} \
    ${RABBITMQ_PLUGINS_EXPAND_DIR}

# tree

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
    RABBITMQ_SERVER_START_ARGS="-ra wal_sync_method sync" \
    RABBITMQ_ENABLED_PLUGINS \
    RABBITMQ_ENABLED_PLUGINS_FILE

export RABBITMQ_ALLOW_INPUT=true
export RABBITMQ_CONFIG_FILE=test.config

cat << EOF > ${RABBITMQ_CONFIG_FILE}
%% vim:ft=erlang:

[
  {rabbit, [
      {loopback_users, []},
      {log, [{file, [{level, debug}]},
             {console, [{level, debug}]}]}
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
  {lager, [
      {colors, [
          %% https://misc.flogisoft.com/bash/tip_colors_and_formatting
          {debug,     "\\\e[0;34m" },
          {info,      "\\\e[1;37m" },
          {notice,    "\\\e[1;36m" },
          {warning,   "\\\e[1;33m" },
          {error,     "\\\e[1;31m" },
          {critical,  "\\\e[1;35m" },
          {alert,     "\\\e[1;44m" },
          {emergency, "\\\e[1;41m" }
      ]}
    ]},
  {osiris, [
      {data_dir, "${RABBITMQ_STREAM_DIR}"}
    ]}
].
EOF

./{RABBITMQ_SERVER_PATH}
