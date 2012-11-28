#!/bin/bash

TARGET_DIR=$1
ERTS_VSN=$2
VERSION=$3

## Here we set the RABBITMQ_HOME variable,
## then we make ERL_DIR point to our released erl
## and we add the paths to our released start_clean and start_sasl boot scripts

sed -e 's:^SYS_PREFIX=$:SYS_PREFIX=\${RABBITMQ_HOME}:' \
    "${TARGET_DIR}"/sbin/rabbitmq-defaults >"${TARGET_DIR}"/sbin/rabbitmq-defaults.tmp \
    && sed -e 's:^ERL_DIR=$:ERL_DIR=\${RABBITMQ_HOME}/erts-'"${ERTS_VSN}"'/bin/:' \
    "${TARGET_DIR}"/sbin/rabbitmq-defaults.tmp >"${TARGET_DIR}"/sbin/rabbitmq-defaults.tmp1 \
    && sed -e 's:start_clean$:"\${SYS_PREFIX}/releases/'"${VERSION}"'/start_clean":' \
    "${TARGET_DIR}"/sbin/rabbitmq-defaults.tmp1 >"${TARGET_DIR}"/sbin/rabbitmq-defaults.tmp \
    && sed -e 's:start_sasl:"\${SYS_PREFIX}/releases/'"${VERSION}"'/start_sasl":' \
    "${TARGET_DIR}"/sbin/rabbitmq-defaults.tmp >"${TARGET_DIR}"/sbin/rabbitmq-defaults

chmod 0755 "${TARGET_DIR}"/sbin/rabbitmq-defaults
