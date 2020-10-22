#!/bin/bash

set -euo pipefail

cd /workspace/rabbitmq/deps/$project

trap 'catch $?' EXIT

SPAN_ID=${GITHUB_RUN_ID}-${project}

catch() {
    buildevents cmd ${GITHUB_RUN_ID} ${SPAN_ID} stop-node -- \
        make stop-node -C ../.. \
            DEPS_DIR=/workspace/rabbitmq/deps \
            PLUGINS='rabbitmq_federation rabbitmq_stomp'

    if [ "$1" != "0" ]; then
        tar -c -f - /tmp/rabbitmq-test-instances/*/log | \
        xz > /workspace/broker-logs/broker-logs.tar.xz
    fi

    buildevents step ${GITHUB_RUN_ID} ${SPAN_ID} ${STEP_START} ${project}
}

buildevents cmd ${GITHUB_RUN_ID} ${SPAN_ID} make -- \
            make DEPS_DIR=/workspace/rabbitmq/deps

buildevents cmd ${GITHUB_RUN_ID} ${SPAN_ID} start-background-broker -- \
            make start-background-broker \
                -C ../.. \
                DEPS_DIR=/workspace/rabbitmq/deps \
                PLUGINS='rabbitmq_federation rabbitmq_stomp'

buildevents cmd ${GITHUB_RUN_ID} ${SPAN_ID} rebar -- \
        mix local.rebar --force

# due to https://github.com/elixir-lang/elixir/issues/7699 we
# "run" the tests, but skip them all, in order to trigger
# compilation of all *_test.exs files before we actually run themq
buildevents cmd ${GITHUB_RUN_ID} ${SPAN_ID} compile-tests -- \
        make tests \
                MIX_TEST_OPTS="--exclude test" \
                DEPS_DIR=/workspace/rabbitmq/deps

# rabbitmq-diagnostics erlang-cookie-sources reads USER from then env
export USER=$(whoami)
buildevents cmd ${GITHUB_RUN_ID} ${SPAN_ID} tests -- \
        make tests \
                DEPS_DIR=/workspace/rabbitmq/deps
