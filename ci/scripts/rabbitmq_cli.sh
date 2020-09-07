#!/bin/bash

set -euo pipefail

cd /workspace/rabbitmq/deps/$project

trap 'catch $?' EXIT

catch() {
    buildevents cmd ${GITHUB_RUN_ID} ${project} stop-node -- \
        make stop-node -C ../.. \
            DEPS_DIR=/workspace/rabbitmq/deps \
            PLUGINS='rabbitmq_federation rabbitmq_stomp' \
            PROJECT_VERSION=3.9.0

    if [ "$1" != "0" ]; then
        tar -c -f - /tmp/rabbitmq-test-instances/*/log | \
        xz > /broker-logs/broker-logs.tar.xz
    fi

    buildevents step ${GITHUB_RUN_ID} ${project} ${STEP_START} ${project}
}

buildevents cmd ${GITHUB_RUN_ID} ${project} make -- \
            make DEPS_DIR=/workspace/rabbitmq/deps

buildevents cmd ${GITHUB_RUN_ID} ${project} start-background-broker -- \
            make start-background-broker \
                -C ../.. \
                DEPS_DIR=/workspace/rabbitmq/deps \
                PLUGINS='rabbitmq_federation rabbitmq_stomp' \
                PROJECT_VERSION=3.9.0

buildevents cmd ${GITHUB_RUN_ID} ${project} rebar -- \
        mix local.rebar --force

# due to https://github.com/elixir-lang/elixir/issues/7699 we
# "run" the tests, but skip them all, in order to trigger
# compilation of all *_test.exs files before we actually run themq
buildevents cmd ${GITHUB_RUN_ID} ${project} compile-tests -- \
        make tests \
                MIX_TEST_OPTS="--exclude test" \
                DEPS_DIR=/workspace/rabbitmq/deps

buildevents cmd ${GITHUB_RUN_ID} ${project} tests -- \
        make tests \
                DEPS_DIR=/workspace/rabbitmq/deps
