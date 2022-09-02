#!/usr/bin/env bash

LOCAL_SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

RABBITMQ_IMAGE_TAG=${RABBITMQ_IMAGE_TAG:-69a4159f3482e5212d364f499b2ca2e05bede0ca-otp-min}
RABBITMQ_IMAGE=${RABBITMQ_IMAGE:-pivotalrabbitmq/rabbitmq}

echo "Running RabbitMQ ${RABBITMQ_IMAGE}:${RABBITMQ_IMAGE_TAG} with $RABBITMQ_CONFIG & $ENABLED_PLUGINS"


docker network inspect rabbitmq_net >/dev/null 2>&1 || docker network create rabbitmq_net
docker rm -f rabbitmq 2>/dev/null || echo "rabbitmq was not running"
docker run -d --name rabbitmq --net rabbitmq_net \
    -p 15672:15672 -p 5672:5672 \
    -v ${RABBITMQ_CONFIG}:/etc/rabbitmq/rabbitmq.config:ro \
    -v ${ENABLED_PLUGINS}:/etc/rabbitmq/enabled_plugins \
    ${RABBITMQ_IMAGE}:${RABBITMQ_IMAGE_TAG}
