#!/usr/bin/env bash

LOCAL_SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

rabbitmq() {

  IMAGE_TAG=${IMAGE_TAG:-69a4159f3482e5212d364f499b2ca2e05bede0ca-otp-min}
  IMAGE=${IMAGE:-pivotalrabbitmq/rabbitmq}

  echo "Running RabbitMQ ${IMAGE}:${IMAGE_TAG} with $RABBITMQ_CONFIG & ${LOCAL_SCRIPT}/enabled_plugins"


  docker network inspect rabbitmq_net >/dev/null 2>&1 || docker network create rabbitmq_net
  docker rm -f local-rabbitmq 2>/dev/null || echo "rabbitmq was not running"
  docker run -d --name local-rabbitmq --net rabbitmq_net \
      -p 15672:15672 -p 5672:5672 \
      -v ${RABBITMQ_CONFIG}:/etc/rabbitmq/rabbitmq.config:ro \
      -v ${LOCAL_SCRIPT}/enabled_plugins:/etc/rabbitmq/enabled_plugins \
      ${IMAGE}:${IMAGE_TAG}
}
