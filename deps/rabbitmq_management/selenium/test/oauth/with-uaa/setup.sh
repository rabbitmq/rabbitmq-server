#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export RABBITMQ_CONFIG=${SCRIPT}/${RABBITMQ_CONFIG_FILE:-rabbitmq.config}
export UAA_CONFIG=${SCRIPT}/${UAA_CONFIG_FOLDER:-uaa}

. $SCRIPT/../../../bin/rabbitmq.sh

start_uaa() {

  UAA_IMAGE_TAG=${UAA_IMAGE_TAG:-75.21.0}
  UAA_IMAGE_NAME=${UAA_IMAGE_NAME:-cloudfoundry/uaa}

  docker network inspect rabbitmq_net >/dev/null 2>&1 || docker network create rabbitmq_net
  docker rm -f uaa 2>/dev/null || echo "uaa was not running"

  echo "Running ${UAA_IMAGE_NAME}:${UAA_IMAGE_TAG} docker image with .."

  docker run \
  		--detach \
      --name uaa --net rabbitmq_net \
  		--publish 8080:8080 \
  		--mount "type=bind,source=${UAA_CONFIG},target=/uaa" \
  		--env UAA_CONFIG_PATH="/uaa" \
  		--env JAVA_OPTS="-Djava.security.egd=file:/dev/./urandom" \
      "${UAA_IMAGE_NAME}:${UAA_IMAGE_TAG}"

}

start_rabbitmq
start_uaa
