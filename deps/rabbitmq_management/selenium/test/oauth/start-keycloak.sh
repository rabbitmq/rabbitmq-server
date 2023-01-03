#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

ROOT=$SCRIPT/keycloak-localhost

docker network inspect rabbitmq_net >/dev/null 2>&1 || docker network create rabbitmq_net
docker rm -f keycloak 2>/dev/null || echo "keycloak was not running"

echo "Running keycloack docker image ..."

docker run \
		--detach \
		--name keycloak --net rabbitmq_net \
		--publish 8080:8080 \
		--env KEYCLOAK_ADMIN=admin \
		--env KEYCLOAK_ADMIN_PASSWORD=admin \
		--mount type=bind,source=${ROOT}/h2,target=/opt/keycloak/data/h2 \
		quay.io/keycloak/keycloak:18.0.0 start-dev
