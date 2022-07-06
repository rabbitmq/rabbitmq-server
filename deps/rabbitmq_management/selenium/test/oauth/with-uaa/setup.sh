#!/usr/bin/env bash

SCRIPT="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export RABBITMQ_CONFIG=${SCRIPT}/rabbitmq.config

. $SCRIPT/../../../bin/rabbitmq.sh

uaa() {

  UAA_IMAGE_TAG=${UAA_IMAGE_TAG:-latest}

  docker network inspect rabbitmq_selenimum_net >/dev/null 2>&1 || docker network create rabbitmq_selenimum_net
  docker rm -f uaa 2>/dev/null || echo "uaa was not running"

  echo "Running uaa:${UAA_IMAGE_TAG} docker image ..."

  docker run \
  		--detach \
      --name local-uaa --net rabbitmq_net \
  		--publish 8080:8080 \
  		--mount type=bind,source=${SCRIPT}/uaa,target=/etc/uaa \
  		--env JAVA_OPTS="-Djava.security.egd=file:/dev/./urandom" \
      uaa:${UAA_IMAGE_TAG}


}

install_uaac() {

  gem list --local | grep cf-uaac || sudo gem install cf-uaac && echo "Already installed"

  target=${UAA_HOST:="http://local-uaa:8080/uaa"}
  # export to use a different ctl, e.g. if the node was built from source

  # Target the server
  uaac target $target

  # Get admin client to manage UAA
  uaac token client get admin -s adminsecret

  # Set permission to list signing keys
  uaac client update admin --authorities "clients.read clients.secret clients.write uaa.admin clients.admin scim.write scim.read uaa.resource"

}

rabbitmq
uaa
sleep 5
install_uaac
${SCRIPT}/uaa/setup
