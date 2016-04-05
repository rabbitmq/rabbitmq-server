#!/bin/sh -eux
# Prepare and run a smoke test against the RabbitMQ OCF RA only if
# the scripts/rabbitmq-server-ha.ocf has changes
if ! git diff HEAD~ --name-only | grep -q scripts/rabbitmq-server-ha.ocf
then
  exit 0
fi

export VAGRANT_VERSION=1.8.1
export DOCKER_IMAGE=bogdando/rabbitmq-cluster-ocf-wily
export UPLOAD_METHOD=none
export DOCKER_MOUNTS="$(pwd)/scripts/rabbitmq-server-ha.ocf:/tmp/rabbitmq-server-ha"

# Install vagrant and requirements
sudo apt-get install -qq git wget
wget --no-verbose https://releases.hashicorp.com/vagrant/${VAGRANT_VERSION}/vagrant_${VAGRANT_VERSION}_x86_64.deb
sudo dpkg -i --force-all ./vagrant_${VAGRANT_VERSION}_x86_64.deb
vagrant plugin install vagrant-triggers

# Update docker and prepare images
sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get -y -o Dpkg::Options::="--force-confdef" -o Dpkg::Options::="--force-confold" install --only-upgrade docker-engine
sudo service docker restart
docker pull $DOCKER_IMAGE

# Prepare and run a smoke test for a rabbitmq cluster by the OCF RA
git clone https://github.com/bogdando/rabbitmq-cluster-ocf-vagrant.git
cd ./rabbitmq-cluster-ocf-vagrant
vagrant up --provider docker
docker exec -it n1 /bin/bash /vagrant/vagrant_script/test_rabbitcluster.sh rabbit@n1 rabbit@n2
