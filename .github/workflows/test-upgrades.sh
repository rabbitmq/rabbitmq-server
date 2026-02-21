#!/bin/sh

set -ex

scenario=$1
node_count=$2

echo "== Testing scenario '$scenario' with $node_count node(s)"

export NOBUILD=1
export NODES=$node_count
export TEST_TMPDIR=$PWD/test-instances

mv bin/omq_* bin/omq
export PATH=$PWD/bin:$PATH
chmod a+x bin/*

make=${MAKE:-make}

old_dist=OLD/deps/rabbit
new_dist=NEW/deps/rabbit
old_rabbitmqctl=OLD/deps/rabbit/sbin/rabbitmqctl
new_rabbitmqctl=NEW/deps/rabbit/sbin/rabbitmqctl

echo "== Start cluster with old RabbitMQ version"
$make -C "$old_dist" start-cluster RABBITMQ_FEATURE_FLAGS=-khepri_db
$old_rabbitmqctl -n rabbit-1 cluster_status

case "$scenario" in
	durable_queue)
		# Publish 1000 messages to one durable classic queue.
		echo "=== Publish 1000 messages to a durable classic queue"
		omq --uri amqp://localhost:5672 amqp091 -C 1000 -x 1 -y 0 --queues classic

		# Ensure we have this queue with 1000 messages.
		$old_rabbitmqctl -n rabbit-1 list_queues name messages | grep -qE 'omq-1\s*1000'

		# Upgrade RabbitMQ.
		echo "=== Upgrade cluster to new RabbitMQ version"
		$make -C "$new_dist" restart-cluster
 
		# Ensure the queue is still there with its messages.
		$new_rabbitmqctl -n rabbit-1 list_queues name messages | grep -qE 'omq-1\s*1000'
		;;

	transient_queue)
		# Declare a transient classic queue.
		echo "=== Declare a transient classic queue"
		omq --uri amqp://localhost:5674 amqp091 -x 1 -y 1 -r 100 --queues exclusive &
		sleep 1

		# Ensure we the queue exists.
		$old_rabbitmqctl -n rabbit-1 list_queues name durable | grep -qE 'omq-1\s*false'

		# Upgrade RabbitMQ on two out of three nodes.
		# We can't upgrade all of them because omq(1) would be
		# disconnected and the transient queue would be deleted.
		echo "=== Upgrade cluster to new RabbitMQ version (2 out of 3 nodes only)"
		$make -C "$new_dist" restart-cluster NODES=2

		# Ensure the queue is still there.
		$new_rabbitmqctl -n rabbit-1 list_queues name durable | grep -qE 'omq-1\s*false'

		# Stop omq(1)
		pkill omq
		wait
		;;

	100k_queues_import)
		# Import 100k classic queues
		echo "=== Import 100k classic queues"
		git clone https://github.com/rabbitmq/sample-configs.git
		time $old_rabbitmqctl  -n rabbit-1 import_definitions \
			sample-configs/queues/100k-classic-queues.json

		# Ensure we have 100k queues
		test \
			$($old_rabbitmqctl -n rabbit-1 list_queues name durable | \
			grep -cE 'q[0-9]+\s*true') = 100000

		# Upgrade RabbitMQ.
		echo "=== Upgrade cluster to new RabbitMQ version"
		$make -C "$new_dist" restart-cluster

		# Ensure we still have 100k queues
		test \
			$($new_rabbitmqctl -n rabbit-1 list_queues name durable | \
			grep -cE 'q[0-9]+\s*true') = 100000
		;;

	topic_bindings_import)
		# Import topic bindings
		echo "=== Import topic bindings"
		git clone https://github.com/rabbitmq/sample-configs.git
		time $old_rabbitmqctl  -n rabbit-1 import_definitions \
			sample-configs/topic-bindings/q0-with-100k-topic-bindings.json

		# Ensure we have 100k bindings
		test \
			$($old_rabbitmqctl -n rabbit-1 list_bindings routing_key | \
			grep -cE 'test\.q0.[0-9]+') = 100000

		# Upgrade RabbitMQ.
		echo "=== Upgrade cluster to new RabbitMQ version"
		$make -C "$new_dist" restart-cluster

		# Ensure we still have 100k bindings
		test \
			$($new_rabbitmqctl -n rabbit-1 list_bindings routing_key | \
			grep -cE 'test\.q0.[0-9]+') = 100000
		;;

	*)
		echo "ERROR: Unknow scenario '$scenario'" >&2
		exit 1
		;;
esac

echo "=== Stop RabbitMQ cluster"
$make -C "$new_dist" stop-cluster
