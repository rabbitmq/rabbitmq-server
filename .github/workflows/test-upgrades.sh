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

old_dist=OLD
new_dist=NEW
old_rabbitmqctl=OLD/sbin/rabbitmqctl
new_rabbitmqctl=NEW/sbin/rabbitmqctl

echo "== Start cluster with old RabbitMQ version"
$make -C "$old_dist" start-cluster RABBITMQ_FEATURE_FLAGS=-khepri_db
$old_rabbitmqctl -n rabbit-1 cluster_status

case "$scenario" in
	durable_queue)
		# Publish 1000 messages to one durable classic queue.
		echo "=== Publish 1000 messages to a durable classic queue"
		omq --uri amqp://localhost:5672 amqp091 -C 1000 -x 1 -y 0 --queues classic

		# Ensure we have this queue with 1000 messages.
		$old_rabbitmqctl -n rabbit-1 list_queues name messages | grep -qE 'omq-0\s*1000'

		# Upgrade RabbitMQ.
		echo "=== Upgrade cluster to new RabbitMQ version"
		$make -C "$new_dist" restart-cluster

		# Ensure the queue is still there with its messages.
		$new_rabbitmqctl -n rabbit-1 list_queues name messages | grep -qE 'omq-0\s*1000'
		;;

	transient_queue)
		# Declare a transient classic queue.
		echo "=== Declare a transient classic queue"
		omq --uri amqp://localhost:5674 amqp091 -x 1 -y 1 -r 100 --queues exclusive &
		sleep 1

		# Ensure we the queue exists.
		# omq 0.49.0+ appends a random hex suffix to exclusive queue names.
		$old_rabbitmqctl -n rabbit-1 list_queues name durable | grep -qE 'omq-0[-0-9a-f]*\s+false'

		# Upgrade RabbitMQ on two out of three nodes.
		# We can't upgrade all of them because omq(1) would be
		# disconnected and the transient queue would be deleted.
		echo "=== Upgrade cluster to new RabbitMQ version (2 out of 3 nodes only)"
		$make -C "$new_dist" restart-cluster NODES=2

		# Ensure the queue is still there.
		$new_rabbitmqctl -n rabbit-1 list_queues name durable | grep -qE 'omq-0[-0-9a-f]*\s+false'

		# Stop omq(1)
		pkill omq
		wait
		;;

	10k_queues_import)
		# Import 10k classic queues
		echo "=== Import 10k classic queues"
		git clone https://github.com/rabbitmq/sample-configs.git
		time $old_rabbitmqctl  -n rabbit-1 import_definitions \
			sample-configs/queues/10k-classic-queues.json

		# Ensure we have 10k queues
		test \
			$($old_rabbitmqctl -n rabbit-1 list_queues name durable | \
			grep -cE 'q[0-9]+\s*true') = 10000

		# Upgrade RabbitMQ.
		echo "=== Upgrade cluster to new RabbitMQ version"
		$make -C "$new_dist" restart-cluster

		# Ensure we still have 10k queues
		test \
			$($new_rabbitmqctl -n rabbit-1 list_queues name durable | \
			grep -cE 'q[0-9]+\s*true') = 10000
		;;

	topic_bindings_import)
		# Generate 10k bindings inline.
		echo "=== Generate 10k topic bindings definitions"
		python3 - <<-'PY' > definitions.json
		import json
		count = 10000
		definitions = {
		    "rabbit_version": "4.0.0",
		    "users": [],
		    "vhosts": [{"name": "/"}],
		    "permissions": [],
		    "policies": [],
		    "queues": [{"name": "q0", "vhost": "/", "durable": True,
		                "auto_delete": False, "arguments": {}}],
		    "exchanges": [{"name": "test", "vhost": "/", "type": "topic",
		                   "durable": True, "auto_delete": False,
		                   "internal": False, "arguments": {}}],
		    "bindings": [
		        {"source": "test", "vhost": "/", "destination": "q0",
		         "destination_type": "queue",
		         "routing_key": "test.q0.%d" % i,
		         "arguments": {}}
		        for i in range(count)
		    ],
		}
		print(json.dumps(definitions))
		PY

		echo "=== Import 10k topic bindings"
		time $old_rabbitmqctl -n rabbit-1 import_definitions definitions.json

		# Ensure we have 10k bindings
		test \
			$($old_rabbitmqctl -n rabbit-1 list_bindings routing_key | \
			grep -cE 'test\.q0.[0-9]+') = 10000

		# Upgrade RabbitMQ.
		echo "=== Upgrade cluster to new RabbitMQ version"
		$make -C "$new_dist" restart-cluster

		# Ensure we still have 10k bindings
		test \
			$($new_rabbitmqctl -n rabbit-1 list_bindings routing_key | \
			grep -cE 'test\.q0.[0-9]+') = 10000
		;;

	*)
		echo "ERROR: Unknow scenario '$scenario'" >&2
		exit 1
		;;
esac

echo "=== Stop RabbitMQ cluster"
$make -C "$new_dist" stop-cluster
