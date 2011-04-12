include ../umbrella.mk

OTHER_NODE=bunny
OTHER_PORT=5673

test: cleantest stop-other-node

cleantest:
	rm -rf tmp /tmp/rabbitmq-$(OTHER_NODE)-mnesia

start-other-node:
	RABBITMQ_MNESIA_BASE=/tmp/rabbitmq-$(OTHER_NODE)-mnesia \
	RABBITMQ_LOG_BASE=/tmp \
	RABBITMQ_NODENAME=$(OTHER_NODE) \
	RABBITMQ_NODE_PORT=$(OTHER_PORT) \
	RABBITMQ_CONFIG_FILE=etc/test-other \
	RABBITMQ_SERVER_ERL_ARGS="-rabbit_mochiweb port 5$(OTHER_PORT)" \
	../rabbitmq-server/scripts/rabbitmq-server -detached
	../rabbitmq-server/scripts/rabbitmqctl -n $(OTHER_NODE) wait

stop-other-node:
	../rabbitmq-server/scripts/rabbitmqctl -n $(OTHER_NODE) stop || true
