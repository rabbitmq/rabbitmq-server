include ../umbrella.mk

OTHER_NODE=undefined
OTHER_PORT=undefined

start-other-node:
	RABBITMQ_MNESIA_BASE=/tmp/rabbitmq-$(OTHER_NODE)-mnesia \
	RABBITMQ_LOG_BASE=/tmp \
	RABBITMQ_NODENAME=$(OTHER_NODE) \
	RABBITMQ_NODE_PORT=$(OTHER_PORT) \
	RABBITMQ_CONFIG_FILE=etc/$(OTHER_NODE) \
	RABBITMQ_PLUGINS_DIR=/tmp/rabbitmq-test/plugins \
	RABBITMQ_PLUGINS_EXPAND_DIR=/tmp/rabbitmq-$(OTHER_NODE)-plugins-expand \
	../rabbitmq-server/scripts/rabbitmq-server -detached
	../rabbitmq-server/scripts/rabbitmqctl -n $(OTHER_NODE) wait

stop-other-node:
	../rabbitmq-server/scripts/rabbitmqctl -n $(OTHER_NODE) stop 2> /dev/null || true
