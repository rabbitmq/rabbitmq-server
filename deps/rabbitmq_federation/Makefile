include ../umbrella.mk

OTHER_NODE=undefined
OTHER_PORT=undefined
OTHER_CONFIG=undefined
PID_FILE=/tmp/$(OTHER_NODE).pid

start-other-node:
	RABBITMQ_MNESIA_BASE=/tmp/rabbitmq-$(OTHER_NODE)-mnesia \
	RABBITMQ_LOG_BASE=/tmp \
	RABBITMQ_NODENAME=$(OTHER_NODE) \
	RABBITMQ_NODE_PORT=$(OTHER_PORT) \
	RABBITMQ_CONFIG_FILE=etc/$(OTHER_CONFIG) \
	RABBITMQ_PLUGINS_DIR=/tmp/rabbitmq-test/plugins \
	RABBITMQ_PLUGINS_EXPAND_DIR=/tmp/rabbitmq-$(OTHER_NODE)-plugins-expand \
	RABBITMQ_PID_FILE=$(PID_FILE) \
	../rabbitmq-server/scripts/rabbitmq-server &
	../rabbitmq-server/scripts/rabbitmqctl -n $(OTHER_NODE) wait $(PID_FILE)

stop-other-node:
	../rabbitmq-server/scripts/rabbitmqctl -n $(OTHER_NODE) stop 2> /dev/null || true
