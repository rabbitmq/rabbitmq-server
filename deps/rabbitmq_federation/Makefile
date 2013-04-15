include ../umbrella.mk

OTHER_NODE=undefined
OTHER_PORT=undefined
OTHER_PLUGINS=undefined
BASEDIR=${TMPDIR}/rabbitmq-federation-tests/$(OTHER_NODE)
PID_FILE=$(BASEDIR)/$(OTHER_NODE).pid

start-other-node:
	rm -f $(PID_FILE)
	RABBITMQ_MNESIA_BASE=$(BASEDIR)/rabbitmq-$(OTHER_NODE)-mnesia \
	RABBITMQ_LOG_BASE=$(BASEDIR) \
	RABBITMQ_NODENAME=$(OTHER_NODE) \
	RABBITMQ_NODE_PORT=$(OTHER_PORT) \
	RABBITMQ_ENABLED_PLUGINS_FILE=${OTHER_PLUGINS} \
	RABBITMQ_PLUGINS_DIR=${TMPDIR}/rabbitmq-test/plugins \
	RABBITMQ_PLUGINS_EXPAND_DIR=$(BASEDIR)/rabbitmq-$(OTHER_NODE)-plugins-expand \
	RABBITMQ_PID_FILE=$(PID_FILE) \
	../rabbitmq-server/scripts/rabbitmq-server &
	../rabbitmq-server/scripts/rabbitmqctl -n $(OTHER_NODE) wait $(PID_FILE)
	sh -e etc/$(OTHER_CONFIG).sh "../rabbitmq-server/scripts/rabbitmqctl -n $(OTHER_NODE)"

stop-other-node:
	../rabbitmq-server/scripts/rabbitmqctl -n $(OTHER_NODE) stop 2> /dev/null || true
