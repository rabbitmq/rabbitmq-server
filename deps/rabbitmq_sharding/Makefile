include ../umbrella.mk

RABBITMQCTL=../rabbitmq-server/scripts/rabbitmqctl
NODENAME=rabbit-test
MAIN_NODE=undefined
OTHER_NODE=undefined
OTHER_PORT=undefined
OTHER_PLUGINS=other_plugins
BASEDIR=${TMPDIR}/rabbitmq-sharding-tests/$(OTHER_NODE)
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
	../rabbitmq-server/scripts/rabbitmq-server >${TMPDIR}/$(OTHER_NODE).out 2>${TMPDIR}/$(OTHER_NODE).err &
	$(RABBITMQCTL) -n $(OTHER_NODE) wait $(PID_FILE)

stop-other-node:
	$(RABBITMQCTL) -n $(OTHER_NODE) stop

cluster-other-node:
	$(RABBITMQCTL) -n $(OTHER_NODE) stop_app
	$(RABBITMQCTL) -n $(OTHER_NODE) reset
	$(RABBITMQCTL) -n $(OTHER_NODE) join_cluster $(MAIN_NODE)
	$(RABBITMQCTL) -n $(OTHER_NODE) start_app

reset-other-node:
	$(RABBITMQCTL) -n $(OTHER_NODE) stop_app
	$(RABBITMQCTL) -n $(OTHER_NODE) reset
	$(RABBITMQCTL) -n $(OTHER_NODE) start_app
