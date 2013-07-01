include ../umbrella.mk

RABBITMQCTL=../rabbitmq-server/scripts/rabbitmqctl
TEST_TMPDIR=$(TMPDIR)/rabbitmq-test
OTHER_NODE=undefined
OTHER_PORT=undefined

start-other-node:
	rm -f $(TEST_TMPDIR)/rabbitmq-$(OTHER_NODE)-pid
	RABBITMQ_MNESIA_BASE=$(TEST_TMPDIR)/rabbitmq-$(OTHER_NODE)-mnesia \
	RABBITMQ_PID_FILE=$(TEST_TMPDIR)/rabbitmq-$(OTHER_NODE)-pid \
	RABBITMQ_LOG_BASE=$(TEST_TMPDIR)/log \
	RABBITMQ_NODENAME=$(OTHER_NODE) \
	RABBITMQ_NODE_PORT=$(OTHER_PORT) \
	RABBITMQ_CONFIG_FILE=etc/$(OTHER_NODE) \
	RABBITMQ_PLUGINS_DIR=$(TEST_TMPDIR)/plugins \
	RABBITMQ_PLUGINS_EXPAND_DIR=$(TEST_TMPDIR)/$(OTHER_NODE)-plugins-expand \
	../rabbitmq-server/scripts/rabbitmq-server >/tmp/$(OTHER_NODE).out 2>/tmp/$(OTHER_NODE).err &
	$(RABBITMQCTL) -n $(OTHER_NODE) wait $(TEST_TMPDIR)/rabbitmq-$(OTHER_NODE)-pid

cluster-other-node:
	$(RABBITMQCTL) -n $(OTHER_NODE) stop_app
	$(RABBITMQCTL) -n $(OTHER_NODE) reset
	$(RABBITMQCTL) -n $(OTHER_NODE) join_cluster rabbit-test@`hostname -s`
	$(RABBITMQCTL) -n $(OTHER_NODE) start_app

stop-other-node:
	$(RABBITMQCTL) -n $(OTHER_NODE) stop
