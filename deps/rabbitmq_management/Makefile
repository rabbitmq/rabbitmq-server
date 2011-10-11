include ../umbrella.mk

RABBITMQCTL=../rabbitmq-server/scripts/rabbitmqctl
TEST_TMPDIR=/tmp/rabbitmq-test


start-second-node:
	rm -f $(TEST_TMPDIR)/rabbitmq-hare-pid
	RABBITMQ_MNESIA_BASE=$(TEST_TMPDIR)/rabbitmq-hare-mnesia \
	RABBITMQ_PID_FILE=$(TEST_TMPDIR)/rabbitmq-hare-pid \
	RABBITMQ_LOG_BASE=$(TEST_TMPDIR)/log \
	RABBITMQ_NODENAME=hare \
	RABBITMQ_NODE_PORT=5673 \
	RABBITMQ_CONFIG_FILE=etc/hare \
	RABBITMQ_PLUGINS_DIR=$(TEST_TMPDIR)/plugins \
	RABBITMQ_PLUGINS_EXPAND_DIR=$(TEST_TMPDIR)/hare-plugins-expand \
	../rabbitmq-server/scripts/rabbitmq-server &
	$(RABBITMQCTL) -n hare wait $(TEST_TMPDIR)/rabbitmq-hare-pid
	$(RABBITMQCTL) -n hare stop_app
	$(RABBITMQCTL) -n hare reset
	$(RABBITMQCTL) -n hare cluster rabbit-test@`hostname -s`
	$(RABBITMQCTL) -n hare start_app

stop-second-node:
	$(RABBITMQCTL) -n hare stop