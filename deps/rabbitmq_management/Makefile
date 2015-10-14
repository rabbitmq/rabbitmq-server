PROJECT = rabbitmq_management

DEPS = amqp_client webmachine rabbitmq_web_dispatch rabbitmq_management_agent

DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
include erlang.mk

WITH_BROKER_TEST_COMMANDS := rabbit_shovel_test_all:all_tests()

# XXX

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
