PROJECT = rabbitmq_shovel_management

DEPS = rabbitmq_management rabbitmq_shovel

DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
include erlang.mk

WITH_BROKER_TEST_MAKEVARS := \
	        RABBITMQ_CONFIG_FILE=$(CURDIR)/etc/rabbit-test
WITH_BROKER_TEST_COMMANDS := rabbit_shovel_mgmt_test_all:all_tests()
