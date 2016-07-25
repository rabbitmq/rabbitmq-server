PROJECT = rabbitmq_stomp

DEPS = amqp_client
TEST_DEPS = rabbit rabbitmq_test

DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk

TEST_DEPS := $(filter-out rabbitmq_test,$(TEST_DEPS))

include erlang.mk


