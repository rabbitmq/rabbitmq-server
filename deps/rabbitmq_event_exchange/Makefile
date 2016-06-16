PROJECT = rabbitmq_event_exchange

TEST_DEPS += amqp_client
TEST_DEPS += rabbit 

DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.
ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk

# --------------------------------------------------------------------
# Testing.
# --------------------------------------------------------------------
TEST_DEPS := $(filter-out rabbitmq_test,$(TEST_DEPS))

include erlang.mk
