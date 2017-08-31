PROJECT = rabbitmq_auth_backend_cache

DEPS = rabbit_common
TEST_DEPS += rabbit rabbitmq_ct_helpers rabbitmq_ct_client_helpers

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk

# FIXME: Remove rabbitmq_test as TEST_DEPS from here for now.
TEST_DEPS := $(filter-out rabbitmq_test,$(TEST_DEPS))

include erlang.mk

