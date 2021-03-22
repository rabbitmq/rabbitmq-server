PROJECT = rabbitmq_web_dispatch
PROJECT_DESCRIPTION = RabbitMQ Web Dispatcher
PROJECT_MOD = rabbit_web_dispatch_app

define PROJECT_APP_EXTRA_KEYS
	{broker_version_requirements, []}
endef

LOCAL_DEPS = inets
DEPS = rabbit_common rabbit cowboy
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers

dep_cowboy = hex 2.0.0

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include ../../rabbitmq-components.mk
TEST_DEPS := $(filter-out rabbitmq_test,$(TEST_DEPS))
include ../../erlang.mk
