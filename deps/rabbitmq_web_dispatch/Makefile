PROJECT = rabbitmq_web_dispatch
PROJECT_DESCRIPTION = RabbitMQ Web Dispatcher
PROJECT_MOD = rabbit_web_dispatch_app

define PROJECT_APP_EXTRA_KEYS
	{broker_version_requirements, []}
endef

LOCAL_DEPS = inets
DEPS = amqp_client rabbit_common rabbit cowboy
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

include ../../rabbitmq-components.mk
TEST_DEPS := $(filter-out rabbitmq_test,$(TEST_DEPS))
include ../../erlang.mk
