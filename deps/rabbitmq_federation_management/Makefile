PROJECT = rabbitmq_federation_management
PROJECT_DESCRIPTION = RabbitMQ Federation Management

define PROJECT_APP_EXTRA_KEYS
	{broker_version_requirements, []}
endef

DEPS = amqp_client rabbit_common rabbit rabbitmq_management rabbitmq_federation
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk
