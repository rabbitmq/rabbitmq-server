PROJECT = rabbitmq_auth_backend_internal_loopback
PROJECT_DESCRIPTION = RabbitMQ Internal Loopback Authentication Backend
PROJECT_MOD = rabbit_auth_backend_internal_loopback_app

define PROJECT_ENV
[

	  ]
endef

define PROJECT_APP_EXTRA_KEYS
	{broker_version_requirements, []}
endef

DEPS = rabbit_common rabbit
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk
