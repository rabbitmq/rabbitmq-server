PROJECT = rabbitmq_federation
PROJECT_DESCRIPTION = RabbitMQ Federation
PROJECT_MOD = rabbit_federation_app

define PROJECT_ENV
[
	    {pgroup_name_cluster_id, false},
	    {internal_exchange_check_interval, 90000}
	  ]
endef

define PROJECT_APP_EXTRA_KEYS
	{broker_version_requirements, []}
endef

DEPS = rabbit_common rabbit amqp_client
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

include rabbitmq-components.mk
include erlang.mk
