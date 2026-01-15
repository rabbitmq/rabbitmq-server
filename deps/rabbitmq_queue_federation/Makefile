PROJECT = rabbitmq_queue_federation
PROJECT_DESCRIPTION = RabbitMQ Queue Federation
PROJECT_MOD = rabbit_queue_federation_app

define PROJECT_ENV
[
	    {pgroup_name_cluster_id, false},
	    {connection_close_timeout, 5000}
	  ]
endef

define PROJECT_APP_EXTRA_KEYS
	{broker_version_requirements, []}
endef

DEPS = rabbit_common rabbit amqp_client rabbitmq_federation_common
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers

PLT_APPS += rabbitmq_cli

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk
