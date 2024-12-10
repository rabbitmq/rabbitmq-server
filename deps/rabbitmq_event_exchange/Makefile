PROJECT = rabbitmq_event_exchange
PROJECT_DESCRIPTION = Event Exchange Type

<<<<<<< HEAD
=======
define PROJECT_ENV
	  [
		{protocol, amqp_0_9_1}
	  ]
endef

>>>>>>> f3540ee7d2 (web_mqtt_shared_SUITE: propagate flow_classic_queue to mqtt_shared_SUITE #12907 12906)
define PROJECT_APP_EXTRA_KEYS
	{broker_version_requirements, []}
endef

DEPS = rabbit_common rabbit
<<<<<<< HEAD
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers amqp_client
=======
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers amqp_client rabbitmq_amqp_client
>>>>>>> f3540ee7d2 (web_mqtt_shared_SUITE: propagate flow_classic_queue to mqtt_shared_SUITE #12907 12906)

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk
