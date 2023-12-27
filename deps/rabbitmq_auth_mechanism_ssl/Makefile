PROJECT = rabbitmq_auth_mechanism_ssl
PROJECT_DESCRIPTION = RabbitMQ SSL authentication (SASL EXTERNAL)
PROJECT_MOD = rabbit_auth_mechanism_ssl_app

define PROJECT_ENV
[
	    {name_from, distinguished_name}
	  ]
endef

define PROJECT_APP_EXTRA_KEYS
	{broker_version_requirements, []}
endef

LOCAL_DEPS = public_key
DEPS = rabbit_common rabbit

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk
