PROJECT = trust_store_http
PROJECT_DESCRIPTION = Trust store HTTP server
PROJECT_VERSION = 1.0.0

define PROJECT_APP_EXTRA_KEYS
	{broker_version_requirements, []}
endef

LOCAL_DEPS = ssl
DEPS = cowboy thoas

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk
