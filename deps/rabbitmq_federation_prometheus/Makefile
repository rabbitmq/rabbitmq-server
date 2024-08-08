PROJECT = rabbitmq_federation_prometheus
PROJECT_DESCRIPTION = Exposes rabbitmq_federation metrics to Prometheus
PROJECT_MOD = rabbit_federation_prometheus_app

define PROJECT_APP_EXTRA_KEYS
	{broker_version_requirements, []}
endef

DEPS = rabbit_common rabbit rabbitmq_federation rabbitmq_prometheus
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers eunit_formatters

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk
