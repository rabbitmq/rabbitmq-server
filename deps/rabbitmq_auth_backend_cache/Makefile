PROJECT = rabbitmq_auth_backend_cache
PROJECT_DESCRIPTION = RabbitMQ Authentication Backend cache
PROJECT_MOD = rabbit_auth_backend_cache_app

define PROJECT_ENV
[
	    {cache_ttl,      15000},
	    {cache_module,   rabbit_auth_cache_ets},
	    {cache_module_args, []},
	    {cached_backend, rabbit_auth_backend_internal},
	    {cache_refusals, false}
	  ]
endef

define PROJECT_APP_EXTRA_KEYS
	{broker_version_requirements, []}
endef

DEPS = rabbit_common rabbit
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# Make sure we know where the applications are located.
ROOT_DIR ?= ../..
DEPS_DIR ?= ../../deps

include $(ROOT_DIR)/rabbitmq-components.mk
include $(ROOT_DIR)/erlang.mk
