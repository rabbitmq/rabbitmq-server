PROJECT = rabbitmq_shovel
PROJECT_DESCRIPTION = Data Shovel for RabbitMQ
PROJECT_MOD = rabbit_shovel

define PROJECT_ENV
[
	    {defaults, [
	        {prefetch_count,     1000},
	        {ack_mode,           on_confirm},
	        {publish_fields,     []},
	        {publish_properties, []},
	        {reconnect_delay,    5}
	      ]}
	  ]
endef

ifneq ($(RABBITMQ_VERSION),)
define PROJECT_APP_EXTRA_KEYS
{broker_version_requirements, ["$(RABBITMQ_VERSION)"]}
endef
endif

DEPS = rabbit_common rabbit amqp_client
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
include erlang.mk
