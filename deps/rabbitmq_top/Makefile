PROJECT = rabbitmq_top
PROJECT_DESCRIPTION = RabbitMQ Top
PROJECT_MOD = rabbit_top_app

define PROJECT_APP_EXTRA_KEYS
	{broker_version_requirements, []}
endef

DEPS = rabbit_common rabbit amqp_client rabbitmq_management

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# Make sure we know where the applications are located.
ROOT_DIR ?= ../..
DEPS_DIR ?= ../../deps

include $(ROOT_DIR)/rabbitmq-components.mk
include $(ROOT_DIR)/erlang.mk
