PROJECT = rabbitmq_ct_client_helpers
PROJECT_DESCRIPTION = Common Test helpers for RabbitMQ (client-side helpers)

DEPS = rabbit_common rabbitmq_ct_helpers amqp_client

DEP_PLUGINS = rabbit_common/mk/rabbitmq-build.mk \
	      rabbit_common/mk/rabbitmq-tools.mk

# Make sure we know where the applications are located.
ROOT_DIR ?= ../..
DEPS_DIR ?= ../../deps

include $(ROOT_DIR)/rabbitmq-components.mk
include $(ROOT_DIR)/erlang.mk
