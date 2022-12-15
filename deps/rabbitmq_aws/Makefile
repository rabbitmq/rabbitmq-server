PROJECT = rabbitmq_aws
PROJECT_DESCRIPTION = A minimalistic AWS API interface used by rabbitmq-autocluster (3.6.x) and other RabbitMQ plugins
PROJECT_MOD = rabbitmq_aws_app
PROJECT_REGISTERED = rabbitmq_aws

define PROJECT_ENV
[]
endef

LOCAL_DEPS = crypto inets ssl xmerl
BUILD_DEPS = rabbit_common
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk
TEST_DEPS = meck

# Make sure we know where the applications are located.
ROOT_DIR ?= ../..
DEPS_DIR ?= ../../deps

include $(ROOT_DIR)/rabbitmq-components.mk
include $(ROOT_DIR)/erlang.mk
