PROJECT = rabbitmq_prelaunch
PROJECT_DESCRIPTION = RabbitMQ prelaunch setup
PROJECT_VERSION = 1.0.0
PROJECT_MOD = rabbit_prelaunch_app

DEPS = rabbit_common cuttlefish thoas
dep_cuttlefish = hex 3.1.0

DEP_PLUGINS = rabbit_common/mk/rabbitmq-build.mk


ROOT_DIR ?= ../../../..
DEPS_DIR ?= $(abspath ../../..)
include $(ROOT_DIR)/rabbitmq-components.mk
include $(ROOT_DIR)/erlang.mk
