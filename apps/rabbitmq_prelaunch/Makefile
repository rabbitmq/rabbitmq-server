PROJECT = rabbitmq_prelaunch
PROJECT_DESCRIPTION = RabbitMQ prelaunch setup
PROJECT_VERSION = 1.0.0
PROJECT_MOD = rabbit_prelaunch_app

# Make sure we know where the applications are located.
ROOT_DIR ?= $(realpath ../..)
APPS_DIR ?= $(realpath ..)
DEPS_DIR ?= $(realpath ../../deps)

LOCAL_DEPS = rabbit_common
DEPS = cuttlefish jsx
dep_cuttlefish = hex 3.0.1

DEP_PLUGINS = $(PROJECT)/../rabbit_common/mk/rabbitmq-build.mk

include $(ROOT_DIR)/rabbitmq-components.mk
include $(ROOT_DIR)/erlang.mk
