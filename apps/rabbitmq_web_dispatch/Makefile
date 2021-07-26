PROJECT = rabbitmq_web_dispatch
PROJECT_DESCRIPTION = RabbitMQ Web Dispatcher
PROJECT_MOD = rabbit_web_dispatch_app

define PROJECT_APP_EXTRA_KEYS
	{broker_version_requirements, []}
endef

# Make sure we know where the applications are located.
ROOT_DIR ?= $(realpath ../..)
APPS_DIR ?= $(realpath ..)
DEPS_DIR ?= $(realpath ../../deps)

LOCAL_DEPS = inets rabbit_common rabbit
DEPS = cowboy
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers

dep_cowboy = hex 2.0.0

DEP_EARLY_PLUGINS = $(PROJECT)/../rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = $(PROJECT)/../rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include $(ROOT_DIR)/rabbitmq-components.mk
TEST_DEPS := $(filter-out rabbitmq_test,$(TEST_DEPS))
include $(ROOT_DIR)/erlang.mk
