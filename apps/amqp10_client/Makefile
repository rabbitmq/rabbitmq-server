PROJECT = amqp10_client
PROJECT_DESCRIPTION = AMQP 1.0 client from the RabbitMQ Project
PROJECT_MOD = amqp10_client_app

# Make sure we know where the applications are located.
ROOT_DIR ?= $(realpath ../..)
APPS_DIR ?= $(realpath ..)
DEPS_DIR ?= $(realpath ../../deps)

BUILD_DEPS = elvis_mk
TEST_DEPS = rabbitmq_ct_helpers
LOCAL_DEPS = ssl inets crypto amqp10_common

DEP_EARLY_PLUGINS = $(PROJECT)/../rabbit_common/mk/rabbitmq-early-test.mk
DEP_PLUGINS = $(PROJECT)/../rabbit_common/mk/rabbitmq-macros.mk \
	      $(PROJECT)/../rabbit_common/mk/rabbitmq-hexpm.mk \
	      $(PROJECT)/../rabbit_common/mk/rabbitmq-dist.mk \
	      $(PROJECT)/../rabbit_common/mk/rabbitmq-run.mk \
	      $(PROJECT)/../rabbit_common/mk/rabbitmq-test.mk \
	      $(PROJECT)/../rabbit_common/mk/rabbitmq-tools.mk

DEP_PLUGINS += elvis_mk
dep_elvis_mk = git https://github.com/inaka/elvis.mk.git master

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include $(ROOT_DIR)/rabbitmq-components.mk
include $(ROOT_DIR)/erlang.mk

# --------------------------------------------------------------------
# Compiler flags.
# --------------------------------------------------------------------

# gen_fsm is deprecated starting from Erlang 20, but we want to support
# Erlang 19 as well.

ERTS_VER := $(shell erl -version 2>&1 | sed -E 's/.* version //')
ERLANG_20_ERTS_VER := 9.0

ifeq ($(call compare_version,$(ERTS_VER),$(ERLANG_20_ERTS_VER),>=),true)
ERLC_OPTS += -Dnowarn_deprecated_gen_fsm
endif

# Dialyze the tests.
DIALYZER_OPTS += --src -r test

# --------------------------------------------------------------------
# ActiveMQ for the testsuite.
# --------------------------------------------------------------------

ACTIVEMQ_VERSION := 5.14.4
ACTIVEMQ_URL := 'https://archive.apache.org/dist/activemq/$(ACTIVEMQ_VERSION)/apache-activemq-$(ACTIVEMQ_VERSION)-bin.tar.gz'

ACTIVEMQ := $(abspath test/system_SUITE_data/apache-activemq-$(ACTIVEMQ_VERSION)/bin/activemq)
export ACTIVEMQ

$(ACTIVEMQ): \
  test/system_SUITE_data/apache-activemq-$(ACTIVEMQ_VERSION)-bin.tar.gz
	$(gen_verbose) cd "$(dir $<)" && tar zxf "$(notdir $<)"

test/system_SUITE_data/apache-activemq-$(ACTIVEMQ_VERSION)-bin.tar.gz:
	$(gen_verbose) $(call core_http_get,$@,$(ACTIVEMQ_URL))

tests:: $(ACTIVEMQ)

ct ct-system: $(ACTIVEMQ)
