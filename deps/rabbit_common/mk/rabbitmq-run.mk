.PHONY: run-broker run-broker-deps

ifeq ($(filter rabbitmq-dist.mk,$(notdir $(MAKEFILE_LIST))),)
include $(dir $(lastword $(MAKEFILE_LIST)))rabbitmq-dist.mk
endif

TMPDIR ?= /tmp
TEST_TMPDIR ?= $(TMPDIR)/rabbitmq-test

RABBITMQ_NODENAME ?= rabbit
NODE_TMPDIR ?= $(TEST_TMPDIR)/$(RABBITMQ_NODENAME)

RABBITMQ_PID_FILE ?= $(NODE_TMPDIR)/$(RABBITMQ_NODENAME).pid
RABBITMQ_LOG_BASE ?= $(NODE_TMPDIR)/log
RABBITMQ_MNESIA_BASE ?= $(NODE_TMPDIR)/mnesia
RABBITMQ_PLUGINS_EXPAND_DIR ?= $(NODE_TMPDIR)/plugins
RABBITMQ_ENABLED_PLUGINS_FILE ?= $(NODE_TMPDIR)/enabled_plugins
RABBITMQ_ALLOW_INPUT ?= true

BASIC_SCRIPT_ENV_SETTINGS = \
			    ERL_LIBS="$(CURDIR)/dist:$(filter-out $(DEPS_DIR),$(subst :, ,$(ERL_LIBS)))" \
			    RABBITMQ_NODENAME="$(RABBITMQ_NODENAME)" \
			    RABBITMQ_NODE_IP_ADDRESS="$(RABBITMQ_NODE_IP_ADDRESS)" \
			    RABBITMQ_NODE_PORT="$(RABBITMQ_NODE_PORT)" \
			    RABBITMQ_PID_FILE="$(RABBITMQ_PID_FILE)" \
			    RABBITMQ_LOG_BASE="$(RABBITMQ_LOG_BASE)" \
			    RABBITMQ_MNESIA_BASE="$(RABBITMQ_MNESIA_BASE)" \
			    RABBITMQ_PLUGINS_DIR="$(CURDIR)/dist" \
			    RABBITMQ_PLUGINS_EXPAND_DIR="$(RABBITMQ_PLUGINS_EXPAND_DIR)" \
			    RABBITMQ_ENABLED_PLUGINS_FILE="$(RABBITMQ_ENABLED_PLUGINS_FILE)" \
			    RABBITMQ_ALLOW_INPUT="$(RABBITMQ_ALLOW_INPUT)"

ifeq ($(PROJECT),rabbit)
run-broker:: BROKER_DIR ?= $(CURDIR)
else
RUN_BROKER_DEPS += rabbit
dep_rabbit ?= git file:///home/dumbbell/Projects/pivotal/rabbitmq-public-umbrella/rabbitmq-server erlang.mk

run-broker:: BROKER_DIR ?= $(DEPS_DIR)/rabbit
endif

ALL_RUN_BROKER_DEPS_DIRS = $(addprefix $(DEPS_DIR)/,$(RUN_BROKER_DEPS))

# Targets.

$(foreach dep,$(RUN_BROKER_DEPS),$(eval $(call dep_target,$(dep))))

ifneq ($(SKIP_DEPS),)
run-broker-deps:
else
run-broker-deps: dist $(ALL_RUN_BROKER_DEPS_DIRS)
	$(verbose) for dep in $(ALL_RUN_BROKER_DEPS_DIRS) ; do $(MAKE) -C $$dep IS_DEP=1; done
endif

$(RABBITMQ_ENABLED_PLUGINS_FILE): run-broker-deps dist
	$(gen_verbose) rm -rf $(NODE_TMPDIR)
	$(verbose) mkdir -p $(foreach D,log plugins $(NODENAME),$(NODE_TMPDIR)/$(D))
	$(verbose) $(BASIC_SCRIPT_ENV_SETTINGS) \
	  $(BROKER_DIR)/scripts/rabbitmq-plugins set --offline \
	  $$($(BASIC_SCRIPT_ENV_SETTINGS) \
	  $(BROKER_DIR)/scripts/rabbitmq-plugins list -m | tr '\n' ' ')

run-broker:: run-broker-deps $(RABBITMQ_ENABLED_PLUGINS_FILE)
	$(BASIC_SCRIPT_ENV_SETTINGS) \
	  MAKE="$(MAKE)" \
	  RABBITMQ_SERVER_START_ARGS=$(RABBITMQ_SERVER_START_ARGS) \
	  $(BROKER_DIR)/scripts/rabbitmq-server
