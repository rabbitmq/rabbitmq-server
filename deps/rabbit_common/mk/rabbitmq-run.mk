.PHONY: run-broker-deps run-broker run-background-broker \
	run-node run-background-node run-tests run-qc \
	start-background-node start-rabbit-on-node \
	stop-rabbit-on-node set-resource-alarm clear-resource-alarm \
	stop-node

ifeq ($(filter rabbitmq-dist.mk,$(notdir $(MAKEFILE_LIST))),)
include $(dir $(lastword $(MAKEFILE_LIST)))rabbitmq-dist.mk
endif

exec_verbose_0 = @echo " EXEC  " $@;
exec_verbose = $(exec_verbose_$(V))

TMPDIR ?= /tmp
TEST_TMPDIR ?= $(TMPDIR)/rabbitmq-test

RABBITMQ_NODENAME ?= rabbit
NODE_TMPDIR ?= $(TEST_TMPDIR)/$(RABBITMQ_NODENAME)

RABBITMQ_PID_FILE ?= $(NODE_TMPDIR)/$(RABBITMQ_NODENAME).pid
RABBITMQ_LOG_BASE ?= $(NODE_TMPDIR)/log
RABBITMQ_MNESIA_BASE ?= $(NODE_TMPDIR)/mnesia
RABBITMQ_PLUGINS_EXPAND_DIR ?= $(NODE_TMPDIR)/plugins
RABBITMQ_ENABLED_PLUGINS_FILE ?= $(NODE_TMPDIR)/enabled_plugins

DIST_ERL_LIBS = $(CURDIR)/dist:$(shell echo "$(filter-out $(DEPS_DIR),$(subst :, ,$(ERL_LIBS)))" | tr ' ' :)

BASIC_SCRIPT_ENV_SETTINGS = \
			    MAKE="$(MAKE)" \
			    ERL_LIBS="$(DIST_ERL_LIBS)" \
			    RABBITMQ_NODENAME="$(RABBITMQ_NODENAME)" \
			    RABBITMQ_NODE_IP_ADDRESS="$(RABBITMQ_NODE_IP_ADDRESS)" \
			    RABBITMQ_NODE_PORT="$(RABBITMQ_NODE_PORT)" \
			    RABBITMQ_PID_FILE="$(RABBITMQ_PID_FILE)" \
			    RABBITMQ_LOG_BASE="$(RABBITMQ_LOG_BASE)" \
			    RABBITMQ_MNESIA_BASE="$(RABBITMQ_MNESIA_BASE)" \
			    RABBITMQ_PLUGINS_DIR="$(CURDIR)/dist" \
			    RABBITMQ_PLUGINS_EXPAND_DIR="$(RABBITMQ_PLUGINS_EXPAND_DIR)" \
			    RABBITMQ_ENABLED_PLUGINS_FILE="$(RABBITMQ_ENABLED_PLUGINS_FILE)" \
			    RABBITMQ_SERVER_START_ARGS="$(RABBITMQ_SERVER_START_ARGS)"

ifeq ($(PROJECT),rabbit)
BROKER_SCRIPTS_DIR ?= $(CURDIR)/scripts
else
RUN_BROKER_DEPS += rabbit
dep_rabbit ?= git file:///home/dumbbell/Projects/pivotal/rabbitmq-public-umbrella/rabbitmq-server erlang.mk

BROKER_SCRIPTS_DIR ?= $(DEPS_DIR)/rabbit/scripts
endif

RABBITMQ_PLUGINS ?= $(BROKER_SCRIPTS_DIR)/rabbitmq-plugins
RABBITMQ_SERVER ?= $(BROKER_SCRIPTS_DIR)/rabbitmq-server
RABBITMQCTL ?= $(BROKER_SCRIPTS_DIR)/rabbitmqctl
ERL_CALL = erl_call -sname $(RABBITMQ_NODENAME) -e

ALL_RUN_BROKER_DEPS_DIRS = $(addprefix $(DEPS_DIR)/,$(RUN_BROKER_DEPS))

$(foreach dep,$(RUN_BROKER_DEPS),$(eval $(call dep_target,$(dep))))

ifneq ($(SKIP_DEPS),)
run-broker-deps:
else
run-broker-deps: dist $(ALL_RUN_BROKER_DEPS_DIRS)
	$(verbose) for dep in $(ALL_RUN_BROKER_DEPS_DIRS); do \
	  $(MAKE) -C $$dep IS_DEP=1; \
	done
endif

$(NODE_TMPDIR):
	$(gen_verbose) rm -rf $(NODE_TMPDIR)
	$(verbose) mkdir -p $(foreach D,log plugins $(NODENAME),$(NODE_TMPDIR)/$(D))

.PHONY: $(NODE_TMPDIR)

$(RABBITMQ_ENABLED_PLUGINS_FILE): $(NODE_TMPDIR) run-broker-deps dist
	$(gen_verbose) $(BASIC_SCRIPT_ENV_SETTINGS) \
	  $(RABBITMQ_PLUGINS) set --offline \
	  $$($(BASIC_SCRIPT_ENV_SETTINGS) $(RABBITMQ_PLUGINS) list -m | tr '\n' ' ')

# --------------------------------------------------------------------
# Run a full RabbitMQ.
# --------------------------------------------------------------------

run-broker:: run-broker-deps $(NODE_TMPDIR) $(RABBITMQ_ENABLED_PLUGINS_FILE)
	$(BASIC_SCRIPT_ENV_SETTINGS) \
	  RABBITMQ_ALLOW_INPUT=true \
	  $(RABBITMQ_SERVER)

run-background-broker: run-broker-deps $(NODE_TMPDIR) $(RABBITMQ_ENABLED_PLUGINS_FILE)
	$(BASIC_SCRIPT_ENV_SETTINGS) \
	  $(RABBITMQ_SERVER) -detached

# --------------------------------------------------------------------
# Run a bare Erlang node.
# --------------------------------------------------------------------

run-node: run-broker-deps $(NODE_TMPDIR) $(RABBITMQ_ENABLED_PLUGINS_FILE)
	$(BASIC_SCRIPT_ENV_SETTINGS) \
	  RABBITMQ_NODE_ONLY=true \
	  RABBITMQ_ALLOW_INPUT=true \
	  $(RABBITMQ_SERVER)

run-background-node: run-broker-deps $(NODE_TMPDIR) $(RABBITMQ_ENABLED_PLUGINS_FILE)
	$(BASIC_SCRIPT_ENV_SETTINGS) \
	  RABBITMQ_NODE_ONLY=true \
	  $(RABBITMQ_SERVER) -detached

# --------------------------------------------------------------------
# Used by rabbitmq-test.
# --------------------------------------------------------------------

run-tests: run-broker-deps $(RABBITMQ_ENABLED_PLUGINS_FILE)
	echo 'code:add_path("$(TEST_EBIN_DIR)").' | $(ERL_CALL)
	echo 'code:add_path("$(TEST_EBIN_DIR)").' | $(ERL_CALL) -n hare || true
	OUT=$$(echo "rabbit_tests:all_tests()." | $(ERL_CALL)) ; \
	  echo $$OUT ; echo $$OUT | grep '^{ok, passed}$$' > /dev/null

run-qc: run-broker-deps $(RABBITMQ_ENABLED_PLUGINS_FILE)
	echo 'code:add_path("$(TEST_EBIN_DIR)").' | $(ERL_CALL)
	./quickcheck $(RABBITMQ_NODENAME) rabbit_backing_queue_qc 100 40
	./quickcheck $(RABBITMQ_NODENAME) gm_qc 1000 200

start-background-node: run-broker-deps $(NODE_TMPDIR) $(RABBITMQ_ENABLED_PLUGINS_FILE)
	$(BASIC_SCRIPT_ENV_SETTINGS) \
	  RABBITMQ_NODE_ONLY=true \
	  $(RABBITMQ_SERVER) \
	  > $(RABBITMQ_LOG_BASE)/startup_log \
	  2> $(RABBITMQ_LOG_BASE)/startup_err &
	ERL_LIBS="$(DIST_ERL_LIBS)" \
	  $(RABBITMQCTL) -n $(RABBITMQ_NODENAME) wait $(RABBITMQ_PID_FILE) kernel

start-rabbit-on-node:
	$(exec_verbose) echo 'rabbit:start().' | $(ERL_CALL) | sed -r '/^{ok, ok}$$/d'
	$(verbose) ERL_LIBS="$(DIST_ERL_LIBS)" \
	  $(RABBITMQCTL) -n $(RABBITMQ_NODENAME) wait $(RABBITMQ_PID_FILE)

stop-rabbit-on-node:
	$(exec_verbose) echo 'rabbit:stop().' | $(ERL_CALL) | sed -r '/^{ok, ok}$$/d'

set-resource-alarm:
	$(exec_verbose) echo 'rabbit_alarm:set_alarm({{resource_limit, $(SOURCE), node()}, []}).' | \
	$(ERL_CALL)

clear-resource-alarm:
	$(exec-verbose) echo 'rabbit_alarm:clear_alarm({resource_limit, $(SOURCE), node()}).' | \
	$(ERL_CALL)

stop-node:
	-$(exec_verbose) ( \
	pid=$$( \
	  ERL_LIBS="$(DIST_ERL_LIBS)" \
	  $(RABBITMQCTL) -n $(RABBITMQ_NODENAME) eval 'os:getpid().') && \
	$(ERL_CALL) -q && \
	while ps -p $$pid >/dev/null 2>&1; do sleep 1; done \
	)
