.PHONY: run-broker-deps run-broker run-background-broker \
	run-node run-background-node run-tests run-qc \
	start-background-node start-rabbit-on-node \
	stop-rabbit-on-node set-resource-alarm clear-resource-alarm \
	stop-node clean-node-db start-cover stop-cover

ifeq ($(filter rabbitmq-dist.mk,$(notdir $(MAKEFILE_LIST))),)
include $(dir $(lastword $(MAKEFILE_LIST)))rabbitmq-dist.mk
endif

exec_verbose_0 = @echo " EXEC  " $@;
exec_verbose = $(exec_verbose_$(V))

TMPDIR ?= /tmp
TEST_TMPDIR ?= $(TMPDIR)/rabbitmq-test-instances

RABBITMQ_NODENAME ?= rabbit
NODE_TMPDIR ?= $(TEST_TMPDIR)/$(RABBITMQ_NODENAME)

RABBITMQ_PID_FILE ?= $(NODE_TMPDIR)/$(RABBITMQ_NODENAME).pid
RABBITMQ_LOG_BASE ?= $(NODE_TMPDIR)/log
RABBITMQ_MNESIA_BASE ?= $(NODE_TMPDIR)/mnesia
RABBITMQ_PLUGINS_EXPAND_DIR ?= $(NODE_TMPDIR)/plugins
RABBITMQ_ENABLED_PLUGINS_FILE ?= $(NODE_TMPDIR)/enabled_plugins

# erlang.mk adds dependencies' ebin directory to ERL_LIBS. This is a
# sane default, but we prefer to rely on the .ez archives in the dist
# directory so the plugin code is executed. The dist directory is added
# to ERL_LIBS by rabbitmq-env.
DIST_ERL_LIBS = $(shell echo "$(filter-out $(DEPS_DIR),$(subst :, ,$(ERL_LIBS)))" | tr ' ' :)

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
ERL_CALL ?= erl_call
ERL_CALL_OPTS ?= -sname $(RABBITMQ_NODENAME) -e

ALL_RUN_BROKER_DEPS_DIRS = $(addprefix $(DEPS_DIR)/,$(RUN_BROKER_DEPS))

$(foreach dep,$(RUN_BROKER_DEPS),$(eval $(call dep_target,$(dep))))

ifneq ($(SKIP_DEPS),)
run-broker-deps:
else
ifeq ($(wildcard ebin/test),)
run-broker-deps: dist
endif

run-broker-deps: $(ALL_RUN_BROKER_DEPS_DIRS)
	$(verbose) for dep in $(ALL_RUN_BROKER_DEPS_DIRS); do \
	  $(MAKE) -C $$dep IS_DEP=1; \
	done
endif

$(NODE_TMPDIR):
	$(gen_verbose) rm -rf $(NODE_TMPDIR)
	$(verbose) mkdir -p $(foreach D,log plugins $(NODENAME),$(NODE_TMPDIR)/$(D))

.PHONY: $(NODE_TMPDIR)

ifeq ($(wildcard ebin/test),)
$(RABBITMQ_ENABLED_PLUGINS_FILE): dist
endif

$(RABBITMQ_ENABLED_PLUGINS_FILE): $(NODE_TMPDIR) run-broker-deps
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

# TODO: Move this to rabbitmq-tests.
run-tests:
	echo 'code:add_path("$(TEST_EBIN_DIR)").' | $(ERL_CALL) $(ERL_CALL_OPTS)
	echo 'code:add_path("$(TEST_EBIN_DIR)").' | $(ERL_CALL) $(ERL_CALL_OPTS) -n hare || true
	OUT=$$(echo "rabbit_tests:all_tests()." | $(ERL_CALL) $(ERL_CALL_OPTS)) ; \
	  echo $$OUT ; echo $$OUT | grep '^{ok, passed}$$' > /dev/null

run-qc:
	echo 'code:add_path("$(TEST_EBIN_DIR)").' | $(ERL_CALL) $(ERL_CALL_OPTS)
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
	$(exec_verbose) echo 'rabbit:start().' | $(ERL_CALL) $(ERL_CALL_OPTS) | sed -r '/^{ok, ok}$$/d'
	$(verbose) ERL_LIBS="$(DIST_ERL_LIBS)" \
	  $(RABBITMQCTL) -n $(RABBITMQ_NODENAME) wait $(RABBITMQ_PID_FILE)

stop-rabbit-on-node:
	$(exec_verbose) echo 'rabbit:stop().' | $(ERL_CALL) $(ERL_CALL_OPTS) | sed -r '/^{ok, ok}$$/d'

set-resource-alarm:
	$(exec_verbose) echo 'rabbit_alarm:set_alarm({{resource_limit, $(SOURCE), node()}, []}).' | \
	$(ERL_CALL) $(ERL_CALL_OPTS)

clear-resource-alarm:
	$(exec-verbose) echo 'rabbit_alarm:clear_alarm({resource_limit, $(SOURCE), node()}).' | \
	$(ERL_CALL) $(ERL_CALL_OPTS)

stop-node:
	$(exec_verbose) ( \
	pid=$$(test -f $(RABBITMQ_PID_FILE) && cat $(RABBITMQ_PID_FILE)) && \
	$(ERL_CALL) $(ERL_CALL_OPTS) -q && \
	while ps -p "$$pid" >/dev/null 2>&1; do sleep 1; done \
	) || :

clean-node-db:
	$(exec_verbose) rm -rf $(RABBITMQ_MNESIA_BASE)/$(RABBITMQ_NODENAME)/*

start-cover:
	echo "rabbit_misc:start_cover([\"rabbit\", \"hare\"])." | $(ERL_CALL) $(ERL_CALL_OPTS)
	echo "rabbit_misc:enable_cover([\"$(CURDIR)\"])." | $(ERL_CALL) $(ERL_CALL_OPTS)

stop-cover:
	echo "rabbit_misc:report_cover(), cover:stop()." | $(ERL_CALL) $(ERL_CALL_OPTS)
	cat cover/summary.txt
