.PHONY: run-broker run-background-broker run-node run-background-node \
	run-tests run-lazy-vq-tests run-qc \
	start-background-node start-rabbit-on-node \
	stop-rabbit-on-node set-resource-alarm clear-resource-alarm \
	stop-node clean-node-db start-cover stop-cover

ifeq ($(filter rabbitmq-dist.mk,$(notdir $(MAKEFILE_LIST))),)
include $(dir $(lastword $(MAKEFILE_LIST)))rabbitmq-dist.mk
endif

exec_verbose_0 = @echo " EXEC  " $@;
exec_verbose_2 = set -x;
exec_verbose = $(exec_verbose_$(V))

ifeq ($(PLATFORM),msys2)
TEST_TMPDIR ?= $(TEMP)/rabbitmq-test-instances
else
TMPDIR ?= /tmp
TEST_TMPDIR ?= $(TMPDIR)/rabbitmq-test-instances
endif

# Location of the scripts controlling the broker.
ifeq ($(PROJECT),rabbit)
RABBITMQ_BROKER_DIR ?= $(CURDIR)
else
RABBITMQ_BROKER_DIR ?= $(DEPS_DIR)/rabbit
endif
RABBITMQ_SCRIPTS_DIR ?= $(RABBITMQ_BROKER_DIR)/scripts

ifeq ($(PLATFORM),msys2)
RABBITMQ_PLUGINS ?= $(RABBITMQ_SCRIPTS_DIR)/rabbitmq-plugins.bat
RABBITMQ_SERVER ?= $(RABBITMQ_SCRIPTS_DIR)/rabbitmq-server.bat
RABBITMQCTL ?= $(RABBITMQ_SCRIPTS_DIR)/rabbitmqctl.bat
else
RABBITMQ_PLUGINS ?= $(RABBITMQ_SCRIPTS_DIR)/rabbitmq-plugins
RABBITMQ_SERVER ?= $(RABBITMQ_SCRIPTS_DIR)/rabbitmq-server
RABBITMQCTL ?= $(RABBITMQ_SCRIPTS_DIR)/rabbitmqctl
endif

export RABBITMQ_SCRIPTS_DIR RABBITMQCTL RABBITMQ_PLUGINS

# We need to pass the location of codegen to the Java client ant
# process.
CODEGEN_DIR = $(DEPS_DIR)/rabbitmq_codegen
PYTHONPATH = $(CODEGEN_DIR)
export PYTHONPATH

ANT ?= ant
ANT_FLAGS += -Dmake.bin=$(MAKE) \
	     -DUMBRELLA_AVAILABLE=true \
	     -Drabbitmqctl.bin=$(RABBITMQCTL) \
	     -Dsibling.codegen.dir=$(CODEGEN_DIR)
ifeq ($(PROJECT),rabbitmq_test)
ANT_FLAGS += -Dsibling.rabbitmq_test.dir=$(CURDIR)
else
ANT_FLAGS += -Dsibling.rabbitmq_test.dir=$(DEPS_DIR)/rabbitmq_test
endif
export ANT ANT_FLAGS

# Broker startup variables for the test environment.
RABBITMQ_NODENAME ?= rabbit
NODE_TMPDIR ?= $(TEST_TMPDIR)/$(RABBITMQ_NODENAME)

RABBITMQ_PID_FILE ?= $(NODE_TMPDIR)/$(RABBITMQ_NODENAME).pid
RABBITMQ_LOG_BASE ?= $(NODE_TMPDIR)/log
RABBITMQ_MNESIA_BASE ?= $(NODE_TMPDIR)/mnesia
RABBITMQ_PLUGINS_EXPAND_DIR ?= $(NODE_TMPDIR)/plugins
RABBITMQ_ENABLED_PLUGINS_FILE ?= $(NODE_TMPDIR)/enabled_plugins

# erlang.mk adds dependencies' ebin directory to ERL_LIBS. This is
# a sane default, but we prefer to rely on the .ez archives in the
# `plugins` directory so the plugin code is executed. The `plugins`
# directory is added to ERL_LIBS by rabbitmq-env.
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
			    RABBITMQ_PLUGINS_DIR="$(CURDIR)/$(DIST_DIR)" \
			    RABBITMQ_PLUGINS_EXPAND_DIR="$(RABBITMQ_PLUGINS_EXPAND_DIR)" \
			    RABBITMQ_ENABLED_PLUGINS_FILE="$(RABBITMQ_ENABLED_PLUGINS_FILE)" \
			    RABBITMQ_SERVER_START_ARGS="$(RABBITMQ_SERVER_START_ARGS)"

# NOTE: Running a plugin requires RabbitMQ itself. As this file is
# loaded *after* erlang.mk, it is too late to add "rabbit" to the
# dependencies. Therefore, this is done in rabbitmq-components.mk.
#
# rabbitmq-components.mk knows the list of targets which starts
# a broker. When we add a target here, it needs to be listed in
# rabbitmq-components.mk as well.
#
# FIXME: This is fragile, how can we fix this?

ERL_CALL ?= erl_call
ERL_CALL_OPTS ?= -sname $(RABBITMQ_NODENAME) -e

test-tmpdir:
	$(verbose) mkdir -p $(TEST_TMPDIR)

virgin-test-tmpdir:
	$(gen_verbose) rm -rf $(TEST_TMPDIR)
	$(verbose) mkdir -p $(TEST_TMPDIR)

node-tmpdir:
	$(verbose) mkdir -p $(foreach D,log plugins $(NODENAME),$(NODE_TMPDIR)/$(D))

virgin-node-tmpdir:
	$(gen_verbose) rm -rf $(NODE_TMPDIR)
	$(verbose) mkdir -p $(foreach D,log plugins $(NODENAME),$(NODE_TMPDIR)/$(D))

.PHONY: test-tmpdir virgin-test-tmpdir node-tmpdir virgin-node-tmpdir

ifeq ($(wildcard ebin/test),)
$(RABBITMQ_ENABLED_PLUGINS_FILE): dist
endif

$(RABBITMQ_ENABLED_PLUGINS_FILE): node-tmpdir
	$(verbose) rm -f $@
	$(gen_verbose) $(BASIC_SCRIPT_ENV_SETTINGS) \
	  $(RABBITMQ_PLUGINS) set --offline \
	  $$($(BASIC_SCRIPT_ENV_SETTINGS) $(RABBITMQ_PLUGINS) list -m | tr '\n' ' ')

# --------------------------------------------------------------------
# Run a full RabbitMQ.
# --------------------------------------------------------------------

run-broker: node-tmpdir $(RABBITMQ_ENABLED_PLUGINS_FILE)
	$(BASIC_SCRIPT_ENV_SETTINGS) \
	  RABBITMQ_ALLOW_INPUT=true \
	  $(RABBITMQ_SERVER)

run-background-broker: node-tmpdir $(RABBITMQ_ENABLED_PLUGINS_FILE)
	$(BASIC_SCRIPT_ENV_SETTINGS) \
	  $(RABBITMQ_SERVER) -detached

# --------------------------------------------------------------------
# Run a bare Erlang node.
# --------------------------------------------------------------------

run-node: node-tmpdir $(RABBITMQ_ENABLED_PLUGINS_FILE)
	$(BASIC_SCRIPT_ENV_SETTINGS) \
	  RABBITMQ_NODE_ONLY=true \
	  RABBITMQ_ALLOW_INPUT=true \
	  $(RABBITMQ_SERVER)

run-background-node: virgin-node-tmpdir $(RABBITMQ_ENABLED_PLUGINS_FILE)
	$(BASIC_SCRIPT_ENV_SETTINGS) \
	  RABBITMQ_NODE_ONLY=true \
	  $(RABBITMQ_SERVER) -detached

# --------------------------------------------------------------------
# Used by rabbitmq-test.
# --------------------------------------------------------------------

# TODO: Move this to rabbitmq-tests.
run-tests:
	$(verbose) echo 'code:add_path("$(TEST_EBIN_DIR)").' | $(ERL_CALL) $(ERL_CALL_OPTS) | sed -E '/^\{ok, true\}$$/d'
	$(verbose) echo 'code:add_path("$(TEST_EBIN_DIR)").' | $(ERL_CALL) $(ERL_CALL_OPTS) -n hare | sed -E '/^\{ok, true\}$$/d'
	OUT=$$(RABBITMQ_PID_FILE='$(RABBITMQ_PID_FILE)' \
	  echo "rabbit_tests:all_tests()." | $(ERL_CALL) $(ERL_CALL_OPTS)) ; \
	  echo $$OUT ; echo $$OUT | grep '^{ok, passed}$$' > /dev/null

run-lazy-vq-tests:
	$(verbose) echo 'code:add_path("$(TEST_EBIN_DIR)").' | $(ERL_CALL) $(ERL_CALL_OPTS) | sed -E '/^\{ok, true\}$$/d'
	$(verbose) echo 'code:add_path("$(TEST_EBIN_DIR)").' | $(ERL_CALL) $(ERL_CALL_OPTS) -n hare | sed -E '/^\{ok, true\}$$/d'
	OUT=$$(RABBITMQ_PID_FILE='$(RABBITMQ_PID_FILE)' \
	  echo "rabbit_tests:test_lazy_variable_queue()." | $(ERL_CALL) $(ERL_CALL_OPTS)) ; \
	  echo $$OUT ; echo $$OUT | grep '^{ok, passed}$$' > /dev/null

run-qc:
	echo 'code:add_path("$(TEST_EBIN_DIR)").' | $(ERL_CALL) $(ERL_CALL_OPTS)
	./quickcheck $(RABBITMQ_NODENAME) rabbit_backing_queue_qc 100 40
	./quickcheck $(RABBITMQ_NODENAME) gm_qc 1000 200

ifneq ($(LOG_TO_STDIO),yes)
REDIRECT_STDIO = > $(RABBITMQ_LOG_BASE)/startup_log \
		 2> $(RABBITMQ_LOG_BASE)/startup_err
endif

start-background-node: node-tmpdir $(RABBITMQ_ENABLED_PLUGINS_FILE)
	$(BASIC_SCRIPT_ENV_SETTINGS) \
	  RABBITMQ_NODE_ONLY=true \
	  $(RABBITMQ_SERVER) \
	  $(REDIRECT_STDIO) &
	ERL_LIBS="$(DIST_ERL_LIBS)" \
	  $(RABBITMQCTL) -n $(RABBITMQ_NODENAME) wait $(RABBITMQ_PID_FILE) kernel

start-rabbit-on-node:
	$(exec_verbose) echo 'rabbit:start().' | $(ERL_CALL) $(ERL_CALL_OPTS) | sed -E '/^\{ok, ok\}$$/d'
	$(verbose) ERL_LIBS="$(DIST_ERL_LIBS)" \
	  $(RABBITMQCTL) -n $(RABBITMQ_NODENAME) wait $(RABBITMQ_PID_FILE)

stop-rabbit-on-node:
	$(exec_verbose) echo 'rabbit:stop().' | $(ERL_CALL) $(ERL_CALL_OPTS) | sed -E '/^\{ok, ok\}$$/d'

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
	$(exec_verbose) echo "rabbit_misc:start_cover([\"rabbit\", \"hare\"])." | $(ERL_CALL) $(ERL_CALL_OPTS) | sed -E '/^\{ok, ok\}$$/d'
	$(verbose) echo "rabbit_misc:enable_cover([\"$(RABBITMQ_BROKER_DIR)\"])." | $(ERL_CALL) $(ERL_CALL_OPTS) | sed -E '/^\{ok, ok\}$$/d'

stop-cover:
	$(exec_verbose) echo "rabbit_misc:report_cover(), cover:stop()." | $(ERL_CALL) $(ERL_CALL_OPTS) | sed -E '/^\{ok, ok\}$$/d'
	$(verbose) cat cover/summary.txt
