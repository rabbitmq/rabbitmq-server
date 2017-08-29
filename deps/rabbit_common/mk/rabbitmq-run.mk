.PHONY: run-broker run-background-broker run-node run-background-node \
	start-background-node start-rabbit-on-node \
	stop-rabbit-on-node set-resource-alarm clear-resource-alarm \
	stop-node

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

export RABBITMQ_SCRIPTS_DIR RABBITMQCTL RABBITMQ_PLUGINS RABBITMQ_SERVER

# We export MAKE to be sure scripts and tests use the proper command.
export MAKE

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

node_tmpdir = $(TEST_TMPDIR)/$(1)
node_pid_file = $(call node_tmpdir,$(1))/$(1).pid
node_log_base = $(call node_tmpdir,$(1))/log
node_mnesia_base = $(call node_tmpdir,$(1))/mnesia
node_mnesia_dir = $(call node_mnesia_base,$(1))/$(1)
node_plugins_expand_dir = $(call node_tmpdir,$(1))/plugins
node_enabled_plugins_file = $(call node_tmpdir,$(1))/enabled_plugins

# Broker startup variables for the test environment.
RABBITMQ_NODENAME ?= rabbit
RABBITMQ_NODENAME_FOR_PATHS ?= $(RABBITMQ_NODENAME)
NODE_TMPDIR ?= $(call node_tmpdir,$(RABBITMQ_NODENAME_FOR_PATHS))

RABBITMQ_PID_FILE ?= $(call node_pid_file,$(RABBITMQ_NODENAME_FOR_PATHS))
RABBITMQ_LOG_BASE ?= $(call node_log_base,$(RABBITMQ_NODENAME_FOR_PATHS))
RABBITMQ_MNESIA_BASE ?= $(call node_mnesia_base,$(RABBITMQ_NODENAME_FOR_PATHS))
RABBITMQ_MNESIA_DIR ?= $(call node_mnesia_dir,$(RABBITMQ_NODENAME_FOR_PATHS))
RABBITMQ_PLUGINS_EXPAND_DIR ?= $(call node_plugins_expand_dir,$(RABBITMQ_NODENAME_FOR_PATHS))
RABBITMQ_ENABLED_PLUGINS_FILE ?= $(call node_enabled_plugins_file,$(RABBITMQ_NODENAME_FOR_PATHS))

# erlang.mk adds dependencies' ebin directory to ERL_LIBS. This is
# a sane default, but we prefer to rely on the .ez archives in the
# `plugins` directory so the plugin code is executed. The `plugins`
# directory is added to ERL_LIBS by rabbitmq-env.
DIST_ERL_LIBS = $(shell echo "$(filter-out $(DEPS_DIR),$(subst :, ,$(ERL_LIBS)))" | tr ' ' :)

define basic_script_env_settings
MAKE="$(MAKE)" \
ERL_LIBS="$(DIST_ERL_LIBS)" \
RABBITMQ_NODENAME="$(1)" \
RABBITMQ_NODE_IP_ADDRESS="$(RABBITMQ_NODE_IP_ADDRESS)" \
RABBITMQ_NODE_PORT="$(3)" \
RABBITMQ_PID_FILE="$(call node_pid_file,$(2))" \
RABBITMQ_LOG_BASE="$(call node_log_base,$(2))" \
RABBITMQ_MNESIA_BASE="$(call node_mnesia_base,$(2))" \
RABBITMQ_MNESIA_DIR="$(call node_mnesia_dir,$(2))" \
RABBITMQ_PLUGINS_DIR="$(CURDIR)/$(DIST_DIR)" \
RABBITMQ_PLUGINS_EXPAND_DIR="$(call node_plugins_expand_dir,$(2))" \
RABBITMQ_SERVER_START_ARGS="$(RABBITMQ_SERVER_START_ARGS)"
endef

BASIC_SCRIPT_ENV_SETTINGS = \
	$(call basic_script_env_settings,$(RABBITMQ_NODENAME),$(RABBITMQ_NODENAME_FOR_PATHS),$(RABBITMQ_NODE_PORT)) \
	RABBITMQ_ENABLED_PLUGINS_FILE="$(RABBITMQ_ENABLED_PLUGINS_FILE)"

ERL_CALL := $(shell $(ERL) -eval 'io:format("~s~n", [filename:join(code:lib_dir(erl_interface, bin), case os:type() of {win32, _} -> "erl_call.exe"; _ -> "erl_call" end)]), halt().')
ERL_CALL_OPTS ?= -sname $(RABBITMQ_NODENAME) -e

test-tmpdir:
	$(verbose) mkdir -p $(TEST_TMPDIR)

virgin-test-tmpdir:
	$(gen_verbose) rm -rf $(TEST_TMPDIR)
	$(verbose) mkdir -p $(TEST_TMPDIR)

node-tmpdir:
	$(verbose) mkdir -p $(RABBITMQ_LOG_BASE) \
		$(RABBITMQ_MNESIA_BASE) \
		$(RABBITMQ_PLUGINS_EXPAND_DIR)

virgin-node-tmpdir:
	$(gen_verbose) rm -rf $(NODE_TMPDIR)
	$(verbose) mkdir -p $(RABBITMQ_LOG_BASE) \
		$(RABBITMQ_MNESIA_BASE) \
		$(RABBITMQ_PLUGINS_EXPAND_DIR)

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

define test_rabbitmq_config
%% vim:ft=erlang:

[
  {rabbit, [
      {loopback_users, []}
    ]}
].
endef

define test_rabbitmq_config_with_tls
%% vim:ft=erlang:

[
  {rabbit, [
      {loopback_users, []},
      {ssl_listeners, [5671]},
      {ssl_options, [
          {cacertfile, "$(TEST_TLS_CERTS_DIR_in_config)/testca/cacert.pem"},
          {certfile,   "$(TEST_TLS_CERTS_DIR_in_config)/server/cert.pem"},
          {keyfile,    "$(TEST_TLS_CERTS_DIR_in_config)/server/key.pem"},
          {verify, verify_peer},
          {fail_if_no_peer_cert, false},
          {honor_cipher_order, true}]}
    ]},
  {rabbitmq_management, [
      {listener, [
          {port, 15671},
          {ssl,  true},
          {ssl_opts, [
            {cacertfile, "$(TEST_TLS_CERTS_DIR_in_config)/testca/cacert.pem"},
            {certfile,   "$(TEST_TLS_CERTS_DIR_in_config)/server/cert.pem"},
            {keyfile,    "$(TEST_TLS_CERTS_DIR_in_config)/server/key.pem"},
            {verify, verify_peer},
            {fail_if_no_peer_cert, false},
            {honor_cipher_order, true}]}
        ]}
  ]}
].
endef

TEST_CONFIG_FILE ?= $(TEST_TMPDIR)/test.config
TEST_TLS_CERTS_DIR = $(TEST_TMPDIR)/tls-certs
ifeq ($(PLATFORM),msys2)
TEST_TLS_CERTS_DIR_in_config = $(shell echo $(TEST_TLS_CERTS_DIR) | sed -E "s,^/([^/]+),\1:,")
else
TEST_TLS_CERTS_DIR_in_config = $(TEST_TLS_CERTS_DIR)
endif

.PHONY: $(TEST_CONFIG_FILE)
$(TEST_CONFIG_FILE): node-tmpdir
	$(gen_verbose) printf "$(subst $(newline),\n,$(subst ",\",$(config)))" > $@

$(TEST_TLS_CERTS_DIR): node-tmpdir
	$(gen_verbose) $(MAKE) -C $(DEPS_DIR)/rabbitmq_ct_helpers/tools/tls-certs \
	  DIR=$(TEST_TLS_CERTS_DIR) all

show-test-tls-certs-dir: $(TEST_TLS_CERTS_DIR)
	@echo $(TEST_TLS_CERTS_DIR)

run-broker run-tls-broker: RABBITMQ_CONFIG_FILE = $(basename $(TEST_CONFIG_FILE))
run-broker:     config := $(test_rabbitmq_config)
run-tls-broker: config := $(test_rabbitmq_config_with_tls)
run-tls-broker: $(TEST_TLS_CERTS_DIR)

run-broker run-tls-broker: node-tmpdir $(RABBITMQ_ENABLED_PLUGINS_FILE) \
    $(TEST_CONFIG_FILE)
	$(BASIC_SCRIPT_ENV_SETTINGS) \
	  RABBITMQ_ALLOW_INPUT=true \
	  RABBITMQ_CONFIG_FILE=$(RABBITMQ_CONFIG_FILE) \
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
# Start RabbitMQ in the background.
# --------------------------------------------------------------------

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

start-background-broker: node-tmpdir $(RABBITMQ_ENABLED_PLUGINS_FILE)
	$(BASIC_SCRIPT_ENV_SETTINGS) \
	  $(RABBITMQ_SERVER) \
	  $(REDIRECT_STDIO) &
	ERL_LIBS="$(DIST_ERL_LIBS)" \
	  $(RABBITMQCTL) -n $(RABBITMQ_NODENAME) wait $(RABBITMQ_PID_FILE) && \
	ERL_LIBS="$(DIST_ERL_LIBS)" \
	  $(RABBITMQCTL) -n $(RABBITMQ_NODENAME) status >/dev/null

start-rabbit-on-node:
	$(exec_verbose) echo 'rabbit:start().' | $(ERL_CALL) $(ERL_CALL_OPTS) | sed -E '/^\{ok, ok\}$$/d'
	$(verbose) ERL_LIBS="$(DIST_ERL_LIBS)" \
	  $(RABBITMQCTL) -n $(RABBITMQ_NODENAME) wait $(RABBITMQ_PID_FILE)

stop-rabbit-on-node:
	$(exec_verbose) echo 'rabbit:stop().' | $(ERL_CALL) $(ERL_CALL_OPTS) | sed -E '/^\{ok, ok\}$$/d'

stop-node:
	$(exec_verbose) ( \
	pid=$$(test -f $(RABBITMQ_PID_FILE) && cat $(RABBITMQ_PID_FILE)) && \
	$(ERL_CALL) $(ERL_CALL_OPTS) -q && \
	while ps -p "$$pid" >/dev/null 2>&1; do sleep 1; done \
	) || :

# " <-- To please Vim syntax hilighting.

# --------------------------------------------------------------------
# Start a RabbitMQ cluster in the background.
# --------------------------------------------------------------------

NODES ?= 2

start-brokers start-cluster:
	@for n in $$(seq $(NODES)); do \
		nodename="rabbit-$$n@$$(hostname -s)"; \
		$(MAKE) start-background-broker \
		  RABBITMQ_NODENAME="$$nodename" \
		  RABBITMQ_NODE_PORT="$$((5672 + $$n - 1))" \
		  RABBITMQ_SERVER_START_ARGS=" \
		  -rabbit loopback_users [] \
		  -rabbitmq_management listener [{port,$$((15672 + $$n - 1))}] \
		  "; \
		if test '$@' = 'start-cluster' && test "$$nodename1"; then \
			ERL_LIBS="$(DIST_ERL_LIBS)" \
			  $(RABBITMQCTL) -n "$$nodename" stop_app; \
			ERL_LIBS="$(DIST_ERL_LIBS)" \
			  $(RABBITMQCTL) -n "$$nodename" join_cluster "$$nodename1"; \
			ERL_LIBS="$(DIST_ERL_LIBS)" \
			  $(RABBITMQCTL) -n "$$nodename" start_app; \
		else \
			nodename1=$$nodename; \
		fi; \
	done

stop-brokers stop-cluster:
	@for n in $$(seq $(NODES) 1); do \
		nodename="rabbit-$$n@$$(hostname -s)"; \
		$(MAKE) stop-node \
		  RABBITMQ_NODENAME="$$nodename"; \
	done

# --------------------------------------------------------------------
# Used by testsuites.
# --------------------------------------------------------------------

set-resource-alarm:
	$(exec_verbose) echo 'rabbit_alarm:set_alarm({{resource_limit, $(SOURCE), node()}, []}).' | \
	$(ERL_CALL) $(ERL_CALL_OPTS)

clear-resource-alarm:
	$(exec-verbose) echo 'rabbit_alarm:clear_alarm({resource_limit, $(SOURCE), node()}).' | \
	$(ERL_CALL) $(ERL_CALL_OPTS)
