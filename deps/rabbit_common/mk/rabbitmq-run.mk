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
RABBITMQ_SCRIPTS_DIR ?= $(CURDIR)/sbin

ifeq ($(PLATFORM),msys2)
RABBITMQ_PLUGINS ?= $(RABBITMQ_SCRIPTS_DIR)/rabbitmq-plugins.bat
RABBITMQ_SERVER ?= $(RABBITMQ_SCRIPTS_DIR)/rabbitmq-server.bat
RABBITMQCTL ?= $(RABBITMQ_SCRIPTS_DIR)/rabbitmqctl.bat
RABBITMQ_UPGRADE ?= $(RABBITMQ_SCRIPTS_DIR)/rabbitmq-upgrade.bat
else
RABBITMQ_PLUGINS ?= $(RABBITMQ_SCRIPTS_DIR)/rabbitmq-plugins
RABBITMQ_SERVER ?= $(RABBITMQ_SCRIPTS_DIR)/rabbitmq-server
RABBITMQCTL ?= $(RABBITMQ_SCRIPTS_DIR)/rabbitmqctl
RABBITMQ_UPGRADE ?= $(RABBITMQ_SCRIPTS_DIR)/rabbitmq-upgrade
endif

export RABBITMQ_SCRIPTS_DIR RABBITMQCTL RABBITMQ_PLUGINS RABBITMQ_SERVER RABBITMQ_UPGRADE

# We export MAKE to be sure scripts and tests use the proper command.
export MAKE

# We need to pass the location of codegen to the Java client ant
# process.
CODEGEN_DIR = $(DEPS_DIR)/rabbitmq_codegen
PYTHONPATH = $(CODEGEN_DIR)
export PYTHONPATH

node_tmpdir = $(TEST_TMPDIR)/$(1)
node_pid_file = $(call node_tmpdir,$(1))/$(1).pid
node_log_base = $(call node_tmpdir,$(1))/log
node_mnesia_base = $(call node_tmpdir,$(1))/mnesia
node_data_dir = $(call node_mnesia_base,$(1))/$(1)
node_quorum_dir = $(call node_data_dir,$(1))/quorum
node_stream_dir = $(call node_data_dir,$(1))/stream
node_plugins_expand_dir = $(call node_tmpdir,$(1))/plugins
node_feature_flags_file = $(call node_tmpdir,$(1))/feature_flags
node_enabled_plugins_file = $(call node_tmpdir,$(1))/enabled_plugins

# Broker startup variables for the test environment.
ifeq ($(PLATFORM),msys2)
HOSTNAME = $(COMPUTERNAME)
else
ifeq ($(PLATFORM),solaris)
HOSTNAME = $(shell hostname | sed 's@\..*@@')
else
HOSTNAME = $(shell hostname -s)
endif
endif

RABBITMQ_NODENAME ?= rabbit@$(HOSTNAME)
RABBITMQ_NODENAME_FOR_PATHS ?= $(RABBITMQ_NODENAME)
NODE_TMPDIR ?= $(call node_tmpdir,$(RABBITMQ_NODENAME_FOR_PATHS))

RABBITMQ_BASE ?= $(NODE_TMPDIR)
RABBITMQ_PID_FILE ?= $(call node_pid_file,$(RABBITMQ_NODENAME_FOR_PATHS))
RABBITMQ_LOG_BASE ?= $(call node_log_base,$(RABBITMQ_NODENAME_FOR_PATHS))
RABBITMQ_MNESIA_BASE ?= $(call node_mnesia_base,$(RABBITMQ_NODENAME_FOR_PATHS))
RABBITMQ_MNESIA_DIR ?= $(call node_data_dir,$(RABBITMQ_NODENAME_FOR_PATHS))
RABBITMQ_QUORUM_DIR ?= $(call node_quorum_dir,$(RABBITMQ_NODENAME_FOR_PATHS))
RABBITMQ_STREAM_DIR ?= $(call node_stream_dir,$(RABBITMQ_NODENAME_FOR_PATHS))
RABBITMQ_PLUGINS_EXPAND_DIR ?= $(call node_plugins_expand_dir,$(RABBITMQ_NODENAME_FOR_PATHS))
RABBITMQ_FEATURE_FLAGS_FILE ?= $(call node_feature_flags_file,$(RABBITMQ_NODENAME_FOR_PATHS))
RABBITMQ_ENABLED_PLUGINS_FILE ?= $(call node_enabled_plugins_file,$(RABBITMQ_NODENAME_FOR_PATHS))

# Enable colourful debug logging by default
# To change this, set RABBITMQ_LOG to info, notice, warning etc.
RABBITMQ_LOG ?= debug,+color
export RABBITMQ_LOG

ifdef PLUGINS_FROM_DEPS_DIR
RMQ_PLUGINS_DIR = $(DEPS_DIR)
DIST_ERL_LIBS = $(ERL_LIBS)
else
RMQ_PLUGINS_DIR = $(CURDIR)/$(DIST_DIR)
# We do not want to add apps/ or deps/ to ERL_LIBS
# when running the release from dist. The `plugins`
# directory is added to ERL_LIBS by rabbitmq-env.
DIST_ERL_LIBS = $(patsubst :%,%,$(patsubst %:,%,$(subst :$(APPS_DIR):,:,$(subst :$(DEPS_DIR):,:,:$(ERL_LIBS):))))
endif

node_plugins_dir = $(if $(RABBITMQ_PLUGINS_DIR),$(RABBITMQ_PLUGINS_DIR),$(if $(EXTRA_PLUGINS_DIR),$(EXTRA_PLUGINS_DIR):$(RMQ_PLUGINS_DIR),$(RMQ_PLUGINS_DIR)))

define basic_script_env_settings
MAKE="$(MAKE)" \
ERL_LIBS="$(DIST_ERL_LIBS)" \
RABBITMQ_NODENAME="$(1)" \
RABBITMQ_NODE_IP_ADDRESS="$(RABBITMQ_NODE_IP_ADDRESS)" \
RABBITMQ_NODE_PORT="$(3)" \
RABBITMQ_BASE="$(call node_tmpdir,$(2))" \
RABBITMQ_PID_FILE="$(call node_pid_file,$(2))" \
RABBITMQ_LOG_BASE="$(call node_log_base,$(2))" \
RABBITMQ_MNESIA_BASE="$(call node_mnesia_base,$(2))" \
RABBITMQ_MNESIA_DIR="$(call node_data_dir,$(2))" \
RABBITMQ_QUORUM_DIR="$(call node_quorum_dir,$(2))" \
RABBITMQ_STREAM_DIR="$(call node_stream_dir,$(2))" \
RABBITMQ_FEATURE_FLAGS_FILE="$(call node_feature_flags_file,$(2))" \
RABBITMQ_PLUGINS_DIR="$(call node_plugins_dir)" \
RABBITMQ_PLUGINS_EXPAND_DIR="$(call node_plugins_expand_dir,$(2))" \
RABBITMQ_SERVER_START_ARGS="$(RABBITMQ_SERVER_START_ARGS)" \
RABBITMQ_ENABLED_PLUGINS="$(RABBITMQ_ENABLED_PLUGINS)"
endef

BASIC_SCRIPT_ENV_SETTINGS = \
	$(call basic_script_env_settings,$(RABBITMQ_NODENAME),$(RABBITMQ_NODENAME_FOR_PATHS),$(RABBITMQ_NODE_PORT)) \
	RABBITMQ_ENABLED_PLUGINS_FILE="$(RABBITMQ_ENABLED_PLUGINS_FILE)"

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

ifdef LEAVE_PLUGINS_DISABLED
RABBITMQ_ENABLED_PLUGINS ?=
else
RABBITMQ_ENABLED_PLUGINS ?= ALL
endif

# --------------------------------------------------------------------
# Run a full RabbitMQ.
# --------------------------------------------------------------------

define test_rabbitmq_config
%% vim:ft=erlang:

[
  {rabbit, [
$(if $(RABBITMQ_NODE_PORT),      {tcp_listeners$(comma) [$(RABBITMQ_NODE_PORT)]}$(comma),)
      {loopback_users, []}
    ]},
  {rabbitmq_management, [
$(if $(RABBITMQ_NODE_PORT),      {listener$(comma) [{port$(comma) $(shell echo "$$(($(RABBITMQ_NODE_PORT) + 10000))")}]},)
    ]},
  {rabbitmq_mqtt, [
$(if $(RABBITMQ_NODE_PORT),      {tcp_listeners$(comma) [$(shell echo "$$((1883 + $(RABBITMQ_NODE_PORT) - 5672))")]},)
    ]},
  {rabbitmq_web_mqtt, [
$(if $(RABBITMQ_NODE_PORT),      {tcp_config$(comma) [{port$(comma) $(shell echo "$$((15675 + $(RABBITMQ_NODE_PORT) - 5672))")}]},)
    ]},
  {rabbitmq_web_mqtt_examples, [
$(if $(RABBITMQ_NODE_PORT),      {listener$(comma) [{port$(comma) $(shell echo "$$((15670 + $(RABBITMQ_NODE_PORT) - 5672))")}]},)
    ]},
  {rabbitmq_stomp, [
$(if $(RABBITMQ_NODE_PORT),      {tcp_listeners$(comma) [$(shell echo "$$((61613 + $(RABBITMQ_NODE_PORT) - 5672))")]},)
    ]},
  {rabbitmq_web_stomp, [
$(if $(RABBITMQ_NODE_PORT),      {tcp_config$(comma) [{port$(comma) $(shell echo "$$((15674 + $(RABBITMQ_NODE_PORT) - 5672))")}]},)
    ]},
  {rabbitmq_web_stomp_examples, [
$(if $(RABBITMQ_NODE_PORT),      {listener$(comma) [{port$(comma) $(shell echo "$$((15670 + $(RABBITMQ_NODE_PORT) - 5672))")}]},)
    ]},
  {rabbitmq_stream, [
$(if $(RABBITMQ_NODE_PORT),      {tcp_listeners$(comma) [$(shell echo "$$((5552 + $(RABBITMQ_NODE_PORT) - 5672))")]},)
    ]},
  {rabbitmq_prometheus, [
$(if $(RABBITMQ_NODE_PORT),      {tcp_config$(comma) [{port$(comma) $(shell echo "$$((15692 + $(RABBITMQ_NODE_PORT) - 5672))")}]},)
    ]},
  {ra, [
      {data_dir, "$(RABBITMQ_QUORUM_DIR)"}
    ]},
  {osiris, [
      {data_dir, "$(RABBITMQ_STREAM_DIR)"}
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
  ]},
  {ra, [
      {data_dir, "$(RABBITMQ_QUORUM_DIR)"}
    ]},
  {osiris, [
      {data_dir, "$(RABBITMQ_STREAM_DIR)"}
    ]}
].
endef

TEST_CONFIG_FILE ?= $(TEST_TMPDIR)/test.config
TEST_TLS_CERTS_DIR := $(TEST_TMPDIR)/tls-certs
ifeq ($(origin TEST_TLS_CERTS_DIR_in_config),undefined)
ifeq ($(PLATFORM),msys2)
TEST_TLS_CERTS_DIR_in_config := $(shell echo $(TEST_TLS_CERTS_DIR) | sed -E "s,^/([^/]+),\1:,")
else
TEST_TLS_CERTS_DIR_in_config := $(TEST_TLS_CERTS_DIR)
endif
export TEST_TLS_CERTS_DIR_in_config
endif

.PHONY: $(TEST_CONFIG_FILE)
$(TEST_CONFIG_FILE): node-tmpdir
	$(gen_verbose) printf "$(subst $(newline),\n,$(subst ",\",$(config)))" > $@

$(TEST_TLS_CERTS_DIR): node-tmpdir
	$(gen_verbose) $(MAKE) -C $(DEPS_DIR)/rabbitmq_ct_helpers/tools/tls-certs \
	  DIR=$(TEST_TLS_CERTS_DIR) all

show-test-tls-certs-dir: $(TEST_TLS_CERTS_DIR)
	@echo $(TEST_TLS_CERTS_DIR)

ifdef NOBUILD
DIST_TARGET ?=
else
ifeq ($(wildcard ebin/test),)
DIST_TARGET ?= dist
else
DIST_TARGET ?= test-dist
endif
endif

run-broker run-tls-broker: RABBITMQ_CONFIG_FILE := $(basename $(TEST_CONFIG_FILE))
run-broker:     config = $(test_rabbitmq_config)
run-tls-broker: config = $(test_rabbitmq_config_with_tls)
run-tls-broker: $(TEST_TLS_CERTS_DIR)

run-broker run-tls-broker: node-tmpdir $(DIST_TARGET) $(TEST_CONFIG_FILE)
	$(BASIC_SCRIPT_ENV_SETTINGS) \
	  RABBITMQ_ALLOW_INPUT=true \
	  RABBITMQ_CONFIG_FILE=$(RABBITMQ_CONFIG_FILE) \
	  $(RABBITMQ_SERVER)

run-background-broker: node-tmpdir $(DIST_TARGET)
	$(BASIC_SCRIPT_ENV_SETTINGS) \
	  $(RABBITMQ_SERVER) -detached

# --------------------------------------------------------------------
# Run a bare Erlang node.
# --------------------------------------------------------------------

run-node: node-tmpdir $(DIST_TARGET)
	$(BASIC_SCRIPT_ENV_SETTINGS) \
	  RABBITMQ_NODE_ONLY=true \
	  RABBITMQ_ALLOW_INPUT=true \
	  $(RABBITMQ_SERVER)

run-background-node: virgin-node-tmpdir $(DIST_TARGET)
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

RMQCTL_WAIT_TIMEOUT ?= 60

start-background-node: node-tmpdir $(DIST_TARGET)
	$(BASIC_SCRIPT_ENV_SETTINGS) \
	  RABBITMQ_NODE_ONLY=true \
	  $(RABBITMQ_SERVER) \
	  $(REDIRECT_STDIO) &
	ERL_LIBS="$(DIST_ERL_LIBS)" \
	  $(RABBITMQCTL) -n $(RABBITMQ_NODENAME) wait --timeout $(RMQCTL_WAIT_TIMEOUT) $(RABBITMQ_PID_FILE) kernel

start-background-broker: node-tmpdir $(DIST_TARGET)
	$(BASIC_SCRIPT_ENV_SETTINGS) \
	  $(RABBITMQ_SERVER) \
	  $(REDIRECT_STDIO) &
	ERL_LIBS="$(DIST_ERL_LIBS)" \
	  $(RABBITMQCTL) -n $(RABBITMQ_NODENAME) wait --timeout $(RMQCTL_WAIT_TIMEOUT) $(RABBITMQ_PID_FILE) && \
	ERL_LIBS="$(DIST_ERL_LIBS)" \
	  $(RABBITMQCTL) --node $(RABBITMQ_NODENAME) await_startup

start-rabbit-on-node:
	$(exec_verbose) ERL_LIBS="$(DIST_ERL_LIBS)" \
	  $(RABBITMQCTL) -n $(RABBITMQ_NODENAME) \
	  eval 'rabbit:start().' | \
	  sed -E -e '/^ completed with .* plugins\.$$/d' -e '/^ok$$/d'
	$(verbose) ERL_LIBS="$(DIST_ERL_LIBS)" \
	  $(RABBITMQCTL) -n $(RABBITMQ_NODENAME) wait --timeout $(RMQCTL_WAIT_TIMEOUT) $(RABBITMQ_PID_FILE)

stop-rabbit-on-node:
	$(exec_verbose) ERL_LIBS="$(DIST_ERL_LIBS)" \
	  $(RABBITMQCTL) -n $(RABBITMQ_NODENAME) \
	  eval 'rabbit:stop().' | \
	  sed -E -e '/^ok$$/d'

stop-node:
	$(exec_verbose) ( \
	  pid=$$(test -f $(RABBITMQ_PID_FILE) && cat $(RABBITMQ_PID_FILE)); \
	  test "$$pid" && \
	  kill -TERM "$$pid" && \
	  echo waiting for process to exit && \
	  while ps -p "$$pid" >/dev/null 2>&1; do sleep 1; done \
	  ) || :

# " <-- To please Vim syntax hilighting.

# --------------------------------------------------------------------
# Start a RabbitMQ cluster in the background.
# --------------------------------------------------------------------

NODES ?= 3

start-brokers start-cluster: $(DIST_TARGET)
	@if test '$@' = 'start-cluster'; then \
		for n in $$(seq $(NODES)); do \
			nodename="rabbit-$$n@$(HOSTNAME)"; \
			if test "$$nodeslist"; then \
				nodeslist="$$nodeslist,'$$nodename'"; \
			else \
				nodeslist="'$$nodename'"; \
			fi; \
		done; \
		cluster_nodes_arg="-rabbit cluster_nodes [$$nodeslist]"; \
	fi; \
	for n in $$(seq $(NODES)); do \
		nodename="rabbit-$$n@$(HOSTNAME)"; \
		$(MAKE) start-background-broker \
		  NOBUILD=1 \
		  RABBITMQ_NODENAME="$$nodename" \
		  RABBITMQ_NODE_PORT="$$((5672 + $$n - 1))" \
		  RABBITMQ_SERVER_START_ARGS=" \
		  -rabbit loopback_users [] \
		  -rabbitmq_management listener [{port,$$((15672 + $$n - 1))}] \
		  -rabbitmq_mqtt tcp_listeners [$$((1883 + $$n - 1))] \
		  -rabbitmq_web_mqtt tcp_config [{port,$$((1893 + $$n - 1))}] \
		  -rabbitmq_web_mqtt_examples listener [{port,$$((1903 + $$n - 1))}] \
		  -rabbitmq_stomp tcp_listeners [$$((61613 + $$n - 1))] \
		  -rabbitmq_web_stomp tcp_config [{port,$$((61623 + $$n - 1))}] \
		  -rabbitmq_web_stomp_examples listener [{port,$$((61633 + $$n - 1))}] \
		  -rabbitmq_prometheus tcp_config [{port,$$((15692 + $$n - 1))}] \
		  -rabbitmq_stream tcp_listeners [$$((5552 + $$n - 1))] \
		  $$cluster_nodes_arg \
		  " & \
	done; \
	wait

stop-brokers stop-cluster:
	@for n in $$(seq $(NODES) -1 1); do \
		nodename="rabbit-$$n@$(HOSTNAME)"; \
		$(MAKE) stop-node \
		  RABBITMQ_NODENAME="$$nodename" & \
	done; \
	wait

NODES ?= 3

# Rolling restart similar to what the Kubernetes Operator does
restart-cluster:
	@for n in $$(seq $(NODES) -1 1); do \
		nodename="rabbit-$$n@$(HOSTNAME)"; \
		$(RABBITMQ_UPGRADE) -n "$$nodename" await_online_quorum_plus_one -t 604800 && \
			$(RABBITMQ_UPGRADE) -n "$$nodename" drain; \
		$(MAKE) stop-node \
		  RABBITMQ_NODENAME="$$nodename"; \
		$(MAKE) start-background-broker \
		  NOBUILD=1 \
		  RABBITMQ_NODENAME="$$nodename" \
		  RABBITMQ_NODE_PORT="$$((5672 + $$n - 1))" \
		  RABBITMQ_SERVER_START_ARGS=" \
		  -rabbit loopback_users [] \
		  -rabbitmq_management listener [{port,$$((15672 + $$n - 1))}] \
		  -rabbitmq_mqtt tcp_listeners [$$((1883 + $$n - 1))] \
		  -rabbitmq_web_mqtt tcp_config [{port,$$((1893 + $$n - 1))}] \
		  -rabbitmq_web_mqtt_examples listener [{port,$$((1903 + $$n - 1))}] \
		  -rabbitmq_stomp tcp_listeners [$$((61613 + $$n - 1))] \
		  -rabbitmq_web_stomp tcp_config [{port,$$((61623 + $$n - 1))}] \
		  -rabbitmq_web_stomp_examples listener [{port,$$((61633 + $$n - 1))}] \
		  -rabbitmq_prometheus tcp_config [{port,$$((15692 + $$n - 1))}] \
		  -rabbitmq_stream tcp_listeners [$$((5552 + $$n - 1))] \
		  " & \
	done; \
	wait

# --------------------------------------------------------------------
# Code reloading.
#
# For `make run-broker` either do:
# * make RELOAD=1
# * make all reload-broker        (can't do this alongside -j flag)
# * make && make reload-broker    (fine with -j flag)
#
# Or if recompiling a specific application:
# * make -C deps/rabbit RELOAD=1
#
# For `make start-cluster` use the `reload-cluster` target.
# Same constraints apply as with `reload-broker`:
# * make all reload-cluster
# * make && make reload-cluster
# --------------------------------------------------------------------

reload-broker:
	$(exec_verbose) ERL_LIBS="$(DIST_ERL_LIBS)" \
		$(RABBITMQCTL) -n $(RABBITMQ_NODENAME) \
		eval "io:format(\"~p~n\", [c:lm()])."

ifeq ($(MAKELEVEL),0)
ifdef RELOAD
all:: reload-broker
endif
endif

reload-cluster:
	@for n in $$(seq $(NODES) -1 1); do \
		nodename="rabbit-$$n@$(HOSTNAME)"; \
		$(MAKE) reload-broker \
			RABBITMQ_NODENAME="$$nodename" & \
	done; \
	wait

# --------------------------------------------------------------------
# Used by testsuites.
# --------------------------------------------------------------------

set-resource-alarm:
	$(exec_verbose) ERL_LIBS="$(DIST_ERL_LIBS)" \
	  $(RABBITMQCTL) -n $(RABBITMQ_NODENAME) \
	  eval 'rabbit_alarm:set_alarm({{resource_limit, $(SOURCE), node()}, []}).'

clear-resource-alarm:
	$(exec_verbose) ERL_LIBS="$(DIST_ERL_LIBS)" \
	  $(RABBITMQCTL) -n $(RABBITMQ_NODENAME) \
	  eval 'rabbit_alarm:clear_alarm({resource_limit, $(SOURCE), node()}).'
