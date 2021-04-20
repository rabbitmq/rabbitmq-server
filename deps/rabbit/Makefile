PROJECT = rabbit
PROJECT_DESCRIPTION = RabbitMQ
PROJECT_MOD = rabbit
PROJECT_REGISTERED = rabbit_amqqueue_sup \
		     rabbit_direct_client_sup \
		     rabbit_log \
		     rabbit_node_monitor \
		     rabbit_router

define PROJECT_ENV
[
	    {tcp_listeners, [5672]},
	    {num_tcp_acceptors, 10},
	    {ssl_listeners, []},
	    {num_ssl_acceptors, 10},
	    {ssl_options, []},
	    {vm_memory_high_watermark, 0.4},
	    {vm_memory_high_watermark_paging_ratio, 0.5},
	    {vm_memory_calculation_strategy, rss},
	    {memory_monitor_interval, 2500},
	    {disk_free_limit, 50000000}, %% 50MB
	    {msg_store_index_module, rabbit_msg_store_ets_index},
	    {backing_queue_module, rabbit_variable_queue},
	    %% 0 ("no limit") would make a better default, but that
	    %% breaks the QPid Java client
	    {frame_max, 131072},
	    %% see rabbitmq-server#1593
	    {channel_max, 2047},
	    {connection_max, infinity},
	    {heartbeat, 60},
	    {msg_store_file_size_limit, 16777216},
	    {msg_store_shutdown_timeout, 600000},
	    {fhc_write_buffering, true},
	    {fhc_read_buffering, false},
	    {queue_index_max_journal_entries, 32768},
	    {queue_index_embed_msgs_below, 4096},
	    {default_user, <<"guest">>},
	    {default_pass, <<"guest">>},
	    {default_user_tags, [administrator]},
	    {default_vhost, <<"/">>},
	    {default_permissions, [<<".*">>, <<".*">>, <<".*">>]},
	    {loopback_users, [<<"guest">>]},
	    {password_hashing_module, rabbit_password_hashing_sha256},
	    {server_properties, []},
	    {collect_statistics, none},
	    {collect_statistics_interval, 5000},
	    {mnesia_table_loading_retry_timeout, 30000},
	    {mnesia_table_loading_retry_limit, 10},
	    {auth_mechanisms, ['PLAIN', 'AMQPLAIN']},
	    {auth_backends, [rabbit_auth_backend_internal]},
	    {delegate_count, 16},
	    {trace_vhosts, []},
	    {ssl_cert_login_from, distinguished_name},
	    {ssl_handshake_timeout, 5000},
	    {ssl_allow_poodle_attack, false},
	    {handshake_timeout, 10000},
	    {reverse_dns_lookups, false},
	    {cluster_partition_handling, ignore},
	    {cluster_keepalive_interval, 10000},
	    {autoheal_state_transition_timeout, 60000},
	    {tcp_listen_options, [{backlog,       128},
	                          {nodelay,       true},
	                          {linger,        {true, 0}},
	                          {exit_on_close, false}
	                         ]},
	    {halt_on_upgrade_failure, true},
	    {ssl_apps, [asn1, crypto, public_key, ssl]},
	    %% see rabbitmq-server#114
	    {mirroring_flow_control, true},
	    {mirroring_sync_batch_size, 4096},
	    %% see rabbitmq-server#227 and related tickets.
	    %% msg_store_credit_disc_bound only takes effect when
	    %% messages are persisted to the message store. If messages
	    %% are embedded on the queue index, then modifying this
	    %% setting has no effect because credit_flow is not used when
	    %% writing to the queue index. See the setting
	    %% queue_index_embed_msgs_below above.
	    {msg_store_credit_disc_bound, {4000, 800}},
	    {msg_store_io_batch_size, 4096},
	    %% see rabbitmq-server#143,
	    %% rabbitmq-server#949, rabbitmq-server#1098
	    {credit_flow_default_credit, {400, 200}},
	    {quorum_commands_soft_limit, 32},
	    {quorum_cluster_size, 3},
	    %% see rabbitmq-server#248
	    %% and rabbitmq-server#667
	    {channel_operation_timeout, 15000},
	    %% 15 minutes
	    {consumer_timeout, 900000},

	    %% see rabbitmq-server#486
	    {autocluster,
              [{peer_discovery_backend, rabbit_peer_discovery_classic_config}]
            },
	    %% used by rabbit_peer_discovery_classic_config
	    {cluster_nodes, {[], disc}},

	    {config_entry_decoder, [{passphrase, undefined}]},

	    %% rabbitmq-server#973
	    {queue_explicit_gc_run_operation_threshold, 1000},
	    {lazy_queue_explicit_gc_run_operation_threshold, 1000},
	    {background_gc_enabled, false},
	    {background_gc_target_interval, 60000},
	    %% rabbitmq-server#589
	    {proxy_protocol, false},
	    {disk_monitor_failure_retries, 10},
	    {disk_monitor_failure_retry_interval, 120000},
	    %% either "stop_node" or "continue".
	    %% by default we choose to not terminate the entire node if one
	    %% vhost had to shut down, see server#1158 and server#1280
	    {vhost_restart_strategy, continue},
	    %% {global, prefetch count}
	    {default_consumer_prefetch, {false, 0}},
		%% interval at which the channel can perform periodic actions
	    {channel_tick_interval, 60000},
	    %% Default max message size is 128 MB
	    {max_message_size, 134217728},
	    %% Socket writer will run GC every 1 GB of outgoing data
	    {writer_gc_threshold, 1000000000},
	    %% interval at which connection/channel tracking executes post operations
	    {tracking_execution_timeout, 15000},
	    {stream_messages_soft_limit, 256},
        {track_auth_attempt_source, false},
        %% Number of entries per index segment.
        %% This value can only be changed safely
        %% on an empty node. Default calculated
        %% as trunc(math:pow(2,?REL_SEQ_BITS))).
        {queue_index_segment_entry_count, 16384}
	  ]
endef

# With Erlang.mk default behavior, the value of `$(APPS_DIR)` is always
# relative to the top-level executed Makefile. In our case, it could be
# a plugin for instance. However, the rabbitmq_prelaunch application is
# in this repository, not the plugin's. That's why we need to override
# this value here.
APPS_DIR := $(CURDIR)/apps

LOCAL_DEPS = sasl rabbitmq_prelaunch os_mon inets compiler public_key crypto ssl syntax_tools xmerl

BUILD_DEPS = rabbitmq_cli
DEPS = ranch rabbit_common ra sysmon_handler stdout_formatter recon observer_cli osiris amqp10_common syslog systemd
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers amqp_client meck proper

PLT_APPS += mnesia

dep_syslog = git https://github.com/schlagert/syslog 4.0.0
dep_osiris = git https://github.com/rabbitmq/osiris master
# TODO: Use systemd from Hex.pm, once there is a new post-0.6.0 release.
dep_systemd = git https://github.com/hauleth/erlang-systemd e732727b0b637eb29e8adc77a4eb46d7ebc0f41a

define usage_xml_to_erl
$(subst __,_,$(patsubst $(DOCS_DIR)/rabbitmq%.1.xml, src/rabbit_%_usage.erl, $(subst -,_,$(1))))
endef

DOCS_DIR     = docs
MANPAGES     = $(wildcard $(DOCS_DIR)/*.[0-9])
WEB_MANPAGES = $(patsubst %,%.html,$(MANPAGES))

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-test.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-build.mk \
	      rabbit_common/mk/rabbitmq-dist.mk \
	      rabbit_common/mk/rabbitmq-run.mk \
	      rabbit_common/mk/rabbitmq-test.mk \
	      rabbit_common/mk/rabbitmq-tools.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include ../../rabbitmq-components.mk
include ../../erlang.mk

# See above why we mess with `$(APPS_DIR)`.
unexport APPS_DIR

ifeq ($(strip $(BATS)),)
BATS := $(ERLANG_MK_TMP)/bats/bin/bats
endif

BATS_GIT ?= https://github.com/sstephenson/bats
BATS_COMMIT ?= v0.4.0

$(BATS):
	$(verbose) mkdir -p $(ERLANG_MK_TMP)
	$(gen_verbose) git clone --depth 1 --branch=$(BATS_COMMIT) $(BATS_GIT) $(ERLANG_MK_TMP)/bats

.PHONY: bats

bats: $(BATS)
	$(verbose) $(BATS) $(TEST_DIR)

tests:: bats

SLOW_CT_SUITES := backing_queue \
		  channel_interceptor \
		  cluster \
		  cluster_rename \
		  clustering_management \
		  config_schema \
		  confirms_rejects \
		  consumer_timeout \
		  crashing_queues \
		  dynamic_ha \
		  dynamic_qq \
		  eager_sync \
		  feature_flags \
		  health_check \
		  lazy_queue \
		  many_node_ha \
		  metrics \
		  msg_store \
		  partitions \
		  per_user_connection_tracking \
		  per_vhost_connection_limit \
		  per_vhost_connection_limit_partitions \
		  per_vhost_msg_store \
		  per_vhost_queue_limit \
		  policy \
		  priority_queue \
		  priority_queue_recovery \
		  publisher_confirms_parallel \
		  queue_master_location \
		  queue_parallel \
		  quorum_queue \
		  rabbit_core_metrics_gc \
		  rabbit_fifo_prop \
		  rabbitmq_queues_cli_integration \
		  rabbitmqctl_integration \
		  simple_ha \
		  sync_detection \
		  unit_inbroker_non_parallel \
		  unit_inbroker_parallel \
		  vhost
FAST_CT_SUITES := $(filter-out $(sort $(SLOW_CT_SUITES)),$(CT_SUITES))

ct-fast: CT_SUITES = $(FAST_CT_SUITES)
ct-slow: CT_SUITES = $(SLOW_CT_SUITES)

# --------------------------------------------------------------------
# Compilation.
# --------------------------------------------------------------------

RMQ_ERLC_OPTS += -I $(DEPS_DIR)/rabbit_common/include
EDOC_OPTS += {preprocess,true}

ifdef INSTRUMENT_FOR_QC
RMQ_ERLC_OPTS += -DINSTR_MOD=gm_qc
EDOC_OPTS += ,{macros,[{'INSTR_MOD',gm_qc}]}
else
RMQ_ERLC_OPTS += -DINSTR_MOD=gm
EDOC_OPTS += ,{macros,[{'INSTR_MOD',gm}]}
endif

ifdef CREDIT_FLOW_TRACING
RMQ_ERLC_OPTS += -DCREDIT_FLOW_TRACING=true
endif

ifdef DEBUG_FF
RMQ_ERLC_OPTS += -DDEBUG_QUORUM_QUEUE_FF=true
endif

ifndef USE_PROPER_QC
# PropEr needs to be installed for property checking
# http://proper.softlab.ntua.gr/
USE_PROPER_QC := $(shell $(ERL) -eval 'io:format({module, proper} =:= code:ensure_loaded(proper)), halt().')
RMQ_ERLC_OPTS += $(if $(filter true,$(USE_PROPER_QC)),-Duse_proper_qc)
endif

# --------------------------------------------------------------------
# Documentation.
# --------------------------------------------------------------------

.PHONY: manpages web-manpages distclean-manpages

docs:: manpages web-manpages

manpages: $(MANPAGES)
	@:

web-manpages: $(WEB_MANPAGES)
	@:

# We use mandoc(1) to convert manpages to HTML plus an awk script which
# does:
#     1. remove tables at the top and the bottom (they recall the
#        manpage name, section and date)
#     2. "downgrade" headers by one level (eg. h1 -> h2)
#     3. annotate .Dl lines with more CSS classes
%.html: %
	$(gen_verbose) mandoc -T html -O 'fragment,man=%N.%S.html' "$<" | \
	  awk '\
	  /^<table class="head">$$/ { remove_table=1; next; } \
	  /^<table class="foot">$$/ { remove_table=1; next; } \
	  /^<\/table>$$/ { if (remove_table) { remove_table=0; next; } } \
	  { if (!remove_table) { \
	    line=$$0; \
	    gsub(/<h2/, "<h3", line); \
	    gsub(/<\/h2>/, "</h3>", line); \
	    gsub(/<h1/, "<h2", line); \
	    gsub(/<\/h1>/, "</h2>", line); \
	    gsub(/class="D1"/, "class=\"D1 lang-bash\"", line); \
	    gsub(/class="Bd Bd-indent"/, "class=\"Bd Bd-indent lang-bash\"", line); \
	    gsub(/&#[xX]201[cCdD];/, "\\&quot;", line); \
	    print line; \
	  } } \
	  ' > "$@"

distclean:: distclean-manpages

distclean-manpages::
	$(gen_verbose) rm -f $(WEB_MANPAGES)
