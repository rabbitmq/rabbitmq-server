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
	    {vm_memory_high_watermark, 0.6},
	    {vm_memory_calculation_strategy, rss},
	    {disk_free_limit, 50000000}, %% 50MB
	    {backing_queue_module, rabbit_variable_queue},
	    %% 0 ("no limit") would make a better default, but that
	    %% breaks the QPid Java client
	    {frame_max, 131072},
	    %% see rabbitmq-server#1593
	    {channel_max, 2047},
	    {session_max_per_connection, 64},
	    {link_max_per_session, 256},
	    {ranch_connection_max, infinity},
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
	    %% The identity to act as for anonymous logins.
	    {anonymous_login_user, <<"guest">>},
	    {anonymous_login_pass, <<"guest">>},
	    %% "The server mechanisms are ordered in decreasing level of preference."
	    %% AMQP §5.3.3.1
	    {auth_mechanisms, ['PLAIN', 'AMQPLAIN', 'ANONYMOUS']},
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
	    {ssl_apps, [asn1, crypto, public_key, ssl]},
	    %% see rabbitmq-server#114
            {classic_queue_flow_control, true},
	    %% see rabbitmq-server#227 and related tickets.
	    %% msg_store_credit_disc_bound only takes effect when
	    %% messages are persisted to the message store. If messages
	    %% are embedded on the queue index, then modifying this
	    %% setting has no effect because credit_flow is not used when
	    %% writing to the queue index. See the setting
	    %% queue_index_embed_msgs_below above.
	    {msg_store_credit_disc_bound, {4000, 800}},
	    %% see rabbitmq-server#143,
	    %% rabbitmq-server#949, rabbitmq-server#1098
	    {credit_flow_default_credit, {400, 200}},
	    {quorum_commands_soft_limit, 32},
	    {quorum_cluster_size, 3},
	    %% see rabbitmq-server#248
	    %% and rabbitmq-server#667
	    {channel_operation_timeout, 15000},
	    %% See https://www.rabbitmq.com/docs/consumers#acknowledgement-timeout
	    %% 30 minutes
	    {consumer_timeout, 1800000},

	    %% used by rabbit_peer_discovery_classic_config
	    {cluster_nodes, {[], disc}},

	    {config_entry_decoder, [{passphrase, undefined}]},
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
	    %% Default max message size is 16 MB
	    {max_message_size, 16777216},
	    %% Socket writer will run GC every 1 GB of outgoing data
	    {writer_gc_threshold, 1000000000},
	    %% interval at which connection/channel tracking executes post operations
	    {tracking_execution_timeout, 15000},
	    {stream_messages_soft_limit, 256},
      	{track_auth_attempt_source, false},
		{credentials_obfuscation_fallback_secret, <<"nocookie">>},
      	{dead_letter_worker_consumer_prefetch, 32},
      	{dead_letter_worker_publisher_confirm_timeout, 180000},
		{vhost_process_reconciliation_run_interval, 30},
		%% for testing
		{vhost_process_reconciliation_enabled, true},
		{license_line, "Licensed under the MPL 2.0. Website: https://rabbitmq.com"}
	  ]
endef

LOCAL_DEPS = sasl os_mon inets compiler public_key crypto ssl syntax_tools xmerl

BUILD_DEPS = rabbitmq_cli
DEPS = ranch cowlib rabbit_common amqp10_common rabbitmq_prelaunch ra sysmon_handler stdout_formatter recon redbug observer_cli osiris syslog systemd seshat horus khepri khepri_mnesia_migration cuttlefish gen_batch_server
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers meck proper amqp_client rabbitmq_amqp_client rabbitmq_amqp1_0

# We pin a version of Horus even if we don't use it directly (it is a
# dependency of Khepri). But currently, we can't update Khepri while still
# needing the fix in Horus 0.3.1. This line and the mention of `horus` above
# should be removed with the next update of Khepri.
dep_horus = hex 0.3.1

PLT_APPS += mnesia runtime_tools

dep_syslog = git https://github.com/schlagert/syslog 4.0.0

define usage_xml_to_erl
$(subst __,_,$(patsubst $(DOCS_DIR)/rabbitmq%.1.xml, src/rabbit_%_usage.erl, $(subst -,_,$(1))))
endef

DOCS_DIR     = docs
MANPAGES     = $(wildcard $(DOCS_DIR)/*.[0-9])
WEB_MANPAGES = $(patsubst %,%.html,$(MANPAGES))
MD_MANPAGES  = $(patsubst %,%.md,$(MANPAGES))

CT_HOOKS = rabbit_ct_hook
DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk

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

SLOW_CT_SUITES := amqp_client \
		  backing_queue \
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
		  many_node_ha \
		  metrics \
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

ct-fast:
	$(MAKE) ct CT_SUITES='$(FAST_CT_SUITES)'

ct-slow:
	$(MAKE) ct CT_SUITES='$(SLOW_CT_SUITES)'

# Parallel CT.
#
# @todo We must ensure that the CT_OPTS also apply to ct-master
# @todo We should probably refactor ct_master.erl to have node init in a separate .erl

define ct_master.erl
	StartOpts = #{
		host => "localhost",
		connection => standard_io,
		args => ["-hidden"]
	},
	{ok, Pid1, _} = peer:start(StartOpts#{name => "rabbit_shard1"}),
	{ok, Pid2, _} = peer:start(StartOpts#{name => "rabbit_shard2"}),
	{ok, Pid3, _} = peer:start(StartOpts#{name => "rabbit_shard3"}),
	{ok, Pid4, _} = peer:start(StartOpts#{name => "rabbit_shard4"}),
	peer:call(Pid1, net_kernel, set_net_ticktime, [5]),
	peer:call(Pid2, net_kernel, set_net_ticktime, [5]),
	peer:call(Pid3, net_kernel, set_net_ticktime, [5]),
	peer:call(Pid4, net_kernel, set_net_ticktime, [5]),
	peer:call(Pid1, persistent_term, put, [rabbit_ct_tcp_port_base, 16000]),
	peer:call(Pid2, persistent_term, put, [rabbit_ct_tcp_port_base, 20000]),
	peer:call(Pid3, persistent_term, put, [rabbit_ct_tcp_port_base, 24000]),
	peer:call(Pid4, persistent_term, put, [rabbit_ct_tcp_port_base, 28000]),
	[{[_], {ok, Results}}] = ct_master_fork:run("$1"),
	peer:stop(Pid4),
	peer:stop(Pid3),
	peer:stop(Pid2),
	peer:stop(Pid1),
	lists:foldl(fun
		({_, {_, 0, {_, 0}}}, Err) -> Err + 1;
		(What, Peer) -> halt(Peer)
	end, 1, Results),
	halt(0)
endef

PARALLEL_CT_SET_1_A = unit_rabbit_ssl unit_cluster_formation_locking_mocks unit_cluster_formation_sort_nodes unit_collections unit_config_value_encryption unit_connection_tracking
PARALLEL_CT_SET_1_B = amqp_address amqp_auth amqp_credit_api_v2 amqp_filter_prop amqp_filter_sql amqp_jms_unit amqp_dotnet amqp_jms signal_handling single_active_consumer unit_access_control_authn_authz_context_propagation unit_access_control_credential_validation unit_amqp091_content_framing unit_amqp091_server_properties unit_app_management
PARALLEL_CT_SET_1_C = amqp_proxy_protocol amqpl_consumer_ack amqpl_direct_reply_to backing_queue bindings rabbit_db_maintenance rabbit_db_msup rabbit_db_policy rabbit_db_queue rabbit_db_topic_exchange rabbit_direct_reply_to_prop cluster_limit cluster_minority term_to_binary_compat_prop topic_permission transactions unicode unit_access_control
PARALLEL_CT_SET_1_D = amqqueue_backward_compatibility channel_interceptor channel_operation_timeout classic_queue classic_queue_prop config_schema peer_discovery_dns peer_discovery_tmp_hidden_node per_node_limit per_user_connection_channel_limit

PARALLEL_CT_SET_2_A = cluster confirms_rejects consumer_timeout rabbit_access_control rabbit_confirms rabbit_core_metrics_gc rabbit_cuttlefish rabbit_db_binding rabbit_db_exchange
PARALLEL_CT_SET_2_B = clustering_recovery crashing_queues deprecated_features direct_exchange_routing_v2 disconnect_detected_during_alarm exchanges unit_gen_server2
PARALLEL_CT_SET_2_C = disk_monitor dynamic_qq unit_disk_monitor unit_file_handle_cache unit_log_management unit_operator_policy prevent_startup_if_node_was_reset
PARALLEL_CT_SET_2_D = queue_length_limits queue_parallel quorum_queue_member_reconciliation rabbit_fifo rabbit_fifo_dlx rabbit_stream_coordinator

PARALLEL_CT_SET_3_A = definition_import per_user_connection_channel_limit_partitions per_vhost_connection_limit_partitions policy priority_queue_recovery rabbit_fifo_v0 rabbit_stream_sac_coordinator_v4 rabbit_stream_sac_coordinator unit_credit_flow unit_queue_consumers unit_queue_location unit_quorum_queue
PARALLEL_CT_SET_3_B = cluster_upgrade list_consumers_sanity_check list_queues_online_and_offline logging lqueue maintenance_mode rabbit_fifo_q
PARALLEL_CT_SET_3_C = cli_forget_cluster_node feature_flags_v2 mc_unit message_containers_deaths_v2 message_size_limit metadata_store_migration
PARALLEL_CT_SET_3_D = metadata_store_phase1 metrics mirrored_supervisor peer_discovery_classic_config proxy_protocol runtime_parameters unit_stats_and_metrics unit_supervisor2 unit_vm_memory_monitor

PARALLEL_CT_SET_4_A = clustering_events rabbit_local_random_exchange rabbit_msg_interceptor rabbitmq_4_0_deprecations unit_pg_local unit_plugin_directories unit_plugin_versioning unit_policy_validators unit_priority_queue
PARALLEL_CT_SET_4_B = per_user_connection_tracking per_vhost_connection_limit rabbit_fifo_dlx_integration rabbit_fifo_int
PARALLEL_CT_SET_4_C = msg_size_metrics unit_msg_size_metrics per_vhost_msg_store per_vhost_queue_limit priority_queue upgrade_preparation vhost
PARALLEL_CT_SET_4_D = per_user_connection_channel_tracking product_info publisher_confirms_parallel queue_type rabbitmq_queues_cli_integration rabbitmqctl_integration rabbitmqctl_shutdown routing rabbit_amqqueue

PARALLEL_CT_SET_1 = $(sort $(PARALLEL_CT_SET_1_A) $(PARALLEL_CT_SET_1_B) $(PARALLEL_CT_SET_1_C) $(PARALLEL_CT_SET_1_D))
PARALLEL_CT_SET_2 = $(sort $(PARALLEL_CT_SET_2_A) $(PARALLEL_CT_SET_2_B) $(PARALLEL_CT_SET_2_C) $(PARALLEL_CT_SET_2_D))
PARALLEL_CT_SET_3 = $(sort $(PARALLEL_CT_SET_3_A) $(PARALLEL_CT_SET_3_B) $(PARALLEL_CT_SET_3_C) $(PARALLEL_CT_SET_3_D))
PARALLEL_CT_SET_4 = $(sort $(PARALLEL_CT_SET_4_A) $(PARALLEL_CT_SET_4_B) $(PARALLEL_CT_SET_4_C) $(PARALLEL_CT_SET_4_D))

SEQUENTIAL_CT_SUITES = amqp_client clustering_management dead_lettering feature_flags metadata_store_clustering quorum_queue rabbit_stream_queue rabbit_fifo_prop
PARALLEL_CT_SUITES = $(PARALLEL_CT_SET_1) $(PARALLEL_CT_SET_2) $(PARALLEL_CT_SET_3) $(PARALLEL_CT_SET_4)

ifeq ($(filter-out $(SEQUENTIAL_CT_SUITES) $(PARALLEL_CT_SUITES),$(CT_SUITES)),)
parallel-ct-sanity-check:
	$(verbose) :
else
parallel-ct-sanity-check:
	$(verbose) printf "%s\n" \
		"In order for new test suites to be run in CI, the test suites" \
		"must be added to one of the PARALLEL_CT_SET_<N>_<M> variables." \
		"" \
		"The following test suites are missing:" \
		"$(filter-out $(SEQUENTIAL_CT_SUITES) $(PARALLEL_CT_SUITES),$(CT_SUITES))"
	$(verbose) exit 1
endif

define tpl_parallel_ct_test_spec
{logdir, "$(CT_LOGS_DIR)"}.
{logdir, master, "$(CT_LOGS_DIR)"}.
{create_priv_dir, all_nodes, auto_per_run}.
{auto_compile, false}.

{node, shard1, 'rabbit_shard1@localhost'}.
{node, shard2, 'rabbit_shard2@localhost'}.
{node, shard3, 'rabbit_shard3@localhost'}.
{node, shard4, 'rabbit_shard4@localhost'}.

{define, 'Set1', [$(call comma_list,$(addsuffix _SUITE,$1))]}.
{define, 'Set2', [$(call comma_list,$(addsuffix _SUITE,$2))]}.
{define, 'Set3', [$(call comma_list,$(addsuffix _SUITE,$3))]}.
{define, 'Set4', [$(call comma_list,$(addsuffix _SUITE,$4))]}.

{suites, shard1, "test/", 'Set1'}.
{suites, shard2, "test/", 'Set2'}.
{suites, shard3, "test/", 'Set3'}.
{suites, shard4, "test/", 'Set4'}.
endef

define parallel_ct_set_target
tpl_parallel_ct_test_spec_set_$1 = $$(call tpl_parallel_ct_test_spec,$(PARALLEL_CT_SET_$(1)_A),$(PARALLEL_CT_SET_$(1)_B),$(PARALLEL_CT_SET_$(1)_C),$(PARALLEL_CT_SET_$(1)_D))

parallel-ct-set-$(1): test-build
	$(verbose) mkdir -p $(CT_LOGS_DIR)
	$(verbose) $$(call core_render,tpl_parallel_ct_test_spec_set_$(1),ct.set-$(1).spec)
	$$(eval ERL := erl -noinput -boot no_dot_erlang)
	$$(call erlang,$$(call ct_master.erl,ct.set-$(1).spec),-sname parallel_ct_$(PROJECT)@localhost -hidden -kernel net_ticktime 5)
endef

$(foreach set,1 2 3 4,$(eval $(call parallel_ct_set_target,$(set))))

# @todo Generate ct.test.spec from Makefile variables instead of hardcoded for ct-master

parallel-ct: test-build
	$(verbose) mkdir -p $(CT_LOGS_DIR)
	$(eval ERL := erl -noinput -boot no_dot_erlang)
	$(call erlang,$(call ct_master.erl,ct.test.spec),-sname parallel_ct_$(PROJECT)@localhost -hidden -kernel net_ticktime 5)

# --------------------------------------------------------------------
# Compilation.
# --------------------------------------------------------------------

RMQ_ERLC_OPTS += -I $(DEPS_DIR)/rabbit_common/include
EDOC_OPTS += {preprocess,true},{includes,["."]}

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

ifdef TRACE_SUPERVISOR2
RMQ_ERLC_OPTS += -DTRACE_SUPERVISOR2=true
endif

# https://www.erlang.org/doc/apps/parsetools/leex.html#file/2
export ERL_COMPILER_OPTIONS := deterministic

# --------------------------------------------------------------------
# Documentation.
# --------------------------------------------------------------------

.PHONY: manpages web-manpages distclean-manpages

docs:: manpages web-manpages

manpages: $(MANPAGES)
	@:

web-manpages: $(WEB_MANPAGES) $(MD_MANPAGES)
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
	    gsub(/\.html/, "", line); \
	    print line; \
	  } } \
	  ' > "$@"

%.md: %
	$(gen_verbose) mandoc -T markdown -O 'fragment,man=%N.%S.md' "$<" | \
	  sed -E -e 's/\{/\&lcub;/g' \
	> "$@"

distclean:: distclean-manpages

distclean-manpages::
	$(gen_verbose) rm -f $(WEB_MANPAGES) $(MD_MANPAGES)
