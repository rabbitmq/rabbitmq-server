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
	    {num_ssl_acceptors, 1},
	    {ssl_options, []},
	    {vm_memory_high_watermark, 0.4},
	    {vm_memory_high_watermark_paging_ratio, 0.5},
	    {memory_monitor_interval, 2500},
	    {disk_free_limit, 50000000}, %% 50MB
	    {msg_store_index_module, rabbit_msg_store_ets_index},
	    {backing_queue_module, rabbit_variable_queue},
	    %% 0 ("no limit") would make a better default, but that
	    %% breaks the QPid Java client
	    {frame_max, 131072},
	    {channel_max, 0},
	    {heartbeat, 60},
	    {msg_store_file_size_limit, 16777216},
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
	    {cluster_nodes, {[], disc}},
	    {server_properties, []},
	    {collect_statistics, none},
	    {collect_statistics_interval, 5000},
	    {mnesia_table_loading_retry_timeout, 30000},
	    {mnesia_table_loading_retry_limit, 10},
	    {auth_mechanisms, ['PLAIN', 'AMQPLAIN']},
	    {auth_backends, [rabbit_auth_backend_internal]},
	    {delegate_count, 16},
	    {trace_vhosts, []},
	    {log_levels, [{connection, info}]},
	    {ssl_cert_login_from, distinguished_name},
	    {ssl_handshake_timeout, 5000},
	    {ssl_allow_poodle_attack, false},
	    {handshake_timeout, 10000},
	    {reverse_dns_lookups, false},
	    {cluster_partition_handling, ignore},
	    {cluster_keepalive_interval, 10000},
	    {tcp_listen_options, [{backlog,       128},
	                          {nodelay,       true},
	                          {linger,        {true, 0}},
	                          {exit_on_close, false}
	                         ]},
	    {halt_on_upgrade_failure, true},
	    {hipe_compile, false},
	    %% see bug 24513 for how this list was created
	    {hipe_modules,
	     [rabbit_reader, rabbit_channel, gen_server2, rabbit_exchange,
	      rabbit_command_assembler, rabbit_framing_amqp_0_9_1, rabbit_basic,
	      rabbit_event, lists, queue, priority_queue, rabbit_router,
	      rabbit_trace, rabbit_misc, rabbit_binary_parser,
	      rabbit_exchange_type_direct, rabbit_guid, rabbit_net,
	      rabbit_amqqueue_process, rabbit_variable_queue,
	      rabbit_binary_generator, rabbit_writer, delegate, gb_sets, lqueue,
	      sets, orddict, rabbit_amqqueue, rabbit_limiter, gb_trees,
	      rabbit_queue_index, rabbit_exchange_decorator, gen, dict, ordsets,
	      file_handle_cache, rabbit_msg_store, array,
	      rabbit_msg_store_ets_index, rabbit_msg_file,
	      rabbit_exchange_type_fanout, rabbit_exchange_type_topic, mnesia,
	      mnesia_lib, rpc, mnesia_tm, qlc, sofs, proplists, credit_flow,
	      pmon, ssl_connection, tls_connection, ssl_record, tls_record,
	      gen_fsm, ssl]},
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
	    %% see rabbitmq-server#248
	    %% and rabbitmq-server#667
	    {channel_operation_timeout, 15000},
	    {config_entry_decoder, [{cipher, aes_cbc256},
	                            {hash, sha512},
	                            {iterations, 1000},
	                            {passphrase, undefined}
	                           ]},
	    %% rabbitmq-server-973
	    {queue_explicit_gc_run_operation_threshold, 1000},
	    {lazy_queue_explicit_gc_run_operation_threshold, 1000},
	    {background_gc_enabled, false},
	    {background_gc_target_interval, 60000},
	    {disk_monitor_failure_retries, 10},
	    {disk_monitor_failure_retry_interval, 120000}
	  ]
endef

LOCAL_DEPS = sasl mnesia os_mon xmerl
DEPS = ranch rabbit_common
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers amqp_client meck proper

define usage_xml_to_erl
$(subst __,_,$(patsubst $(DOCS_DIR)/rabbitmq%.1.xml, src/rabbit_%_usage.erl, $(subst -,_,$(1))))
endef

DOCS_DIR     = docs
MANPAGES     = $(patsubst %.xml, %, $(wildcard $(DOCS_DIR)/*.[0-9].xml))
WEB_MANPAGES = $(patsubst %.xml, %.man.xml, $(wildcard $(DOCS_DIR)/*.[0-9].xml) $(DOCS_DIR)/rabbitmq-service.xml $(DOCS_DIR)/rabbitmq-echopid.xml)
USAGES_XML   = $(DOCS_DIR)/rabbitmqctl.1.xml $(DOCS_DIR)/rabbitmq-plugins.1.xml
USAGES_ERL   = $(foreach XML, $(USAGES_XML), $(call usage_xml_to_erl, $(XML)))

EXTRA_SOURCES += $(USAGES_ERL)

.DEFAULT_GOAL = all
$(PROJECT).d:: $(EXTRA_SOURCES)

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

include rabbitmq-components.mk
include erlang.mk

SLOW_CT_SUITES := backing_queue \
		  cluster_rename \
		  clustering_management \
		  dynamic_ha \
		  eager_sync \
		  health_check \
		  partitions \
		  priority_queue \
		  queue_master_location \
		  simple_ha
FAST_CT_SUITES := $(filter-out $(sort $(SLOW_CT_SUITES)),$(CT_SUITES))

ct-fast: CT_SUITES = $(FAST_CT_SUITES)
ct-slow: CT_SUITES = $(SLOW_CT_SUITES)

# --------------------------------------------------------------------
# Compilation.
# --------------------------------------------------------------------

RMQ_ERLC_OPTS += -I $(DEPS_DIR)/rabbit_common/include

ifdef INSTRUMENT_FOR_QC
RMQ_ERLC_OPTS += -DINSTR_MOD=gm_qc
else
RMQ_ERLC_OPTS += -DINSTR_MOD=gm
endif

ifdef CREDIT_FLOW_TRACING
RMQ_ERLC_OPTS += -DCREDIT_FLOW_TRACING=true
endif

ifndef USE_PROPER_QC
# PropEr needs to be installed for property checking
# http://proper.softlab.ntua.gr/
USE_PROPER_QC := $(shell $(ERL) -eval 'io:format({module, proper} =:= code:ensure_loaded(proper)), halt().')
RMQ_ERLC_OPTS += $(if $(filter true,$(USE_PROPER_QC)),-Duse_proper_qc)
endif

clean:: clean-extra-sources

clean-extra-sources:
	$(gen_verbose) rm -f $(EXTRA_SOURCES)

# --------------------------------------------------------------------
# Documentation.
# --------------------------------------------------------------------

# xmlto can not read from standard input, so we mess with a tmp file.
%: %.xml $(DOCS_DIR)/examples-to-end.xsl
	$(gen_verbose) xmlto --version | \
	    grep -E '^xmlto version 0\.0\.([0-9]|1[1-8])$$' >/dev/null || \
	    opt='--stringparam man.indent.verbatims=0' ; \
	xsltproc --novalid $(DOCS_DIR)/examples-to-end.xsl $< > $<.tmp && \
	xmlto -vv -o $(DOCS_DIR) $$opt man $< 2>&1 | (grep -v '^Note: Writing' || :) && \
	awk -F"'u " '/^\.HP / { print $$1; print $$2; next; } { print; }' "$@" > "$@.tmp" && \
	mv "$@.tmp" "$@" && \
	test -f $@ && \
	rm $<.tmp

# Use tmp files rather than a pipeline so that we get meaningful errors
# Do not fold the cp into previous line, it's there to stop the file being
# generated but empty if we fail
define usage_dep
$(call usage_xml_to_erl, $(1)):: $(1) $(DOCS_DIR)/usage.xsl
	$$(gen_verbose) xsltproc --novalid --stringparam modulename "`basename $$@ .erl`" \
	    $(DOCS_DIR)/usage.xsl $$< > $$@.tmp && \
	sed -e 's/"/\\"/g' -e 's/%QUOTE%/"/g' $$@.tmp > $$@.tmp2 && \
	fold -s $$@.tmp2 > $$@.tmp3 && \
	mv $$@.tmp3 $$@ && \
	rm $$@.tmp $$@.tmp2
endef

$(foreach XML,$(USAGES_XML),$(eval $(call usage_dep, $(XML))))

# We rename the file before xmlto sees it since xmlto will use the name of
# the file to make internal links.
%.man.xml: %.xml $(DOCS_DIR)/html-to-website-xml.xsl
	$(gen_verbose) cp $< `basename $< .xml`.xml && \
	    xmlto xhtml-nochunks `basename $< .xml`.xml ; \
	rm `basename $< .xml`.xml && \
	cat `basename $< .xml`.html | \
	    xsltproc --novalid $(DOCS_DIR)/remove-namespaces.xsl - | \
	      xsltproc --novalid --stringparam original `basename $<` $(DOCS_DIR)/html-to-website-xml.xsl - | \
	      xmllint --format - > $@ && \
	rm `basename $< .xml`.html

.PHONY: manpages web-manpages distclean-manpages

docs:: manpages web-manpages

manpages: $(MANPAGES)
	@:

web-manpages: $(WEB_MANPAGES)
	@:

distclean:: distclean-manpages

distclean-manpages::
	$(gen_verbose) rm -f $(MANPAGES) $(WEB_MANPAGES)
