PROJECT = rabbit

DEPS += rabbit_common
dep_rabbit_common = git file:///home/dumbbell/Projects/pivotal/other-repos/rabbitmq-common master

define usage_xml_to_erl
$(subst __,_,$(patsubst $(DOCS_DIR)/rabbitmq%.1.xml, src/rabbit_%_usage.erl, $(subst -,_,$(1))))
endef

define usage_dep
$(call usage_xml_to_erl, $(1)):: $(1) $(DOCS_DIR)/usage.xsl
endef

DOCS_DIR     = docs
MANPAGES     = $(patsubst %.xml, %.gz, $(wildcard $(DOCS_DIR)/*.[0-9].xml))
WEB_MANPAGES = $(patsubst %.xml, %.man.xml, $(wildcard $(DOCS_DIR)/*.[0-9].xml) $(DOCS_DIR)/rabbitmq-service.xml $(DOCS_DIR)/rabbitmq-echopid.xml)
USAGES_XML   = $(DOCS_DIR)/rabbitmqctl.1.xml $(DOCS_DIR)/rabbitmq-plugins.1.xml
USAGES_ERL   = $(foreach XML, $(USAGES_XML), $(call usage_xml_to_erl, $(XML)))

EXTRA_SOURCES += $(USAGES_ERL)

ERLANG_MK_DISABLE_PLUGINS = edoc

include erlang.mk

# TODO: Simplify this when support is added to erlang.mk.
ERLANG_MK_3RDPARTY_PLUGINS = $(DEPS_DIR)/rabbit_common/mk/rabbitmq-dist.mk
-include $(ERLANG_MK_3RDPARTY_PLUGINS)
$(ERLANG_MK_3RDPARTY_PLUGINS): $(DEPS_DIR)/rabbit_common
	@:

# --------------------------------------------------------------------
# Compilation.
# --------------------------------------------------------------------

COMPILE_FIRST = $(basename \
		$(notdir \
		$(shell grep -lw '^behaviour_info' src/*.erl)))

RMQ_ERLC_OPTS += -I $(DEPS_DIR)/rabbit_common/include

ifdef INSTRUMENT_FOR_QC
RMQ_ERLC_OPTS += -DINSTR_MOD=gm_qc
else
RMQ_ERLC_OPTS += -DINSTR_MOD=gm
endif

ifdef CREDIT_FLOW_TRACING
RMQ_ERLC_OPTS += -DCREDIT_FLOW_TRACING=true
endif

# Our type specs rely on dict:dict/0 etc, which are only available in
# 17.0 upwards.
define compare_version
$(shell awk 'BEGIN {
	split("$(1)", v1, "\.");
	version1 = v1[1] * 1000000 + v1[2] * 10000 + v1[3] * 100 + v1[4];

	split("$(2)", v2, "\.");
	version2 = v2[1] * 1000000 + v2[2] * 10000 + v2[3] * 100 + v2[4];

	if (version1 $(3) version2) {
		print "true";
	} else {
		print "false";
	}
}')
endef

ERTS_VER = $(shell erl -version 2>&1 | sed -E 's/.* version //')
USE_SPECS_MIN_ERTS_VER = 5.11
ifeq ($(call compare_version,$(ERTS_VER),$(USE_SPECS_MIN_ERTS_VER),>=),true)
RMQ_ERLC_OPTS += -Duse_specs
endif

ifndef USE_PROPER_QC
# PropEr needs to be installed for property checking
# http://proper.softlab.ntua.gr/
USE_PROPER_QC = $(shell $(ERL) -eval 'io:format({module, proper} =:= code:ensure_loaded(proper)), halt().')
RMQ_ERLC_OPTS += $(if $(filter true,$(USE_PROPER_QC)),-Duse_proper_qc)
endif

ERLC_OPTS += $(RMQ_ERLC_OPTS)

# --------------------------------------------------------------------
# Tests.
# --------------------------------------------------------------------

TEST_ERLC_OPTS += $(RMQ_ERLC_OPTS)

# --------------------------------------------------------------------
# Documentation.
# --------------------------------------------------------------------

# xmlto can not read from standard input, so we mess with a tmp file.
%.gz: %.xml $(DOCS_DIR)/examples-to-end.xsl
	$(gen_verbose) xmlto --version | \
	    grep -E '^xmlto version 0\.0\.([0-9]|1[1-8])$$' >/dev/null || \
	    opt='--stringparam man.indent.verbatims=0' ; \
	xsltproc --novalid $(DOCS_DIR)/examples-to-end.xsl $< > $<.tmp && \
	(xmlto -o $(DOCS_DIR) $$opt man $<.tmp 2>&1 | (grep -qv '^Note: Writing' || :)) && \
	gzip -f $(DOCS_DIR)/`basename $< .xml` && \
	rm -f $<.tmp

# Use tmp files rather than a pipeline so that we get meaningful errors
# Do not fold the cp into previous line, it's there to stop the file being
# generated but empty if we fail
src/%_usage.erl:
	$(gen_verbose) xsltproc --novalid --stringparam modulename "`basename $@ .erl`" \
	    $(DOCS_DIR)/usage.xsl $< > $@.tmp && \
	sed -e 's/"/\\"/g' -e 's/%QUOTE%/"/g' $@.tmp > $@.tmp2 && \
	fold -s $@.tmp2 > $@.tmp3 && \
	mv $@.tmp3 $@ && \
	rm $@.tmp $@.tmp2

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

$(foreach XML,$(USAGES_XML),$(eval $(call usage_dep, $(XML))))

docs:: $(MANPAGES) $(WEB_MANPAGES)

distclean:: distclean-manpages

distclean-manpages::
	$(gen_verbose) rm -f $(MANPAGES) $(WEB_MANPAGES)

# --------------------------------------------------------------------
# Running RabbitMQ.
# --------------------------------------------------------------------

TMPDIR ?= /tmp

RABBITMQ_NODENAME ?= rabbit
RABBITMQ_SERVER_START_ARGS ?=
RABBITMQ_MNESIA_DIR ?= $(TMPDIR)/rabbitmq-$(RABBITMQ_NODENAME)-mnesia
RABBITMQ_PLUGINS_EXPAND_DIR ?= $(TMPDIR)/rabbitmq-$(RABBITMQ_NODENAME)-plugins-scratch
RABBITMQ_LOG_BASE ?= $(TMPDIR)

BASIC_SCRIPT_ENVIRONMENT_SETTINGS=\
	RABBITMQ_NODE_IP_ADDRESS="$(RABBITMQ_NODE_IP_ADDRESS)" \
	RABBITMQ_NODE_PORT="$(RABBITMQ_NODE_PORT)" \
	RABBITMQ_LOG_BASE="$(RABBITMQ_LOG_BASE)" \
	RABBITMQ_MNESIA_DIR="$(RABBITMQ_MNESIA_DIR)" \
	RABBITMQ_PLUGINS_EXPAND_DIR="$(RABBITMQ_PLUGINS_EXPAND_DIR)" \
	ERL_LIBS="$(CURDIR)/dist:$(filter-out $(DEPS_DIR),$(subst :, ,$(ERL_LIBS)))"

run: dist
	$(BASIC_SCRIPT_ENVIRONMENT_SETTINGS) \
		RABBITMQ_ALLOW_INPUT=true \
		RABBITMQ_SERVER_START_ARGS="$(RABBITMQ_SERVER_START_ARGS)" \
		./scripts/rabbitmq-server

run-background: all
	$(BASIC_SCRIPT_ENVIRONMENT_SETTINGS) \
		RABBITMQ_SERVER_START_ARGS="$(RABBITMQ_SERVER_START_ARGS)" \
		./scripts/rabbitmq-server -detached

run-node: all
	$(BASIC_SCRIPT_ENVIRONMENT_SETTINGS) \
		RABBITMQ_NODE_ONLY=true \
		RABBITMQ_ALLOW_INPUT=true \
		RABBITMQ_SERVER_START_ARGS="$(RABBITMQ_SERVER_START_ARGS)" \
		./scripts/rabbitmq-server

run-background-node: all
	$(BASIC_SCRIPT_ENVIRONMENT_SETTINGS) \
		RABBITMQ_NODE_ONLY=true \
		RABBITMQ_SERVER_START_ARGS="$(RABBITMQ_SERVER_START_ARGS)" \
		./scripts/rabbitmq-server -detached

run-tests: all
	echo 'code:add_path("$(TEST_EBIN_DIR)").' | $(ERL_CALL)
	echo 'code:add_path("$(TEST_EBIN_DIR)").' | $(ERL_CALL) -n hare || true
	OUT=$$(echo "rabbit_tests:all_tests()." | $(ERL_CALL)) ; \
	  echo $$OUT ; echo $$OUT | grep '^{ok, passed}$$' > /dev/null

run-qc: all
	echo 'code:add_path("$(TEST_EBIN_DIR)").' | $(ERL_CALL)
	./quickcheck $(RABBITMQ_NODENAME) rabbit_backing_queue_qc 100 40
	./quickcheck $(RABBITMQ_NODENAME) gm_qc 1000 200

start-background-node: all
	-rm -f $(RABBITMQ_MNESIA_DIR).pid
	mkdir -p $(RABBITMQ_MNESIA_DIR)
	$(BASIC_SCRIPT_ENVIRONMENT_SETTINGS) \
		RABBITMQ_NODE_ONLY=true \
		RABBITMQ_SERVER_START_ARGS="$(RABBITMQ_SERVER_START_ARGS)" \
		./scripts/rabbitmq-server \
		> $(RABBITMQ_MNESIA_DIR)/startup_log \
		2> $(RABBITMQ_MNESIA_DIR)/startup_err &
	./scripts/rabbitmqctl -n $(RABBITMQ_NODENAME) wait $(RABBITMQ_MNESIA_DIR).pid kernel

start-rabbit-on-node: all
	echo "rabbit:start()." | $(ERL_CALL)
	./scripts/rabbitmqctl -n $(RABBITMQ_NODENAME) wait $(RABBITMQ_MNESIA_DIR).pid

stop-rabbit-on-node: all
	echo "rabbit:stop()." | $(ERL_CALL)

set-resource-alarm: all
	echo "rabbit_alarm:set_alarm({{resource_limit, $(SOURCE), node()}, []})." | \
	$(ERL_CALL)

clear-resource-alarm: all
	echo "rabbit_alarm:clear_alarm({resource_limit, $(SOURCE), node()})." | \
	$(ERL_CALL)

stop-node:
	-( \
	pid=$$(./scripts/rabbitmqctl -n $(RABBITMQ_NODENAME) eval 'os:getpid().') && \
	$(ERL_CALL) -q && \
	while ps -p $$pid >/dev/null 2>&1; do sleep 1; done \
	)

.PHONY: run run-background run-node run-background-node run-tests run-qc \
	start-background-node start-rabbit-on-node stop-rabbit-on-node \
	set-resource-alarm clear-resource-alarm stop-node
