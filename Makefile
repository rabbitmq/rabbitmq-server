PROJECT = rabbit
VERSION ?= $(call get_app_version,src/$(PROJECT).app.src)

DEPS = ranch rabbit_common
TEST_DEPS = rabbitmq_ct_helpers amqp_client meck proper

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

DEP_PLUGINS = rabbit_common/mk/rabbitmq-build.mk \
	      rabbit_common/mk/rabbitmq-run.mk \
	      rabbit_common/mk/rabbitmq-dist.mk \
	      rabbit_common/mk/rabbitmq-tools.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
include erlang.mk

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
