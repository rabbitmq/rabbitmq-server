PROJECT = rabbit

DEPS = rabbitmq_common
dep_rabbitmq_common= git file:///home/dumbbell/Projects/pivotal/other-repos/rabbitmq-common master

.DEFAULT_GOAL = all

# --------------------------------------------------------------------
# Man pages.
# --------------------------------------------------------------------

DOCS_DIR     = docs
MANPAGES     = $(patsubst %.xml, %.gz, $(wildcard $(DOCS_DIR)/*.[0-9].xml))
WEB_MANPAGES = $(patsubst %.xml, %.man.xml, $(wildcard $(DOCS_DIR)/*.[0-9].xml) $(DOCS_DIR)/rabbitmq-service.xml $(DOCS_DIR)/rabbitmq-echopid.xml)
USAGES_XML   = $(DOCS_DIR)/rabbitmqctl.1.xml $(DOCS_DIR)/rabbitmq-plugins.1.xml
USAGES_ERL   = $(foreach XML, $(USAGES_XML), $(call usage_xml_to_erl, $(XML)))

# xmlto can not read from standard input, so we mess with a tmp file.
%.gz: %.xml $(DOCS_DIR)/examples-to-end.xsl
	$(gen_verbose) xmlto --version | \
	    grep -E '^xmlto version 0\.0\.([0-9]|1[1-8])$$' >/dev/null || \
	    opt='--stringparam man.indent.verbatims=0' ; \
	xsltproc --novalid $(DOCS_DIR)/examples-to-end.xsl $< > $<.tmp && \
	xmlto -o $(DOCS_DIR) $$opt man $<.tmp && \
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

define usage_xml_to_erl
$(subst __,_,$(patsubst $(DOCS_DIR)/rabbitmq%.1.xml, src/rabbit_%_usage.erl, $(subst -,_,$(1))))
endef

define usage_dep
$(call usage_xml_to_erl, $(1)):: $(1) $(DOCS_DIR)/usage.xsl
endef

$(foreach XML,$(USAGES_XML),$(eval $(call usage_dep, $(XML))))

# --------------------------------------------------------------------
# Back to main targets.
# --------------------------------------------------------------------

include erlang.mk

ebin/$(PROJECT).app:: $(USAGES_ERL)
	$(if $(strip $?),$(call compile_erl,$?))

clean:: clean-generated

clean-generated:
	$(gen_verbose) rm -f $(USAGES_ERL)

COMPILE_FIRST = $(basename \
		$(notdir \
		$(shell grep -lw '^behaviour_info' src/*.erl)))

ERLC_OPTS += -I $(DEPS_DIR)/rabbitmq_common/include

ifdef INSTRUMENT_FOR_QC
ERLC_OPTS += -DINSTR_MOD=gm_qc
else
ERLC_OPTS += -DINSTR_MOD=gm
endif

ifdef CREDIT_FLOW_TRACING
ERLC_OPTS += -DCREDIT_FLOW_TRACING=true
endif

define boolean_macro
$(if $(filter true,$(1)),-D$(2))
endef

ifndef USE_SPECS
# our type specs rely on dict:dict/0 etc, which are only available in 17.0
# upwards.
USE_SPECS := $(shell erl -noshell -eval 'io:format([list_to_integer(X) || X <- string:tokens(erlang:system_info(version), ".")] >= [5,11]), halt().')
ERLC_OPTS += $(call boolean_macro,$(USE_SPECS),use_specs)
endif

ifndef USE_PROPER_QC
# PropEr needs to be installed for property checking
# http://proper.softlab.ntua.gr/
USE_PROPER_QC := $(shell erl -noshell -eval 'io:format({module, proper} =:= code:ensure_loaded(proper)), halt().')
ERLC_OPTS     += $(call boolean_macro,$(USE_PROPER_QC),use_proper_qc)
endif
