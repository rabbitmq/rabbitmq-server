PROJECT = rabbit

DEPS = rabbit_common

# For RabbitMQ repositories, we want to checkout branches which match
# the parent porject. For instance, if the parent project is on a
# release tag, dependencies must be on the same release tag. If the
# parent project is on a topic branch, dependencies must be on the same
# topic branch or fallback to `stable` or `master` whichever was the
# base of the topic branch.

ifeq ($(origin current_rmq_ref),undefined)
current_rmq_ref := $(shell git symbolic-ref -q --short HEAD || git describe --tags --exact-match)
export current_rmq_ref
endif
ifeq ($(origin base_rmq_ref),undefined)
base_rmq_ref := $(shell git merge-base --is-ancestor $$(git merge-base master HEAD) stable && echo stable || echo master)
export base_rmq_ref
endif

dep_rabbit_common = git https://github.com/rabbitmq/rabbitmq-common.git $(current_rmq_ref) $(base_rmq_ref)

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

.DEFAULT_GOAL = all

EXTRA_SOURCES += $(USAGES_ERL)

$(PROJECT).d:: $(EXTRA_SOURCES)

DEP_PLUGINS = rabbit_common/mk/rabbitmq-run.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_GIT_REPOSITORY = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_GIT_REF = rabbitmq-tmp

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

clean:: clean-extra-sources

clean-extra-sources:
	$(gen_verbose) rm -f $(EXTRA_SOURCES)

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
src/%_usage.erl::
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
# Distribution.
# --------------------------------------------------------------------

.PHONY: source-dist

SOURCE_DIST_BASE ?= rabbitmq-server
SOURCE_DIST_SUFFIXES ?= tar.xz
SOURCE_DIST ?= $(SOURCE_DIST_BASE)-$(VERSION)

SOURCE_DIST_FILES = $(addprefix $(SOURCE_DIST).,$(SOURCE_DIST_SUFFIXES))

SRCDIST_DEPS ?= rabbitmq_shovel

dep_rabbitmq_shovel = git https://github.com/rabbitmq/rabbitmq-shovel.git $(current_rmq_ref) $(base_rmq_ref)

ALL_SRCDIST_DEPS_DIRS = $(addprefix $(DEPS_DIR)/,$(SRCDIST_DEPS))

$(foreach dep,$(SRCDIST_DEPS),$(eval $(call dep_target,$(dep))))

.PHONY: $(SOURCE_DIST_FILES)

source-dist: $(SOURCE_DIST_FILES)
	@:

TAR ?= tar
TAR_VERSION = $(shell $(TAR) --version)

TAR_V_0 =
TAR_V_1 = -v
TAR_V = $(TAR_V_$(V))
TAR_FLAGS = $(TAR_V) -cf -				\
	    --exclude '.sw?' --exclude '.*.sw?'		\
	    --exclude '*.beam'				\
	    --exclude '.git*'				\
	    --exclude '$(notdir $(ERLANG_MK_TMP))'	\
	    --exclude '$(SOURCE_DIST_BASE)-*'		\
	    --exclude 'packaging'

ifneq (,$(findstring GNU tar,$(TAR_VERSION)))
define tar_source_dist
$(TAR) $(TAR_FLAGS) \
	--transform 's/^\./$(SOURCE_DIST)/' \
	--show-transformed \
	.
endef
endif

ifneq (,$(findstring bsdtar,$(TAR_VERSION)))
define tar_source_dist
$(TAR) $(TAR_FLAGS) \
	-s '/^\./$(SOURCE_DIST)/' \
	.
endef
endif

$(SOURCE_DIST).tar.gz: $(ALL_DEPS_DIRS) $(ALL_SRCDIST_DEPS_DIRS)
	$(gen_verbose) $(call tar_source_dist) \
	| gzip --best > $@

$(SOURCE_DIST).tar.bz2: $(ALL_DEPS_DIRS) $(ALL_SRCDIST_DEPS_DIRS)
	$(gen_verbose) $(call tar_source_dist) \
	| bzip2 > $@

$(SOURCE_DIST).tar.xz: $(ALL_DEPS_DIRS) $(ALL_SRCDIST_DEPS_DIRS)
	$(gen_verbose) $(call tar_source_dist) \
	| xz > $@

clean:: clean-source-dist

clean-source-dist:
	$(gen_verbose) rm -f $(SOURCE_DIST).*
