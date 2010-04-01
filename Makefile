
TMPDIR ?= /tmp

RABBITMQ_NODENAME ?= rabbit
RABBITMQ_SERVER_START_ARGS ?=
RABBITMQ_MNESIA_DIR ?= $(TMPDIR)/rabbitmq-$(RABBITMQ_NODENAME)-mnesia
RABBITMQ_LOG_BASE ?= $(TMPDIR)

DEPS_FILE=deps.mk
SOURCE_DIR=src
EBIN_DIR=ebin
INCLUDE_DIR=include
DOCS_DIR=docs
INCLUDES=$(wildcard $(INCLUDE_DIR)/*.hrl) $(INCLUDE_DIR)/rabbit_framing.hrl
SOURCES=$(wildcard $(SOURCE_DIR)/*.erl) $(SOURCE_DIR)/rabbit_framing.erl $(USAGES_ERL)
BEAM_TARGETS=$(patsubst $(SOURCE_DIR)/%.erl, $(EBIN_DIR)/%.beam, $(SOURCES))
TARGETS=$(EBIN_DIR)/rabbit.app $(INCLUDE_DIR)/rabbit_framing.hrl $(BEAM_TARGETS)
WEB_URL=http://stage.rabbitmq.com/
MANPAGES=$(patsubst %.xml, %.gz, $(wildcard $(DOCS_DIR)/*.[0-9].xml))
WEB_MANPAGES=$(patsubst %.xml, %.man.xml, $(wildcard $(DOCS_DIR)/*.[0-9].xml) $(DOCS_DIR)/rabbitmq-service.xml)
USAGES_XML=$(DOCS_DIR)/rabbitmqctl.1.xml $(DOCS_DIR)/rabbitmq-multi.1.xml
USAGES_ERL=$(foreach XML, $(USAGES_XML), $(call usage_xml_to_erl, $(XML)))

ifeq ($(shell python -c 'import simplejson' 2>/dev/null && echo yes),yes)
PYTHON=python
else
ifeq ($(shell python2.6 -c 'import simplejson' 2>/dev/null && echo yes),yes)
PYTHON=python2.6
else
ifeq ($(shell python2.5 -c 'import simplejson' 2>/dev/null && echo yes),yes)
PYTHON=python2.5
else
# Hmm. Missing simplejson?
PYTHON=python
endif
endif
endif

BASIC_PLT=basic.plt
RABBIT_PLT=rabbit.plt

ifndef USE_SPECS
# our type specs rely on features / bug fixes in dialyzer that are
# only available in R13B01 upwards (R13B01 is eshell 5.7.2)
#
# NB: the test assumes that version number will only contain single digits
USE_SPECS=$(shell if [ $$(erl -noshell -eval 'io:format(erlang:system_info(version)), halt().') \> "5.7.1" ]; then echo "true"; else echo "false"; fi)
endif

#other args: +native +"{hipe,[o3,verbose]}" -Ddebug=true +debug_info +no_strict_record_tests
ERLC_OPTS=-I $(INCLUDE_DIR) -o $(EBIN_DIR) -Wall -v +debug_info $(shell [ $(USE_SPECS) = "true" ] && echo "-Duse_specs")

VERSION=0.0.0
TARBALL_NAME=rabbitmq-server-$(VERSION)
TARGET_SRC_DIR=dist/$(TARBALL_NAME)

SIBLING_CODEGEN_DIR=../rabbitmq-codegen/
AMQP_CODEGEN_DIR=$(shell [ -d $(SIBLING_CODEGEN_DIR) ] && echo $(SIBLING_CODEGEN_DIR) || echo codegen)
AMQP_SPEC_JSON_PATH=$(AMQP_CODEGEN_DIR)/amqp-0.8.json

ERL_CALL=erl_call -sname $(RABBITMQ_NODENAME) -e

ERL_EBIN=erl -noinput -pa $(EBIN_DIR)

define usage_xml_to_erl
  $(subst __,_,$(patsubst $(DOCS_DIR)/rabbitmq%.1.xml, $(SOURCE_DIR)/rabbit_%_usage.erl, $(subst -,_,$(1))))
endef

define usage_dep
  $(call usage_xml_to_erl, $(1)): $(1) $(DOCS_DIR)/usage.xsl
endef

all: $(TARGETS)

$(DEPS_FILE): $(SOURCES) $(INCLUDES)
	escript generate_deps $(INCLUDE_DIR) $(SOURCE_DIR) \$$\(EBIN_DIR\) $@

$(EBIN_DIR)/rabbit.app: $(EBIN_DIR)/rabbit_app.in $(BEAM_TARGETS) generate_app
	escript generate_app $(EBIN_DIR) $@ < $<

$(EBIN_DIR)/%.beam:
	erlc $(ERLC_OPTS) -pa $(EBIN_DIR) $<

$(INCLUDE_DIR)/rabbit_framing.hrl: codegen.py $(AMQP_CODEGEN_DIR)/amqp_codegen.py $(AMQP_SPEC_JSON_PATH)
	$(PYTHON) codegen.py header $(AMQP_SPEC_JSON_PATH) $@

$(SOURCE_DIR)/rabbit_framing.erl: codegen.py $(AMQP_CODEGEN_DIR)/amqp_codegen.py $(AMQP_SPEC_JSON_PATH)
	$(PYTHON) codegen.py body   $(AMQP_SPEC_JSON_PATH) $@

dialyze: $(BEAM_TARGETS) $(BASIC_PLT)
	$(ERL_EBIN) -eval \
		"rabbit_dialyzer:halt_with_code(rabbit_dialyzer:dialyze_files(\"$(BASIC_PLT)\", \"$(BEAM_TARGETS)\"))."

# rabbit.plt is used by rabbitmq-erlang-client's dialyze make target
create-plt: $(RABBIT_PLT)

$(RABBIT_PLT): $(BEAM_TARGETS) $(BASIC_PLT)
	cp $(BASIC_PLT) $@
	$(ERL_EBIN) -eval \
	    "rabbit_dialyzer:halt_with_code(rabbit_dialyzer:add_to_plt(\"$@\", \"$(BEAM_TARGETS)\"))."

$(BASIC_PLT): $(BEAM_TARGETS)
	if [ -f $@ ]; then \
	    touch $@; \
	else \
	    $(ERL_EBIN) -eval \
	        "rabbit_dialyzer:halt_with_code(rabbit_dialyzer:create_basic_plt(\"$@\"))."; \
	fi

clean:
	rm -f $(EBIN_DIR)/*.beam
	rm -f $(EBIN_DIR)/rabbit.app $(EBIN_DIR)/rabbit.boot $(EBIN_DIR)/rabbit.script $(EBIN_DIR)/rabbit.rel
	rm -f $(INCLUDE_DIR)/rabbit_framing.hrl $(SOURCE_DIR)/rabbit_framing.erl codegen.pyc
	rm -f $(DOCS_DIR)/*.[0-9].gz $(DOCS_DIR)/*.man.xml $(DOCS_DIR)/*.erl $(USAGES_ERL)
	rm -f $(RABBIT_PLT)
	rm -f $(DEPS_FILE)

cleandb:
	rm -rf $(RABBITMQ_MNESIA_DIR)/*

############ various tasks to interact with RabbitMQ ###################

BASIC_SCRIPT_ENVIRONMENT_SETTINGS=\
	RABBITMQ_NODE_IP_ADDRESS="$(RABBITMQ_NODE_IP_ADDRESS)" \
	RABBITMQ_NODE_PORT="$(RABBITMQ_NODE_PORT)" \
	RABBITMQ_LOG_BASE="$(RABBITMQ_LOG_BASE)" \
	RABBITMQ_MNESIA_DIR="$(RABBITMQ_MNESIA_DIR)"

run: all
	$(BASIC_SCRIPT_ENVIRONMENT_SETTINGS) \
		RABBITMQ_ALLOW_INPUT=true \
		RABBITMQ_SERVER_START_ARGS="$(RABBITMQ_SERVER_START_ARGS)" \
		./scripts/rabbitmq-server

run-node: all
	$(BASIC_SCRIPT_ENVIRONMENT_SETTINGS) \
		RABBITMQ_NODE_ONLY=true \
		RABBITMQ_ALLOW_INPUT=true \
		RABBITMQ_SERVER_START_ARGS="$(RABBITMQ_SERVER_START_ARGS)" \
		./scripts/rabbitmq-server

run-tests: all
	echo "rabbit_tests:all_tests()." | $(ERL_CALL)

start-background-node:
	$(BASIC_SCRIPT_ENVIRONMENT_SETTINGS) \
		RABBITMQ_NODE_ONLY=true \
		RABBITMQ_SERVER_START_ARGS="$(RABBITMQ_SERVER_START_ARGS) -detached" \
		./scripts/rabbitmq-server ; sleep 1

start-rabbit-on-node: all
	echo "rabbit:start()." | $(ERL_CALL)

stop-rabbit-on-node: all
	echo "rabbit:stop()." | $(ERL_CALL)

force-snapshot: all
	echo "rabbit_persister:force_snapshot()." | $(ERL_CALL)

stop-node:
	-$(ERL_CALL) -q

# code coverage will be created for subdirectory "ebin" of COVER_DIR
COVER_DIR=.

start-cover: all
	echo "cover:start(), rabbit_misc:enable_cover([\"$(COVER_DIR)\"])." | $(ERL_CALL)

stop-cover: all
	echo "rabbit_misc:report_cover(), cover:stop()." | $(ERL_CALL)
	cat cover/summary.txt

########################################################################

srcdist: distclean
	mkdir -p $(TARGET_SRC_DIR)/codegen
	cp -r ebin src include LICENSE LICENSE-MPL-RabbitMQ $(TARGET_SRC_DIR)
	cp INSTALL.in $(TARGET_SRC_DIR)/INSTALL
	elinks -dump -no-references -no-numbering $(WEB_URL)install.html \
		>> $(TARGET_SRC_DIR)/INSTALL
	cp README.in $(TARGET_SRC_DIR)/README
	elinks -dump -no-references -no-numbering $(WEB_URL)build-server.html \
		>> $(TARGET_SRC_DIR)/BUILD
	sed -i.save 's/%%VSN%%/$(VERSION)/' $(TARGET_SRC_DIR)/ebin/rabbit_app.in && rm -f $(TARGET_SRC_DIR)/ebin/rabbit_app.in.save

	cp -r $(AMQP_CODEGEN_DIR)/* $(TARGET_SRC_DIR)/codegen/
	cp codegen.py Makefile generate_app generate_deps calculate-relative $(TARGET_SRC_DIR)

	cp -r scripts $(TARGET_SRC_DIR)
	cp -r $(DOCS_DIR) $(TARGET_SRC_DIR)
	chmod 0755 $(TARGET_SRC_DIR)/scripts/*

	(cd dist; tar -zcf $(TARBALL_NAME).tar.gz $(TARBALL_NAME))
	(cd dist; zip -r $(TARBALL_NAME).zip $(TARBALL_NAME))
	rm -rf $(TARGET_SRC_DIR)

distclean: clean
	$(MAKE) -C $(AMQP_CODEGEN_DIR) distclean
	rm -rf dist
	find . -regex '.*\(~\|#\|\.swp\|\.dump\)' -exec rm {} \;

# xmlto can not read from standard input, so we mess with a tmp file.
%.gz: %.xml $(DOCS_DIR)/examples-to-end.xsl
	xsltproc $(DOCS_DIR)/examples-to-end.xsl $< > $<.tmp && \
	xmlto man -o $(DOCS_DIR) --stringparam man.indent.verbatims=0 $<.tmp && \
	gzip -f $(DOCS_DIR)/`basename $< .xml`
	rm -f $<.tmp

# Use tmp files rather than a pipeline so that we get meaningful errors
# Do not fold the cp into previous line, it's there to stop the file being
# generated but empty if we fail
$(SOURCE_DIR)/%_usage.erl:
	xsltproc --stringparam modulename "`basename $@ .erl`" \
		$(DOCS_DIR)/usage.xsl $< > $@.tmp && \
		sed -e s/\\\"/\\\\\\\"/g -e s/%QUOTE%/\\\"/g $@.tmp > $@.tmp2 && \
		fold -s $@.tmp2 > $@.tmp3 && \
		cp $@.tmp3 $@ && \
		rm $@.tmp $@.tmp2 $@.tmp3

# We rename the file before xmlto sees it since xmlto will use the name of
# the file to make internal links.
%.man.xml: %.xml $(DOCS_DIR)/html-to-website-xml.xsl
	cp $< `basename $< .xml`.xml && \
		xmlto xhtml-nochunks `basename $< .xml`.xml ; rm `basename $< .xml`.xml
	cat `basename $< .xml`.html | \
	    xsltproc --novalid $(DOCS_DIR)/remove-namespaces.xsl - | \
		xsltproc --stringparam original `basename $<` $(DOCS_DIR)/html-to-website-xml.xsl - | \
		xmllint --format - > $@
	rm `basename $< .xml`.html

docs_all: $(MANPAGES) $(WEB_MANPAGES)

install: SCRIPTS_REL_PATH=$(shell ./calculate-relative $(TARGET_DIR)/sbin $(SBIN_DIR))
install: all docs_all install_dirs
	@[ -n "$(TARGET_DIR)" ] || (echo "Please set TARGET_DIR."; false)
	@[ -n "$(SBIN_DIR)" ] || (echo "Please set SBIN_DIR."; false)
	@[ -n "$(MAN_DIR)" ] || (echo "Please set MAN_DIR."; false)

	mkdir -p $(TARGET_DIR)
	cp -r ebin include LICENSE LICENSE-MPL-RabbitMQ INSTALL $(TARGET_DIR)

	chmod 0755 scripts/*
	for script in rabbitmq-env rabbitmq-server rabbitmqctl rabbitmq-multi rabbitmq-activate-plugins rabbitmq-deactivate-plugins; do \
		cp scripts/$$script $(TARGET_DIR)/sbin; \
		[ -e $(SBIN_DIR)/$$script ] || ln -s $(SCRIPTS_REL_PATH)/$$script $(SBIN_DIR)/$$script; \
	done
	for section in 1 5; do \
		mkdir -p $(MAN_DIR)/man$$section; \
		for manpage in $(DOCS_DIR)/*.$$section.gz; do \
			cp $$manpage $(MAN_DIR)/man$$section; \
		done; \
	done

install_dirs:
	mkdir -p $(SBIN_DIR)
	mkdir -p $(TARGET_DIR)/sbin

$(foreach XML, $(USAGES_XML), $(eval $(call usage_dep, $(XML))))

# Note that all targets which depend on clean must have clean in their
# name.  Also any target that doesn't depend on clean should not have
# clean in its name, unless you know that you don't need any of the
# automatic dependency generation for that target (eg cleandb).

# We want to load the dep file if *any* target *doesn't* contain
# "clean" - i.e. if removing all clean-like targets leaves something

ifeq "$(MAKECMDGOALS)" ""
TESTABLEGOALS:=$(.DEFAULT_GOAL)
else
TESTABLEGOALS:=$(MAKECMDGOALS)
endif

ifneq "$(strip $(TESTABLEGOALS))" "$(DEPS_FILE)"
ifneq "$(strip $(patsubst clean%,,$(patsubst %clean,,$(TESTABLEGOALS))))" ""
ifeq "$(strip $(wildcard $(DEPS_FILE)))" ""
$(info $(shell $(MAKE) $(DEPS_FILE)))
endif
include $(DEPS_FILE)
endif
endif
