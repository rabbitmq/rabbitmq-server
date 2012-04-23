TMPDIR ?= /tmp

RABBITMQ_NODENAME ?= rabbit
RABBITMQ_SERVER_START_ARGS ?=
RABBITMQ_MNESIA_DIR ?= $(TMPDIR)/rabbitmq-$(RABBITMQ_NODENAME)-mnesia
RABBITMQ_PLUGINS_EXPAND_DIR ?= $(TMPDIR)/rabbitmq-$(RABBITMQ_NODENAME)-plugins-scratch
RABBITMQ_LOG_BASE ?= $(TMPDIR)

DEPS_FILE=deps.mk
SOURCE_DIR=src
EBIN_DIR=ebin
INCLUDE_DIR=include
DOCS_DIR=docs
INCLUDES=$(wildcard $(INCLUDE_DIR)/*.hrl) $(INCLUDE_DIR)/rabbit_framing.hrl
SOURCES=$(wildcard $(SOURCE_DIR)/*.erl) $(SOURCE_DIR)/rabbit_framing_amqp_0_9_1.erl $(SOURCE_DIR)/rabbit_framing_amqp_0_8.erl $(USAGES_ERL)
BEAM_TARGETS=$(patsubst $(SOURCE_DIR)/%.erl, $(EBIN_DIR)/%.beam, $(SOURCES))
TARGETS=$(EBIN_DIR)/rabbit.app $(INCLUDE_DIR)/rabbit_framing.hrl $(BEAM_TARGETS) plugins
WEB_URL=http://www.rabbitmq.com/
MANPAGES=$(patsubst %.xml, %.gz, $(wildcard $(DOCS_DIR)/*.[0-9].xml))
WEB_MANPAGES=$(patsubst %.xml, %.man.xml, $(wildcard $(DOCS_DIR)/*.[0-9].xml) $(DOCS_DIR)/rabbitmq-service.xml $(DOCS_DIR)/rabbitmq-echopid.xml)
USAGES_XML=$(DOCS_DIR)/rabbitmqctl.1.xml $(DOCS_DIR)/rabbitmq-plugins.1.xml
USAGES_ERL=$(foreach XML, $(USAGES_XML), $(call usage_xml_to_erl, $(XML)))
QC_MODULES := rabbit_backing_queue_qc
QC_TRIALS ?= 100

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
# our type specs rely on callback specs, which are available in R15B
# upwards.
USE_SPECS:=$(shell erl -noshell -eval 'io:format([list_to_integer(X) || X <- string:tokens(erlang:system_info(version), ".")] >= [5,9]), halt().')
endif

ifndef USE_PROPER_QC
# PropEr needs to be installed for property checking
# http://proper.softlab.ntua.gr/
USE_PROPER_QC:=$(shell erl -noshell -eval 'io:format({module, proper} =:= code:ensure_loaded(proper)), halt().')
endif

#other args: +native +"{hipe,[o3,verbose]}" -Ddebug=true +debug_info +no_strict_record_tests
ERLC_OPTS=-I $(INCLUDE_DIR) -o $(EBIN_DIR) -Wall -v +debug_info $(call boolean_macro,$(USE_SPECS),use_specs) $(call boolean_macro,$(USE_PROPER_QC),use_proper_qc)

VERSION?=0.0.0
PLUGINS_SRC_DIR?=$(shell [ -d "plugins-src" ] && echo "plugins-src" || echo )
PLUGINS_DIR=plugins
TARBALL_NAME=rabbitmq-server-$(VERSION)
TARGET_SRC_DIR=dist/$(TARBALL_NAME)

SIBLING_CODEGEN_DIR=../rabbitmq-codegen/
AMQP_CODEGEN_DIR=$(shell [ -d $(SIBLING_CODEGEN_DIR) ] && echo $(SIBLING_CODEGEN_DIR) || echo codegen)
AMQP_SPEC_JSON_FILES_0_9_1=$(AMQP_CODEGEN_DIR)/amqp-rabbitmq-0.9.1.json
AMQP_SPEC_JSON_FILES_0_8=$(AMQP_CODEGEN_DIR)/amqp-rabbitmq-0.8.json

ERL_CALL=erl_call -sname $(RABBITMQ_NODENAME) -e

ERL_EBIN=erl -noinput -pa $(EBIN_DIR)

define usage_xml_to_erl
  $(subst __,_,$(patsubst $(DOCS_DIR)/rabbitmq%.1.xml, $(SOURCE_DIR)/rabbit_%_usage.erl, $(subst -,_,$(1))))
endef

define usage_dep
  $(call usage_xml_to_erl, $(1)): $(1) $(DOCS_DIR)/usage.xsl
endef

define boolean_macro
$(if $(filter true,$(1)),-D$(2))
endef

ifneq "$(SBIN_DIR)" ""
ifneq "$(TARGET_DIR)" ""
SCRIPTS_REL_PATH=$(shell ./calculate-relative $(TARGET_DIR)/sbin $(SBIN_DIR))
endif
endif

# Versions prior to this are not supported
NEED_MAKE := 3.80
ifneq "$(NEED_MAKE)" "$(firstword $(sort $(NEED_MAKE) $(MAKE_VERSION)))"
$(error Versions of make prior to $(NEED_MAKE) are not supported)
endif

# .DEFAULT_GOAL introduced in 3.81
DEFAULT_GOAL_MAKE := 3.81
ifneq "$(DEFAULT_GOAL_MAKE)" "$(firstword $(sort $(DEFAULT_GOAL_MAKE) $(MAKE_VERSION)))"
.DEFAULT_GOAL=all
endif

all: $(TARGETS)

.PHONY: plugins
ifneq "$(PLUGINS_SRC_DIR)" ""
plugins:
	[ -d "$(PLUGINS_SRC_DIR)/rabbitmq-server" ] || ln -s "$(CURDIR)" "$(PLUGINS_SRC_DIR)/rabbitmq-server"
	mkdir -p $(PLUGINS_DIR)
	PLUGINS_SRC_DIR="" $(MAKE) -C "$(PLUGINS_SRC_DIR)" plugins-dist PLUGINS_DIST_DIR="$(CURDIR)/$(PLUGINS_DIR)" VERSION=$(VERSION)
	echo "Put your EZs here and use rabbitmq-plugins to enable them." > $(PLUGINS_DIR)/README
	rm -f $(PLUGINS_DIR)/rabbit_common*.ez
else
plugins:
# Not building plugins
endif

$(DEPS_FILE): $(SOURCES) $(INCLUDES)
	rm -f $@
	echo $(subst : ,:,$(foreach FILE,$^,$(FILE):)) | escript generate_deps $@ $(EBIN_DIR)

$(EBIN_DIR)/rabbit.app: $(EBIN_DIR)/rabbit_app.in $(SOURCES) generate_app
	escript generate_app $< $@ $(SOURCE_DIR)

$(EBIN_DIR)/%.beam: $(SOURCE_DIR)/%.erl | $(DEPS_FILE)
	erlc $(ERLC_OPTS) -pa $(EBIN_DIR) $<

$(INCLUDE_DIR)/rabbit_framing.hrl: codegen.py $(AMQP_CODEGEN_DIR)/amqp_codegen.py $(AMQP_SPEC_JSON_FILES_0_9_1) $(AMQP_SPEC_JSON_FILES_0_8)
	$(PYTHON) codegen.py --ignore-conflicts header $(AMQP_SPEC_JSON_FILES_0_9_1) $(AMQP_SPEC_JSON_FILES_0_8) $@

$(SOURCE_DIR)/rabbit_framing_amqp_0_9_1.erl: codegen.py $(AMQP_CODEGEN_DIR)/amqp_codegen.py $(AMQP_SPEC_JSON_FILES_0_9_1)
	$(PYTHON) codegen.py body $(AMQP_SPEC_JSON_FILES_0_9_1) $@

$(SOURCE_DIR)/rabbit_framing_amqp_0_8.erl: codegen.py $(AMQP_CODEGEN_DIR)/amqp_codegen.py $(AMQP_SPEC_JSON_FILES_0_8)
	$(PYTHON) codegen.py body $(AMQP_SPEC_JSON_FILES_0_8) $@

dialyze: $(BEAM_TARGETS) $(BASIC_PLT)
	dialyzer --plt $(BASIC_PLT) --no_native --fullpath \
	  -Wrace_conditions $(BEAM_TARGETS)

# rabbit.plt is used by rabbitmq-erlang-client's dialyze make target
create-plt: $(RABBIT_PLT)

$(RABBIT_PLT): $(BEAM_TARGETS) $(BASIC_PLT)
	dialyzer --plt $(BASIC_PLT) --output_plt $@ --no_native \
	  --add_to_plt $(BEAM_TARGETS)

$(BASIC_PLT): $(BEAM_TARGETS)
	if [ -f $@ ]; then \
	    touch $@; \
	else \
	    dialyzer --output_plt $@ --build_plt \
		--apps erts kernel stdlib compiler sasl os_mon mnesia tools \
		  public_key crypto ssl; \
	fi

clean:
	rm -f $(EBIN_DIR)/*.beam
	rm -f $(EBIN_DIR)/rabbit.app $(EBIN_DIR)/rabbit.boot $(EBIN_DIR)/rabbit.script $(EBIN_DIR)/rabbit.rel
	rm -f $(PLUGINS_DIR)/*.ez
	[ -d "$(PLUGINS_SRC_DIR)" ] && PLUGINS_SRC_DIR="" PRESERVE_CLONE_DIR=1 make -C $(PLUGINS_SRC_DIR) clean || true
	rm -f $(INCLUDE_DIR)/rabbit_framing.hrl $(SOURCE_DIR)/rabbit_framing_amqp_*.erl codegen.pyc
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
	RABBITMQ_MNESIA_DIR="$(RABBITMQ_MNESIA_DIR)" \
	RABBITMQ_PLUGINS_EXPAND_DIR="$(RABBITMQ_PLUGINS_EXPAND_DIR)"

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

run-background-node: all
	$(BASIC_SCRIPT_ENVIRONMENT_SETTINGS) \
		RABBITMQ_NODE_ONLY=true \
		RABBITMQ_SERVER_START_ARGS="$(RABBITMQ_SERVER_START_ARGS)" \
		./scripts/rabbitmq-server

run-tests: all
	OUT=$$(echo "rabbit_tests:all_tests()." | $(ERL_CALL)) ; \
	  echo $$OUT ; echo $$OUT | grep '^{ok, passed}$$' > /dev/null

run-qc: all
	$(foreach MOD,$(QC_MODULES),./quickcheck $(RABBITMQ_NODENAME) $(MOD) $(QC_TRIALS))

start-background-node: all
	-rm -f $(RABBITMQ_MNESIA_DIR).pid
	mkdir -p $(RABBITMQ_MNESIA_DIR)
	setsid sh -c "$(MAKE) run-background-node > $(RABBITMQ_MNESIA_DIR)/startup_log 2> $(RABBITMQ_MNESIA_DIR)/startup_err" &
	./scripts/rabbitmqctl -n $(RABBITMQ_NODENAME) wait $(RABBITMQ_MNESIA_DIR).pid kernel

start-rabbit-on-node: all
	echo "rabbit:start()." | $(ERL_CALL)
	./scripts/rabbitmqctl -n $(RABBITMQ_NODENAME) wait $(RABBITMQ_MNESIA_DIR).pid

stop-rabbit-on-node: all
	echo "rabbit:stop()." | $(ERL_CALL)

set-resource-alarm: all
	echo "alarm_handler:set_alarm({{resource_limit, $(SOURCE), node()}, []})." | \
	$(ERL_CALL)

clear-resource-alarm: all
	echo "alarm_handler:clear_alarm({resource_limit, $(SOURCE), node()})." | \
	$(ERL_CALL)

stop-node:
	-$(ERL_CALL) -q

# code coverage will be created for subdirectory "ebin" of COVER_DIR
COVER_DIR=.

start-cover: all
	echo "rabbit_misc:start_cover([\"rabbit\", \"hare\"])." | $(ERL_CALL)
	echo "rabbit_misc:enable_cover([\"$(COVER_DIR)\"])." | $(ERL_CALL)

start-secondary-cover: all
	echo "rabbit_misc:start_cover([\"hare\"])." | $(ERL_CALL)

stop-cover: all
	echo "rabbit_misc:report_cover(), cover:stop()." | $(ERL_CALL)
	cat cover/summary.txt

########################################################################

srcdist: distclean
	mkdir -p $(TARGET_SRC_DIR)/codegen
	cp -r ebin src include LICENSE LICENSE-MPL-RabbitMQ INSTALL README $(TARGET_SRC_DIR)
	sed 's/%%VSN%%/$(VERSION)/' $(TARGET_SRC_DIR)/ebin/rabbit_app.in > $(TARGET_SRC_DIR)/ebin/rabbit_app.in.tmp && \
		mv $(TARGET_SRC_DIR)/ebin/rabbit_app.in.tmp $(TARGET_SRC_DIR)/ebin/rabbit_app.in

	cp -r $(AMQP_CODEGEN_DIR)/* $(TARGET_SRC_DIR)/codegen/
	cp codegen.py Makefile generate_app generate_deps calculate-relative $(TARGET_SRC_DIR)

	cp -r scripts $(TARGET_SRC_DIR)
	cp -r $(DOCS_DIR) $(TARGET_SRC_DIR)
	chmod 0755 $(TARGET_SRC_DIR)/scripts/*

ifneq "$(PLUGINS_SRC_DIR)" ""
	cp -r $(PLUGINS_SRC_DIR) $(TARGET_SRC_DIR)/plugins-src
	rm $(TARGET_SRC_DIR)/LICENSE
	cat packaging/common/LICENSE.head >> $(TARGET_SRC_DIR)/LICENSE
	cat $(AMQP_CODEGEN_DIR)/license_info >> $(TARGET_SRC_DIR)/LICENSE
	find $(PLUGINS_SRC_DIR)/licensing -name "license_info_*" -exec cat '{}' >> $(TARGET_SRC_DIR)/LICENSE \;
	cat packaging/common/LICENSE.tail >> $(TARGET_SRC_DIR)/LICENSE
	find $(PLUGINS_SRC_DIR)/licensing -name "LICENSE-*" -exec cp '{}' $(TARGET_SRC_DIR) \;
	rm -rf $(TARGET_SRC_DIR)/licensing
else
	@echo No plugins source distribution found
endif

	(cd dist; tar -zchf $(TARBALL_NAME).tar.gz $(TARBALL_NAME))
	(cd dist; zip -q -r $(TARBALL_NAME).zip $(TARBALL_NAME))
	rm -rf $(TARGET_SRC_DIR)

distclean: clean
	$(MAKE) -C $(AMQP_CODEGEN_DIR) distclean
	rm -rf dist
	find . -regex '.*\(~\|#\|\.swp\|\.dump\)' -exec rm {} \;

# xmlto can not read from standard input, so we mess with a tmp file.
%.gz: %.xml $(DOCS_DIR)/examples-to-end.xsl
	xmlto --version | grep -E '^xmlto version 0\.0\.([0-9]|1[1-8])$$' >/dev/null || opt='--stringparam man.indent.verbatims=0' ; \
	    xsltproc --novalid $(DOCS_DIR)/examples-to-end.xsl $< > $<.tmp && \
	    xmlto -o $(DOCS_DIR) $$opt man $<.tmp && \
	    gzip -f $(DOCS_DIR)/`basename $< .xml`
	rm -f $<.tmp

# Use tmp files rather than a pipeline so that we get meaningful errors
# Do not fold the cp into previous line, it's there to stop the file being
# generated but empty if we fail
$(SOURCE_DIR)/%_usage.erl:
	xsltproc --novalid --stringparam modulename "`basename $@ .erl`" \
		$(DOCS_DIR)/usage.xsl $< > $@.tmp
	sed -e 's/"/\\"/g' -e 's/%QUOTE%/"/g' $@.tmp > $@.tmp2
	fold -s $@.tmp2 > $@.tmp3
	mv $@.tmp3 $@
	rm $@.tmp $@.tmp2

# We rename the file before xmlto sees it since xmlto will use the name of
# the file to make internal links.
%.man.xml: %.xml $(DOCS_DIR)/html-to-website-xml.xsl
	cp $< `basename $< .xml`.xml && \
		xmlto xhtml-nochunks `basename $< .xml`.xml ; rm `basename $< .xml`.xml
	cat `basename $< .xml`.html | \
	    xsltproc --novalid $(DOCS_DIR)/remove-namespaces.xsl - | \
		xsltproc --novalid --stringparam original `basename $<` $(DOCS_DIR)/html-to-website-xml.xsl - | \
		xmllint --format - > $@
	rm `basename $< .xml`.html

docs_all: $(MANPAGES) $(WEB_MANPAGES)

install: install_bin install_docs

install_bin: all install_dirs
	cp -r ebin include LICENSE* INSTALL $(TARGET_DIR)

	chmod 0755 scripts/*
	for script in rabbitmq-env rabbitmq-server rabbitmqctl rabbitmq-plugins rabbitmq-defaults; do \
		cp scripts/$$script $(TARGET_DIR)/sbin; \
		[ -e $(SBIN_DIR)/$$script ] || ln -s $(SCRIPTS_REL_PATH)/$$script $(SBIN_DIR)/$$script; \
	done

	mkdir -p $(TARGET_DIR)/$(PLUGINS_DIR)
	[ -d "$(PLUGINS_DIR)" ] && cp $(PLUGINS_DIR)/*.ez $(PLUGINS_DIR)/README $(TARGET_DIR)/$(PLUGINS_DIR) || true

install_docs: docs_all install_dirs
	for section in 1 5; do \
		mkdir -p $(MAN_DIR)/man$$section; \
		for manpage in $(DOCS_DIR)/*.$$section.gz; do \
			cp $$manpage $(MAN_DIR)/man$$section; \
		done; \
	done

install_dirs:
	@ OK=true && \
	  { [ -n "$(TARGET_DIR)" ] || { echo "Please set TARGET_DIR."; OK=false; }; } && \
	  { [ -n "$(SBIN_DIR)" ] || { echo "Please set SBIN_DIR."; OK=false; }; } && \
	  { [ -n "$(MAN_DIR)" ] || { echo "Please set MAN_DIR."; OK=false; }; } && $$OK

	mkdir -p $(TARGET_DIR)/sbin
	mkdir -p $(SBIN_DIR)
	mkdir -p $(MAN_DIR)

$(foreach XML,$(USAGES_XML),$(eval $(call usage_dep, $(XML))))

# Note that all targets which depend on clean must have clean in their
# name.  Also any target that doesn't depend on clean should not have
# clean in its name, unless you know that you don't need any of the
# automatic dependency generation for that target (e.g. cleandb).

# We want to load the dep file if *any* target *doesn't* contain
# "clean" - i.e. if removing all clean-like targets leaves something.

ifeq "$(MAKECMDGOALS)" ""
TESTABLEGOALS:=$(.DEFAULT_GOAL)
else
TESTABLEGOALS:=$(MAKECMDGOALS)
endif

ifneq "$(strip $(patsubst clean%,,$(patsubst %clean,,$(TESTABLEGOALS))))" ""
-include $(DEPS_FILE)
endif

.PHONY: run-qc
