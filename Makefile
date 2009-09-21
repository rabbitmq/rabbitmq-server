ifndef TMPDIR
TMPDIR := /tmp
endif

RABBITMQ_NODENAME=rabbit
RABBITMQ_SERVER_START_ARGS=
RABBITMQ_MNESIA_DIR=$(TMPDIR)/rabbitmq-$(RABBITMQ_NODENAME)-mnesia
RABBITMQ_LOG_BASE=$(TMPDIR)

SOURCE_DIR=src
EBIN_DIR=ebin
INCLUDE_DIR=include
SOURCES=$(wildcard $(SOURCE_DIR)/*.erl)
BEAM_TARGETS=$(EBIN_DIR)/rabbit_framing.beam $(patsubst $(SOURCE_DIR)/%.erl, $(EBIN_DIR)/%.beam,$(SOURCES))
TARGETS=$(EBIN_DIR)/rabbit.app $(BEAM_TARGETS)
WEB_URL=http://stage.rabbitmq.com/
MANPAGES=$(patsubst %.pod, %.gz, $(wildcard docs/*.[0-9].pod))

PYTHON=python

ifndef USE_SPECS
# our type specs rely on features / bug fixes in dialyzer that are
# only available in R13B upwards (R13B is eshell 5.7.1)
#
# NB: the test assumes that version number will only contain single digits
USE_SPECS=$(shell if [ $$(erl -noshell -eval 'io:format(erlang:system_info(version)), halt().') \> "5.7.0" ]; then echo "true"; else echo "false"; fi)
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

all: $(TARGETS)

$(EBIN_DIR)/rabbit.app: $(EBIN_DIR)/rabbit_app.in $(BEAM_TARGETS) generate_app
	escript generate_app $(EBIN_DIR) < $< > $@

$(EBIN_DIR)/gen_server2.beam: $(SOURCE_DIR)/gen_server2.erl
	erlc $(ERLC_OPTS) $<

$(EBIN_DIR)/%.beam: $(SOURCE_DIR)/%.erl $(INCLUDE_DIR)/rabbit_framing.hrl $(INCLUDE_DIR)/rabbit.hrl $(EBIN_DIR)/gen_server2.beam
	erlc $(ERLC_OPTS) -pa $(EBIN_DIR) $<
#	ERLC_EMULATOR="erl -smp" erlc $(ERLC_OPTS) -pa $(EBIN_DIR) $<

$(INCLUDE_DIR)/rabbit_framing.hrl: codegen.py $(AMQP_CODEGEN_DIR)/amqp_codegen.py $(AMQP_SPEC_JSON_PATH)
	$(PYTHON) codegen.py header $(AMQP_SPEC_JSON_PATH) $@

$(SOURCE_DIR)/rabbit_framing.erl: codegen.py $(AMQP_CODEGEN_DIR)/amqp_codegen.py $(AMQP_SPEC_JSON_PATH)
	$(PYTHON) codegen.py body   $(AMQP_SPEC_JSON_PATH) $@

dialyze: $(BEAM_TARGETS)
	dialyzer -c $?

clean:
	rm -f $(EBIN_DIR)/*.beam
	rm -f $(EBIN_DIR)/rabbit.app $(EBIN_DIR)/rabbit.boot $(EBIN_DIR)/rabbit.script $(EBIN_DIR)/rabbit.rel
	rm -f $(INCLUDE_DIR)/rabbit_framing.hrl $(SOURCE_DIR)/rabbit_framing.erl codegen.pyc
	rm -f docs/*.[0-9].gz

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
		RABBITMQ_NODE_ONLY=true \
		RABBITMQ_SERVER_START_ARGS="$(RABBITMQ_SERVER_START_ARGS) -s rabbit" \
		./scripts/rabbitmq-server

run-node: all
	$(BASIC_SCRIPT_ENVIRONMENT_SETTINGS) \
		RABBITMQ_NODE_ONLY=true \
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
	cp codegen.py Makefile generate_app calculate-relative $(TARGET_SRC_DIR)

	cp -r scripts $(TARGET_SRC_DIR)
	cp -r docs $(TARGET_SRC_DIR)
	chmod 0755 $(TARGET_SRC_DIR)/scripts/*

	(cd dist; tar -zcf $(TARBALL_NAME).tar.gz $(TARBALL_NAME))
	(cd dist; zip -r $(TARBALL_NAME).zip $(TARBALL_NAME))
	rm -rf $(TARGET_SRC_DIR)

distclean: clean
	$(MAKE) -C $(AMQP_CODEGEN_DIR) distclean
	rm -rf dist
	find . -regex '.*\(~\|#\|\.swp\|\.dump\)' -exec rm {} \;

%.gz: %.pod
	pod2man \
		-n `echo $$(basename $*) | sed -e 's/\.[[:digit:]]\+//'` \
		-s `echo $$(basename $*) | sed -e 's/.*\.\([^.]\+\)/\1/'` \
		-c "RabbitMQ AMQP Server" \
		-d "" \
		-r "" \
		$< | gzip --best > $@

docs_all: $(MANPAGES)

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
		for manpage in docs/*.$$section.pod; do \
			cp docs/`basename $$manpage .pod`.gz $(MAN_DIR)/man$$section; \
		done; \
	done

install_dirs:
	mkdir -p $(SBIN_DIR)
	mkdir -p $(TARGET_DIR)/sbin
