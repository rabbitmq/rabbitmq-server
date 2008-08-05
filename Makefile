NODENAME=rabbit
RABBIT_ARGS=
SOURCE_DIR=src
EBIN_DIR=ebin
INCLUDE_DIR=include
SOURCES=$(wildcard $(SOURCE_DIR)/*.erl)
TARGETS=$(EBIN_DIR)/rabbit_framing.beam $(patsubst $(SOURCE_DIR)/%.erl, $(EBIN_DIR)/%.beam,$(SOURCES))
WEB_URL=http://stage.rabbitmq.com/

ifndef USE_SPECS
# our type specs rely on features / bug fixes in dialyzer that are
# only available in R12B-3 upwards
#
# NB: the test assumes that version number will only contain single digits
USE_SPECS=$(shell if [ $$(erl -noshell -eval 'io:format(erlang:system_info(version)), halt().') \> "5.6.2" ]; then echo "true"; else echo "false"; fi)
endif

#other args: +native +"{hipe,[o3,verbose]}" -Ddebug=true +debug_info +no_strict_record_tests
ERLC_OPTS=-I $(INCLUDE_DIR) -o $(EBIN_DIR) -Wall -v +debug_info $(shell [ $(USE_SPECS) = "true" ] && echo "-Duse_specs")

MNESIA_DIR=/tmp/rabbitmq-$(NODENAME)-mnesia
LOG_BASE=/tmp

VERSION=0.0.0
TARBALL_NAME=rabbitmq-server-$(VERSION)

SIBLING_CODEGEN_DIR=../rabbitmq-codegen/
AMQP_CODEGEN_DIR=$(shell [ -d $(SIBLING_CODEGEN_DIR) ] && echo $(SIBLING_CODEGEN_DIR) || echo codegen)
AMQP_SPEC_JSON_PATH=$(AMQP_CODEGEN_DIR)/amqp-0.8.json

ERL_CALL=erl_call -sname $(NODENAME) -e

# for the moment we don't use boot files because they introduce a
# dependency on particular versions of OTP applications
#all: $(EBIN_DIR)/rabbit.boot
all: $(TARGETS)

$(EBIN_DIR)/%.beam: $(SOURCE_DIR)/%.erl $(INCLUDE_DIR)/rabbit_framing.hrl $(INCLUDE_DIR)/rabbit.hrl
	erlc $(ERLC_OPTS) $<
#	ERLC_EMULATOR="erl -smp" erlc $(ERLC_OPTS) $<

$(INCLUDE_DIR)/rabbit_framing.hrl $(SOURCE_DIR)/rabbit_framing.erl: codegen.py $(AMQP_CODEGEN_DIR)/amqp_codegen.py $(AMQP_SPEC_JSON_PATH)
	python codegen.py header $(AMQP_SPEC_JSON_PATH) > $(INCLUDE_DIR)/rabbit_framing.hrl
	python codegen.py body   $(AMQP_SPEC_JSON_PATH) > $(SOURCE_DIR)/rabbit_framing.erl

$(EBIN_DIR)/rabbit.boot $(EBIN_DIR)/rabbit.script: $(EBIN_DIR)/rabbit.app $(EBIN_DIR)/rabbit.rel $(TARGETS)
	erl -noshell -eval 'systools:make_script("ebin/rabbit", [{path, ["ebin"]}]), halt().'

dialyze: $(TARGETS)
	dialyzer -c $?

clean: cleandb
	rm -f $(EBIN_DIR)/*.beam
	rm -f $(EBIN_DIR)/rabbit.boot $(EBIN_DIR)/rabbit.script
	rm -f $(INCLUDE_DIR)/rabbit_framing.hrl $(SOURCE_DIR)/rabbit_framing.erl codegen.pyc

cleandb: stop-node
	erl -mnesia dir '"$(MNESIA_DIR)"' -noshell -eval 'lists:foreach(fun file:delete/1, filelib:wildcard(mnesia:system_info(directory) ++ "/*")), halt().'

############ various tasks to interact with RabbitMQ ###################

run: all
	NODE_IP_ADDRESS=$(NODE_IP_ADDRESS) NODE_PORT=$(NODE_PORT) NODE_ONLY=true LOG_BASE=$(LOG_BASE)  RABBIT_ARGS="$(RABBIT_ARGS) -s rabbit" MNESIA_DIR=$(MNESIA_DIR) ./scripts/rabbitmq-server

run-node: all
	NODE_IP_ADDRESS=$(NODE_IP_ADDRESS) NODE_PORT=$(NODE_PORT) NODE_ONLY=true LOG_BASE=$(LOG_BASE)  RABBIT_ARGS="$(RABBIT_ARGS)" MNESIA_DIR=$(MNESIA_DIR) ./scripts/rabbitmq-server

run-tests: all
	echo "rabbit_tests:all_tests()." | $(ERL_CALL)

start-background-node: stop-node
	NODE_IP_ADDRESS=$(NODE_IP_ADDRESS) NODE_PORT=$(NODE_PORT) NODE_ONLY=true LOG_BASE=$(LOG_BASE) RABBIT_ARGS="$(RABBIT_ARGS) -detached" MNESIA_DIR=$(MNESIA_DIR) ./scripts/rabbitmq-server ; sleep 1

start-rabbit-on-node: all
	echo "rabbit:start()." | $(ERL_CALL)

stop-rabbit-on-node: all
	echo "rabbit:stop()." | $(ERL_CALL)

force-snapshot: all
	echo "rabbit_persister:force_snapshot()." | $(ERL_CALL)

stop-node:
	-$(ERL_CALL) -q

start-cover: all
	echo "cover:start(), rabbit_misc:enable_cover()." | $(ERL_CALL)

stop-cover: all
	echo "rabbit_misc:report_cover(), cover:stop()." | $(ERL_CALL)
	cat cover/summary.txt

########################################################################

generic_stage:
	mkdir -p $(GENERIC_STAGE_DIR)
	cp -r ebin include src $(GENERIC_STAGE_DIR)
	cp LICENSE LICENSE-MPL-RabbitMQ $(GENERIC_STAGE_DIR)

	if [ -f INSTALL.in ]; then \
		cp INSTALL.in $(GENERIC_STAGE_DIR)/INSTALL; \
		elinks -dump -no-references -no-numbering $(WEB_URL)install.html \
			>> $(GENERIC_STAGE_DIR)/INSTALL; \
		cp BUILD.in $(GENERIC_STAGE_DIR)/BUILD; \
		elinks -dump -no-references -no-numbering $(WEB_URL)build.html \
			>> $(GENERIC_STAGE_DIR)/BUILD; \
	else \
		cp INSTALL $(GENERIC_STAGE_DIR); \
		cp BUILD $(GENERIC_STAGE_DIR); \
	fi

	sed -i 's/%%VERSION%%/$(VERSION)/' $(GENERIC_STAGE_DIR)/ebin/rabbit.app

srcdist: distclean
	$(MAKE) VERSION=$(VERSION) GENERIC_STAGE_DIR=dist/$(TARBALL_NAME) generic_stage

	mkdir -p dist/$(TARBALL_NAME)/codegen
	cp -r $(AMQP_CODEGEN_DIR)/* dist/$(TARBALL_NAME)/codegen/.
	cp codegen.py Makefile dist/$(TARBALL_NAME)

	cp -r scripts dist/$(TARBALL_NAME)
	chmod 0755 dist/$(TARBALL_NAME)/scripts/*

	(cd dist; tar -zcf $(TARBALL_NAME).tar.gz $(TARBALL_NAME))
	(cd dist; zip -r $(TARBALL_NAME).zip $(TARBALL_NAME))
	rm -rf dist/$(TARBALL_NAME)

distclean: clean
	make -C $(AMQP_CODEGEN_DIR) clean
	rm -rf dist
	find . -name '*~' -exec rm {} \;

install: all
	@[ -n "$(TARGET_DIR)" ] || (echo "Please set TARGET_DIR."; false)
	@[ -n "$(SBIN_DIR)" ] || (echo "Please set SBIN_DIR."; false)

	$(MAKE) VERSION=$(VERSION) GENERIC_STAGE_DIR=$(TARGET_DIR) generic_stage

	chmod 0755 scripts/*
	mkdir -p $(SBIN_DIR)
	cp scripts/rabbitmq-server $(SBIN_DIR)
	cp scripts/rabbitmqctl $(SBIN_DIR)
	cp scripts/rabbitmq-multi $(SBIN_DIR)
	rm -f $(TARGET_DIR)/BUILD
