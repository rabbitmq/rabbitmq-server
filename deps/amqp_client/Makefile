#   The contents of this file are subject to the Mozilla Public License
#   Version 1.1 (the "License"); you may not use this file except in
#   compliance with the License. You may obtain a copy of the License at
#   http://www.mozilla.org/MPL/
#
#   Software distributed under the License is distributed on an "AS IS"
#   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
#   License for the specific language governing rights and limitations
#   under the License.
#
#   The Original Code is the RabbitMQ Erlang Client.
#
#   The Initial Developers of the Original Code are LShift Ltd.,
#   Cohesive Financial Technologies LLC., and Rabbit Technologies Ltd.
#
#   Portions created by LShift Ltd., Cohesive Financial
#   Technologies LLC., and Rabbit Technologies Ltd. are Copyright (C) 
#   2007 LShift Ltd., Cohesive Financial Technologies LLC., and Rabbit 
#   Technologies Ltd.; 
#
#   All Rights Reserved.
#
#   Contributor(s): Ben Hood <0x6e6562@gmail.com>.
#

EBIN_DIR=ebin
export BROKER_DIR=../rabbitmq-server
export INCLUDE_DIR=include
export INCLUDE_SERV_DIR=$(BROKER_DIR)/include
TEST_DIR=test
SOURCE_DIR=src
DIST_DIR=rabbitmq-erlang-client

INCLUDES=$(wildcard $(INCLUDE_DIR)/*.hrl)
SOURCES=$(wildcard $(SOURCE_DIR)/*.erl)
TARGETS=$(patsubst $(SOURCE_DIR)/%.erl, $(EBIN_DIR)/%.beam, $(SOURCES))
TEST_SOURCES=$(wildcard $(TEST_DIR)/*.erl)
TEST_TARGETS=$(patsubst $(TEST_DIR)/%.erl, $(TEST_DIR)/%.beam, $(TEST_SOURCES))

LOAD_PATH=$(EBIN_DIR) $(BROKER_DIR)/ebin $(TEST_DIR)

ifndef USE_SPECS
# our type specs rely on features / bug fixes in dialyzer that are
# only available in R12B-3 upwards
#
# NB: the test assumes that version number will only contain single digits
export USE_SPECS=$(shell if [ $$(erl -noshell -eval 'io:format(erlang:system_info(version)), halt().') \> "5.6.2" ]; then echo "true"; else echo "false"; fi)
endif

ERLC_OPTS=-I $(INCLUDE_DIR) -I $(INCLUDE_SERV_DIR) -o $(EBIN_DIR) -Wall -v +debug_info $(shell [ $(USE_SPECS) = "true" ] && echo "-Duse_specs")

RABBITMQ_NODENAME=rabbit
PA_LOAD_PATH=-pa $(realpath $(LOAD_PATH))

PLT=$(HOME)/.dialyzer_plt
DIALYZER_CALL=dialyzer --plt $(PLT)

.PHONY: all compile compile_tests run dialyzer dialyze_all add_broker_to_plt \
	prepare_tests all_tests all_tests_coverage run_test_broker \
	run_test_broker_cover test_network test_direct test_network_coverage \
	test_direct_coverage clean source_tarball

all: compile

compile: $(TARGETS)

compile_tests: $(TEST_DIR)
	$(MAKE) -C $(TEST_DIR)

$(EBIN_DIR)/%.beam: $(SOURCE_DIR)/%.erl $(INCLUDES) $(BROKER_DIR)
	mkdir -p $(EBIN_DIR); erlc $(ERLC_OPTS) $<

$(BROKER_DIR):
	test -e $(BROKER_DIR)

run: compile
	erl -pa $(LOAD_PATH)

dialyze: $(TARGETS)
	$(DIALYZER_CALL) -c $^

dialyze_all: $(TARGETS) $(TEST_TARGETS)
	$(DIALYZER_CALL) -c $^

add_broker_to_plt: $(BROKER_DIR)/ebin
	$(DIALYZER_CALL) --add_to_plt -r $<

prepare_tests: compile compile_tests

all_tests: prepare_tests
	OK=true && \
	{ $(MAKE) test_network || OK=false; } && \
	$(MAKE) test_direct && $$OK

all_tests_coverage: prepare_tests
	OK=true && \
	{ $(MAKE) test_network_coverage || OK=false; } && \
	$(MAKE) test_direct_coverage && $$OK

run_test_broker:
	OK=true && \
	TMPFILE=$$(mktemp) && \
	{ $(MAKE) -C $(BROKER_DIR) run-node \
		RABBITMQ_SERVER_START_ARGS="$(PA_LOAD_PATH) \
		-s rabbit $(RUN_TEST_BROKER_ARGS) -s init stop" 2>&1 | \
	tee $$TMPFILE || OK=false; } && \
	{ grep "All .\+ tests passed." $$TMPFILE || OK=false; } && \
	rm $$TMPFILE && $$OK

run_test_broker_cover:
	OK=true && \
	TMPFILE=$$(mktemp) && \
	{ $(MAKE) -C $(BROKER_DIR) run-node \
		RABBITMQ_SERVER_START_ARGS="$(PA_LOAD_PATH) \
		-s rabbit -s cover start -s rabbit_misc enable_cover \
		-s rabbit $(RUN_TEST_BROKER_ARGS) -s rabbit_misc report_cover \
		-s cover stop -s init stop" 2>&1 | \
	tee $$TMPFILE || OK=false; } && \
	{ grep "All .\+ tests passed." $$TMPFILE || OK=false; } && \
	rm $$TMPFILE && $$OK

test_network: prepare_tests
	$(MAKE) run_test_broker RUN_TEST_BROKER_ARGS="-s network_client_SUITE test"

test_direct: prepare_tests
	$(MAKE) run_test_broker RUN_TEST_BROKER_ARGS="-s direct_client_SUITE test"

test_network_coverage: prepare_tests
	$(MAKE) run_test_broker_cover RUN_TEST_BROKER_ARGS="-s network_client_SUITE test"

test_direct_coverage: prepare_tests
	$(MAKE) run_test_broker_cover RUN_TEST_BROKER_ARGS="-s direct_client_SUITE test"

clean:
	rm -f $(EBIN_DIR)/*.beam
	rm -f erl_crash.dump
	rm -fr dist tmp
	$(MAKE) -C $(TEST_DIR) clean

source_tarball:
	mkdir -p dist/$(DIST_DIR)
	cp -a README Makefile dist/$(DIST_DIR)/
	mkdir -p dist/$(DIST_DIR)/$(SOURCE_DIR)
	cp -a $(SOURCE_DIR)/*.erl dist/$(DIST_DIR)/$(SOURCE_DIR)/
	mkdir -p dist/$(DIST_DIR)/$(INCLUDE_DIR)
	cp -a $(INCLUDE_DIR)/*.hrl dist/$(DIST_DIR)/$(INCLUDE_DIR)/
	mkdir -p dist/$(DIST_DIR)/$(TEST_DIR)
	cp -a $(TEST_DIR)/*.erl dist/$(DIST_DIR)/$(TEST_DIR)/
	cp -a $(TEST_DIR)/Makefile dist/$(DIST_DIR)/$(TEST_DIR)/
	cd dist ; tar cvzf $(DIST_DIR).tar.gz $(DIST_DIR)
