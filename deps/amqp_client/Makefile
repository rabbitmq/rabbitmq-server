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
DIST_DIR=dist
DEPS_DIR=deps

DEPS=$(shell erl -noshell -eval '{ok,[{_,_,[_,_,{modules, Mods},_,_,_]}]} = \
                                 file:consult("rabbit_common.app"), \
                                 [io:format("~p ",[M]) || M <- Mods], halt().')

PACKAGE=amqp_client
PACKAGE_NAME=$(PACKAGE).ez
COMMON_PACKAGE=rabbit_common
COMMON_PACKAGE_NAME=$(COMMON_PACKAGE).ez

COMPILE_DEPS=$(DEPS_DIR)/$(COMMON_PACKAGE)/$(INCLUDE_DIR)/rabbit.hrl \
             $(DEPS_DIR)/$(COMMON_PACKAGE)/$(INCLUDE_DIR)/rabbit_framing.hrl \
             $(DEPS_DIR)/$(COMMON_PACKAGE)/$(EBIN_DIR)

INCLUDES=$(wildcard $(INCLUDE_DIR)/*.hrl)
SOURCES=$(wildcard $(SOURCE_DIR)/*.erl)
TARGETS=$(patsubst $(SOURCE_DIR)/%.erl, $(EBIN_DIR)/%.beam, $(SOURCES))
TEST_SOURCES=$(wildcard $(TEST_DIR)/*.erl)
TEST_TARGETS=$(patsubst $(TEST_DIR)/%.erl, $(TEST_DIR)/%.beam, $(TEST_SOURCES))

BROKER_HEADERS=$(wildcard $(BROKER_DIR)/$(INCLUDE_DIR)/*.hrl)
BROKER_SOURCES=$(wildcard $(BROKER_DIR)/$(SOURCE_DIR)/*.erl)

LIBS_PATH=ERL_LIBS=$(DEPS_DIR):$(DIST_DIR)
LOAD_PATH=$(EBIN_DIR) $(BROKER_DIR)/ebin $(TEST_DIR)

COVER_START := -s cover start -s rabbit_misc enable_cover ../rabbitmq-erlang-client
COVER_STOP := -s rabbit_misc report_cover ../rabbitmq-erlang-client -s cover stop

MKTEMP=$$(mktemp /tmp/tmp.XXXXXXXXXX)

ifndef USE_SPECS
# our type specs rely on features / bug fixes in dialyzer that are
# only available in R12B-3 upwards
#
# NB: the test assumes that version number will only contain single digits
export USE_SPECS=$(shell if [ $$(erl -noshell -eval 'io:format(erlang:system_info(version)), halt().') \> "5.6.2" ]; then echo "true"; else echo "false"; fi)
endif

ERLC_OPTS=-I $(INCLUDE_DIR) -o $(EBIN_DIR) -Wall -v +debug_info $(shell [ $(USE_SPECS) = "true" ] && echo "-Duse_specs")

RABBITMQ_NODENAME=rabbit
PA_LOAD_PATH=-pa $(realpath $(LOAD_PATH))
RABBITMQCTL=$(BROKER_DIR)/scripts/rabbitmqctl

PLT=$(HOME)/.dialyzer_plt
DIALYZER_CALL=dialyzer --plt $(PLT)

.PHONY: all compile compile_tests run run_in_broker dialyzer dialyze_all \
	add_broker_to_plt prepare_tests all_tests test_suites \
	test_suites_coverage run_test_broker start_test_broker_node \
	stop_test_broker_node test_network test_direct test_network_coverage \
	test_direct_coverage test_common_package clean source_tarball package \
	common_package boot_broker unboot_broker

all: package

compile: $(TARGETS)

compile_tests: $(TEST_DIR)
	$(MAKE) -C $(TEST_DIR)

run: $(TARGETS)
	erl -pa $(LOAD_PATH)

run_in_broker: $(TARGETS) $(BROKER_DIR)
	$(MAKE) RABBITMQ_SERVER_START_ARGS='$(PA_LOAD_PATH)' -C $(BROKER_DIR) run

dialyze: $(TARGETS)
	$(DIALYZER_CALL) -c $^

dialyze_all: $(TARGETS) $(TEST_TARGETS)
	$(DIALYZER_CALL) -c $^

add_broker_to_plt: $(BROKER_DIR)/ebin
	$(DIALYZER_CALL) --add_to_plt -r $<

clean:
	rm -f $(EBIN_DIR)/*.beam
	rm -f erl_crash.dump
	rm -fr $(DIST_DIR)
	rm -fr $(DEPS_DIR)
	$(MAKE) -C $(TEST_DIR) clean

##############################################################################
##  Testing
###############################################################################

prepare_tests: compile compile_tests

all_tests: prepare_tests
	OK=true && \
	{ $(MAKE) test_suites || OK=false; } && \
	{ $(MAKE) test_common_package || OK=false; } && \
	$$OK

test_suites: prepare_tests
	OK=true && \
	{ $(MAKE) test_network || OK=false; } && \
	{ $(MAKE) test_direct || OK=false; } && \
	$$OK

test_suites_coverage: prepare_tests
	OK=true && \
	{ $(MAKE) test_network_coverage || OK=false; } && \
	{ $(MAKE) test_direct_coverage || OK=false; } && \
	$$OK

## This performs test setup and teardown procedures to ensure that
## that the correct users are configured in the test instance
run_test_broker: start_test_broker_node unboot_broker
	OK=true && \
	TMPFILE=$(MKTEMP) && \
	{ $(MAKE) -C $(BROKER_DIR) run-node \
		RABBITMQ_SERVER_START_ARGS="$(PA_LOAD_PATH) \
		-noshell \
		-s rabbit \
		$(RUN_TEST_BROKER_ARGS) \
		-s init stop" 2>&1 | \
		tee $$TMPFILE || OK=false; } && \
	{ egrep "All .+ tests (successful|passed)." $$TMPFILE || OK=false; } && \
	rm $$TMPFILE && \
	$(MAKE) boot_broker && \
	$(MAKE) stop_test_broker_node && \
	$$OK

start_test_broker_node: boot_broker
	$(RABBITMQCTL) delete_user test_user_bum 2>/dev/null || true
	$(RABBITMQCTL) delete_vhost test_vhost_bum 2>/dev/null || true
	$(RABBITMQCTL) delete_user test_user_no_perm 2>/dev/null || true
	$(RABBITMQCTL) add_user test_user_no_perm test_user_no_perm

stop_test_broker_node:
	$(RABBITMQCTL) delete_user test_user_no_perm
	$(MAKE) unboot_broker

boot_broker:
	$(MAKE) -C $(BROKER_DIR) start-background-node
	$(MAKE) -C $(BROKER_DIR) start-rabbit-on-node

unboot_broker:
	$(MAKE) -C $(BROKER_DIR) stop-rabbit-on-node
	$(MAKE) -C $(BROKER_DIR) stop-node

test_network: prepare_tests
	$(MAKE) run_test_broker RUN_TEST_BROKER_ARGS="-s network_client_SUITE test"

test_direct: prepare_tests
	$(MAKE) run_test_broker RUN_TEST_BROKER_ARGS="-s direct_client_SUITE test"

test_network_coverage: prepare_tests
	$(MAKE) run_test_broker \
	RUN_TEST_BROKER_ARGS="$(COVER_START) -s network_client_SUITE test $(COVER_STOP)"

test_direct_coverage: prepare_tests
	$(MAKE) run_test_broker \
	RUN_TEST_BROKER_ARGS="$(COVER_START) -s direct_client_SUITE test $(COVER_STOP)"

test_common_package: common_package package prepare_tests
	$(MAKE) start_test_broker_node
	OK=true && \
	TMPFILE=$(MKTEMP) && \
	    { $(LIBS_PATH) erl -noshell -pa $(TEST_DIR) \
	    -eval 'network_client_SUITE:test(), halt().' 2>&1 | \
		tee $$TMPFILE || OK=false; } && \
	{ egrep "All .+ tests (successful|passed)." $$TMPFILE || OK=false; } && \
	rm $$TMPFILE && \
	$(MAKE) stop_test_broker_node && \
	$$OK

###############################################################################
##  Packaging
###############################################################################

source_tarball: $(DIST_DIR)
	cp -a README Makefile dist/$(DIST_DIR)/
	mkdir -p dist/$(DIST_DIR)/$(SOURCE_DIR)
	cp -a $(SOURCE_DIR)/*.erl dist/$(DIST_DIR)/$(SOURCE_DIR)/
	mkdir -p dist/$(DIST_DIR)/$(INCLUDE_DIR)
	cp -a $(INCLUDE_DIR)/*.hrl dist/$(DIST_DIR)/$(INCLUDE_DIR)/
	mkdir -p dist/$(DIST_DIR)/$(TEST_DIR)
	cp -a $(TEST_DIR)/*.erl dist/$(DIST_DIR)/$(TEST_DIR)/
	cp -a $(TEST_DIR)/Makefile dist/$(DIST_DIR)/$(TEST_DIR)/
	cd dist ; tar cvzf $(DIST_DIR).tar.gz $(DIST_DIR)

$(DIST_DIR)/$(PACKAGE_NAME): $(TARGETS)
	rm -rf $(DIST_DIR)/$(PACKAGE)
	mkdir -p $(DIST_DIR)/$(PACKAGE)
	cp -r $(EBIN_DIR) $(DIST_DIR)/$(PACKAGE)
	cp -r $(INCLUDE_DIR) $(DIST_DIR)/$(PACKAGE)
	(cd $(DIST_DIR); rm $(PACKAGE_NAME); zip -r $(PACKAGE_NAME) $(PACKAGE))

package: $(DIST_DIR)/$(PACKAGE_NAME)

common_package: $(DIST_DIR)/$(COMMON_PACKAGE_NAME)

$(DIST_DIR)/$(COMMON_PACKAGE_NAME): $(BROKER_SOURCES) $(BROKER_HEADERS)
	$(MAKE) -C $(BROKER_DIR)
	mkdir -p $(DIST_DIR)/$(COMMON_PACKAGE)/$(INCLUDE_DIR)
	mkdir -p $(DIST_DIR)/$(COMMON_PACKAGE)/$(EBIN_DIR)
	cp $(COMMON_PACKAGE).app $(DIST_DIR)/$(COMMON_PACKAGE)/$(EBIN_DIR)
	$(foreach DEP, $(DEPS), \
        ( cp $(BROKER_DIR)/$(EBIN_DIR)/$(DEP).beam \
          $(DIST_DIR)/$(COMMON_PACKAGE)/$(EBIN_DIR) \
        );)
	cp $(BROKER_DIR)/$(INCLUDE_DIR)/*.hrl $(DIST_DIR)/$(COMMON_PACKAGE)/$(INCLUDE_DIR)
	(cd $(DIST_DIR); zip -r $(COMMON_PACKAGE_NAME) $(COMMON_PACKAGE))

###############################################################################
##  Internal targets
###############################################################################

$(COMPILE_DEPS): $(DIST_DIR)/$(COMMON_PACKAGE_NAME)
	mkdir -p $(DEPS_DIR)
	unzip -o -d $(DEPS_DIR) $(DIST_DIR)/$(COMMON_PACKAGE_NAME)

$(EBIN_DIR)/%.beam: $(SOURCE_DIR)/%.erl $(INCLUDES) $(COMPILE_DEPS)
	$(LIBS_PATH) erlc $(ERLC_OPTS) $<

$(BROKER_DIR):
	test -e $(BROKER_DIR)
	$(MAKE_BROKER)

$(DIST_DIR):
	mkdir -p $@

