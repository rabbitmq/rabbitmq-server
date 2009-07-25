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
export INCLUDE_DIR=include
export INCLUDE_SERV_DIR=$(BROKER_DIR)/include
TEST_DIR=test
SOURCE_DIR=src
DIST_DIR=dist

DEPS=$(shell erl -noshell -eval '{ok,[{_,_,[_,_,{modules, Mods},_,_,_]}]} = \
                                 file:consult("rabbit_common.app"), \
                                 [io:format("~p ",[M]) || M <- Mods], halt().')

PACKAGE=amqp_client
PACKAGE_NAME=$(PACKAGE).ez
COMMON_PACKAGE=rabbit_common
COMMON_PACKAGE_NAME=$(COMMON_PACKAGE).ez

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

export BROKER_DIR=../rabbitmq-server
RABBITMQ_NODENAME=rabbit
BROKER_START_ARGS=-pa $(realpath $(LOAD_PATH))
MAKE_BROKER=$(MAKE) RABBITMQ_SERVER_START_ARGS='$(BROKER_START_ARGS)' -C $(BROKER_DIR)
ERL_CALL_BROKER=erl_call -sname $(RABBITMQ_NODENAME) -e

PLT=$(HOME)/.dialyzer_plt
DIALYZER_CALL=dialyzer --plt $(PLT)


###############################################################################
##  Regular targets
###############################################################################

all: compile

compile: $(BROKER_DIR) $(TARGETS)

compile_tests: $(BROKER_DIR) $(TEST_DIR)

run: compile
	erl -pa $(LOAD_PATH)

run_in_broker: $(BROKER_DIR) compile
	$(MAKE_BROKER) run

clean:
	rm -f $(EBIN_DIR)/*.beam
	rm -f erl_crash.dump
	rm -f .test_error
	rm -fr dist tmp
	$(MAKE) -C $(TEST_DIR) clean


###############################################################################
##  Utils
###############################################################################

dialyze: $(TARGETS)
	$(DIALYZER_CALL) -c $^

dialyze_all: $(TARGETS) $(TEST_TARGETS)
	$(DIALYZER_CALL) -c $^

add_broker_to_plt: $(BROKER_DIR)
	$(DIALYZER_CALL) --add_to_plt -r $</ebin


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

package: clean compile $(DIST_DIR)
	mkdir -p $(DIST_DIR)/$(PACKAGE)
	cp -r $(EBIN_DIR) $(DIST_DIR)/$(PACKAGE)
	cp -r $(INCLUDE_DIR) $(DIST_DIR)/$(PACKAGE)
	(cd $(DIST_DIR); zip -r $(PACKAGE_NAME) $(PACKAGE))

common_package: $(BROKER_DIR)
	mkdir -p $(DIST_DIR)/$(COMMON_PACKAGE)/$(EBIN_DIR)
	cp $(COMMON_PACKAGE).app $(DIST_DIR)/$(COMMON_PACKAGE)/$(EBIN_DIR)
	$(foreach DEP, $(DEPS), \
        ( cp $(BROKER_DIR)/$(EBIN_DIR)/$(DEP).beam \
          $(DIST_DIR)/$(COMMON_PACKAGE)/$(EBIN_DIR) \
        );)
	(cd $(DIST_DIR); zip -r $(COMMON_PACKAGE_NAME) $(COMMON_PACKAGE))


###############################################################################
##  Testing
###############################################################################

all_tests: clean \
           prepare_for_testing \
           test_direct_on_node \
           test_network_on_node \
		   test_common_package_with_node \
           cleanup_after_testing

test_network: prepare_for_testing \
              test_network_on_node \
              cleanup_after_testing

test_direct: prepare_for_testing \
             test_direct_on_node \
             cleanup_after_testing

test_common_package: clean \
                     prepare_for_testing \
	                 test_common_package_with_node \
                     cleanup_after_testing

test_network_coverage: prepare_for_testing \
                       start_cover_on_node \
                       test_network_on_node \
                       stop_cover_on_node \
                       cleanup_after_testing

test_direct_coverage: prepare_for_testing \
                      start_cover_on_node \
                      test_direct_on_node \
                      stop_cover_on_node \
                      cleanup_after_testing


###############################################################################
##  Internal targets
###############################################################################

$(TEST_TARGETS): $(TEST_DIR)

.PHONY: $(TEST_DIR)
$(TEST_DIR):
	$(MAKE) -C $(TEST_DIR)

$(EBIN_DIR):
	mkdir -p $(EBIN_DIR)

$(EBIN_DIR)/%.beam: $(SOURCE_DIR)/%.erl $(INCLUDES) | $(EBIN_DIR)
	erlc $(ERLC_OPTS) $<

.PHONY: $(BROKER_DIR)
$(BROKER_DIR):
	test -e $(BROKER_DIR)
	$(MAKE_BROKER)


$(DIST_DIR):
	mkdir -p $@


.PHONY: start_background_node_in_broker
start_background_node_in_broker: $(BROKER_DIR) compile
	$(MAKE_BROKER) start-background-node
	$(MAKE_BROKER) start-rabbit-on-node

.PHONY: stop_background_node_in_broker
stop_background_node_in_broker: $(BROKER_DIR)
	$(MAKE_BROKER) stop-rabbit-on-node
	$(MAKE_BROKER) stop-node

.PHONY: start_cover_on_node
start_cover_on_node: $(BROKER_DIR)
	$(MAKE_BROKER) start-cover

.PHONY: stop_cover_on_node
stop_cover_on_node: $(BROKER_DIR)
	$(MAKE_BROKER) stop-cover

.PHONY: test_network_on_node
test_network_on_node:
	echo 'network_client_SUITE:test().' | $(ERL_CALL_BROKER) | egrep '^\{ok, ok\}$$' || touch .test_error

.PHONY: test_direct_on_node
test_direct_on_node:
	echo 'direct_client_SUITE:test().' | $(ERL_CALL_BROKER) | egrep '^\{ok, ok\}$$' || touch .test_error

.PHONY: test_common_package_with_node
test_common_package_with_node: package common_package compile_tests
	ERL_LIBS=$(DIST_DIR) erl -pa $(TEST_DIR) -eval 'network_client_SUITE:test(), halt().' | egrep 'All .* tests successful.' || touch .test_error


.PHONY: clean_test_error_flag
clean_test_error_flag:
	rm -f .test_error

.PHONY: prepare_for_testing
prepare_for_testing: compile compile_tests \
                     start_background_node_in_broker \
                     clean_test_error_flag

.PHONY: cleanup_after_testing
cleanup_after_testing: stop_background_node_in_broker
	@test -e .test_error || echo "All tests successful."
	@test ! -e .test_error || echo "*** One or more tests FAILED! See SASL log for details. ***"
	@test ! -e .test_error
