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
export INCLUDE_SERV_DIR=$(BROKER_SYMLINK)/include
TEST_DIR=test
SOURCE_DIR=src
DIST_DIR=rabbitmq-erlang-client
PACKAGE=amqp_client
PACKAGE_NAME=$(PACKAGE).ez
BROKER_PLUGINS_LIB_DIR=$(BROKER_DIR)/plugins/lib

INCLUDES=$(wildcard $(INCLUDE_DIR)/*.hrl)
SOURCES=$(wildcard $(SOURCE_DIR)/*.erl)
TARGETS=$(patsubst $(SOURCE_DIR)/%.erl, $(EBIN_DIR)/%.beam, $(SOURCES))
TEST_SOURCES=$(wildcard $(TEST_DIR)/*.erl)
TEST_TARGETS=$(patsubst $(TEST_DIR)/%.erl, $(TEST_DIR)/%.beam, $(TEST_SOURCES))

LOAD_PATH=$(EBIN_DIR) $(BROKER_SYMLINK)/ebin $(TEST_DIR)

ifndef USE_SPECS
# our type specs rely on features / bug fixes in dialyzer that are
# only available in R12B-3 upwards
#
# NB: the test assumes that version number will only contain single digits
export USE_SPECS=$(shell if [ $$(erl -noshell -eval 'io:format(erlang:system_info(version)), halt().') \> "5.6.2" ]; then echo "true"; else echo "false"; fi)
endif

ERLC_OPTS=-I $(INCLUDE_DIR) -I $(INCLUDE_SERV_DIR) -o $(EBIN_DIR) -Wall -v +debug_info $(shell [ $(USE_SPECS) = "true" ] && echo "-Duse_specs")

LOG_BASE=/tmp
LOG_IN_FILE=true
ERL_WITH_BROKER=erl -pa $(LOAD_PATH) -mnesia dir tmp -boot start_sasl -s rabbit \
	$(shell [ $(LOG_IN_FILE) = "true" ] && echo "-sasl sasl_error_logger '{file, \"'${LOG_BASE}'/rabbit-sasl.log\"}' -kernel error_logger '{file, \"'${LOG_BASE}'/rabbit.log\"}'")

PLT=$(HOME)/.dialyzer_plt
DIALYZER_CALL=dialyzer --plt $(PLT)

BROKER_DIR=../rabbitmq-server
BROKER_SYMLINK=rabbitmq_server


all: compile

compile: $(TARGETS)

compile_tests: $(TEST_DIR)


dialyze: $(TARGETS)
	$(DIALYZER_CALL) -c $^

dialyze_all: $(TARGETS) $(TEST_TARGETS)
	$(DIALYZER_CALL) -c $^

add_broker_to_plt: $(BROKER_SYMLINK)/ebin
	$(DIALYZER_CALL) --add_to_plt -r $<

$(TEST_TARGETS): $(TEST_DIR)

.PHONY: $(TEST_DIR)
$(TEST_DIR): $(BROKER_SYMLINK)
	$(MAKE) -C $(TEST_DIR)

$(BROKER_SYMLINK):
ifdef BROKER_DIR
	ln -sf $(BROKER_DIR) $(BROKER_SYMLINK)
endif

$(EBIN_DIR)/%.beam: $(SOURCE_DIR)/%.erl $(INCLUDES) $(BROKER_SYMLINK)
	mkdir -p $(EBIN_DIR); erlc $(ERLC_OPTS) $<


run: compile
	erl -pa $(LOAD_PATH)

run_with_broker: compile
	$(ERL_WITH_BROKER)


all_tests: compile compile_tests
	$(ERL_WITH_BROKER) -eval 'network_client_SUITE:test(),direct_client_SUITE:test(),halt()'

all_tests_coverage: compile compile_tests
	$(ERL_WITH_BROKER) -eval 'rabbit_misc:enable_cover(),network_client_SUITE:test(),direct_client_SUITE:test(),rabbit_misc:report_cover(),halt()'

test_network: compile compile_tests
	$(ERL_WITH_BROKER) -eval 'network_client_SUITE:test(),halt().'

test_network_coverage: compile compile_tests
	$(ERL_WITH_BROKER) -eval 'network_client_SUITE:test_coverage(),halt().'

test_direct: compile compile_tests
	$(ERL_WITH_BROKER) -eval 'direct_client_SUITE:test(),halt().'

test_direct_coverage: compile compile_tests
	$(ERL_WITH_BROKER) -eval 'direct_client_SUITE:test_coverage(),halt().'


clean:
	rm -f $(EBIN_DIR)/*.beam
	rm -f rabbitmq_server erl_crash.dump
	rm -fr cover dist tmp
	$(MAKE) -C $(TEST_DIR) clean

$(DIST_DIR):
	mkdir -p $@

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

package: clean $(DIST_DIR) $(TARGETS)
	mkdir -p $(DIST_DIR)/$(PACKAGE)
	cp -r $(EBIN_DIR) $(DIST_DIR)/$(PACKAGE)
	cp -r $(INCLUDE_DIR) $(DIST_DIR)/$(PACKAGE)
	(cd $(DIST_DIR); zip -r $(PACKAGE_NAME) $(PACKAGE))

install: package
	cp $(DIST_DIR)/$(PACKAGE_NAME) $(BROKER_PLUGINS_LIB_DIR)
