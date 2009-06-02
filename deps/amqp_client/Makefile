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
SOURCE_DIR=src
TEST_SOURCE_DIR=tests
INCLUDE_DIR=include
DIST_DIR=rabbitmq-erlang-client

LOAD_PATH=ebin rabbitmq_server/ebin

INCLUDES=$(wildcard $(INCLUDE_DIR)/*.hrl)
SOURCES=$(wildcard $(SOURCE_DIR)/*.erl)
TEST_SOURCES=$(wildcard $(TEST_SOURCE_DIR)/*.erl)
TARGETS=$(patsubst $(SOURCE_DIR)/%.erl, $(EBIN_DIR)/%.beam,$(SOURCES))
TEST_TARGETS=$(patsubst $(TEST_SOURCE_DIR)/%.erl, $(EBIN_DIR)/%.beam,$(TEST_SOURCES))

ERLC_OPTS=-I $(INCLUDE_DIR) -o $(EBIN_DIR) -Wall -v +debug_info

BROKER_DIR=../rabbitmq-server
BROKER_SYMLINK=rabbitmq_server

NODENAME=rabbit_test_direct
MNESIA_DIR=/tmp/rabbitmq_$(NODENAME)_mnesia
LOG_BASE=/tmp

ERL_CALL=erl_call -sname $(NODENAME) -e

all: compile

compile: $(EBIN_DIR) $(TARGETS)

compile_tests: $(EBIN_DIR) $(TEST_TARGETS)

$(BROKER_SYMLINK):
ifdef BROKER_DIR
	ln -sf $(BROKER_DIR) $(BROKER_SYMLINK)
endif

$(EBIN_DIR):
	mkdir -p $@

$(EBIN_DIR)/%.beam: $(SOURCE_DIR)/%.erl $(INCLUDES) $(BROKER_SYMLINK)
	erlc $(ERLC_OPTS) $<

$(EBIN_DIR)/%.beam: $(TEST_SOURCE_DIR)/%.erl $(INCLUDES) $(BROKER_SYMLINK)
	erlc $(ERLC_OPTS) $<

run:
	erl -pa $(LOAD_PATH)


all_tests: test_network test_network_coverage test_direct test_direct_coverage
	$(ERL_CALL) -q

tests_network: test_network test_network_coverage
	$(ERL_CALL) -q

test_network: compile compile_tests
	erl -pa $(LOAD_PATH) -noshell -eval 'network_client_test:test(),halt().'

test_network_coverage: compile compile_tests
	erl -pa $(LOAD_PATH) -noshell -eval 'network_client_test:test_coverage(),halt().'

tests_direct: test_direct test_direct_coverage
	$(ERL_CALL) -q
	rm -rf $(MNESIA_DIR)

test_direct: compile compile_tests
	erl -pa $(LOAD_PATH) -noshell -mnesia dir tmp -boot start_sasl -s rabbit -noshell \
	-sasl sasl_error_logger '{file, "'${LOG_BASE}'/rabbit-sasl.log"}' \
	-kernel error_logger '{file, "'${LOG_BASE}'/rabbit.log"}' \
	-eval 'direct_client_test:test(),halt().'

test_direct_coverage: compile compile_tests
	erl -pa $(LOAD_PATH) -noshell -mnesia dir tmp -boot start_sasl -s rabbit -noshell \
	-sasl sasl_error_logger '{file, "'${LOG_BASE}'/rabbit-sasl.log"}' \
	-kernel error_logger '{file, "'${LOG_BASE}'/rabbit.log"}' \
	-eval 'direct_client_test:test_coverage(),halt().'

clean:
	rm -f $(EBIN_DIR)/*.beam
	rm -f rabbitmq_server erl_crash.dump
	rm -fr cover dist

source_tarball:
	mkdir -p dist/$(DIST_DIR)
	cp -a README Makefile src include dist/$(DIST_DIR)
	cd dist ; tar cvzf $(DIST_DIR).tar.gz $(DIST_DIR)

