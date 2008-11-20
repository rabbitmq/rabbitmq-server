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
INCLUDE_DIR=include
DIST_DIR=rabbitmq-erlang-client

LOAD_PATH=ebin rabbitmq_server/ebin

INCLUDES=$(wildcard $(INCLUDE_DIR)/*.hrl)
SOURCES=$(wildcard $(SOURCE_DIR)/*.erl)
TARGETS=$(patsubst $(SOURCE_DIR)/%.erl, $(EBIN_DIR)/%.beam,$(SOURCES))

ERLC_OPTS=-I $(INCLUDE_DIR) -o $(EBIN_DIR) -Wall -v +debug_info

BROKER_DIR=../rabbitmq-server
BROKER_SYMLINK=rabbitmq_server

NODENAME=rabbit_test_direct
MNESIA_DIR=/tmp/rabbitmq_$(NODENAME)_mnesia
LOG_BASE=/tmp

ERL_CALL=erl_call -sname $(NODENAME) -e

all: $(EBIN_DIR) $(TARGETS)

$(BROKER_SYMLINK):
ifdef BROKER_DIR
	ln -sf $(BROKER_DIR) $(BROKER_SYMLINK)
endif

$(EBIN_DIR):
	mkdir -p $@

$(EBIN_DIR)/%.beam: $(SOURCE_DIR)/%.erl $(INCLUDES) $(BROKER_SYMLINK)
	erlc $(ERLC_OPTS) $<

run_server:
	NODE_IP_ADDRESS=$(NODE_IP_ADDRESS) NODE_PORT=$(NODE_PORT) NODE_ONLY=true LOG_BASE=$(LOG_BASE) RABBIT_ARGS="$(RABBIT_ARGS) -s rabbit" MNESIA_DIR=$(MNESIA_DIR) $(BROKER_DIR)/scripts/rabbitmq-server
	sleep 2 # so that the node is initialized when the tests are run

all_tests: test_network test_network_coverage test_direct test_direct_coverage
	$(ERL_CALL) -q

tests_network: test_network test_network_coverage
	$(ERL_CALL) -q

test_network: $(TARGETS)
	erl -pa $(LOAD_PATH) -noshell -eval 'network_client_test:test(),halt().'

test_network_coverage: $(TARGETS)
	erl -pa $(LOAD_PATH) -noshell -eval 'network_client_test:test_coverage(),halt().'

tests_direct: test_direct test_direct_coverage
	$(ERL_CALL) -q
	rm -rf $(MNESIA_DIR)

test_direct: $(TARGETS)
	erl -pa $(LOAD_PATH) -mnesia dir tmp -boot start_sasl -s rabbit -noshell \
	-sasl sasl_error_logger '{file, "/dev/null"}' \
	-kernel error_logger '{file, "/dev/null"}' \
	-eval 'direct_client_test:test(),halt().'

test_direct_coverage: $(TARGETS)
	erl -pa $(LOAD_PATH) -mnesia dir tmp -boot start_sasl -s rabbit -noshell -eval \
	'direct_client_test:test_coverage(),halt().'

clean:
	rm -f $(EBIN_DIR)/*.beam
	rm -f rabbitmq_server erl_crash.dump
	rm -fr cover dist

source-tarball:
	mkdir -p dist/$(DIST_DIR)
	cp -a README Makefile src include dist/$(DIST_DIR)
	cd dist ; tar cvzf $(DIST_DIR).tar.gz $(DIST_DIR)

