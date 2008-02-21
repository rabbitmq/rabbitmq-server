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
ERLC_FLAGS=-W0
DIST_DIR=rabbitmq-erlang-client

NODENAME=rabbit_test_direct
MNESIA_DIR=/tmp/rabbitmq_$(NODENAME)_mnesia
LOG_BASE=/tmp


ERL_CALL=erl_call -sname $(NODENAME) -e


compile:
	mkdir -p $(EBIN_DIR)
	erlc +debug_info -I $(INCLUDE_DIR) -o $(EBIN_DIR) $(ERLC_FLAGS) $(SOURCE_DIR)/*.erl

all: compile


run_node: compile
	LOG_BASE=/tmp SKIP_HEART=true SKIP_LOG_ARGS=true MNESIA_DIR=$(MNESIA_DIR) RABBIT_ARGS="-detached -pa ./ebin" NODENAME=$(NODENAME) rabbitmq-server
	sleep 2 # so that the node is initialized when the tests are run

all_tests: test_network test_network_coverage test_direct test_direct_coverage
	$(ERL_CALL) -q

tests_network: test_network test_network_coverage
	$(ERL_CALL) -q

test_network: run_node
	erl -pa ebin -noshell -eval 'network_client_test:test(),halt().'

test_network_coverage: run_node
	erl -pa ebin -noshell -eval 'network_client_test:test_coverage(),halt().'

tests_direct: test_direct test_direct_coverage
	$(ERL_CALL) -q
	rm -rf $(MNESIA_DIR)

test_direct: run_node
	echo 'direct_client_test:test_wrapper("$(NODENAME)").' | $(ERL_CALL)

test_direct_coverage: run_node
	echo 'direct_client_test:test_coverage("$(NODENAME)").' | $(ERL_CALL)

clean:
	rm $(EBIN_DIR)/*.beam

source-tarball:
	mkdir -p dist/$(DIST_DIR)
	cp -a README Makefile src include dist/$(DIST_DIR)
	cd dist ; tar cvzf $(DIST_DIR).tar.gz $(DIST_DIR)

