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

NODENAME=rabbit-test-direct
NODENAME2=rabbit-test-direct-coverage
MNESIA_DIR=/tmp/rabbitmq-$(NODENAME)-mnesia
MNESIA_DIR2=/tmp/rabbitmq-$(NODENAME)-mnesia
LOG_BASE=/tmp


ERL_CALL=erl_call -sname $(NODENAME) -e
ERL_CALL2=erl_call -sname $(NODENAME2) -e


compile:
	mkdir -p $(EBIN_DIR)
	erlc +debug_info -I $(INCLUDE_DIR) -o $(EBIN_DIR) $(ERLC_FLAGS) $(SOURCE_DIR)/*.erl

all: compile

test_network: compile
	erl -pa ebin -noshell -eval 'network_client_test:test(),halt().'

test_network_coverage: compile
	erl -pa ebin -noshell -eval 'network_client_test:test_coverage(),halt().'

# You may have to run twice either of test_direct* to run it effectively
# (because of logging/restoring I guess)
test_direct: compile
	LOG_BASE=/tmp SKIP_HEART=true SKIP_LOG_ARGS=true MNESIA_DIR=/tmp/rabbitmq-test-direct-mnesia RABBIT_ARGS="-detached -pa ./ebin" NODENAME=rabbit-test-direct rabbitmq-server
	echo 'direct_client_test:test_wrapper("rabbit-test-direct").' | $(ERL_CALL)
	@echo 'rabbit:stop_and_halt().' | $(ERL_CALL)

test_direct_coverage: compile
	LOG_BASE=/tmp SKIP_HEART=true SKIP_LOG_ARGS=true MNESIA_DIR=/tmp/rabbitmq-test-direct-coverage-mnesia RABBIT_ARGS="-detached -pa ./ebin" NODENAME=rabbit-test-direct-coverage rabbitmq-server
	echo 'direct_client_test:test_coverage("rabbit-test-direct-coverage").' | $(ERL_CALL2)
	@echo 'rabbit:stop_and_halt().' | $(ERL_CALL2)

clean:
	rm $(EBIN_DIR)/*.beam

source-tarball:
	mkdir -p dist/$(DIST_DIR)
	cp -a README Makefile src include dist/$(DIST_DIR)
	cd dist ; tar cvzf $(DIST_DIR).tar.gz $(DIST_DIR)

