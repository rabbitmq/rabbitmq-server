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

compile:
	mkdir -p $(EBIN_DIR)
	erlc +debug_info -I $(INCLUDE_DIR) -o $(EBIN_DIR) $(ERLC_FLAGS) $(SOURCE_DIR)/*.erl

all: compile

test_network: compile
		erl -pa ebin -noshell -eval 'network_client_test:basic_get_test(), network_client_test:basic_return_test(), network_client_test:basic_qos_test(), network_client_test:basic_recover_test(), network_client_test:basic_consume_test(), network_client_test:basic_ack_test(), network_client_test:lifecycle_test(), network_client_test:channel_lifecycle_test(), network_client_test:test_coverage(),halt().'

test_network_coverage: compile
	erl -pa ebin -noshell -eval 'network_client_test:test_coverage(),halt().'

test_direct: compile
	echo 'direct_client_test:test_wrapper("rabbit-test"),halt().' | SKIP_HEART=true SKIP_LOG_ARGS=true MNESIA_DIR=/tmp/rabbitmq-test-mnesia RABBIT_ARGS="-s rabbit -pa ./ebin" NODENAME=rabbit-test rabbitmq-server

test_direct_coverage: compile
	echo 'direct_client_test:test_coverage("rabbit-test"),halt().' | SKIP_HEART=true SKIP_LOG_ARGS=true MNESIA_DIR=/tmp/rabbitmq-test-mnesia RABBIT_ARGS="-s rabbit -pa ./ebin" NODENAME=rabbit-test rabbitmq-server

clean:
	rm $(EBIN_DIR)/*.beam

source-tarball:
	mkdir -p dist/$(DIST_DIR)
	cp -a README Makefile src include dist/$(DIST_DIR)
	cd dist ; tar cvzf $(DIST_DIR).tar.gz $(DIST_DIR)

