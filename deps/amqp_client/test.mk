# The contents of this file are subject to the Mozilla Public License
# Version 1.1 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
# http://www.mozilla.org/MPL/
#
# Software distributed under the License is distributed on an "AS IS"
# basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
# License for the specific language governing rights and limitations
# under the License.
#
# The Original Code is RabbitMQ.
#
# The Initial Developer of the Original Code is GoPivotal, Inc.
# Copyright (c) 2007-2014 GoPivotal, Inc.  All rights reserved.
#

IS_SUCCESS:=egrep "(All .+ tests (successful|passed).|Test passed.)"
TESTING_MESSAGE:=-eval "error_logger:tty(false), error_logger:logfile({open, \"$(TMPDIR)/erlang-client-tests.log\"}), io:format(\"~nTesting in progress. Please wait...~n~n\")."

prepare_tests: compile compile_tests

all_tests: prepare_tests
	OK=true && \
	{ $(MAKE) test_suites || OK=false; } && \
	{ $(MAKE) test_common_package || OK=false; } && \
	{ $(MAKE) test_direct || OK=false; } && \
	$$OK

test_suites: prepare_tests
	OK=true && \
	{ $(MAKE) test_network || OK=false; } && \
	{ $(MAKE) test_remote_direct || OK=false; } && \
	$(ALL_SSL) && \
	$$OK

test_suites_coverage: prepare_tests
	OK=true && \
	{ $(MAKE) test_network_coverage || OK=false; } && \
	{ $(MAKE) test_direct_coverage || OK=false; } && \
	$(ALL_SSL_COVERAGE) && \
	$$OK

## Starts a broker, configures users and runs the tests on the same node
run_test_in_broker:
	$(MAKE) start_test_broker_node
	$(MAKE) unboot_broker
	OK=true && \
	TMPFILE=$(MKTEMP) && echo "Redirecting output to $$TMPFILE" && \
	{ $(MAKE) -C $(BROKER_DIR) run-node \
		RABBITMQ_SERVER_START_ARGS="$(PA_LOAD_PATH) $(SSL_BROKER_ARGS) \
		-noshell -s rabbit $(RUN_TEST_ARGS) -s init stop" 2>&1 | \
		tee $$TMPFILE || OK=false; } && \
	{ $(IS_SUCCESS) $$TMPFILE || OK=false; } && \
	rm $$TMPFILE && \
	$(MAKE) boot_broker && \
	$(MAKE) stop_test_broker_node && \
	$$OK

## Starts a broker, configures users and runs the tests from a different node
run_test_detached: start_test_broker_node
	OK=true && \
	TMPFILE=$(MKTEMP) && echo "Redirecting output to $$TMPFILE" && \
	{ $(RUN) -noinput $(TESTING_MESSAGE) \
	   $(SSL_CLIENT_ARGS) $(RUN_TEST_ARGS) \
	    -s init stop 2>&1 | tee $$TMPFILE || OK=false; } && \
	{ $(IS_SUCCESS) $$TMPFILE || OK=false; } && \
	rm $$TMPFILE && \
	$(MAKE) stop_test_broker_node && \
	$$OK

## Starts a broker, configures users and runs the tests from a different node
run_test_foreground: start_test_broker_node
	OK=true && \
	{ $(RUN) -noinput $(TESTING_MESSAGE) \
	   $(SSL_CLIENT_ARGS) $(RUN_TEST_ARGS) \
	    -s init stop || OK=false; } && \
	$(MAKE) stop_test_broker_node && \
	$$OK

start_test_broker_node: boot_broker
	sleep 1
	- $(RABBITMQCTL) delete_user test_user_no_perm
	$(RABBITMQCTL) add_user test_user_no_perm test_user_no_perm
	sleep 1

stop_test_broker_node:
	sleep 1
	$(RABBITMQCTL) delete_user test_user_no_perm
	$(MAKE) unboot_broker

boot_broker:
	$(MAKE) -C $(BROKER_DIR) start-background-node RABBITMQ_SERVER_START_ARGS="$(RABBITMQ_SERVER_START_ARGS) $(SSL_BROKER_ARGS)"
	$(MAKE) -C $(BROKER_DIR) start-rabbit-on-node

unboot_broker:
	$(MAKE) -C $(BROKER_DIR) stop-rabbit-on-node
	$(MAKE) -C $(BROKER_DIR) stop-node

ssl:
	$(SSL)

test_ssl: prepare_tests ssl
	$(MAKE) run_test_detached AMQP_CLIENT_TEST_CONNECTION_TYPE="network_ssl" RUN_TEST_ARGS="-s amqp_client_SUITE test"

test_network: prepare_tests
	$(MAKE) run_test_detached AMQP_CLIENT_TEST_CONNECTION_TYPE="network" RUN_TEST_ARGS="-s amqp_client_SUITE test"

test_direct: prepare_tests
	$(MAKE) run_test_in_broker AMQP_CLIENT_TEST_CONNECTION_TYPE="direct" RUN_TEST_ARGS="-s amqp_client_SUITE test"

test_remote_direct: prepare_tests
	$(MAKE) run_test_detached AMQP_CLIENT_TEST_CONNECTION_TYPE="direct" RUN_TEST_ARGS="-s amqp_client_SUITE test"

test_common_package: $(DIST_DIR)/$(COMMON_PACKAGE_EZ) package prepare_tests
	$(MAKE) run_test_detached RUN="$(LIBS_PATH) erl -pa $(TEST_DIR)" \
	    AMQP_CLIENT_TEST_CONNECTION_TYPE="network" RUN_TEST_ARGS="-s amqp_client_SUITE test"
	$(MAKE) run_test_detached RUN="$(LIBS_PATH) erl -pa $(TEST_DIR) -sname amqp_client" \
	    AMQP_CLIENT_TEST_CONNECTION_TYPE="direct" RUN_TEST_ARGS="-s amqp_client_SUITE test"

test_ssl_coverage: prepare_tests ssl
	$(MAKE) run_test_detached AMQP_CLIENT_TEST_CONNECTION_TYPE="network_ssl" RUN_TEST_ARGS="-s amqp_client_SUITE test_coverage"

test_network_coverage: prepare_tests
	$(MAKE) run_test_detached AMQP_CLIENT_TEST_CONNECTION_TYPE="network" RUN_TEST_ARGS="-s amqp_client_SUITE test_coverage"

test_remote_direct_coverage: prepare_tests
	$(MAKE) run_test_detached AMQP_CLIENT_TEST_CONNECTION_TYPE="direct" RUN_TEST_ARGS="-s amqp_client_SUITE test_coverage"

test_direct_coverage: prepare_tests
	$(MAKE) run_test_in_broker AMQP_CLIENT_TEST_CONNECTION_TYPE="direct" RUN_TEST_ARGS="-s amqp_client_SUITE test_coverage"
