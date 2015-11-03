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
# Copyright (c) 2007-2015 Pivotal Software, Inc.  All rights reserved.
#

IS_SUCCESS:=egrep -E "(All .+ tests (successful|passed).|Test passed.)"
TESTING_MESSAGE:=-eval "error_logger:tty(false), error_logger:logfile({open, \"$(TMPDIR)/erlang-client-tests.log\"}), io:format(\"~nTesting in progress. Please wait...~n~n\")."

NODE_NAME := amqp_client
RUN := erl -pa test -sname $(NODE_NAME)

MKTEMP=$$(mktemp $(TMPDIR)/tmp.XXXXXXXXXX)

ifdef SSL_CERTS_DIR
SSL := true
ALL_SSL := $(MAKE) --no-print-directory test_ssl
ALL_SSL_COVERAGE := $(MAKE) --no-print-directory test_ssl_coverage
SSL_BROKER_ARGS := -rabbit ssl_listeners [{\\\"0.0.0.0\\\",5671},{\\\"::1\\\",5671}] \
	-rabbit ssl_options [{cacertfile,\\\"$(SSL_CERTS_DIR)/testca/cacert.pem\\\"},{certfile,\\\"$(SSL_CERTS_DIR)/server/cert.pem\\\"},{keyfile,\\\"$(SSL_CERTS_DIR)/server/key.pem\\\"},{verify,verify_peer},{fail_if_no_peer_cert,true}]
SSL_CLIENT_ARGS := -erlang_client_ssl_dir $(SSL_CERTS_DIR)
else
SSL := @echo No SSL_CERTS_DIR defined. && false
ALL_SSL := true
ALL_SSL_COVERAGE := true
SSL_BROKER_ARGS :=
SSL_CLIENT_ARGS :=
endif

all_tests:
	$(test_verbose) rm -f failed-$@
	-$(verbose) $(MAKE) --no-print-directory test_suites || touch failed-$@
	-$(verbose) $(MAKE) --no-print-directory test_common_package || touch failed-$@
	-$(verbose) $(MAKE) --no-print-directory test_direct || touch failed-$@
	$(verbose) ! rm failed-$@ 2>/dev/null

test_suites:
	$(test_verbose) rm -f failed-$@
	-$(verbose) $(MAKE) --no-print-directory test_network || touch failed-$@
	-$(verbose) $(MAKE) --no-print-directory test_remote_direct || touch failed-$@
	-$(verbose) $(ALL_SSL) || touch failed-$@
	$(verbose) ! rm failed-$@ 2>/dev/null

test_suites_coverage:
	$(test_verbose) rm -f failed-$@
	-$(verbose) $(MAKE) --no-print-directory test_network_coverage || touch failed-$@
	-$(verbose) $(MAKE) --no-print-directory test_direct_coverage || touch failed-$@
	$(ALL_SSL_COVERAGE) || touch failed-$@
	$(verbose) ! rm failed-$@ 2>/dev/null

## Starts a broker, configures users and runs the tests on the same node
run_test_in_broker:
	$(verbose) $(MAKE) --no-print-directory start_test_broker_node
	$(verbose) $(MAKE) --no-print-directory unboot_broker
	$(verbose) rm -f failed-$@
	-$(verbose) TMPFILE=$(MKTEMP) && \
		( echo "Redirecting output to $$TMPFILE" && \
		$(MAKE) --no-print-directory run-node \
		RABBITMQ_SERVER_START_ARGS="-pa test $(SSL_BROKER_ARGS) \
		-noshell -s rabbit $(RUN_TEST_ARGS) -s init stop" 2>&1 | \
		tee $$TMPFILE && \
		$(IS_SUCCESS) $$TMPFILE ) || touch failed-$@; \
		rm $$TMPFILE
	-$(verbose) $(MAKE) --no-print-directory boot_broker || touch failed-$@
	-$(verbose) $(MAKE) --no-print-directory stop_test_broker_node || touch failed-$@
	$(verbose) ! rm failed-$@ 2>/dev/null

## Starts a broker, configures users and runs the tests from a different node
run_test_detached: start_test_broker_node
	$(verbose) rm -f failed-$@
	-$(verbose) TMPFILE=$(MKTEMP) && \
		( echo "Redirecting output to $$TMPFILE" && \
		MAKE=$(MAKE) \
		ERL_LIBS='$(CURDIR)/$(DIST_DIR):$(DIST_ERL_LIBS)' \
		$(RUN) -noinput $(TESTING_MESSAGE) \
		$(SSL_CLIENT_ARGS) $(RUN_TEST_ARGS) -s init stop 2>&1 | \
		tee $$TMPFILE && \
		$(IS_SUCCESS) $$TMPFILE ) || touch failed-$@; \
		rm $$TMPFILE
	-$(verbose) $(MAKE) --no-print-directory stop_test_broker_node || touch failed-$@
	$(verbose) ! rm failed-$@ 2>/dev/null

## Starts a broker, configures users and runs the tests from a different node
run_test_foreground: start_test_broker_node
	$(verbose) rm -f failed-$@
	-$(verbose) MAKE=$(MAKE) $(RUN) -noinput $(TESTING_MESSAGE) \
	   $(SSL_CLIENT_ARGS) $(RUN_TEST_ARGS) -s init stop || touch failed-$@
	-$(verbose) $(MAKE) --no-print-directory stop_test_broker_node || touch failed-$@
	$(verbose) ! rm failed-$@ 2>/dev/null

start_test_broker_node: boot_broker
	$(exec_verbose) sleep 1
	$(verbose) $(RABBITMQCTL) -n $(RABBITMQ_NODENAME) delete_user test_user_no_perm || :
	$(verbose) $(RABBITMQCTL) -n $(RABBITMQ_NODENAME) add_user test_user_no_perm test_user_no_perm
	$(verbose) sleep 1

stop_test_broker_node:
	$(exec_verbose) sleep 1
	-$(verbose) $(RABBITMQCTL) -n $(RABBITMQ_NODENAME) delete_user test_user_no_perm
	$(verbose) $(MAKE) --no-print-directory unboot_broker

boot_broker: virgin-test-tmpdir
	$(exec_verbose) $(MAKE) --no-print-directory start-background-node \
		RABBITMQ_SERVER_START_ARGS="$(RABBITMQ_SERVER_START_ARGS) \
		$(SSL_BROKER_ARGS)"
	$(verbose) $(MAKE) --no-print-directory start-rabbit-on-node

unboot_broker:
	$(exec_verbose) $(MAKE) --no-print-directory stop-rabbit-on-node
	$(verbose) $(MAKE) --no-print-directory stop-node

ssl:
	$(verbose) $(SSL)

test_ssl: test-dist ssl
	$(test_verbose) $(MAKE) --no-print-directory run_test_detached \
		AMQP_CLIENT_TEST_CONNECTION_TYPE="network_ssl" \
		RUN_TEST_ARGS="-s amqp_client_SUITE test"

test_network: test-dist
	$(test_verbose) $(MAKE) --no-print-directory run_test_detached \
		AMQP_CLIENT_TEST_CONNECTION_TYPE="network" \
		RUN_TEST_ARGS="-s amqp_client_SUITE test"

test_direct: test-dist
	$(test_verbose) $(MAKE) --no-print-directory run_test_in_broker \
		AMQP_CLIENT_TEST_CONNECTION_TYPE="direct" \
		RUN_TEST_ARGS="-s amqp_client_SUITE test"

test_remote_direct: test-dist
	$(test_verbose) $(MAKE) --no-print-directory run_test_detached \
		AMQP_CLIENT_TEST_CONNECTION_TYPE="direct" \
		RUN_TEST_ARGS="-s amqp_client_SUITE test"

test_common_package: test-dist
	$(test_verbose) $(MAKE) --no-print-directory run_test_detached \
		RUN="erl -pa test" \
		AMQP_CLIENT_TEST_CONNECTION_TYPE="network" \
		RUN_TEST_ARGS="-s amqp_client_SUITE test"
	$(verbose) $(MAKE) --no-print-directory run_test_detached \
		RUN="erl -pa test -sname amqp_client" \
		AMQP_CLIENT_TEST_CONNECTION_TYPE="direct" \
		RUN_TEST_ARGS="-s amqp_client_SUITE test"

test_ssl_coverage: test-dist ssl
	$(test_verbose) $(MAKE) --no-print-directory run_test_detached \
		AMQP_CLIENT_TEST_CONNECTION_TYPE="network_ssl" \
		RUN_TEST_ARGS="-s amqp_client_SUITE test_coverage"

test_network_coverage: test-dist
	$(test_verbose) $(MAKE) --no-print-directory run_test_detached \
		AMQP_CLIENT_TEST_CONNECTION_TYPE="network" \
		RUN_TEST_ARGS="-s amqp_client_SUITE test_coverage"

test_remote_direct_coverage: test-dist
	$(test_verbose) $(MAKE) --no-print-directory run_test_detached \
		AMQP_CLIENT_TEST_CONNECTION_TYPE="direct" \
		RUN_TEST_ARGS="-s amqp_client_SUITE test_coverage"

test_direct_coverage: test-dist
	$(test_verbose) $(MAKE) --no-print-directory run_test_in_broker \
		AMQP_CLIENT_TEST_CONNECTION_TYPE="direct" \
		RUN_TEST_ARGS="-s amqp_client_SUITE test_coverage"
