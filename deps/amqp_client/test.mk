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

IS_SUCCESS:=egrep "(All .+ tests (successful|passed).|Test passed.)"
TESTING_MESSAGE:=-eval "error_logger:tty(false), error_logger:logfile({open, \"$(TMPDIR)/erlang-client-tests.log\"}), io:format(\"~nTesting in progress. Please wait...~n~n\")."

NODE_NAME := amqp_client
RUN := erl -pa test -sname $(NODE_NAME)

MKTEMP=$$(mktemp $(TMPDIR)/tmp.XXXXXXXXXX)

ifdef SSL_CERTS_DIR
SSL := true
ALL_SSL := { $(MAKE) test_ssl || OK=false; }
ALL_SSL_COVERAGE := { $(MAKE) test_ssl_coverage || OK=false; }
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
	OK=true && \
	{ $(MAKE) --no-print-directory test_suites || OK=false; } && \
	{ $(MAKE) --no-print-directory test_common_package || OK=false; } && \
	{ $(MAKE) --no-print-directory test_direct || OK=false; } && \
	$$OK

test_suites:
	OK=true && \
	{ $(MAKE) --no-print-directory test_network || OK=false; } && \
	{ $(MAKE) --no-print-directory test_remote_direct || OK=false; } && \
	$(ALL_SSL) && \
	$$OK

test_suites_coverage:
	OK=true && \
	{ $(MAKE) --no-print-directory test_network_coverage || OK=false; } && \
	{ $(MAKE) --no-print-directory test_direct_coverage || OK=false; } && \
	$(ALL_SSL_COVERAGE) && \
	$$OK

## Starts a broker, configures users and runs the tests on the same node
run_test_in_broker:
	$(MAKE) --no-print-directory start_test_broker_node
	$(MAKE) --no-print-directory unboot_broker
	OK=true && \
	TMPFILE=$(MKTEMP) && echo "Redirecting output to $$TMPFILE" && \
	{ $(MAKE) --no-print-directory run-node \
		RABBITMQ_SERVER_START_ARGS="-pa test $(SSL_BROKER_ARGS) \
		-noshell -s rabbit $(RUN_TEST_ARGS) -s init stop" 2>&1 | \
		tee $$TMPFILE || OK=false; } && \
	{ $(IS_SUCCESS) $$TMPFILE || OK=false; } && \
	rm $$TMPFILE && \
	$(MAKE) --no-print-directory boot_broker && \
	$(MAKE) --no-print-directory stop_test_broker_node && \
	$$OK

## Starts a broker, configures users and runs the tests from a different node
run_test_detached: start_test_broker_node
	OK=true && \
	TMPFILE=$(MKTEMP) && echo "Redirecting output to $$TMPFILE" && \
	{ MAKE=$(MAKE) \
	  $(RUN) -noinput $(TESTING_MESSAGE) \
	   $(SSL_CLIENT_ARGS) $(RUN_TEST_ARGS) \
	    -s init stop 2>&1 | tee $$TMPFILE || OK=false; } && \
	{ $(IS_SUCCESS) $$TMPFILE || OK=false; } && \
	rm $$TMPFILE && \
	$(MAKE) --no-print-directory stop_test_broker_node && \
	$$OK

## Starts a broker, configures users and runs the tests from a different node
run_test_foreground: start_test_broker_node
	OK=true && \
	{ MAKE=$(MAKE) \
	  $(RUN) -noinput $(TESTING_MESSAGE) \
	   $(SSL_CLIENT_ARGS) $(RUN_TEST_ARGS) \
	    -s init stop || OK=false; } && \
	$(MAKE) --no-print-directory stop_test_broker_node && \
	$$OK

start_test_broker_node: boot_broker
	sleep 1
	$(RABBITMQCTL) delete_user test_user_no_perm || :
	$(RABBITMQCTL) add_user test_user_no_perm test_user_no_perm
	sleep 1

stop_test_broker_node:
	sleep 1
	$(RABBITMQCTL) delete_user test_user_no_perm
	$(MAKE) --no-print-directory unboot_broker

boot_broker:
	$(MAKE) --no-print-directory start-background-node \
		RABBITMQ_SERVER_START_ARGS="$(RABBITMQ_SERVER_START_ARGS) $(SSL_BROKER_ARGS)"
	$(MAKE) --no-print-directory start-rabbit-on-node

unboot_broker:
	$(MAKE) --no-print-directory stop-rabbit-on-node
	$(MAKE) --no-print-directory stop-node

ssl:
	$(SSL)

test_ssl: ssl
	$(MAKE) --no-print-directory run_test_detached \
		AMQP_CLIENT_TEST_CONNECTION_TYPE="network_ssl" \
		RUN_TEST_ARGS="-s amqp_client_SUITE test"

test_network:
	$(MAKE) --no-print-directory run_test_detached \
		AMQP_CLIENT_TEST_CONNECTION_TYPE="network" \
		RUN_TEST_ARGS="-s amqp_client_SUITE test"

test_direct:
	$(MAKE) --no-print-directory run_test_in_broker \
		AMQP_CLIENT_TEST_CONNECTION_TYPE="direct" \
		RUN_TEST_ARGS="-s amqp_client_SUITE test"

test_remote_direct:
	$(MAKE) --no-print-directory run_test_detached \
		AMQP_CLIENT_TEST_CONNECTION_TYPE="direct" \
		RUN_TEST_ARGS="-s amqp_client_SUITE test"

test_common_package: test-dist
	$(MAKE) --no-print-directory run_test_detached \
		RUN="erl -pa test" \
		AMQP_CLIENT_TEST_CONNECTION_TYPE="network" \
		RUN_TEST_ARGS="-s amqp_client_SUITE test"
	$(MAKE) --no-print-directory run_test_detached \
		RUN="erl -pa test -sname amqp_client" \
		AMQP_CLIENT_TEST_CONNECTION_TYPE="direct" \
		RUN_TEST_ARGS="-s amqp_client_SUITE test"

test_ssl_coverage: ssl
	$(MAKE) --no-print-directory run_test_detached \
		AMQP_CLIENT_TEST_CONNECTION_TYPE="network_ssl" \
		RUN_TEST_ARGS="-s amqp_client_SUITE test_coverage"

test_network_coverage:
	$(MAKE) --no-print-directory run_test_detached \
		AMQP_CLIENT_TEST_CONNECTION_TYPE="network" \
		RUN_TEST_ARGS="-s amqp_client_SUITE test_coverage"

test_remote_direct_coverage:
	$(MAKE) --no-print-directory run_test_detached \
		AMQP_CLIENT_TEST_CONNECTION_TYPE="direct" \
		RUN_TEST_ARGS="-s amqp_client_SUITE test_coverage"

test_direct_coverage:
	$(MAKE) --no-print-directory run_test_in_broker \
		AMQP_CLIENT_TEST_CONNECTION_TYPE="direct" \
		RUN_TEST_ARGS="-s amqp_client_SUITE test_coverage"
