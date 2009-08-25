prepare_tests: compile compile_tests

all_tests: prepare_tests
	OK=true && \
	{ $(MAKE) test_suites || OK=false; } && \
	{ $(MAKE) test_common_package || OK=false; } && \
	$$OK

test_suites: prepare_tests
	OK=true && \
	{ $(MAKE) test_network || OK=false; } && \
	{ $(MAKE) test_direct || OK=false; } && \
	$(ALL_SSL) && \
	$$OK

test_suites_coverage: prepare_tests
	OK=true && \
	{ $(MAKE) test_network_coverage || OK=false; } && \
	{ $(MAKE) test_direct_coverage || OK=false; } && \
	$(ALL_SSL_COVERAGE) && \
	$$OK

## This performs test setup and teardown procedures to ensure that
## that the correct users are configured in the test instance
run_test_broker: start_test_broker_node unboot_broker
	OK=true && \
	TMPFILE=$(MKTEMP) && \
	{ $(MAKE) -C $(BROKER_DIR) run-node \
		RABBITMQ_SERVER_START_ARGS="$(PA_LOAD_PATH) $(SSL_BROKER_ARGS) \
		-noshell -s rabbit $(RUN_TEST_BROKER_ARGS) -s init stop" 2>&1 | \
		tee $$TMPFILE || OK=false; } && \
	{ egrep "All .+ tests (successful|passed)." $$TMPFILE || OK=false; } && \
	rm $$TMPFILE && \
	$(MAKE) boot_broker && \
	$(MAKE) stop_test_broker_node && \
	$$OK

start_test_broker_node: boot_broker
	$(RABBITMQCTL) delete_user test_user_no_perm 2>/dev/null || true
	$(RABBITMQCTL) add_user test_user_no_perm test_user_no_perm

stop_test_broker_node:
	$(RABBITMQCTL) delete_user test_user_no_perm
	$(MAKE) unboot_broker

boot_broker:
	$(MAKE) -C $(BROKER_DIR) start-background-node
	$(MAKE) -C $(BROKER_DIR) start-rabbit-on-node

unboot_broker:
	$(MAKE) -C $(BROKER_DIR) stop-rabbit-on-node
	$(MAKE) -C $(BROKER_DIR) stop-node

ssl:
	$(SSL)

test_ssl: prepare_tests ssl
	$(MAKE) run_test_broker RUN_TEST_BROKER_ARGS="-s ssl_client_SUITE test"

test_network: prepare_tests
	$(MAKE) run_test_broker RUN_TEST_BROKER_ARGS="-s network_client_SUITE test"

test_direct: prepare_tests
	$(MAKE) run_test_broker RUN_TEST_BROKER_ARGS="-s direct_client_SUITE test"

test_ssl_coverage: prepare_tests ssl
	$(MAKE) run_test_broker \
	RUN_TEST_BROKER_ARGS="$(COVER_START) -s ssl_client_SUITE test $(COVER_STOP)"

test_network_coverage: prepare_tests
	$(MAKE) run_test_broker \
	RUN_TEST_BROKER_ARGS="$(COVER_START) -s network_client_SUITE test $(COVER_STOP)"

test_direct_coverage: prepare_tests
	$(MAKE) run_test_broker \
	RUN_TEST_BROKER_ARGS="$(COVER_START) -s direct_client_SUITE test $(COVER_STOP)"
