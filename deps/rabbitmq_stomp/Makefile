PROJECT = rabbitmq_stomp

DEPS = rabbit amqp_client
TEST_DEPS = rabbitmq_test

DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
include erlang.mk

# --------------------------------------------------------------------
# Testing.
# --------------------------------------------------------------------

WITH_BROKER_TEST_MAKEVARS := \
	RABBITMQ_CONFIG_FILE=$(CURDIR)/etc/rabbit-test
WITH_BROKER_TEST_SCRIPTS := \
	test/src/test.py \
	test/src/test_connect_options.py \
	test/src/test_ssl.py
WITH_BROKER_TEST_COMMANDS := \
	rabbit_stomp_test:all_tests() \
	rabbit_stomp_amqqueue_test:all_tests()

STANDALONE_TEST_COMMANDS := \
	eunit:test([rabbit_stomp_test_util,rabbit_stomp_test_frame],[verbose])

pre-standalone-tests:: test-tmpdir
	$(verbose) rm -rf $(TEST_TMPDIR)/etc
	$(exec_verbose) mkdir -p $(TEST_TMPDIR)/etc/certs
	$(verbose) sed -E -e "s|%%CERTS_DIR%%|$(TEST_TMPDIR)/etc/certs|g" \
		< test/src/test.config > $(TEST_TMPDIR)/etc/test.config
	$(verbose) $(MAKE) -C $(DEPS_DIR)/rabbitmq_test/certs all PASSWORD=bunnychow \
		DIR=$(TEST_TMPDIR)/etc/certs
	$(verbose) $(MAKE) -C test/deps/stomppy
	$(verbose) $(MAKE) -C test/deps/pika
