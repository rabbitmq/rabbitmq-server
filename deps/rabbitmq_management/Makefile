PROJECT = rabbitmq_management

DEPS = amqp_client webmachine rabbitmq_web_dispatch rabbitmq_management_agent

DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
include erlang.mk

# --------------------------------------------------------------------
# Distribution.
# --------------------------------------------------------------------

list-dist-deps::
	@echo bin/rabbitmqadmin

prepare-dist::
	$(verbose) sed 's/%%VSN%%/$(VSN)/' bin/rabbitmqadmin \
		> $(EZ_DIR)/priv/www/cli/rabbitmqadmin

# --------------------------------------------------------------------
# Testing.
# --------------------------------------------------------------------

FILTER := all
COVER := false

WITH_BROKER_TEST_MAKEVARS := \
	RABBITMQ_CONFIG_FILE=$(CURDIR)/etc/rabbit-test
WITH_BROKER_TEST_ENVVARS := \
	RABBITMQADMIN=$(CURDIR)/bin/rabbitmqadmin
WITH_BROKER_TEST_COMMANDS := \
	rabbit_test_runner:run_in_broker(\"$(CURDIR)/test\",\"$(FILTER)\")
WITH_BROKER_TEST_SCRIPTS := $(CURDIR)/test/src/rabbitmqadmin-test-wrapper.sh

TEST_PLUGINS_ROOTDIR = $(TEST_TMPDIR)/PLUGINS

STANDALONE_TEST_COMMANDS := \
	rabbit_test_runner:run_multi(\"$(DEPS_DIR)\",\"$(CURDIR)/test\",\"$(FILTER)\",$(COVER),\"$(TEST_PLUGINS_ROOTDIR)\")

pre-standalone-tests:: test-tmpdir test-dist
	$(verbose) rm -rf $(TEST_PLUGINS_ROOTDIR)
	$(exec_verbose) mkdir -p $(TEST_PLUGINS_ROOTDIR)
	$(verbose) cp -a $(DIST_DIR) $(TEST_PLUGINS_ROOTDIR)
