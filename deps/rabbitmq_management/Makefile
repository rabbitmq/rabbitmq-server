PROJECT = rabbitmq_management

DEPS = amqp_client cowboy rabbitmq_web_dispatch rabbitmq_management_agent
dep_cowboy_commit = 1.0.3

# FIXME: Add Ranch as a BUILD_DEPS to be sure the correct version is picked.
# See rabbitmq-components.mk.
BUILD_DEPS += ranch

TEST_DEPS += rabbit

DEP_PLUGINS = rabbit_common/mk/rabbitmq-dist.mk \
	      rabbit_common/mk/rabbitmq-run.mk \
	      rabbit_common/mk/rabbitmq-tools.mk

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

WITH_BROKER_TEST_ENVVARS := \
	RABBITMQADMIN=$(CURDIR)/bin/rabbitmqadmin
WITH_BROKER_TEST_SCRIPTS := $(CURDIR)/test/src/rabbitmqadmin-test-wrapper.sh

TEST_PLUGINS_ROOTDIR = $(TEST_TMPDIR)/PLUGINS

pre-standalone-tests:: test-tmpdir test-dist
	$(verbose) rm -rf $(TEST_PLUGINS_ROOTDIR)
	$(exec_verbose) mkdir -p $(TEST_PLUGINS_ROOTDIR)
	$(verbose) cp -a $(DIST_DIR) $(TEST_PLUGINS_ROOTDIR)
