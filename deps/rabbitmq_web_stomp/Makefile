PROJECT = rabbitmq_web_stomp

DEPS = cowboy sockjs rabbitmq_stomp
TEST_DEPS := $(filter-out rabbitmq_test,$(TEST_DEPS))
dep_cowboy_commit = 1.0.3

# FIXME: Add Ranch as a BUILD_DEPS to be sure the correct version is picked.
# See rabbitmq-components.mk.
BUILD_DEPS += ranch

DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

# We need to patch SockJS' Makefile to be able to pass ERLC_OPTS to it.
.DEFAULT_GOAL = all
deps:: patch-sockjs

include rabbitmq-components.mk
include erlang.mk

# --------------------------------------------------------------------
# Compilation.
# --------------------------------------------------------------------

SOCKJS_ERLC_OPTS += $(RMQ_ERLC_OPTS)
export SOCKJS_ERLC_OPTS

.PHONY: patch-sockjs
patch-sockjs: $(DEPS_DIR)/sockjs
	$(exec_verbose) if ! grep -qw SOCKJS_ERLC_OPTS $(DEPS_DIR)/sockjs/Makefile; then \
		echo >> $(DEPS_DIR)/sockjs/Makefile; \
		echo >> $(DEPS_DIR)/sockjs/Makefile; \
		echo 'ERLC_OPTS += $$(SOCKJS_ERLC_OPTS)' >> $(DEPS_DIR)/sockjs/Makefile; \
	fi
