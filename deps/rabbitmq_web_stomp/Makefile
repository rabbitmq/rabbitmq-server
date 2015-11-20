PROJECT = rabbitmq_web_stomp

DEPS = cowboy sockjs rabbitmq_stomp
dep_cowboy_commit = 1.0.3

# FIXME: Add Ranch as a BUILD_DEPS to be sure Ranch 1.2.0 is picked. See
# rabbitmq-components.mk.
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

ERTS_VER = $(shell erl -version 2>&1 | sed -E 's/.* version //')
USE_SPECS_MIN_ERTS_VER = 6.0
ifeq ($(call compare_version,$(ERTS_VER),$(USE_SPECS_MIN_ERTS_VER),<),true)
SOCKJS_ERLC_OPTS += -Dpre17_type_specs
export SOCKJS_ERLC_OPTS
endif

.PHONY: patch-sockjs
patch-sockjs: $(DEPS_DIR)/sockjs
	$(exec_verbose) if ! grep -qw SOCKJS_ERLC_OPTS $(DEPS_DIR)/sockjs/Makefile; then \
		echo >> $(DEPS_DIR)/sockjs/Makefile; \
		echo >> $(DEPS_DIR)/sockjs/Makefile; \
		echo 'ERLC_OPTS += $$(SOCKJS_ERLC_OPTS)' >> $(DEPS_DIR)/sockjs/Makefile; \
	fi

# --------------------------------------------------------------------
# Testing.
# --------------------------------------------------------------------

WITH_BROKER_TEST_COMMANDS := rabbit_ws_test_all:all_tests()
