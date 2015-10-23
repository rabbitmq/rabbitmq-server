PROJECT = rabbitmq_web_dispatch

DEPS = mochiweb webmachine
TESTS_DEPS = amqp_client

DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
include erlang.mk

# --------------------------------------------------------------------
# Compilation.
# --------------------------------------------------------------------

ERTS_VER = $(shell erl -version 2>&1 | sed -E 's/.* version //')
USE_SPECS_MIN_ERTS_VER = 6.0
ifeq ($(call compare_version,$(ERTS_VER),$(USE_SPECS_MIN_ERTS_VER),<),true)
RMQ_ERLC_OPTS += -Dpre17_type_specs
endif

ERLC_OPTS += $(RMQ_ERLC_OPTS)

# --------------------------------------------------------------------
# Testing.
# --------------------------------------------------------------------

TEST_ERLC_OPTS += $(RMQ_ERLC_OPTS)

WITH_BROKER_TEST_COMMANDS := rabbit_web_dispatch_test:test()
STANDALONE_TEST_COMMANDS := rabbit_web_dispatch_test_unit:test()
