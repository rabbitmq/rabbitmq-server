PROJECT = amqp_client

DEPS = rabbit_common
dep_rabbit_common = git https://github.com/rabbitmq/rabbitmq-common.git master

DEP_PLUGINS = rabbit_common/mk/rabbitmq-dist.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_GIT_REPOSITORY = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_GIT_REF = rabbitmq-tmp

include erlang.mk

# --------------------------------------------------------------------
# Compilation.
# --------------------------------------------------------------------

RMQ_ERLC_OPTS += -pa $(DEPS_DIR)/rabbit_common/ebin

ERLC_OPTS += $(RMQ_ERLC_OPTS)

# --------------------------------------------------------------------
# Tests.
# --------------------------------------------------------------------

TEST_ERLC_OPTS += $(RMQ_ERLC_OPTS)
