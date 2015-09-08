PROJECT = amqp_client

DEPS = rabbit_common

# For RabbitMQ repositories, we want to checkout branches which match
# the parent porject. For instance, if the parent project is on a
# release tag, dependencies must be on the same release tag. If the
# parent project is on a topic branch, dependencies must be on the same
# topic branch or fallback to `stable` or `master` whichever was the
# base of the topic branch.

ifeq ($(origin current_rmq_ref),undefined)
current_rmq_ref := $(shell git symbolic-ref -q --short HEAD || git describe --tags --exact-match)
export current_rmq_ref
endif
ifeq ($(origin base_rmq_ref),undefined)
base_rmq_ref := $(shell git merge-base --is-ancestor $$(git merge-base master HEAD) stable && echo stable || echo master)
export base_rmq_ref
endif

dep_rabbit_common = git https://github.com/rabbitmq/rabbitmq-common.git $(current_rmq_ref) $(base_rmq_ref)

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
