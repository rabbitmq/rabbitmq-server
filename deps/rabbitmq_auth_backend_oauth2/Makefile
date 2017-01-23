PROJECT = rabbitmq_auth_backend_uaa

DEPS = amqp_client uaa_jwt rabbit
TEST_DEPS = cowboy rabbitmq_web_dispatch rabbit rabbitmq_ct_helpers jose

DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

dep_uaa_jwt = git_rmq uaa_jwt $(current_rmq_ref) $(base_rmq_ref) master
dep_jose = hex 1.8.0

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
include erlang.mk


MIX_ERL_LIBS = $(shell echo $$(for i in $(DEPS_DIR)/*/_build/dev/lib; do echo $$sep$$i; sep=:; done))

# # Space character
# space := $(subst ,, )
# MIX_ERL_LIBS = $(subst $(space),:,$(wildcard $(DEPS_DIR)/*/_build/dev/lib))

ifeq ($(ERL_LIBS),)
	ERL_LIBS = $(MIX_ERL_LIBS)
else
	ERL_LIBS := $(ERL_LIBS):$(MIX_ERL_LIBS)
endif