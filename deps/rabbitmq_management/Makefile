PROJECT = rabbitmq_management

DEPS = rabbit_common rabbit amqp_client cowboy cowlib rabbitmq_web_dispatch rabbitmq_management_agent
TEST_DEPS = rabbitmq_ct_helpers proper
dep_cowboy_commit = 1.0.3
dep_rabbitmq_web_dispatch = git https://github.com/rabbitmq/rabbitmq-web-dispatch.git stable
LOCAL_DEPS += xmerl mnesia ranch ssl crypto public_key

# FIXME: Add Ranch as a BUILD_DEPS to be sure the correct version is picked.
# See rabbitmq-components.mk.
BUILD_DEPS += ranch

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
