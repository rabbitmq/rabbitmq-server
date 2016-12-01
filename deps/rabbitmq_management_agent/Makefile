PROJECT = rabbitmq_management_agent

DEPS = rabbit_common rabbit
TEST_DEPS = rabbitmq_ct_helpers
LOCAL_DEPS += xmerl mnesia ranch ssl crypto public_key
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
include erlang.mk
