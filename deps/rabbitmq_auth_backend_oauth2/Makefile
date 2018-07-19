PROJECT = rabbitmq_auth_backend_oauth2
PROJECT_DESCRIPTION = OAuth 2 and JWT-based AuthN and AuthZ backend

BUILD_DEPS = rabbit_common
DEPS = rabbit cowlib jose
TEST_DEPS = cowboy rabbitmq_web_dispatch rabbitmq_ct_helpers

DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

dep_jose = hex 1.8.4

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
include erlang.mk
