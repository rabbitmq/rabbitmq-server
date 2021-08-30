PROJECT = rabbitmq_auth_backend_oauth2
PROJECT_DESCRIPTION = OAuth 2 and JWT-based AuthN and AuthZ backend

BUILD_DEPS = rabbit_common
DEPS = rabbit cowlib jose base64url
TEST_DEPS = cowboy rabbitmq_web_dispatch rabbitmq_ct_helpers rabbitmq_ct_client_helpers amqp_client

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

dep_jose = git https://github.com/potatosalad/erlang-jose 2b1d66b
dep_base64url = hex 1.0.1

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include ../../rabbitmq-components.mk
include ../../erlang.mk
