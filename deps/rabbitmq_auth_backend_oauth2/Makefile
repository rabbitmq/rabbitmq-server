PROJECT = rabbitmq_auth_backend_oauth2
PROJECT_DESCRIPTION = OAuth 2 and JWT-based AuthN and AuthZ backend

BUILD_DEPS = rabbit_common
DEPS = rabbit cowlib jose base64url
TEST_DEPS = cowboy rabbitmq_web_dispatch rabbitmq_ct_helpers rabbitmq_ct_client_helpers amqp_client

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

dep_jose = git https://github.com/michaelklishin/erlang-jose mk-thoas-support
dep_base64url = hex 1.0.1

# Make sure we know where the applications are located.
ROOT_DIR ?= $(abspath ../..)
DEPS_DIR ?= $(abspath ../../deps)

include $(ROOT_DIR)/rabbitmq-components.mk
include $(ROOT_DIR)/erlang.mk
