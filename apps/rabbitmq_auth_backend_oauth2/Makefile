PROJECT = rabbitmq_auth_backend_oauth2
PROJECT_DESCRIPTION = OAuth 2 and JWT-based AuthN and AuthZ backend

# Make sure we know where the applications are located.
ROOT_DIR ?= $(realpath ../..)
APPS_DIR ?= $(realpath ..)
DEPS_DIR ?= $(realpath ../../deps)

LOCAL_DEPS = rabbit
DEPS = cowlib jose base64url
TEST_DEPS = cowboy rabbitmq_ct_helpers rabbitmq_ct_client_helpers

DEP_EARLY_PLUGINS = $(PROJECT)/../rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = $(PROJECT)/../rabbit_common/mk/rabbitmq-plugin.mk

dep_jose = git https://github.com/potatosalad/erlang-jose 2b1d66b
dep_base64url = hex 1.0.1

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include $(ROOT_DIR)/rabbitmq-components.mk
include $(ROOT_DIR)/erlang.mk
