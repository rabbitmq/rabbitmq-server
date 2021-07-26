PROJECT = amqp10_common
PROJECT_DESCRIPTION = Modules shared by rabbitmq-amqp1.0 and rabbitmq-amqp1.0-client

DIALYZER_OPTS += --src -r test

# Make sure we know where the applications are located.
ROOT_DIR ?= $(realpath ../..)
APPS_DIR ?= $(realpath ..)
DEPS_DIR ?= $(realpath ../../deps)

DEP_EARLY_PLUGINS = $(PROJECT)/../rabbit_common/mk/rabbitmq-early-test.mk
DEP_PLUGINS = $(PROJECT)/../rabbit_common/mk/rabbitmq-macros.mk \
	      $(PROJECT)/../rabbit_common/mk/rabbitmq-hexpm.mk \
	      $(PROJECT)/../rabbit_common/mk/rabbitmq-dist.mk \
	      $(PROJECT)/../rabbit_common/mk/rabbitmq-test.mk \
	      $(PROJECT)/../rabbit_common/mk/rabbitmq-tools.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

# Variables and recipes in development.*.mk are meant to be used from
# any Git clone. They are excluded from the files published to Hex.pm.
# Generated files are published to Hex.pm however so people using this
# source won't have to depend on Python and rabbitmq-codegen.
#
# That's why those Makefiles are included with `-include`: we ignore any
# inclusion errors.

-include development.pre.mk

include $(ROOT_DIR)/rabbitmq-components.mk
include $(ROOT_DIR)/erlang.mk

-include development.post.mk
