PROJECT = rabbitmq_aws
PROJECT_DESCRIPTION = A minimalistic AWS API interface used by rabbitmq-autocluster (3.6.x) and other RabbitMQ plugins
PROJECT_MOD = rabbitmq_aws_app
PROJECT_REGISTERED = rabbitmq_aws

define PROJECT_ENV
[]
endef

LOCAL_DEPS = crypto ssl inets
BUILD_DEPS = rabbit_common
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

current_rmq_ref = stable

include rabbitmq-components.mk
include erlang.mk


# --------------------------------------------------------------------
# Testing.
# --------------------------------------------------------------------

WITH_BROKER_TEST_COMMANDS := rabbitmq_aws_all_tests:run()
