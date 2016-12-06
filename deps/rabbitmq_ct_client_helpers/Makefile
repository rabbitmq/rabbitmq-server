PROJECT = rabbitmq_ct_client_helpers
PROJECT_DESCRIPTION = Common Test helpers for RabbitMQ (client-side helpers)

DEPS = rabbit_common rabbitmq_ct_helpers amqp_client

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

DEP_PLUGINS = rabbit_common/mk/rabbitmq-build.mk \
	      rabbit_common/mk/rabbitmq-tools.mk

include rabbitmq-components.mk
include erlang.mk
