PROJECT = rabbitmq_stream_common
PROJECT_DESCRIPTION = RabbitMQ Stream Common
PROJECT_MOD = rabbit_stream_common

define PROJECT_ENV
[
]
endef


DEPS =
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk
