PROJECT = rabbitmq_stream_management
PROJECT_DESCRIPTION = RabbitMQ Stream Management
PROJECT_MOD = rabbit_stream_management

define PROJECT_ENV
[
]
endef


DEPS = rabbit rabbitmq_management rabbitmq_stream osiris
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk
