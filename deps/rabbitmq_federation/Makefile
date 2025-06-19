PROJECT = rabbitmq_federation
PROJECT_DESCRIPTION = Deprecated no-op RabbitMQ Federation

DEPS = rabbitmq_queue_federation rabbitmq_exchange_federation
LOCAL_DEPS = rabbit

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-build.mk

.DEFAULT_GOAL = all

include ../../rabbitmq-components.mk
include ../../erlang.mk
