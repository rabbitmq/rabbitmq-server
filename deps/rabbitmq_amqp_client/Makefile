PROJECT = rabbitmq_amqp_client
PROJECT_DESCRIPTION = AMQP 1.0 client for RabbitMQ

DEPS = amqp10_client
TEST_DEPS = rabbitmq_ct_helpers

BUILD_DEPS = rabbit_common
DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
TEST_DEPS = rabbit rabbitmq_ct_helpers

DEP_PLUGINS = rabbit_common/mk/rabbitmq-macros.mk \
	      rabbit_common/mk/rabbitmq-build.mk \
	      rabbit_common/mk/rabbitmq-hexpm.mk \
	      rabbit_common/mk/rabbitmq-dist.mk \
	      rabbit_common/mk/rabbitmq-run.mk \
	      rabbit_common/mk/rabbitmq-test.mk \
	      rabbit_common/mk/rabbitmq-tools.mk

.DEFAULT_GOAL = all

include rabbitmq-components.mk
include erlang.mk
