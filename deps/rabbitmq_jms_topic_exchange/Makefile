PROJECT = rabbitmq_jms_topic_exchange
PROJECT_DESCRIPTION = RabbitMQ JMS topic selector exchange plugin

DEPS = rabbit_common rabbit khepri khepri_mnesia_migration
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers amqp_client
LOCAL_DEPS = mnesia

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk
