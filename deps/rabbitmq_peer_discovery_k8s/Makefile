PROJECT = rabbitmq_peer_discovery_k8s
PROJECT_DESCRIPTION = Kubernetes-based RabbitMQ peer discovery backend
PROJECT_MOD = rabbitmq_peer_discovery_k8s_app

DEPS = rabbit_common rabbitmq_peer_discovery_common rabbit
TEST_DEPS = meck

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk
