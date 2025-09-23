PROJECT = rabbitmq_aws
PROJECT_DESCRIPTION = AWS API library used by rabbitmq_peer_discovery_aws
PROJECT_MOD = rabbitmq_aws
PROJECT_REGISTERED = rabbitmq_aws_sup

define PROJECT_ENV
[]
endef

LOCAL_DEPS = crypto inets ssl xmerl public_key
BUILD_DEPS = rabbit_common
TEST_DEPS = meck rabbit rabbitmq_ct_helpers rabbitmq_ct_client_helpers

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk
