PROJECT = rabbitmq_stream_common
PROJECT_DESCRIPTION = RabbitMQ Stream Common

define PROJECT_ENV
[
]
endef


DEPS =
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers

PLT_APPS = osiris

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
# We do not depend on rabbit therefore can't run the broker.
DEP_PLUGINS = rabbit_common/mk/rabbitmq-build.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk
