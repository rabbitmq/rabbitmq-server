PROJECT = oauth2_client
PROJECT_DESCRIPTION = OAuth2 client from the RabbitMQ Project

BUILD_DEPS = rabbit jose
DEPS = rabbit_common
TEST_DEPS = rabbitmq_ct_helpers cowboy meck
LOCAL_DEPS = ssl inets crypto public_key

PLT_APPS = rabbit

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

include rabbitmq-components.mk
include erlang.mk
