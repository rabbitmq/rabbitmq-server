PROJECT = oauth2_client
PROJECT_DESCRIPTION = OAuth2 client from the RabbitMQ Project

BUILD_DEPS = rabbit
DEPS = rabbit_common jose
TEST_DEPS = rabbitmq_ct_helpers cowboy
LOCAL_DEPS = ssl inets crypto public_key

PLT_APPS = rabbit

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# Required to properly autopatch jose.
ELIXIR = system

include rabbitmq-components.mk
include erlang.mk
