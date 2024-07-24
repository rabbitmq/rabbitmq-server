PROJECT = oauth2_client
PROJECT_DESCRIPTION = OAuth2 client from the RabbitMQ Project
PROJECT_MOD = oauth2_client_app

BUILD_DEPS = rabbit
DEPS = rabbit_common jose
TEST_DEPS = rabbitmq_ct_helpers cowboy
LOCAL_DEPS = ssl inets crypto public_key

PLT_APPS = rabbit

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-test.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-build.mk \
	      rabbit_common/mk/rabbitmq-hexpm.mk \
	      rabbit_common/mk/rabbitmq-dist.mk \
	      rabbit_common/mk/rabbitmq-run.mk

include rabbitmq-components.mk
include erlang.mk
