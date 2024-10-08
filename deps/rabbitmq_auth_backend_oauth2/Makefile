PROJECT = rabbitmq_auth_backend_oauth2
PROJECT_DESCRIPTION = OAuth 2 and JWT-based AuthN and AuthZ backend

# We do not need QUIC as dependency of emqtt.
BUILD_WITHOUT_QUIC=1
export BUILD_WITHOUT_QUIC

LOCAL_DEPS = inets public_key
BUILD_DEPS = rabbit_common rabbitmq_cli
DEPS = rabbit cowlib jose base64url oauth2_client
TEST_DEPS = cowboy rabbitmq_web_dispatch rabbitmq_ct_helpers rabbitmq_ct_client_helpers amqp_client rabbitmq_web_mqtt emqtt rabbitmq_amqp_client

PLT_APPS += rabbitmqctl

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

dep_base64url = hex 1.0.1

dep_emqtt = git https://github.com/emqx/emqtt.git 1.11.0

include ../../rabbitmq-components.mk
include ../../erlang.mk
