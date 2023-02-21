PROJECT = rabbitmq_auth_backend_oauth2
PROJECT_DESCRIPTION = OAuth 2 and JWT-based AuthN and AuthZ backend

# We do not need QUIC as dependency of emqtt.
BUILD_WITHOUT_QUIC=1
export BUILD_WITHOUT_QUIC

BUILD_DEPS = rabbit_common
DEPS = rabbit cowlib jose base64url
TEST_DEPS = cowboy rabbitmq_web_dispatch rabbitmq_ct_helpers rabbitmq_ct_client_helpers amqp_client rabbitmq_mqtt emqtt

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

dep_jose = git https://github.com/michaelklishin/erlang-jose mk-thoas-support
dep_base64url = hex 1.0.1

dep_emqtt = git https://github.com/emqx/emqtt.git 1.8.2

include ../../rabbitmq-components.mk
include ../../erlang.mk
