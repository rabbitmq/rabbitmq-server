PROJECT = rabbitmq_auth_backend_oauth2
PROJECT_DESCRIPTION = OAuth 2 and JWT-based AuthN and AuthZ backend

<<<<<<< HEAD
=======
# We do not need QUIC as dependency of emqtt.
BUILD_WITHOUT_QUIC=1
export BUILD_WITHOUT_QUIC

LOCAL_DEPS = inets public_key
>>>>>>> 8d107818bf (Add a workflow to compare the bazel/erlang.mk output)
BUILD_DEPS = rabbit_common
DEPS = rabbit cowlib jose base64url
TEST_DEPS = cowboy rabbitmq_web_dispatch rabbitmq_ct_helpers rabbitmq_ct_client_helpers amqp_client

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

dep_jose = git https://github.com/potatosalad/erlang-jose 2b1d66b
dep_base64url = hex 1.0.1

include ../../rabbitmq-components.mk
include ../../erlang.mk
