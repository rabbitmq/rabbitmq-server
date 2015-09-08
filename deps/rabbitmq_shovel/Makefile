PROJECT = rabbitmq_shovel

DEPS = rabbit_common amqp_client
dep_amqp_client = git https://github.com/rabbitmq/rabbitmq-erlang-client.git erlang.mk
dep_rabbit_common = git https://github.com/rabbitmq/rabbitmq-common.git master

DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_GIT_REPOSITORY = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_GIT_REF = rabbitmq-tmp

include erlang.mk

WITH_BROKER_TEST_COMMANDS := rabbit_shovel_test_all:all_tests()
