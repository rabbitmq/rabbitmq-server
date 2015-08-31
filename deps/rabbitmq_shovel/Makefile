PROJECT = rabbitmq_shovel

DEPS = rabbit_common amqp_client
dep_amqp_client = git file:///home/dumbbell/Projects/pivotal/rabbitmq-public-umbrella/rabbitmq-erlang-client erlang.mk
dep_rabbit_common = git file:///home/dumbbell/Projects/pivotal/other-repos/rabbitmq-common master

DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk
ERLANG_MK_DISABLE_PLUGINS = eunit

WITH_BROKER_TEST_COMMANDS := rabbit_shovel_test_all:all_tests()

include erlang.mk
