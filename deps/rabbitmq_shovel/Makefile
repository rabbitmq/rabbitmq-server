PROJECT = rabbitmq_shovel

DEPS = rabbit_common amqp_client
dep_amqp_client = git file:///home/dumbbell/Projects/pivotal/rabbitmq-public-umbrella/rabbitmq-erlang-client erlang.mk
dep_rabbit_common = git file:///home/dumbbell/Projects/pivotal/other-repos/rabbitmq-common master

ERLANG_MK_DISABLE_PLUGINS = eunit

include erlang.mk

# TODO: Simplify this when support is added to erlang.mk.
ERLANG_MK_3RDPARTY_PLUGINS = $(DEPS_DIR)/rabbit_common/mk/rabbitmq-plugin.mk
-include $(ERLANG_MK_3RDPARTY_PLUGINS)
$(ERLANG_MK_3RDPARTY_PLUGINS): $(DEPS_DIR)/rabbit_common
	@:
