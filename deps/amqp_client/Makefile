PROJECT = amqp_client

DEPS = rabbit_common
dep_rabbit_common = git https://github.com/rabbitmq/rabbitmq-common.git master

ERLANG_MK_DISABLE_PLUGINS = ct eunit

include erlang.mk

# TODO: Simplify this when support is added to erlang.mk.
ERLANG_MK_3RDPARTY_PLUGINS = $(DEPS_DIR)/rabbit_common/mk/rabbitmq-dist.mk
-include $(ERLANG_MK_3RDPARTY_PLUGINS)
$(ERLANG_MK_3RDPARTY_PLUGINS): $(DEPS_DIR)/rabbit_common
	@:

# --------------------------------------------------------------------
# Compilation.
# --------------------------------------------------------------------

COMPILE_FIRST = $(basename \
		$(notdir \
		$(shell grep -lw '^behaviour_info' src/*.erl)))

RMQ_ERLC_OPTS += -pa $(DEPS_DIR)/rabbit_common/ebin

ERLC_OPTS += $(RMQ_ERLC_OPTS)

# --------------------------------------------------------------------
# Tests.
# --------------------------------------------------------------------

TEST_ERLC_OPTS += $(RMQ_ERLC_OPTS)
