PROJECT = amqp_client

DEPS = rabbit_common
dep_rabbit_common = git file:///home/dumbbell/Projects/pivotal/other-repos/rabbitmq-common master

include erlang.mk

COMPILE_FIRST = $(basename \
		$(notdir \
		$(shell grep -lw '^behaviour_info' src/*.erl)))

RMQ_ERLC_OPTS += -pa $(DEPS_DIR)/rabbit_common/ebin

ERLC_OPTS += $(RMQ_ERLC_OPTS)

# --------------------------------------------------------------------
# Tests.
# --------------------------------------------------------------------

TEST_ERLC_OPTS += $(RMQ_ERLC_OPTS)
