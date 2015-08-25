PROJECT = amqp_client

DEPS = rabbitmq_common
dep_rabbitmq_common = git file:///home/dumbbell/Projects/pivotal/other-repos/rabbitmq-common master

include erlang.mk

COMPILE_FIRST = $(basename \
		$(notdir \
		$(shell grep -lw '^behaviour_info' src/*.erl)))

RMQ_ERLC_OPTS += -I $(DEPS_DIR)/rabbitmq_common/include

ERLC_OPTS += $(RMQ_ERLC_OPTS)
