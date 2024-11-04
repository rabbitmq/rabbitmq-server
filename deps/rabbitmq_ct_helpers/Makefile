PROJECT = rabbitmq_ct_helpers
PROJECT_DESCRIPTION = Common Test helpers for RabbitMQ

<<<<<<< HEAD
DEPS = rabbit_common proper inet_tcp_proxy meck
TEST_DEPS = rabbit
=======
DEPS = rabbit_common amqp10_common rabbitmq_stream_common proper inet_tcp_proxy meck
LOCAL_DEPS = common_test eunit inets
#TEST_DEPS = rabbit

# We are calling one function from 'rabbit' so we need it in the PLT.
# But really this should be a full dependency; or we don't use the
# function anymore; or move it to rabbit_common. @todo
dialyze: DEPS += rabbit
>>>>>>> 9bb7530d04 (Move client-side stream protocol test helpers to a separate module)

XREF_IGNORE = [ \
    {'Elixir.OptionParser',split,1}, \
    {'Elixir.RabbitMQCtl',exec_command,2}]

dep_inet_tcp_proxy = git https://github.com/rabbitmq/inet_tcp_proxy master

DEP_PLUGINS = rabbit_common/mk/rabbitmq-build.mk \
	      rabbit_common/mk/rabbitmq-dist.mk \
	      rabbit_common/mk/rabbitmq-run.mk \
	      rabbit_common/mk/rabbitmq-tools.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk

ERLC_OPTS += +nowarn_export_all
