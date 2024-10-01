PROJECT = rabbitmq_ct_helpers
PROJECT_DESCRIPTION = Common Test helpers for RabbitMQ

DEPS = rabbit_common proper inet_tcp_proxy meck
LOCAL_DEPS = common_test eunit inets
#TEST_DEPS = rabbit

# We are calling one function from 'rabbit' so we need it in the PLT.
# But really this should be a full dependency; or we don't use the
# function anymore; or move it to rabbit_common. @todo
dialyze: DEPS += rabbit

XREF_IGNORE = [ \
    {'Elixir.OptionParser',split,1}, \
    {'Elixir.RabbitMQCtl',exec_command,2}]

dep_inet_tcp_proxy = git https://github.com/rabbitmq/inet_tcp_proxy master

# As this is a helper application we don't need other plugins;
# however we can run a test broker in the test suites.
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk

ERLC_OPTS += +nowarn_export_all
