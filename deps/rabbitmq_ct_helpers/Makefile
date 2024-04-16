PROJECT = rabbitmq_ct_helpers
PROJECT_DESCRIPTION = Common Test helpers for RabbitMQ

DEPS = rabbit_common proper inet_tcp_proxy meck
TEST_DEPS = rabbit

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
