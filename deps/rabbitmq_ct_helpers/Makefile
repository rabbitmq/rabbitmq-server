PROJECT = rabbitmq_ct_helpers
PROJECT_DESCRIPTION = Common Test helpers for RabbitMQ

DEPS = rabbit_common proper inet_tcp_proxy meck
TEST_DEPS = rabbit

dep_rabbit_common  = git-subfolder https://github.com/rabbitmq/rabbitmq-server main deps/rabbit_common
dep_rabbit         = git-subfolder https://github.com/rabbitmq/rabbitmq-server main deps/rabbit
dep_inet_tcp_proxy = git https://github.com/rabbitmq/inet_tcp_proxy master

DEP_PLUGINS = rabbit_common/mk/rabbitmq-build.mk \
	      rabbit_common/mk/rabbitmq-dist.mk \
	      rabbit_common/mk/rabbitmq-run.mk \
	      rabbit_common/mk/rabbitmq-tools.mk

include ../../rabbitmq-components.mk
include ../../erlang.mk

ERLC_OPTS += +nowarn_export_all
