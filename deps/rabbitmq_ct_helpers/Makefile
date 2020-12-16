PROJECT = rabbitmq_ct_helpers
PROJECT_DESCRIPTION = Common Test helpers for RabbitMQ

DEPS = rabbit_common proper inet_tcp_proxy
TEST_DEPS = rabbit

dep_rabbit_common  = git-subfolder https://github.com/rabbitmq/rabbitmq-server master deps/rabbit_common
dep_rabbit         = git-subfolder https://github.com/rabbitmq/rabbitmq-server master deps/rabbit
dep_inet_tcp_proxy = git https://github.com/rabbitmq/inet_tcp_proxy master

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

DEP_PLUGINS = rabbit_common/mk/rabbitmq-build.mk \
	      rabbit_common/mk/rabbitmq-dist.mk \
	      rabbit_common/mk/rabbitmq-run.mk \
	      rabbit_common/mk/rabbitmq-tools.mk

include rabbitmq-components.mk
include erlang.mk

ERLC_OPTS += +nowarn_export_all
