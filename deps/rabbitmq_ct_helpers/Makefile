PROJECT = rabbitmq_ct_helpers
PROJECT_DESCRIPTION = Common Test helpers for RabbitMQ

DEPS = rabbit_common proper inet_tcp_proxy
TEST_DEPS = rabbit

dep_inet_tcp_proxy = git https://github.com/rabbitmq/inet_tcp_proxy master

DEP_PLUGINS = rabbit_common/mk/rabbitmq-build.mk \
	      rabbit_common/mk/rabbitmq-dist.mk \
	      rabbit_common/mk/rabbitmq-run.mk \
	      rabbit_common/mk/rabbitmq-tools.mk

# Make sure we know where the applications are located.
ROOT_DIR ?= $(abspath ../..)
DEPS_DIR ?= $(abspath ../../deps)

include $(ROOT_DIR)/rabbitmq-components.mk
include $(ROOT_DIR)/erlang.mk

ERLC_OPTS += +nowarn_export_all
