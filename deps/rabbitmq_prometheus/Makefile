PROJECT = rabbitmq_prometheus
PROJECT_MOD = rabbit_prometheus_app

dep_prometheus = hex 3.5.1
dep_prometheus_httpd = hex 2.1.8
dep_accept = hex 0.3.3
dep_prometheus_cowboy = hex 0.1.4

DEPS = rabbit_common rabbit prometheus prometheus_httpd accept prometheus_cowboy rabbitmq_web_dispatch

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
include erlang.mk
