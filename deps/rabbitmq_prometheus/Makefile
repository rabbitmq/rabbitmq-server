PROJECT = rabbitmq_prometheus
PROJECT_MOD = rabbit_prometheus_app

TEST_DEPS = eunit_formatters
EUNIT_OPTS = no_tty, {report, {eunit_progress, [colored, profile]}}

DEPS = rabbit_common rabbit rabbitmq_management_agent prometheus accept rabbitmq_web_dispatch

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
include erlang.mk
