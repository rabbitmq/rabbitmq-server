PROJECT = rabbitmq_web_dispatch

DEPS = mochiweb webmachine

DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
include erlang.mk

WITH_BROKER_TEST_COMMANDS := rabbit_web_dispatch_test:test()
STANDALONE_TEST_COMMANDS := rabbit_web_dispatch_test_unit:test()
