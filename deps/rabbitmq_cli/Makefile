PROJECT = rabbitmq_cli
VERSION ?= 0.0.1

DEPS = rabbit_common amqp_client amqp

dep_amqp = git https://github.com/pma/amqp.git master

DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

include rabbitmq-components.mk
include erlang.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

deps::
	mix deps.get

app::
	mix deps.compile
	mix escript.build

rel::
	rm -f escript/rabbitmq-plugins
	ln -s rabbitmqctl escript/rabbitmq-plugins
	rm -f escript/rabbitmq-diagnostics
	ln -s rabbitmqctl escript/rabbitmq-diagnostics
tests:: all
	mix test --trace

clean::
	mix clean