PROJECT = rabbitmq_cli
VERSION ?= 0.0.1

BUILD_DEPS = rabbit_common amqp_client

DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

TEST_FILE ?= ""

include rabbitmq-components.mk
include erlang.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

ESCRIPTS = escript/rabbitmqctl \
	   escript/rabbitmq-plugins \
	   escript/rabbitmq-diagnostics

deps::
	if test ! -d $(HOME)/.mix/archives; then mix local.hex --force; fi
	mix deps.get
	mix deps.compile

app:: $(ESCRIPTS)
	@:

rabbitmqctl_srcs := mix.exs \
		    $(shell find config lib -name "*.ex" -o -name "*.exs")

ebin: $(rabbitmqctl_srcs)
	mix deps.get
	mix deps.compile
	rm -rf ebin
	mix compile
	mkdir -p ebin
	cp -r _build/dev/lib/rabbitmqctl/ebin/* ebin

escript/rabbitmqctl: ebin
	mix escript.build

escript/rabbitmq-plugins escript/rabbitmq-diagnostics: escript/rabbitmqctl
	ln -sf rabbitmqctl $@

rel:: $(ESCRIPTS)
	@:

tests:: all
	mix test --trace

test:: all
	mix test --trace $(TEST_FILE)

clean::
	rm -f $(ESCRIPTS)
	- mix local.hex --force
	rm -rf ebin
	mix clean

repl:
	iex -S mix
