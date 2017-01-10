PROJECT = rabbitmq_cli

BUILD_DEPS = rabbit_common
TEST_DEPS = amqp_client

DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

VERBOSE_TEST ?= true

ifeq ($(VERBOSE_TEST),true)
MIX_TEST = mix test --trace
else
MIX_TEST = mix test --max-cases=1
endif

include rabbitmq-components.mk
include erlang.mk

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

ESCRIPTS = escript/rabbitmqctl \
	   escript/rabbitmq-plugins \
	   escript/rabbitmq-diagnostics

$(HOME)/.mix/archives/hex-*:
	$(verbose) mix local.hex --force

hex: $(HOME)/.mix/archives/hex-*

deps:: hex
	$(verbose) mix make_deps

app:: $(ESCRIPTS)
	@:

rabbitmqctl_srcs := mix.exs \
		    $(shell find config lib -name "*.ex" -o -name "*.exs")

escript/rabbitmqctl: $(rabbitmqctl_srcs) deps
	$(gen_verbose) mix make_all

escript/rabbitmq-plugins escript/rabbitmq-diagnostics: escript/rabbitmqctl
	$(gen_verbose) ln -sf rabbitmqctl $@

rel:: $(ESCRIPTS)
	@:

tests:: $(ESCRIPTS)
	$(gen_verbose) $(MIX_TEST)

test:: $(ESCRIPTS)
	$(gen_verbose) $(MIX_TEST) $(TEST_FILE)

clean:: clean-mix

clean-mix: hex
	$(gen_verbose) rm -f $(ESCRIPTS)
	$(verbose) mix clean

repl:
	$(verbose) iex -S mix
