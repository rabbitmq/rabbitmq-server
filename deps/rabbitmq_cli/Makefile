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

app:: $(ESCRIPTS)
	@:

rabbitmqctl_srcs := mix.exs \
		    $(shell find config lib -name "*.ex" -o -name "*.exs")

# Elixir dependencies are fetched and compiled as part of the alias
# `mix make_all`. We do not fetch and build them in `make deps` because
# mix(1) startup time is quite high. Thus we prefer to run it once, even
# though it kind of breaks the Erlang.mk model.
#
# We write `y` on mix stdin because it asks approval to install Hex if
# it's missing. Another way to do it is to use `mix local.hex` but it
# can't be integrated in an alias and doing it from the Makefile isn't
# practical.
escript/rabbitmqctl: $(rabbitmqctl_srcs) deps
	$(gen_verbose) echo y | mix make_all

# We create hard links for rabbitmq-plugins and rabbitmq-diagnostics
# pointing to rabbitmqctl. We use hard links instead of symlinks because
# they also work on Windows (symlinks are supported on Windows but
# apparently require privileges).
#
# Also, we change to the `escript` directory to create the link
# because, on Windows, the target of the link must exist (unlike on
# Unix). As we want the link to point to `rabbitmqctl` in the same
# directory, we need to cd first.
escript/rabbitmq-plugins escript/rabbitmq-diagnostics: escript/rabbitmqctl
	$(gen_verbose) cd $(dir $@) && ln -f rabbitmqctl $(notdir $@)

rel:: $(ESCRIPTS)
	@:

tests:: $(ESCRIPTS)
	$(gen_verbose) $(MIX_TEST)

test:: $(ESCRIPTS)
	$(gen_verbose) $(MIX_TEST) $(TEST_FILE)

clean:: clean-mix

clean-mix:
	$(gen_verbose) rm -f $(ESCRIPTS)
	$(verbose) echo y | mix clean

repl:
	$(verbose) iex -S mix
