PROJECT = rabbitmq_cli

BUILD_DEPS = rabbit_common
DEPS = observer_cli
TEST_DEPS = amqp_client rabbit

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

VERBOSE_TEST ?= true
MAX_CASES ?= 1

MIX_TEST_OPTS ?= ""
MIX_TEST = mix test --max-cases=$(MAX_CASES)

ifneq ("",$(MIX_TEST_OPTS))
MIX_TEST := $(MIX_TEST) $(MIX_TEST_OPTS)
endif

ifeq ($(VERBOSE_TEST),true)
MIX_TEST := $(MIX_TEST) --trace
endif

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

WITHOUT = plugins/cover \
	  plugins/ct \
	  plugins/dialyzer \
	  plugins/eunit \
	  plugins/proper \
	  plugins/triq

include ../../rabbitmq-components.mk
include ../../erlang.mk

# rabbitmq-mix.mk is generated during the creation of the RabbitMQ
# source archive. It sets some environment variables to allow
# rabbitmq_cli to build offline, using the bundled sources only.
-include rabbitmq-mix.mk

ACTUAL_ESCRIPTS = escript/rabbitmqctl
LINKED_ESCRIPTS = escript/rabbitmq-plugins \
		  escript/rabbitmq-diagnostics \
		  escript/rabbitmq-queues \
                  escript/rabbitmq-upgrade
ESCRIPTS = $(ACTUAL_ESCRIPTS) $(LINKED_ESCRIPTS)

# Record the build and link dependency: the target files are linked to
# their first dependency.
rabbitmq-plugins = escript/rabbitmqctl
rabbitmq-diagnostics = escript/rabbitmqctl
rabbitmq-queues = escript/rabbitmqctl
rabbitmq-upgrade = escript/rabbitmqctl
escript/rabbitmq-plugins escript/rabbitmq-diagnostics escript/rabbitmq-queues escript/rabbitmq-upgrade: escript/rabbitmqctl

# We use hardlinks or symlinks in the `escript` directory and
# install's PREFIX when a single escript can have several names (eg.
# rabbitmq-plugins, rabbitmq-plugins and rabbitmq-diagnostics).
#
# Hardlinks and symlinks work on Windows. However, symlinks require
# privileges unlike hardlinks. That's why we default to hardlinks,
# unless USE_SYMLINKS_IN_ESCRIPTS_DIR is set.
#
# The link_escript function is called as:
#     $(call link_escript,source,target)
#
# The function assumes all escripts live in the same directory and that
# the source was previously copied in that directory.

ifdef USE_SYMLINKS_IN_ESCRIPTS_DIR
link_escript = ln -sf "$(notdir $(1))" "$(2)"
else
link_escript = ln -f "$(dir $(2))$(notdir $(1))" "$(2)"
endif

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
#
# We also verify if the CLI is built from the RabbitMQ source archive
# (by checking if the Hex registry/cache is present). If it is, we use
# another alias. This alias does exactly the same thing as `make_all`,
# but calls `deps.get --only prod` instead of `deps.get`. This is what
# we do to create the source archive, and we must do the same here,
# otherwise mix(1) complains about missing dependencies (the non-prod
# ones).
$(ACTUAL_ESCRIPTS): $(rabbitmqctl_srcs)
	$(gen_verbose) if test -d ../.hex; then \
		echo y | mix make_all_in_src_archive; \
	else \
		echo y | mix make_all; \
	fi

$(LINKED_ESCRIPTS):
	$(verbose) rm -f "$@"
	$(gen_verbose) $(call link_escript,$<,$@)

rel:: $(ESCRIPTS)
	@:

tests:: $(ESCRIPTS)
	$(gen_verbose) $(MIX_TEST) $(TEST_FILE)

.PHONY: test

test:: $(ESCRIPTS)
ifdef TEST_FILE
	$(gen_verbose) $(MIX_TEST) $(TEST_FILE)
else
	$(verbose) echo "TEST_FILE must be set, e.g. TEST_FILE=./test/ctl" 1>&2; false
endif

dialyzer:: $(ESCRIPTS)
	MIX_ENV=test mix dialyzer

.PHONY: install

install: $(ESCRIPTS)
ifdef PREFIX
	$(gen_verbose) mkdir -p "$(DESTDIR)$(PREFIX)"
	$(verbose) $(foreach script,$(ACTUAL_ESCRIPTS), \
		cmp -s "$(script)" "$(DESTDIR)$(PREFIX)/$(notdir $(script))" || \
		cp "$(script)" "$(DESTDIR)$(PREFIX)/$(notdir $(script))";)
	$(verbose) $(foreach script,$(LINKED_ESCRIPTS), \
		$(call link_escript,$($(notdir $(script))),$(DESTDIR)$(PREFIX)/$(notdir $(script)));)
else
	$(verbose) echo "You must specify a PREFIX" 1>&2; false
endif

clean:: clean-mix

clean-mix:
	$(gen_verbose) rm -f $(ESCRIPTS)
	$(verbose) echo y | mix clean

format:
	$(verbose) mix format lib/**/*.ex

repl:
	$(verbose) iex --sname repl -S mix
