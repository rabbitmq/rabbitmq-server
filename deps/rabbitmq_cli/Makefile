PROJECT = rabbitmqctl

define PROJECT_ENV
[{scopes, #{
    rabbitmqctl => ctl,
    'rabbitmq-diagnostics' => diagnostics,
    'rabbitmq-plugins' => plugins,
    'rabbitmq-queues' => queues,
    'rabbitmq-streams' => streams,
    'rabbitmq-upgrade' => upgrade,
    'vmware-rabbitmq' => vmware
}}]
endef

BUILD_DEPS = rabbit_common
DEPS = csv json stdout_formatter
LOCAL_DEPS = elixir

TEST_DEPS = amqp amqp_client temp x509 rabbit

dep_amqp = hex 3.3.0
dep_csv = hex 3.2.1
dep_json = hex 1.4.1
dep_temp = hex 0.4.9
dep_x509 = hex 0.9.0

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-build.mk

VERBOSE_TEST ?= true
MAX_CASES ?= 1

# Force enable Elixir in this project since
# this is an Elixir application.
ELIXIR = system

# We are still using Mix for testing.
MIX_TEST_OPTS ?= ""
MIX_TEST = ERL_COMPILER_OPTIONS=deterministic MIX_ENV=test mix do compile --warnings-as-errors, test --max-cases=$(MAX_CASES) --warnings-as-errors

ifneq ("",$(MIX_TEST_OPTS))
MIX_TEST := $(MIX_TEST) $(MIX_TEST_OPTS)
endif

ifeq ($(VERBOSE_TEST),true)
MIX_TEST := $(MIX_TEST) --trace
endif

EUNIT = disable

export MAKE

ESCRIPT_NAME = Elixir.RabbitMQCtl
ESCRIPT_FILE = escript/rabbitmqctl

.DEFAULT_GOAL = $(ESCRIPT_FILE)

escript::
	$(verbose) mkdir -p escript/

include ../../rabbitmq-components.mk
include ../../erlang.mk

$(ESCRIPT_FILE): $(EX_FILES)
	$(verbose) $(MAKE) escript

ESCRIPT_EMU_ARGS += -hidden

escript-zip::
	$(verbose) $(ESCRIPT_ZIP) $(ESCRIPT_ZIP_FILE) $(ELIXIR_LIBS)/*

LINKED_ESCRIPTS = escript/rabbitmq-plugins \
	escript/rabbitmq-diagnostics \
	escript/rabbitmq-queues \
	escript/rabbitmq-streams \
	escript/vmware-rabbitmq \
	escript/rabbitmq-upgrade

escript:: $(LINKED_ESCRIPTS)

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

# Erlang.mk will fetch dependencies as it now has native Elixir support.
# However we are still using Mix for tests and this means Mix will fetch
# test dependencies.
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

$(LINKED_ESCRIPTS): $(ESCRIPT_FILE)
	$(verbose) rm -f "$@"
	$(gen_verbose) $(call link_escript,$<,$@)

tests:: escript test-deps
	$(verbose) $(MAKE) -C ../../ install-cli
	$(verbose) $(MAKE) -C ../../ start-background-broker \
		PLUGINS="rabbitmq_federation rabbitmq_stomp rabbitmq_stream_management amqp_client" \
		$(if $(filter khepri,$(RABBITMQ_METADATA_STORE)),,RABBITMQ_FEATURE_FLAGS="-khepri_db")
	$(gen_verbose) $(MIX_TEST) \
		$(if $(RABBITMQ_METADATA_STORE),--exclude $(filter-out $(RABBITMQ_METADATA_STORE),khepri mnesia),) \
		$(TEST_FILE); \
		RES=$$?; \
		$(MAKE) -C ../../ stop-node; \
		exit $$RES

.PHONY: test

test:: escript test-deps
ifdef TEST_FILE
	$(gen_verbose) $(MIX_TEST) $(TEST_FILE)
else
	$(verbose) echo "TEST_FILE must be set, e.g. TEST_FILE=./test/ctl" 1>&2; false
endif

dialyzer:: escript
	MIX_ENV=test mix dialyzer

.PHONY: install

install: $(ESCRIPT_FILE)
ifdef PREFIX
	$(gen_verbose) mkdir -p "$(DESTDIR)$(PREFIX)"
	$(verbose) \
		cmp -s "$(ESCRIPT_FILE)" "$(DESTDIR)$(PREFIX)/$(notdir $(ESCRIPT_FILE))" || \
		cp "$(ESCRIPT_FILE)" "$(DESTDIR)$(PREFIX)/$(notdir $(ESCRIPT_FILE))"
	$(verbose) $(foreach script,$(LINKED_ESCRIPTS), \
		$(call link_escript,$(ESCRIPT_FILE),$(DESTDIR)$(PREFIX)/$(notdir $(script)));)
else
	$(verbose) echo "You must specify a PREFIX" 1>&2; false
endif

clean:: clean-mix

clean-mix:
	$(gen_verbose) rm -f $(ESCRIPT_FILE) $(LINKED_ESCRIPTS)
	$(verbose) echo y | mix clean

format:
	$(verbose) mix format lib/**/*.ex

repl:
	$(verbose) iex --sname repl -S mix
