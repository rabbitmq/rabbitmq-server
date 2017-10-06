PROJECT = rabbit_common
PROJECT_DESCRIPTION = Modules shared by rabbitmq-server and rabbitmq-erlang-client

define PROJECT_APP_EXTRA_KEYS
%% Hex.pm package informations.
	{maintainers, [
	    "RabbitMQ Team <info@rabbitmq.com>",
	    "Jean-Sebastien Pedron <jean-sebastien@rabbitmq.com>"
	  ]},
	{licenses, ["MPL 1.1"]},
	{links, [
	    {"Website", "http://www.rabbitmq.com/"},
	    {"GitHub", "https://github.com/rabbitmq/rabbitmq-common"}
	  ]},
	{build_tools, ["make", "rebar3"]},
	{files, [
	    $(RABBITMQ_HEXPM_DEFAULT_FILES),
	    "mk"
	  ]}
endef

LOCAL_DEPS = compiler syntax_tools xmerl

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

# Variables and recipes in development.*.mk are meant to be used from
# any Git clone. They are excluded from the files published to Hex.pm.
# Generated files are published to Hex.pm however so people using this
# source won't have to depend on Python and rabbitmq-codegen.
#
# That's why those Makefiles are included with `-include`: we ignore any
# inclusion errors.

-include development.pre.mk

DEP_EARLY_PLUGINS = $(PROJECT)/mk/rabbitmq-early-test.mk
DEP_PLUGINS = $(PROJECT)/mk/rabbitmq-build.mk \
	      $(PROJECT)/mk/rabbitmq-hexpm.mk \
	      $(PROJECT)/mk/rabbitmq-dist.mk \
	      $(PROJECT)/mk/rabbitmq-test.mk \
	      $(PROJECT)/mk/rabbitmq-tools.mk

include mk/rabbitmq-components.mk
include erlang.mk

# --------------------------------------------------------------------
# Compilation.
# --------------------------------------------------------------------

# $(ERTS_VER) is set in `rabbitmq-build.mk` above.
tls_atom_version_MAX_ERTS_VER = 6.0
ifeq ($(call compare_version,$(ERTS_VER),$(tls_atom_version_MAX_ERTS_VER),<),true)
RMQ_ERLC_OPTS += -Ddefine_tls_atom_version
endif

# For src/*_compat.erl modules, we don't want to set -Werror because for
# instance, warnings about removed functions (e.g. ssl:connection_info/1
# in Erlang 20) can't be turned off.
#
# Erlang.mk doesn't provide a way to define per-file ERLC_OPTS because
# it compiles all files in a single run of erlc(1). So the solution is
# to declare those compat modules in $(ERLC_EXCLUDE) so that Erlang.mk
# skips them. Then we compile them ourselves below and update the .app
# file to include those compat modules.
#
# Because they are in $(ERLC_EXCLUDE), the line printed by
# $(erlc_verbose) is empty ("ERLC" alone instead of "ERLC <files>").

COMPAT_FILES = $(wildcard src/*_compat.erl)
COMPAT_MODS = $(patsubst src/%.erl,%,$(COMPAT_FILES))
ERLC_EXCLUDE += $(COMPAT_MODS)

ebin/$(PROJECT).app:: ebin/ $(COMPAT_FILES)
	$(erlc_verbose) erlc -v $(filter-out -Werror,$(ERLC_OPTS)) -o ebin/ \
		-pa ebin/ -I include/ $(COMPAT_FILES)
	$(eval COMPAT_MODULES := $(patsubst %,'%',$(COMPAT_MODS)))
	$(verbose) awk "\
		/{modules,/ { \
		line=\$$0; \
		sub(/$(lastword $(MODULES))]}/, \"$(lastword $(MODULES)),$(call comma_list,$(COMPAT_MODULES))]}\", line); \
		print line; \
		next; \
		} { print; }" < "$@" > "$@.compat"
	$(verbose) mv "$@.compat" "$@"

-include development.post.mk
