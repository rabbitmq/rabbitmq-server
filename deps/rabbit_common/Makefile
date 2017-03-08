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

include mk/rabbitmq-components.mk
include mk/rabbitmq-build.mk
include erlang.mk
include mk/rabbitmq-dist.mk
include mk/rabbitmq-tools.mk

# --------------------------------------------------------------------
# Compilation.
# --------------------------------------------------------------------

# $(ERTS_VER) is set in `rabbitmq-build.mk` above.
tls_atom_version_MAX_ERTS_VER = 6.0
ifeq ($(call compare_version,$(ERTS_VER),$(tls_atom_version_MAX_ERTS_VER),<),true)
RMQ_ERLC_OPTS += -Ddefine_tls_atom_version
endif

-include development.post.mk
