PROJECT = rabbit_common
PROJECT_DESCRIPTION = Modules shared by rabbitmq-server and rabbitmq-erlang-client

define PROJECT_APP_EXTRA_KEYS
%% Hex.pm package informations.
	{licenses, ["MPL-2.0"]},
	{links, [
	    {"Website", "https://www.rabbitmq.com/"},
	    {"GitHub", "https://github.com/rabbitmq/rabbitmq-common"}
	  ]},
	{build_tools, ["make", "rebar3"]},
	{files, [
	    $(RABBITMQ_HEXPM_DEFAULT_FILES),
	    "mk"
	  ]}
endef

LOCAL_DEPS = compiler crypto public_key sasl ssl syntax_tools tools xmerl
DEPS = jsx recon credentials_obfuscation

dep_credentials_obfuscation = git https://github.com/rabbitmq/credentials-obfuscation.git  master

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

WITHOUT = plugins/proper

PLT_APPS += mnesia crypto ssl

include ../../rabbitmq-components.mk
include ../../erlang.mk

-include development.post.mk
