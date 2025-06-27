PROJECT = rabbit_common
PROJECT_DESCRIPTION = Modules shared by rabbitmq-server and rabbitmq-erlang-client

define PROJECT_APP_EXTRA_KEYS
%% Hex.pm package informations.
	{licenses, ["MPL-2.0"]},
	{links, [
	    {"Website", "https://www.rabbitmq.com/"},
	    {"GitHub", "https://github.com/rabbitmq/rabbitmq-server/tree/main/deps/rabbit_common"}
	  ]},
	{build_tools, ["make", "rebar3"]},
	{files, [
	    $(RABBITMQ_HEXPM_DEFAULT_FILES),
	    "mk"
	  ]}
endef

define HEX_TARBALL_EXTRA_METADATA
#{
	licenses => [<<"MPL-2.0">>],
	links => #{
		<<"Website">> => <<"https://www.rabbitmq.com">>,
		<<"GitHub">> => <<"https://github.com/rabbitmq/rabbitmq-server/tree/main/deps/rabbit_common">>
	}
}
endef

LOCAL_DEPS = compiler crypto public_key sasl ssl syntax_tools tools xmerl runtime_tools
DEPS = thoas ranch recon credentials_obfuscation

# Variables and recipes in development.*.mk are meant to be used from
# any Git clone. They are excluded from the files published to Hex.pm.
# Generated files are published to Hex.pm however so people using this
# source won't have to depend on Python and rabbitmq-codegen.
#
# That's why those Makefiles are included with `-include`: we ignore any
# inclusion errors.

-include development.pre.mk

DEP_EARLY_PLUGINS = $(PROJECT)/mk/rabbitmq-early-plugin.mk
# We do not depend on rabbit therefore can't run the broker.
DEP_PLUGINS = $(PROJECT)/mk/rabbitmq-build.mk \
	      $(PROJECT)/mk/rabbitmq-hexpm.mk

PLT_APPS += mnesia crypto ssl xmerl

include ../../rabbitmq-components.mk
include ../../erlang.mk

HEX_TARBALL_FILES += ../../rabbitmq-components.mk \
		     git-revisions.txt \
		     mk/rabbitmq-build.mk \
		     mk/rabbitmq-dist.mk \
		     mk/rabbitmq-early-plugin.mk \
		     mk/rabbitmq-hexpm.mk

-include development.post.mk
