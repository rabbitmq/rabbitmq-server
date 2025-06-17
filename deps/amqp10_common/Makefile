PROJECT = amqp10_common
PROJECT_DESCRIPTION = Modules shared by rabbitmq-amqp1.0 and rabbitmq-amqp1.0-client

define PROJECT_APP_EXTRA_KEYS
%% Hex.pm package informations.
	{licenses, ["MPL-2.0"]},
	{links, [
	    {"Website", "https://www.rabbitmq.com/"},
	    {"GitHub", "https://github.com/rabbitmq/rabbitmq-server/tree/main/deps/amqp10_common"}
	  ]},
	{build_tools, ["make", "rebar3"]},
	{files, [
	    $(RABBITMQ_HEXPM_DEFAULT_FILES)
	  ]}
endef

define HEX_TARBALL_EXTRA_METADATA
#{
	licenses => [<<"MPL-2.0">>],
	links => #{
		<<"Website">> => <<"https://www.rabbitmq.com">>,
		<<"GitHub">> => <<"https://github.com/rabbitmq/rabbitmq-server/tree/main/deps/amqp10_common">>
	}
}
endef

#DIALYZER_OPTS += --src -r test -DTEST
BUILD_DEPS = rabbit_common
TEST_DEPS = rabbitmq_ct_helpers proper

# Variables and recipes in development.*.mk are meant to be used from
# any Git clone. They are excluded from the files published to Hex.pm.
# Generated files are published to Hex.pm however so people using this
# source won't have to depend on Python and rabbitmq-codegen.
#
# That's why those Makefiles are included with `-include`: we ignore any
# inclusion errors.

-include development.pre.mk

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
# We do not depend on rabbit therefore can't run the broker.
DEP_PLUGINS = rabbit_common/mk/rabbitmq-build.mk \
	      rabbit_common/mk/rabbitmq-hexpm.mk

PLT_APPS = eunit

include ../../rabbitmq-components.mk
include ../../erlang.mk

HEX_TARBALL_FILES += ../../rabbitmq-components.mk \
		     git-revisions.txt

-include development.post.mk
