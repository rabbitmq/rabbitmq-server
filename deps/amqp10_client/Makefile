PROJECT = amqp10_client
PROJECT_DESCRIPTION = AMQP 1.0 client from the RabbitMQ Project
PROJECT_MOD = amqp10_client_app

define PROJECT_APP_EXTRA_KEYS
%% Hex.pm package informations.
	{licenses, ["MPL-2.0"]},
	{links, [
	    {"Website", "https://www.rabbitmq.com/"},
	    {"GitHub", "https://github.com/rabbitmq/rabbitmq-server/tree/main/deps/amqp10_client"}
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
		<<"GitHub">> => <<"https://github.com/rabbitmq/rabbitmq-server/tree/main/deps/amqp10_client">>
	}
}
endef

# Release artifacts are put in $(PACKAGES_DIR).
PACKAGES_DIR ?= $(abspath PACKAGES)

BUILD_DEPS = rabbit_common elvis_mk
DEPS = amqp10_common credentials_obfuscation
TEST_DEPS = rabbit rabbitmq_amqp1_0 rabbitmq_ct_helpers
LOCAL_DEPS = ssl inets crypto public_key

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-test.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-macros.mk \
	      rabbit_common/mk/rabbitmq-build.mk \
	      rabbit_common/mk/rabbitmq-hexpm.mk \
	      rabbit_common/mk/rabbitmq-dist.mk \
	      rabbit_common/mk/rabbitmq-run.mk \
	      rabbit_common/mk/rabbitmq-test.mk \
	      rabbit_common/mk/rabbitmq-tools.mk

DEP_PLUGINS += elvis_mk
dep_elvis_mk = git https://github.com/inaka/elvis.mk.git master

include rabbitmq-components.mk
include erlang.mk

HEX_TARBALL_FILES += rabbitmq-components.mk \
		     git-revisions.txt

# --------------------------------------------------------------------
# Compiler flags.
# --------------------------------------------------------------------

# gen_fsm is deprecated starting from Erlang 20, but we want to support
# Erlang 19 as well.

ERTS_VER := $(shell erl -version 2>&1 | sed -E 's/.* version //')
ERLANG_20_ERTS_VER := 9.0

ifeq ($(call compare_version,$(ERTS_VER),$(ERLANG_20_ERTS_VER),>=),true)
ERLC_OPTS += -Dnowarn_deprecated_gen_fsm
endif

# Dialyze the tests.
DIALYZER_OPTS += --src -r test

# --------------------------------------------------------------------
# ActiveMQ for the testsuite.
# --------------------------------------------------------------------

ACTIVEMQ_VERSION := 5.14.4
ACTIVEMQ_URL := 'https://archive.apache.org/dist/activemq/$(ACTIVEMQ_VERSION)/apache-activemq-$(ACTIVEMQ_VERSION)-bin.tar.gz'

ACTIVEMQ := $(abspath test/system_SUITE_data/apache-activemq-$(ACTIVEMQ_VERSION)/bin/activemq)
export ACTIVEMQ

$(ACTIVEMQ): \
  test/system_SUITE_data/apache-activemq-$(ACTIVEMQ_VERSION)-bin.tar.gz
	$(gen_verbose) cd "$(dir $<)" && tar zxf "$(notdir $<)"

test/system_SUITE_data/apache-activemq-$(ACTIVEMQ_VERSION)-bin.tar.gz:
	$(gen_verbose) $(call core_http_get,$@,$(ACTIVEMQ_URL))

tests:: $(ACTIVEMQ)

ct ct-system: $(ACTIVEMQ)
