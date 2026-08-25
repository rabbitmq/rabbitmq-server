PROJECT = rabbitmq_consistent_hash_exchange
PROJECT_DESCRIPTION = Consistent Hash Exchange Type

define PROJECT_APP_EXTRA_KEYS
	{broker_version_requirements, []}
endef

DEPS = rabbit_common rabbit khepri khepri_mnesia_migration
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers amqp_client rabbitmq_amqp_client

PLT_APPS += mnesia rabbitmq_cli

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

ELIXIR = system

include ../../rabbitmq-components.mk
include ../../erlang.mk

ELIXIR_ERL_EBIN := $(shell dirname $$(elixir -e 'IO.puts(:code.which(:elixir_erl))' 2>/dev/null))
DIALYZER_PLT_OPTS += -pa $(ELIXIR_ERL_EBIN)
DIALYZER_OPTS += -pa $(ELIXIR_ERL_EBIN)
