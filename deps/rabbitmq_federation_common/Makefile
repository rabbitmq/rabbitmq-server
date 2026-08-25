PROJECT = rabbitmq_federation_common
PROJECT_DESCRIPTION = RabbitMQ Federation Common
PROJECT_MOD = rabbit_federation_common_app

define PROJECT_ENV
[
	    {pgroup_name_cluster_id, false},
	    {internal_exchange_check_interval, 90000}
	  ]
endef

define PROJECT_APP_EXTRA_KEYS
	{broker_version_requirements, []}
endef

DEPS = rabbit_common rabbit amqp_client
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers proper

PLT_APPS += rabbitmq_cli

DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk
DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

ELIXIR = system

include ../../rabbitmq-components.mk
include ../../erlang.mk

ELIXIR_ERL_EBIN := $(shell dirname $$(elixir -e 'IO.puts(:code.which(:elixir_erl))' 2>/dev/null))
DIALYZER_PLT_OPTS += -pa $(ELIXIR_ERL_EBIN)
DIALYZER_OPTS += -pa $(ELIXIR_ERL_EBIN)
